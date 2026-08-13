//! A library responsible for orchestrating the L2 chain based on data received from L1 and over the
//! L2 p2p network.

use alloy_eips::Encodable2718;
use alloy_primitives::{b256, bytes::Bytes, keccak256, B256};
use alloy_provider::Provider;
use alloy_rpc_types_engine::ExecutionPayloadV1;
use dogeos_hardforks::DogeosHardforks;
use dogeos_protocol_types::{ScrollTxEnvelope, TxL1Message};
use dogeos_reth_primitives::DogeosBlock;
use dogeos_rpc_types::Scroll;
use futures::{stream, StreamExt, TryStreamExt};
use reth_chainspec::EthChainSpec;
use reth_network_api::{BlockDownloaderProvider, FullNetwork};
use reth_network_p2p::{sync::SyncState as RethSyncState, FullBlockClient};
use reth_tokio_util::{EventSender, EventStream};
use rollup_node_primitives::{
    BatchCommitData, BatchInfo, BatchStatus, BlockConsolidationOutcome, BlockInfo, ChainImport,
    ConsensusUpdate, L1MessageEnvelope, L2BlockInfoWithL1Messages,
};
use rollup_node_providers::L1MessageProvider;
use rollup_node_sequencer::{Sequencer, SequencerEvent};
use rollup_node_signer::{SignatureAsBytes, SignerEvent, SignerHandle};
use rollup_node_watcher::{L1Notification, L1WatcherHandle};
use scroll_db::{
    Database, DatabaseError, DatabaseReadOperations, DatabaseWriteOperations, L1MessageKey,
    UnwindResult,
};
use scroll_derivation_pipeline::{BatchDerivationResult, DerivationPipeline};
use scroll_engine::{Engine, ScrollEngineApi};
use scroll_network::{
    BlockImportOutcome, DogeosNetworkPrimitives, NewBlockWithPeer, ScrollNetwork,
    ScrollNetworkManagerEvent,
};
use std::{collections::VecDeque, sync::Arc, time::Instant, vec};
use tokio::sync::mpsc::{self, UnboundedReceiver};

mod build;
use build::{build_block_channel, BuildBlockCompletion};
pub use build::{BuildBlockOutcome, BuildBlockTicket};

mod config;
pub use config::ChainOrchestratorConfig;

mod consensus;
pub use consensus::{Consensus, NoopConsensus, SystemContractConsensus};

mod consolidation;
use consolidation::{reconcile_batch, BlockConsolidationAction};

mod event;
pub use event::ChainOrchestratorEvent;

mod error;
pub use error::{BuildBlockError, ChainOrchestratorError, ImportBlockError, ResetCommandError};

mod handle;
pub use handle::{ChainOrchestratorCommand, ChainOrchestratorHandle, DatabaseQuery};

mod metrics;
use metrics::{MetricsHandler, Task};

mod sync;
pub use sync::{SyncMode, SyncState};

mod status;
pub use status::ChainOrchestratorStatus;

/// Wraps a future, metering the completion of it.
macro_rules! metered {
    ($task:expr, $self:ident, $method:ident($($args:expr),*)) => {
        {
            let metric = $self.metric_handler.get($task).expect("metric exists").clone();
            let now = Instant::now();
            let res =$self.$method($($args),*).await;
            metric.task_duration.record(now.elapsed().as_secs_f64());
            res
        }
    };
}

/// The mask used to mask the L1 message queue hash.
const L1_MESSAGE_QUEUE_HASH_MASK: B256 =
    b256!("ffffffffffffffffffffffffffffffffffffffffffffffffffffffff00000000");

/// The number of headers to fetch in each request when fetching headers from peers.
const HEADER_FETCH_COUNT: u64 = 100;

/// The size of the event channel used to broadcast events to listeners.
const EVENT_CHANNEL_SIZE: usize = 5000;

/// Bounded backoff applied after a failed L1-notification handler before retrying it, to cap retry
/// pressure on a persistent structural failure while the authorization barrier stays fail-closed.
#[cfg(not(any(test, feature = "test-utils")))]
const L1_NOTIFICATION_RETRY_BACKOFF: std::time::Duration = std::time::Duration::from_millis(250);
#[cfg(any(test, feature = "test-utils"))]
const L1_NOTIFICATION_RETRY_BACKOFF: std::time::Duration = std::time::Duration::from_millis(1);

/// The batch size for batch validation.
#[cfg(not(any(test, feature = "test-utils")))]
const BATCH_SIZE: usize = 100;
#[cfg(any(test, feature = "test-utils"))]
const BATCH_SIZE: usize = 1;

/// Returns whether a failed L1 notification must be retained and retried before any later
/// notification is applied.
///
/// Retention exists **solely** to enforce one dynamic-mode invariant: a phase-two
/// [`L1Notification::AuthorizedSigner`] — the authorization-barrier close — must not overtake the
/// structural transition it follows. So retention is inert unless `dynamic_authorization` is set:
/// a static/no-op watcher never emits that phase two, and applying the retry there would be a
/// separate default-path liveness change (a failed handler would block the whole L1 stream) that
/// this PR deliberately does not make.
///
/// When dynamic, only head transitions (`Reorg`/`NewBlock`) precede and gate the barrier close, so
/// only those are retained: a failed reorg/forkchoice repair must block the barrier close until it
/// succeeds, while an unrelated transient failure elsewhere (for example chain consolidation on
/// `Synced`) is logged and skipped so it does not stall the whole L1 stream.
const fn retains_failed_l1_notification(
    dynamic_authorization: bool,
    notification: &L1Notification,
) -> bool {
    dynamic_authorization &&
        matches!(notification, L1Notification::Reorg(_) | L1Notification::NewBlock(_))
}

/// Returns whether the ordinary L1 stream may make progress.
///
/// Phase two of a dynamic authorization update is delivered on this stream, so it must remain
/// reachable while the authorization barrier is open even when optimistic sync has marked L2 as
/// syncing. The derivation and committed-reset gates still preserve structural-before-phase-two
/// ordering.
const fn should_process_l1_notification(
    l2_synced: bool,
    authorization_pending: bool,
    derivation_empty: bool,
    reset_tail_pending: bool,
) -> bool {
    (l2_synced || authorization_pending) && derivation_empty && !reset_tail_pending
}

/// Maps payload processing onto the terminal result for the uniquely associated manual build.
fn requested_build_outcome(
    result: &Result<Option<ChainOrchestratorEvent>, ChainOrchestratorError>,
) -> BuildBlockOutcome {
    match result {
        Ok(Some(ChainOrchestratorEvent::BlockSequenced(block))) => {
            BuildBlockOutcome::Sequenced(block.clone())
        }
        Ok(Some(ChainOrchestratorEvent::BlockBuildingSkipped)) => BuildBlockOutcome::Skipped,
        Err(err) => BuildBlockOutcome::Failed(err.to_string()),
        Ok(_) => BuildBlockOutcome::Failed(
            "payload processing ended without a terminal outcome".to_string(),
        ),
    }
}

fn complete_requested_build_from_payload_result(
    completion: &mut Option<BuildBlockCompletion>,
    result: &Result<Option<ChainOrchestratorEvent>, ChainOrchestratorError>,
) {
    if let Some(completion) = completion.take() {
        completion.complete(requested_build_outcome(result));
    }
}

/// A committed administrative reset whose forkchoice/watcher tail has not yet fully succeeded.
///
/// `Database::unwind` commits its transaction before the fallible forkchoice repair and watcher
/// reset. Staging the committed unwind here lets a retry resume the tail *without unwinding again*
/// — a second unwind for the same target would observe the already-removed rows, return an empty
/// `UnwindResult`, lose the original forkchoice target, and falsely "succeed" without repairing
/// forkchoice (the same non-idempotent replay that reorg staging avoids). While a reset is staged
/// the node is held fail-closed (see [`ChainOrchestrator::reset_tail_pending`]) so no
/// authorization-sensitive work runs against the partially-reset database.
#[derive(Debug)]
struct StagedReset {
    /// The L1 block number the reset targets.
    block_number: u64,
    /// The committed unwind (including the forkchoice safe-head target) to resume the tail from.
    unwind_result: UnwindResult,
}

/// The action an incoming `RevertToL1Block(target)` should take given any currently staged reset.
///
/// Pure and unit-testable; the actual database unwind, forkchoice repair, and watcher reset are
/// performed by the caller around this decision.
#[derive(Debug, PartialEq, Eq)]
enum ResetDecision {
    /// No reset is staged: perform a fresh database unwind to `target`.
    Unwind,
    /// A committed reset for exactly `target` is staged: resume its tail without unwinding again.
    Resume,
    /// A committed reset for a *different* target is staged: reject. Composing a second unwind
    /// onto the first is unsafe — a higher target would roll the already-unwound database
    /// forward without restoring the deleted data, and a lower target would discard the first
    /// reset's still-unapplied forkchoice target. The staged reset must be completed (or
    /// retried) first.
    Reject {
        /// The target of the reset already staged.
        staged: u64,
    },
}

/// Decides what an incoming `RevertToL1Block(target)` should do given the currently staged reset.
const fn reset_decision(staged: &Option<StagedReset>, target: u64) -> ResetDecision {
    match staged {
        None => ResetDecision::Unwind,
        Some(staged) if staged.block_number == target => ResetDecision::Resume,
        Some(staged) => ResetDecision::Reject { staged: staged.block_number },
    }
}

/// Whether an asynchronously-produced signer result tagged with `result_generation` still belongs
/// to the current chain `generation`.
///
/// A committed unwind — an administrative reset or an ordinary L1 reorg — bumps the generation (see
/// [`ChainOrchestrator::unwind_generation`]), so a signer result requested before that unwind
/// carries an older tag and is stale: it was built on a chain generation the unwind discarded and
/// must not be persisted or announced.
const fn signer_result_is_current(result_generation: u64, current_generation: u64) -> bool {
    result_generation == current_generation
}

/// Applies the irreversible authorization/recovery transitions taken the moment an administrative
/// reset's database unwind commits: it drops the old watcher generation's retained ordinary-reorg
/// recovery work ([`ChainOrchestrator::pending_l1_retry`]/[`ChainOrchestrator::staged_reorg`]) so
/// it can never replay its stale tail/forkchoice target against the newly unwound database, and, in
/// dynamic mode, fails authorization closed for the duration of the staged reset tail.
///
/// Call only once the unwind has committed (from [`ChainOrchestrator::begin_committed_reset`]); a
/// unwind that *fails* commits nothing, so this must not run and the pre-reset state stays intact
/// and retryable. Static/no-op mode never re-establishes the barrier, so authorization is left
/// untouched there — the staged-reset guard alone holds those nodes fail-closed.
fn commit_reset_generation(
    dynamic_authorization: bool,
    pending_l1_retry: &mut Option<Arc<L1Notification>>,
    staged_reorg: &mut Option<UnwindResult>,
    consensus: &mut dyn Consensus,
) {
    *pending_l1_retry = None;
    *staged_reorg = None;
    if dynamic_authorization {
        consensus.suspend_authorization();
    }
}

/// The [`ChainOrchestrator`] is responsible for orchestrating the progression of the L2 chain
/// based on data consolidated from L1 and the data received over the p2p network.
#[derive(Debug)]
pub struct ChainOrchestrator<
    N: FullNetwork<Primitives = DogeosNetworkPrimitives>,
    ChainSpec,
    L1MP,
    L2P,
    EC,
> {
    /// The configuration for the chain orchestrator.
    config: ChainOrchestratorConfig<ChainSpec>,
    /// The receiver for commands sent to the chain orchestrator.
    handle_rx: UnboundedReceiver<ChainOrchestratorCommand<N>>,
    /// The `BlockClient` that is used to fetch blocks from peers over p2p.
    block_client: Arc<FullBlockClient<<N as BlockDownloaderProvider>::Client>>,
    /// The L2 client that is used to interact with the L2 chain.
    l2_client: Arc<L2P>,
    /// The reference to database.
    database: Arc<Database>,
    /// The current sync state of the [`ChainOrchestrator`].
    sync_state: SyncState,
    /// A handle for the [`rollup_node_watcher::L1Watcher`].
    l1_watcher: L1WatcherHandle,
    /// The dedicated authorization-control receiver, owned directly so it can be polled at a
    /// higher priority than, and independently of, the ordinary L1 notification receiver held
    /// by [`Self::l1_watcher`].
    consensus_control_rx: mpsc::UnboundedReceiver<ConsensusUpdate>,
    /// An L1 notification whose handler failed and must be retried before any later notification
    /// is applied.
    ///
    /// **Dynamic mode only** (see [`Self::dynamic_authorization`]): this enforces successful
    /// application order, not merely dequeue order, so a failed structural transition
    /// (`Reorg`/`NewBlock`) is retried and blocks the following phase-two
    /// [`L1Notification::AuthorizedSigner`] — the authorization-barrier close — until it succeeds,
    /// so the barrier cannot clear onto a partially-unwound or unrepaired L2 state. In
    /// static/no-op mode there is no barrier close to gate, so nothing is retained and the
    /// default L1 liveness is unchanged. Scoped to the current watcher/reset generation:
    /// cleared once an administrative reset commits, so stale old-generation work cannot
    /// replay across the reset boundary.
    pending_l1_retry: Option<Arc<L1Notification>>,
    /// The committed unwind of an in-progress reorg whose L2/engine tail has not yet succeeded.
    ///
    /// **Dynamic mode only** (see [`Self::dynamic_authorization`]), because it is populated only
    /// on the same retained-and-retried reorg path as [`Self::pending_l1_retry`].
    /// `database.unwind` commits before the fallible L2-lookup/forkchoice tail. Replaying the
    /// whole `Reorg` would re-run the committed unwind and lose the original `UnwindResult` (a
    /// second unwind returns empty effects and would falsely "succeed" without repairing
    /// forkchoice), so the committed result is staged here and a retry resumes only the tail.
    /// Cleared on a successful reorg and once an administrative reset commits.
    staged_reorg: Option<UnwindResult>,
    /// A committed administrative reset ([`ChainOrchestratorCommand::RevertToL1Block`]) whose
    /// forkchoice/watcher tail has not yet fully succeeded.
    ///
    /// The database unwind commits before the fallible forkchoice repair and (fallible) watcher
    /// reset, so on a mid-tail failure the committed unwind is staged here and a retry of the same
    /// target resumes the tail without unwinding again (see [`StagedReset`]). While it is `Some`
    /// the node is held fail-closed via [`Self::reset_tail_pending`], independent of mode, so no
    /// L1/sequencing/import work runs against the partially-reset database. Cleared only once the
    /// full reset tail (forkchoice repair *and* watcher reset) succeeds.
    staged_reset: Option<StagedReset>,
    /// A monotonic tag identifying the current chain generation for asynchronously-produced signer
    /// results.
    ///
    /// Incremented exactly once whenever a fresh unwind commits and discards chain state — an
    /// administrative reset *or* an ordinary L1 reorg ([`Self::begin_committed_unwind`]), but not
    /// when a staged tail is retried. Each block-signing request is tagged with the value current
    /// at request time; a returned [`SignerEvent::SignedBlock`] whose tag no longer matches
    /// was built on a chain generation an unwind has since discarded, so it is dropped rather
    /// than persisted/announced. This closes the race where a payload finalized/queued before
    /// the unwind is signed and released afterwards (the signer runs independently of the
    /// single orchestrator loop, so a branch gate only *delays* such a result, it does not
    /// invalidate it).
    unwind_generation: u64,
    /// The network manager that manages the scroll p2p network.
    network: ScrollNetwork<N>,
    /// The consensus algorithm used by the rollup node.
    consensus: Box<dyn Consensus + 'static>,
    /// Whether the authorized signer is refreshed from L1 at runtime (dynamic mode).
    ///
    /// Only in dynamic mode does an administrative reset synchronously suspend authorization: the
    /// fresh watcher re-establishes and closes the head-qualified barrier. In static/no-op mode
    /// the watcher never re-establishes it, so suspending would stall.
    dynamic_authorization: bool,
    /// The engine used to communicate with the execution layer.
    engine: Engine<EC>,
    /// The sequencer used to build blocks.
    sequencer: Option<Sequencer<L1MP, ChainSpec>>,
    /// Completion channel for the currently admitted manual build, if any.
    requested_build_completion: Option<BuildBlockCompletion>,
    /// The signer used to sign messages.
    signer: Option<SignerHandle>,
    /// The derivation pipeline used to derive L2 blocks from batches.
    derivation_pipeline: DerivationPipeline,
    /// Optional event sender for broadcasting events to listeners.
    event_sender: Option<EventSender<ChainOrchestratorEvent>>,
    /// The metrics handler.
    metric_handler: MetricsHandler,
}

impl<
        N: FullNetwork<Primitives = DogeosNetworkPrimitives> + Send + Sync + 'static,
        ChainSpec: DogeosHardforks + EthChainSpec + Send + Sync + 'static,
        L1MP: L1MessageProvider + Unpin + Clone + Send + Sync + 'static,
        L2P: Provider<Scroll> + 'static,
        EC: ScrollEngineApi + Sync + Send + 'static,
    > ChainOrchestrator<N, ChainSpec, L1MP, L2P, EC>
{
    /// Creates a new chain orchestrator.
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        database: Arc<Database>,
        config: ChainOrchestratorConfig<ChainSpec>,
        block_client: Arc<FullBlockClient<<N as BlockDownloaderProvider>::Client>>,
        l2_provider: L2P,
        mut l1_watcher: L1WatcherHandle,
        network: ScrollNetwork<N>,
        consensus: Box<dyn Consensus + 'static>,
        engine: Engine<EC>,
        sequencer: Option<Sequencer<L1MP, ChainSpec>>,
        signer: Option<SignerHandle>,
        derivation_pipeline: DerivationPipeline,
        dynamic_authorization: bool,
    ) -> Result<(Self, ChainOrchestratorHandle<N>), ChainOrchestratorError> {
        let (handle_tx, handle_rx) = mpsc::unbounded_channel();
        let handle = ChainOrchestratorHandle::new(handle_tx);
        let consensus_control_rx = l1_watcher
            .take_consensus_control_receiver()
            .expect("authorization-control receiver must be present on a fresh L1 watcher handle");
        Ok((
            Self {
                block_client,
                l2_client: Arc::new(l2_provider),
                database,
                config,
                sync_state: SyncState::default(),
                l1_watcher,
                consensus_control_rx,
                pending_l1_retry: None,
                staged_reorg: None,
                staged_reset: None,
                unwind_generation: 0,
                network,
                consensus,
                dynamic_authorization,
                engine,
                sequencer,
                requested_build_completion: None,
                signer,
                derivation_pipeline,
                handle_rx,
                event_sender: None,
                metric_handler: MetricsHandler::default(),
            },
            handle,
        ))
    }

    /// Drives the [`ChainOrchestrator`] future until a shutdown signal is received.
    pub async fn run_until_shutdown(
        mut self,
        mut shutdown: impl std::future::Future<Output = ()> + Unpin,
    ) {
        loop {
            tokio::select! {
                biased;

                _guard = &mut shutdown => {
                    self.notify(ChainOrchestratorEvent::Shutdown);
                    break;
                }
                // Authorization control is polled above commands and above the signer, sequencer,
                // derivation, network, and ordinary L1 data branches, and is intentionally free of
                // any sync/derivation guard. Placing it above commands closes the narrow entry race
                // where a queued `BuildBlock` would otherwise be dequeued before a queued
                // `AuthorizationPending` and start a payload against the old signer. It also lets the
                // head-qualified barrier open/close promptly even while the ordinary L1 notification
                // branch is derivation-gated, so a pending authorization is never starved by data
                // backpressure or branch ordering.
                Some(update) = self.consensus_control_rx.recv() => {
                    self.handle_consensus_control(&update);
                }
                Some(command) = self.handle_rx.recv() => {
                    if let Err(err) = self.handle_command(command).await {
                        tracing::error!(target: "scroll::chain_orchestrator", ?err, "Error handling command");
                    }
                }
                // While the authorization barrier is open, the signer branch is withheld so a block
                // signed just before the rotation stays queued (unbounded mpsc). It is drained after
                // the barrier closes and revalidated against the now-current signer in
                // `handle_signer_event`, so an old-signer result cannot leak through.
                Some(event) = async {
                    if let Some(event) = self.signer.as_mut() {
                        event.next().await
                    } else {
                        unreachable!()
                    }
                }, if self.signer.is_some() && !self.consensus.authorization_pending() && !self.reset_tail_pending() => {
                    let res = self.handle_signer_event(event).await;
                    self.handle_outcome(res);
                }
                // Sequencing is withheld while the barrier is open so no block is produced against a
                // signer that may have rotated.
                Some(event) = async {
                    if let Some(seq) = self.sequencer.as_mut() {
                        seq.next().await
                    } else {
                        unreachable!()
                    }
                }, if self.sequencer.is_some() && self.sync_state.is_synced() && !self.consensus.authorization_pending() && !self.reset_tail_pending() => {
                    let payload_ready = matches!(&event, SequencerEvent::PayloadReady(_));
                    let res = self.handle_sequencer_event(event).await;
                    if payload_ready {
                        complete_requested_build_from_payload_result(
                            &mut self.requested_build_completion,
                            &res,
                        );
                    }
                    self.handle_outcome(res);
                }
                Some(batch) = self.derivation_pipeline.next(), if !self.reset_tail_pending() => {
                    let res = metered!(Task::BatchReconciliation, self, handle_derived_batch(batch));
                    self.handle_outcome(res);
                }
                // Inbound block import is withheld while the barrier is open so a peer's blocks are
                // retained in the event channel rather than consumed and rejected. As a defensive
                // backstop for any path that reaches validation while pending, `validate_new_block`
                // returns the non-penalizing `AuthorizationPending`. A prolonged barrier (persistent
                // signer-read failure) can overflow the bounded broadcast channel; recovery then
                // relies on later block/range synchronization.
                Some(event) = self.network.events().next(), if !self.consensus.authorization_pending() && !self.reset_tail_pending() => {
                    let res = self.handle_network_event(event).await;
                    self.handle_outcome(res);
                }
                // In dynamic mode only, a retained (failed) head-transition notification is retried
                // before any later item is read, so a failed structural transition (reorg unwind /
                // forkchoice repair) blocks the following phase-two `AuthorizedSigner` — the barrier
                // close — until it succeeds. This enforces successful *application* order for the
                // head transition, not merely dequeue order. Only `Reorg`/`NewBlock` are retained:
                // they alone precede and gate a barrier close, so an unrelated transient failure
                // elsewhere (e.g. chain consolidation on `Synced`) does not stall the whole L1
                // stream. In static/no-op mode there is no barrier close to gate, so nothing is
                // retained and the default L1 liveness is unchanged (see
                // `retains_failed_l1_notification`).
                Some(notification) = async {
                    if let Some(retry) = self.pending_l1_retry.clone() {
                        Some(retry)
                    } else {
                        self.l1_watcher.l1_notification_receiver().recv().await
                    }
                }, if should_process_l1_notification(
                    self.sync_state.l2().is_synced(),
                    self.consensus.authorization_pending(),
                    self.derivation_pipeline.is_empty(),
                    self.reset_tail_pending(),
                ) => {
                    let res = self.handle_l1_notification(notification.clone()).await;
                    if res.is_err() &&
                        retains_failed_l1_notification(self.dynamic_authorization, &notification)
                    {
                        self.pending_l1_retry = Some(notification);
                        tokio::time::sleep(L1_NOTIFICATION_RETRY_BACKOFF).await;
                    } else {
                        self.pending_l1_retry = None;
                    }
                    self.handle_outcome(res);
                }

            }
        }
    }

    /// Handles the outcome of an operation, logging errors and notifying event listeners as
    /// appropriate.
    fn handle_outcome(
        &self,
        outcome: Result<Option<ChainOrchestratorEvent>, ChainOrchestratorError>,
    ) {
        match outcome {
            Ok(Some(event)) => self.notify(event),
            Err(err) => {
                tracing::error!(target: "scroll::chain_orchestrator", ?err, "Encountered error in the chain orchestrator");
            }
            Ok(None) => {}
        }
    }

    /// Applies phase one of the head-qualified authorized-signer refresh — the
    /// [`ConsensusUpdate::AuthorizationPending`] barrier open — delivered on the priority control
    /// channel so old-signer work is withheld promptly. Phase two (the barrier close) arrives on
    /// the ordinary FIFO channel and is handled in [`Self::handle_l1_notification`].
    ///
    /// Whenever the barrier is open afterwards, any in-flight payload-building job is cancelled so
    /// a block cannot be produced against a signer that may have rotated during the window.
    fn handle_consensus_control(&mut self, update: &ConsensusUpdate) {
        self.consensus.update_config(update);
        if self.consensus.authorization_pending() {
            self.cancel_payload_building_job();
        }
    }

    /// Cancels an accepted payload job and reports that outcome to any caller waiting for its
    /// completion. No event is emitted when there was no active job to remove.
    fn cancel_payload_building_job(&mut self) -> bool {
        let cancelled = self.sequencer.as_mut().is_some_and(Sequencer::cancel_payload_building_job);
        if cancelled {
            self.complete_requested_block_build(BuildBlockOutcome::Skipped);
            self.notify(ChainOrchestratorEvent::BlockBuildingSkipped);
        }
        cancelled
    }

    /// Resolves the per-request waiter for the active manual build, if there is one.
    fn complete_requested_block_build(&mut self, outcome: BuildBlockOutcome) {
        if let Some(completion) = self.requested_build_completion.take() {
            completion.complete(outcome);
        }
    }

    /// Enters L1 syncing only after cancelling any payload that the full-sync sequencer guard would
    /// otherwise leave permanently unpolled. This runs before the fallible reset unwind.
    fn set_l1_syncing(&mut self) {
        self.cancel_payload_building_job();
        self.sync_state.l1_mut().set_syncing();
    }

    /// Enters L2 optimistic syncing only after cancelling any payload that the full-sync sequencer
    /// guard would otherwise leave permanently unpolled.
    fn set_l2_syncing(&mut self) {
        self.cancel_payload_building_job();
        self.sync_state.l2_mut().set_syncing();
    }

    /// Drains and applies any queued authorization-control updates, returning whether the barrier
    /// is open afterwards.
    ///
    /// The select branch guards only reflect the barrier as of branch selection; a running handler
    /// cannot be interrupted. Calling this immediately before an authorization-sensitive side
    /// effect (starting/finalizing a payload, validating/importing a peer block) makes an
    /// `AuthorizationPending` that arrived *during* the handler visible before the side effect
    /// commits, so the handler can defer instead of mutating forkchoice/database state under a
    /// signer that is being revoked. It cannot preempt an in-flight external engine call, so a
    /// residual window remains inside such a call; this narrows it to that boundary.
    fn apply_pending_authorization_control(&mut self) -> bool {
        while let Ok(update) = self.consensus_control_rx.try_recv() {
            self.handle_consensus_control(&update);
        }
        self.consensus.authorization_pending()
    }

    /// Handles an event from the signer.
    async fn handle_signer_event(
        &self,
        event: SignerEvent,
    ) -> Result<Option<ChainOrchestratorEvent>, ChainOrchestratorError> {
        tracing::info!(target: "scroll::chain_orchestrator", ?event, "Handling signer event");
        match event {
            SignerEvent::SignedBlock { block, signature, generation } => {
                // Discard a block signed for a chain generation that a committed unwind — an
                // administrative reset or an ordinary L1 reorg — has since invalidated. The signer
                // runs independently of this loop, so a payload finalized and queued for signing
                // before the unwind can return afterwards; it was built on the pre-unwind chain
                // (its parent and L1-message set belong to the discarded
                // generation), so persisting or announcing it would partially undo
                // the unwind. A branch gate only delays such a result; this
                // generation check is what invalidates it.
                if !signer_result_is_current(generation, self.unwind_generation) {
                    tracing::warn!(
                        target: "scroll::chain_orchestrator",
                        block_number = block.header.number,
                        block_generation = generation,
                        current_generation = self.unwind_generation,
                        "Discarding locally signed block from a stale (pre-unwind) generation"
                    );
                    return Ok(Some(ChainOrchestratorEvent::BlockBuildingSkipped));
                }

                // A signed block may have been produced just before an authorization rotation and
                // queued while the barrier was open. Now that the barrier is closed, re-validate
                // the local signer against the current consensus before persisting
                // or announcing, so an old-signer block cannot leak through after
                // the signer has rotated away from us.
                if let Some(address) = self.signer.as_ref().map(|s| s.address) {
                    if !self.consensus.should_sequence_block(&address) {
                        tracing::warn!(
                            target: "scroll::chain_orchestrator",
                            block_number = block.header.number,
                            "Discarding locally signed block: this node's signer is no longer authorized after rotation"
                        );
                        return Ok(Some(ChainOrchestratorEvent::BlockBuildingSkipped));
                    }
                }

                let hash = block.hash_slow();
                self.database
                    .tx_mut(move |tx| async move {
                        tx.set_l2_head_block_number(block.header.number).await?;
                        tx.insert_signature(hash, signature).await
                    })
                    .await?;
                self.network.handle().announce_block(block.clone(), signature);
                Ok(Some(ChainOrchestratorEvent::SignedBlock { block, signature }))
            }
        }
    }

    /// Handles an event from the sequencer.
    async fn handle_sequencer_event(
        &mut self,
        event: SequencerEvent,
    ) -> Result<Option<ChainOrchestratorEvent>, ChainOrchestratorError> {
        tracing::info!(target: "scroll::chain_orchestrator", ?event, "Handling sequencer event");
        match event {
            SequencerEvent::NewSlot => {
                if self.consensus.should_sequence_block(
                    self.signer
                        .as_ref()
                        .map(|s| &s.address)
                        .expect("signer must be set if sequencer is present"),
                ) {
                    self.metric_handler.start_block_building_recording();
                    self.sequencer
                        .as_mut()
                        .expect("sequencer must be present")
                        .start_payload_building(&mut self.engine)
                        .await?;
                }
            }
            SequencerEvent::PayloadReady(payload_id) => {
                // Drain any authorization-control update that arrived after this `PayloadReady` was
                // selected. If a barrier opened, do not finalize: `finalize_payload_building`
                // updates forkchoice internally, which must not happen for a
                // payload built under a signer that may now be revoked. The payload
                // is abandoned and rebuilt on the next slot once the barrier
                // closes; the later signer revalidation would in any case discard the
                // announcement, but this also prevents the earlier FCS mutation.
                if self.apply_pending_authorization_control() {
                    return Ok(Some(ChainOrchestratorEvent::BlockBuildingSkipped));
                }
                let block = self
                    .sequencer
                    .as_mut()
                    .expect("sequencer must be present")
                    .finalize_payload_building(payload_id, &mut self.engine)
                    .await?;

                self.metric_handler.finish_block_building_recording(block.as_ref());

                if let Some(block) = block {
                    let block_info: L2BlockInfoWithL1Messages = (&block).into();
                    self.database
                        .update_l1_messages_from_l2_blocks(vec![block_info.clone()])
                        .await?;
                    self.signer
                        .as_mut()
                        .expect("signer must be present")
                        .sign_block(block.clone(), self.unwind_generation)?;
                    return Ok(Some(ChainOrchestratorEvent::BlockSequenced(block)));
                }
                return Ok(Some(ChainOrchestratorEvent::BlockBuildingSkipped));
            }
        }

        Ok(None)
    }

    /// Whether a committed administrative reset tail is still pending (the database unwind has
    /// committed but its forkchoice/watcher tail has not fully succeeded).
    ///
    /// While this holds, the node is fail-closed: authorization-sensitive branches (sequencing,
    /// signer, network, L1 notifications) are withheld so no work runs against the partially-reset
    /// database, in every consensus mode. It is independent of the authorization barrier, so even a
    /// static node — or a dynamic node whose old watcher closes the sentinel barrier during the
    /// window — cannot resume work until the reset completes.
    const fn reset_tail_pending(&self) -> bool {
        self.staged_reset.is_some()
    }

    /// Records that a fresh unwind — an administrative reset or an ordinary L1 reorg — has
    /// committed and discarded chain state, invalidating in-flight asynchronous block work.
    ///
    /// It advances [`Self::unwind_generation`] so an already-queued signing result built on the
    /// now-discarded generation is dropped when it returns, and cancels any in-flight payload job
    /// in *every* consensus mode so a payload started before the unwind is not finalized (and
    /// its forkchoice/L1-message effects applied) afterwards. Called exactly once per committed
    /// unwind; **not** called when a staged tail is retried, so the generation advances once
    /// per unwind.
    fn begin_committed_unwind(&mut self) {
        self.unwind_generation = self.unwind_generation.wrapping_add(1);
        self.cancel_payload_building_job();
    }

    /// Records that an administrative reset's database unwind for `block_number` has committed.
    ///
    /// This is the point of no return: the shared committed-unwind transitions run
    /// ([`Self::begin_committed_unwind`] — advance the generation, cancel any payload), the
    /// pre-reset ordinary-reorg recovery generation
    /// ([`Self::pending_l1_retry`]/[`Self::staged_reorg`]) is dropped so it cannot replay its
    /// stale tail/forkchoice target against the newly unwound database, dynamic authorization
    /// is failed closed, and the committed `unwind_result` is staged so a retry resumes the
    /// tail without unwinding again. It runs exactly once per reset, only after the unwind has
    /// committed.
    fn begin_committed_reset(&mut self, block_number: u64, unwind_result: UnwindResult) {
        self.begin_committed_unwind();

        commit_reset_generation(
            self.dynamic_authorization,
            &mut self.pending_l1_retry,
            &mut self.staged_reorg,
            self.consensus.as_mut(),
        );

        self.staged_reset = Some(StagedReset { block_number, unwind_result });
    }

    /// Repairs the forkchoice safe head from the currently staged reset's committed unwind.
    ///
    /// Idempotent: it drives the engine to the same safe-head target, so re-running it on a retry
    /// (for example after a watcher-reset failure) is harmless. A failure leaves the staged reset
    /// in place, so the reset stays fail-closed and a retry resumes here without unwinding
    /// again.
    async fn repair_fcs_from_staged_reset(&mut self) -> Result<(), ChainOrchestratorError> {
        // Copy the safe-head target out by value so the staged reset is not borrowed across the
        // engine call.
        let Some(safe_block_info) =
            self.staged_reset.as_ref().and_then(|staged| staged.unwind_result.l2_safe_block_info)
        else {
            return Ok(());
        };

        // If the new safe head is at or above the current finalized head, update the fcs safe head
        // to it; otherwise clamp to the finalized head.
        let finalized = *self.engine.fcs().finalized_block_info();
        let target =
            if safe_block_info.number >= finalized.number { safe_block_info } else { finalized };
        self.engine.update_fcs(None, Some(target), None).await?;
        Ok(())
    }

    /// Reverts the rollup node state to `block_number` as a staged, commit-aware transition across
    /// the two commit boundaries — the database unwind (a committed transaction) and the fallible
    /// forkchoice/watcher tail — so a failure at any stage is retryable and never corrupts recovery
    /// state. Returns `Ok(())` once the full reset has committed; the caller maps the outcome onto
    /// the command's typed response.
    async fn handle_revert_to_l1_block(
        &mut self,
        block_number: u64,
    ) -> Result<(), ChainOrchestratorError> {
        // Reject a request for a *different* target while a reset is already staged, before
        // touching any state: the staged unwind may not have applied its forkchoice target
        // yet, and composing a second unwind delta onto it cannot be done safely (a higher
        // target cannot restore already-deleted data; a lower target would discard the
        // first reset's unapplied forkchoice target). The operator must complete/retry the
        // staged reset first.
        if let ResetDecision::Reject { staged } = reset_decision(&self.staged_reset, block_number) {
            tracing::warn!(
                target: "scroll::chain_orchestrator",
                staged,
                requested = block_number,
                "Rejecting RevertToL1Block: a reset to a different L1 block is already staged"
            );
            return Err(ChainOrchestratorError::ResetInProgress { staged, requested: block_number });
        }

        // Cancellation precedes the fallible unwind so even an unwind error cannot strand a build
        // after this reset transition disables the sequencer branch.
        self.set_l1_syncing();

        // Stage 1 — reuse the committed unwind already staged for this exact target (a prior
        // attempt failed in the tail); otherwise unwind fresh. `staged_reset` here is
        // either absent or for this same target (a different target was rejected above), so
        // `is_none()` distinguishes the two cases. Never unwind the same reset twice, which
        // would observe the already-removed rows, lose the original forkchoice target, and
        // falsely succeed. A fresh unwind that fails commits nothing, so the pre-reset
        // generation and authorization stay intact and the reset is retryable from scratch.
        if self.staged_reset.is_none() {
            let unwind_result = self.database.unwind(block_number).await?;
            // Unwind committed: drop the stale ordinary-reorg recovery generation (so it cannot
            // replay against the newly unwound database), fail dynamic authorization closed, and
            // stage the committed unwind for the tail.
            self.begin_committed_reset(block_number, unwind_result);
        }

        // Stage 2 — repair forkchoice from the staged (committed) unwind. Idempotent, so a retry
        // re-applies the same target harmlessly. A failure keeps the staged reset in place; the
        // node stays fail-closed (`reset_tail_pending`) and a retry resumes here without
        // unwinding again.
        self.repair_fcs_from_staged_reset().await?;

        // Stage 3 — reset the watcher. This is fallible: if the watcher task is gone the reset
        // cannot be delivered, so it returns an error and leaves the handle's receivers
        // untouched. We propagate that error *before* clearing the staged reset or swapping
        // the control receiver, so the reset is not reported as successful and a retry
        // resumes it; the node remains fail-closed meanwhile.
        let consensus_control_rx = self.l1_watcher.revert_to_l1_block(block_number)?;

        // Full tail succeeded — commit the reset: install the fresh authorization-control receiver
        // (so post-reset control messages arrive on the new channel and any stale queued update is
        // discarded with the old one) and clear the staged reset, which lifts the fail-closed hold.
        // These final swaps are synchronous with no `.await` between them.
        self.consensus_control_rx = consensus_control_rx;
        self.staged_reset = None;

        self.notify(ChainOrchestratorEvent::UnwoundToL1Block(block_number));
        Ok(())
    }

    /// Applies every gate for an explicitly requested payload build. The command handler sends
    /// this outcome through its oneshot on every path, including configuration and start errors.
    async fn start_requested_block_build(&mut self) -> Result<(), BuildBlockError> {
        if self.apply_pending_authorization_control() {
            return Err(BuildBlockError::AuthorizationPending);
        }
        if self.reset_tail_pending() {
            return Err(BuildBlockError::ResetInProgress);
        }
        if self.sequencer.is_none() {
            return Err(BuildBlockError::MissingSequencer);
        }
        if !self.sync_state.is_synced() {
            return Err(BuildBlockError::NotSynced);
        }
        if self.requested_build_completion.is_some() ||
            self.sequencer
                .as_ref()
                .is_some_and(|sequencer| sequencer.payload_building_job().is_some())
        {
            return Err(BuildBlockError::BuildInProgress);
        }

        let signer = self.signer.as_ref().ok_or(BuildBlockError::MissingSigner)?.address;
        if !self.consensus.should_sequence_block(&signer) {
            return Err(BuildBlockError::UnauthorizedSigner { signer });
        }

        let sequencer = self.sequencer.as_mut().expect("sequencer existence checked above");
        sequencer
            .start_payload_building(&mut self.engine)
            .await
            .map_err(|err| BuildBlockError::PayloadStartFailed(err.to_string()))
    }

    /// Handles a command sent to the chain orchestrator.
    async fn handle_command(
        &mut self,
        command: ChainOrchestratorCommand<N>,
    ) -> Result<(), ChainOrchestratorError> {
        tracing::debug!(target: "scroll::chain_orchestrator", ?command, "Handling command");
        match command {
            ChainOrchestratorCommand::BuildBlock(response) => {
                // A `BuildBlock` command (admin/debug handle, or the optional remote block source
                // after a trusted import) is a state-changing block-production entry point. Drain
                // any queued authorization-control update first so a barrier that
                // opened after this command was enqueued is visible, then apply the
                // same authorization gate as `SequencerEvent::NewSlot`: a payload
                // is never started while the barrier is open or when this node is
                // not the authorized signer, so engine forkchoice and L1-message
                // mappings are not mutated before the later signer revalidation runs. It is also
                // withheld while a committed administrative reset tail is pending, so no block is
                // produced against a partially-reset database (this is the only fail-closed gate in
                // static mode, where authorization is never suspended).
                let result = self.start_requested_block_build().await;
                let result = result.map(|()| {
                    let (completion, ticket) = build_block_channel();
                    self.requested_build_completion = Some(completion);
                    ticket
                });
                match &result {
                    Err(
                        err @ (BuildBlockError::AuthorizationPending |
                        BuildBlockError::ResetInProgress |
                        BuildBlockError::UnauthorizedSigner { .. } |
                        BuildBlockError::BuildInProgress |
                        BuildBlockError::NotSynced),
                    ) => {
                        tracing::warn!(
                            target: "scroll::chain_orchestrator",
                            ?err,
                            "BuildBlock command rejected by sequencing policy"
                        );
                    }
                    Err(err) => {
                        tracing::error!(
                            target: "scroll::chain_orchestrator",
                            ?err,
                            "BuildBlock command failed"
                        );
                    }
                    Ok(_) => {}
                }
                let _ = response.send(result);
            }
            ChainOrchestratorCommand::EventListener(tx) => {
                let _ = tx.send(self.event_listener());
            }
            ChainOrchestratorCommand::Status(tx) => {
                let (l1_latest, l1_finalized, l1_processed) = self
                    .database
                    .tx(|tx| async move {
                        let l1_latest = tx.get_latest_l1_block_number().await?;
                        let l1_finalized = tx.get_finalized_l1_block_number().await?;
                        let l1_processed = tx.get_processed_l1_block_number().await?;
                        Ok::<_, ChainOrchestratorError>((l1_latest, l1_finalized, l1_processed))
                    })
                    .await?;
                let status = ChainOrchestratorStatus::new(
                    &self.sync_state,
                    l1_latest,
                    l1_finalized,
                    l1_processed,
                    self.engine.fcs().clone(),
                );
                let _ = tx.send(status);
            }
            ChainOrchestratorCommand::NetworkHandle(tx) => {
                let _ = tx.send(self.network.handle().clone());
            }
            ChainOrchestratorCommand::UpdateFcsHead((head, sender)) => {
                // This admin command mutates engine forkchoice and the database, so it must not run
                // against a partially-reset database while a committed reset tail is pending. Drop
                // the acknowledgement without applying the update so the operator observes it did
                // not take effect (rather than a false success) and can retry once the reset
                // completes.
                if self.reset_tail_pending() {
                    tracing::warn!(
                        target: "scroll::chain_orchestrator",
                        "Ignoring UpdateFcsHead command: an administrative reset is in progress; retry once it completes"
                    );
                } else {
                    // Collect transactions of reverted blocks from l2 client.
                    let reverted_transactions = self
                        .collect_reverted_txs_in_range(
                            head.number.saturating_add(1),
                            self.engine.fcs().head_block_info().number,
                        )
                        .await?;
                    self.engine.update_fcs(Some(head), None, None).await?;
                    self.database
                        .tx_mut(move |tx| async move {
                            tx.purge_l1_message_to_l2_block_mappings(Some(head.number + 1)).await?;
                            tx.set_l2_head_block_number(head.number).await
                        })
                        .await?;

                    // Add all reverted transactions to the transaction pool.
                    self.reinsert_txs_into_pool(reverted_transactions).await;
                    self.notify(ChainOrchestratorEvent::FcsHeadUpdated(head));
                    let _ = sender.send(());
                }
            }
            ChainOrchestratorCommand::EnableAutomaticSequencing(tx) => {
                if let Some(sequencer) = self.sequencer.as_mut() {
                    sequencer.enable();
                    let _ = tx.send(true);
                } else {
                    tracing::error!(target: "scroll::chain_orchestrator", "Received EnableAutomaticSequencing command but sequencer is not configured");
                    let _ = tx.send(false);
                }
            }
            ChainOrchestratorCommand::DisableAutomaticSequencing(tx) => {
                if let Some(sequencer) = self.sequencer.as_mut() {
                    let cancelled = sequencer.disable();
                    if cancelled {
                        self.complete_requested_block_build(BuildBlockOutcome::Skipped);
                        self.notify(ChainOrchestratorEvent::BlockBuildingSkipped);
                    }
                    let _ = tx.send(true);
                } else {
                    tracing::error!(target: "scroll::chain_orchestrator", "Received DisableAutomaticSequencing command but sequencer is not configured");
                    let _ = tx.send(false);
                }
            }
            ChainOrchestratorCommand::DatabaseQuery(query) => match query {
                DatabaseQuery::GetL1MessageByKey(l1_message_key, sender) => {
                    let l1_message =
                        self.database.get_n_l1_messages(Some(l1_message_key), 1).await?.pop();
                    let _ = sender.send(l1_message);
                }
            },
            ChainOrchestratorCommand::RevertToL1Block((block_number, tx)) => {
                // Surface the typed outcome to the caller: `Ok(true)` on a completed reset, or a
                // `ResetCommandError` (notably `ResetInProgress` for a rejected different target).
                // The reset body drives the staged transition and is not `?`-propagated here, so
                // the operator receives the specific error rather than a
                // dropped-channel error.
                let result = self.handle_revert_to_l1_block(block_number).await;
                let _ = tx.send(result.map(|()| true).map_err(ResetCommandError::from));
            }
            ChainOrchestratorCommand::ImportBlock { block_with_peer, response } => {
                // A trusted `ImportBlock` (from the optional remote block source) mutates the
                // canonical chain and engine forkchoice, so it must observe the same authorization
                // barrier as the network/sequencer branches. Drain any control update that arrived
                // after this command was enqueued, then, if the barrier is open, defer the import
                // instead of applying it: the caller retries on its next poll once phase two closes
                // the barrier. We must not block here waiting for the barrier — that would stall
                // the single orchestrator loop that is responsible for processing
                // the phase-two close. It is likewise deferred while a committed administrative
                // reset tail is pending, so no block is imported against a partially-reset database
                // (the reset guard is the only fail-closed gate in static mode).
                let barrier_pending = self.apply_pending_authorization_control();
                if barrier_pending || self.reset_tail_pending() {
                    tracing::debug!(
                        target: "scroll::chain_orchestrator",
                        barrier_pending,
                        reset_tail_pending = self.reset_tail_pending(),
                        "Deferring trusted ImportBlock: authorization pending or reset in progress",
                    );
                    let _ = response.send(Err(ImportBlockError::AuthorizationPending));
                } else {
                    let result = self
                        .import_chain(vec![block_with_peer.block.clone()], block_with_peer)
                        .await
                        .map_err(|e| ImportBlockError::Other(e.to_string()));
                    let _ = response.send(result);
                }
            }
            #[cfg(feature = "test-utils")]
            ChainOrchestratorCommand::SetGossip((enabled, tx)) => {
                self.network.handle().set_gossip(enabled).await;
                let _ = tx.send(());
            }
            #[cfg(feature = "test-utils")]
            ChainOrchestratorCommand::DatabaseHandle(tx) => {
                let _ = tx.send(self.database.clone());
            }
        }

        Ok(())
    }

    /// Returns a new event listener for the rollup node manager.
    pub fn event_listener(&mut self) -> EventStream<ChainOrchestratorEvent> {
        if let Some(event_sender) = &self.event_sender {
            return event_sender.new_listener();
        };

        let event_sender = EventSender::new(EVENT_CHANNEL_SIZE);
        let event_listener = event_sender.new_listener();
        self.event_sender = Some(event_sender);

        event_listener
    }

    /// Notifies all event listeners of the given event.
    fn notify(&self, event: ChainOrchestratorEvent) {
        if let Some(s) = self.event_sender.as_ref() {
            s.notify(event);
        }
    }

    /// Handles a derived batch by inserting the derived blocks into the database.
    async fn handle_derived_batch(
        &mut self,
        batch: BatchDerivationResult,
    ) -> Result<Option<ChainOrchestratorEvent>, ChainOrchestratorError> {
        let batch_info = batch.batch_info;
        tracing::info!(target: "scroll::chain_orchestrator", batch_info = ?batch_info, num_blocks = batch.attributes.len(), "Handling derived batch");

        let skipped_l1_messages = batch.skipped_l1_messages.clone();
        let batch_reconciliation_result =
            reconcile_batch(&self.l2_client, batch, self.engine.fcs()).await?;
        let aggregated_actions = batch_reconciliation_result.aggregate_actions();

        let mut reorg_results = vec![];
        for action in aggregated_actions.actions {
            let outcome = match action {
                BlockConsolidationAction::Skip(_) => {
                    unreachable!("Skip actions have been filtered out in aggregation")
                }
                BlockConsolidationAction::UpdateFcs(block_info) => {
                    tracing::info!(target: "scroll::chain_orchestrator", ?block_info, "Updating safe head to consolidated block");
                    let finalized_block_info = batch_reconciliation_result
                        .target_status
                        .is_finalized()
                        .then_some(block_info.block_info);
                    self.engine
                        .update_fcs(None, Some(block_info.block_info), finalized_block_info)
                        .await?;
                    BlockConsolidationOutcome::UpdateFcs(block_info)
                }
                BlockConsolidationAction::Reorg(attributes) => {
                    tracing::info!(target: "scroll::chain_orchestrator", block_number = ?attributes.block_number, "Reorging chain to derived block");
                    // We reorg the head to the safe block and then build the payload for the
                    // attributes.
                    let head = *self.engine.fcs().safe_block_info();
                    if head.number != attributes.block_number - 1 {
                        return Err(ChainOrchestratorError::InvalidBatchReorg {
                            batch_info,
                            safe_block_number: head.number,
                            derived_block_number: attributes.block_number,
                        });
                    }
                    let fcu = self.engine.build_payload(Some(head), attributes.attributes).await?;
                    let payload = self
                        .engine
                        .get_payload(fcu.payload_id.expect("payload_id can not be None"))
                        .await?;

                    let block_info: L2BlockInfoWithL1Messages = (&payload)
                        .try_into()
                        .map_err(ChainOrchestratorError::RollupNodePrimitiveError)?;
                    let result = self.engine.new_payload(payload).await?;
                    if result.is_invalid() {
                        return Err(ChainOrchestratorError::InvalidBatch(
                            block_info.block_info,
                            batch_info,
                        ));
                    }

                    // Update the forkchoice state to the new head.
                    let finalized_block_info = batch_reconciliation_result
                        .target_status
                        .is_finalized()
                        .then_some(block_info.block_info);
                    self.engine
                        .update_fcs(
                            Some(block_info.block_info),
                            Some(block_info.block_info),
                            finalized_block_info,
                        )
                        .await?;

                    reorg_results.push(block_info.clone());
                    BlockConsolidationOutcome::Reorged(block_info)
                }
            };

            self.notify(ChainOrchestratorEvent::BlockConsolidated(outcome.clone()));
        }

        let batch_consolidation_outcome =
            batch_reconciliation_result.into_batch_consolidation_outcome(reorg_results).await?;

        // Insert the batch consolidation outcome into the database.
        let mut consolidation_outcome = batch_consolidation_outcome.clone();
        consolidation_outcome.with_skipped_l1_messages(skipped_l1_messages);

        self.database.insert_batch_consolidation_outcome(consolidation_outcome).await?;

        Ok(Some(ChainOrchestratorEvent::BatchConsolidated(batch_consolidation_outcome)))
    }

    /// Handles an L1 notification.
    async fn handle_l1_notification(
        &mut self,
        notification: Arc<L1Notification>,
    ) -> Result<Option<ChainOrchestratorEvent>, ChainOrchestratorError> {
        match &*notification {
            L1Notification::Processed(block_number) => {
                let block_number = *block_number;
                self.database.set_processed_l1_block_number(block_number).await?;
                Ok(None)
            }
            L1Notification::Reorg(block_number) => {
                metered!(Task::L1Reorg, self, handle_l1_reorg(*block_number))
            }
            L1Notification::NewBlock(block_info) => self.handle_l1_new_block(*block_info).await,
            L1Notification::AuthorizedSigner { head, signer } => {
                // Phase two closes the head-qualified authorization barrier. It arrives on the
                // ordinary FIFO channel *after* the head's `Reorg`/`NewBlock`, so the structural
                // transition (database unwind, forkchoice repair) is already applied before the
                // barrier clears and sequencer/signer/network work resumes. A stale head
                // (superseded by a newer pending barrier) is ignored by
                // `update_config`.
                self.consensus.update_config(&ConsensusUpdate::AuthorizedSigner {
                    head: *head,
                    signer: *signer,
                });
                Ok(None)
            }
            L1Notification::Finalized(block_number) => {
                metered!(Task::L1Finalization, self, handle_l1_finalized(*block_number))
            }
            L1Notification::BatchCommit { block_info, data } => {
                metered!(Task::BatchCommit, self, handle_batch_commit(*block_info, data.clone()))
            }
            L1Notification::BatchRevert { batch_info, block_info } => {
                metered!(
                    Task::BatchRevert,
                    self,
                    handle_batch_revert(batch_info.index, batch_info.index, *block_info)
                )
            }
            L1Notification::BatchRevertRange { start, end, block_info } => {
                metered!(
                    Task::BatchRevertRange,
                    self,
                    handle_batch_revert(*start, *end, *block_info)
                )
            }
            L1Notification::L1Message { message, block_info, block_timestamp: _ } => {
                metered!(Task::L1Message, self, handle_l1_message(message.clone(), *block_info))
            }
            L1Notification::Synced => {
                tracing::info!(target: "scroll::chain_orchestrator", "L1 is now synced");
                self.sync_state.l1_mut().set_synced();
                if self.sync_state.is_synced() {
                    metered!(Task::ChainConsolidation, self, consolidate_chain())?;
                }
                self.notify(ChainOrchestratorEvent::L1Synced);
                Ok(None)
            }
            L1Notification::BatchFinalization { hash: _hash, index, block_info } => {
                metered!(
                    Task::BatchFinalization,
                    self,
                    handle_batch_finalization(*index, *block_info)
                )
            }
        }
    }

    async fn handle_l1_new_block(
        &self,
        block_info: BlockInfo,
    ) -> Result<Option<ChainOrchestratorEvent>, ChainOrchestratorError> {
        self.database.set_latest_l1_block_number(block_info.number).await?;
        Ok(Some(ChainOrchestratorEvent::NewL1Block(block_info.number)))
    }

    /// Collects reverted L2 transactions in [from, to], excluding L1 messages.
    async fn collect_reverted_txs_in_range(
        &self,
        from: u64,
        to: u64,
    ) -> Result<Vec<ScrollTxEnvelope>, ChainOrchestratorError> {
        let mut reverted_transactions: Vec<ScrollTxEnvelope> = Vec::new();
        for number in from..=to {
            let block = self
                .l2_client
                .get_block_by_number(number.into())
                .full()
                .await?
                .ok_or_else(|| ChainOrchestratorError::L2BlockNotFoundInL2Client(number))?;

            let block = block.into_consensus().map_transactions(|tx| tx.inner.into_inner());
            reverted_transactions.extend(
                block.into_body().transactions.into_iter().filter(|tx| !tx.is_l1_message()),
            );
        }
        Ok(reverted_transactions)
    }

    /// Reinserts given L2 transactions into the transaction pool.
    async fn reinsert_txs_into_pool(&self, txs: Vec<ScrollTxEnvelope>) {
        for tx in txs {
            let encoded_tx = tx.encoded_2718();
            if let Err(err) = self.l2_client.send_raw_transaction(&encoded_tx).await {
                tracing::warn!(
                    target: "scroll::chain_orchestrator",
                    ?err,
                    "failed to reinsert reverted transaction into pool"
                );
            }
        }
    }

    /// Handles a reorganization event by deleting all indexed data which is greater than the
    /// provided block number.
    async fn handle_l1_reorg(
        &mut self,
        block_number: u64,
    ) -> Result<Option<ChainOrchestratorEvent>, ChainOrchestratorError> {
        // Resume from a staged (already-committed) unwind if a prior attempt for this reorg failed
        // in the L2/engine tail; otherwise perform the unwind, which commits before the fallible
        // tail. The committed unwind is never re-run, so a retry cannot lose the original
        // `UnwindResult` or falsely succeed with an empty second unwind while forkchoice stays
        // unrepaired.
        let unwind = match self.staged_reorg.take() {
            Some(unwind) => unwind,
            None => {
                // A fresh reorg unwind commits here and discards chain state, so run the shared
                // committed-unwind transitions once: advance the chain generation (so a signer
                // result queued for a pre-reorg block is discarded when it returns) and cancel any
                // in-flight payload. A staged-tail resume above skips this, so the generation
                // advances exactly once per committed reorg unwind.
                let unwind = self.database.unwind(block_number).await?;
                self.begin_committed_unwind();
                unwind
            }
        };

        match self.apply_reorg_tail(&unwind).await {
            Ok(event) => Ok(event),
            Err(err) => {
                // Preserve the committed unwind so a retry resumes only the tail — but only in
                // dynamic mode, which is the sole reason the run loop retains and retries a failed
                // head transition (to gate the barrier close). In static/no-op mode the failed
                // `Reorg` is not retained, so a staged unwind would never be resumed; leave staging
                // off there and surface the error, matching the pre-PR default reorg behavior.
                if self.dynamic_authorization {
                    self.staged_reorg = Some(unwind);
                }
                Err(err)
            }
        }
    }

    /// Applies the L2/engine tail of a reorg (L2 head lookup, reverted-transaction collection,
    /// forkchoice repair, transaction reinsertion) from an already-committed [`UnwindResult`]. This
    /// is the fallible, resumable part of [`Self::handle_l1_reorg`]; it never touches the database
    /// unwind, so it is safe to retry after a mid-tail failure.
    async fn apply_reorg_tail(
        &mut self,
        unwind: &UnwindResult,
    ) -> Result<Option<ChainOrchestratorEvent>, ChainOrchestratorError> {
        let &UnwindResult {
            l1_block_number,
            queue_index,
            l2_head_block_number,
            l2_safe_block_info,
        } = unwind;

        let (l2_head_block_info, reverted_transactions) =
            if let Some(block_number) = l2_head_block_number {
                // Fetch the block hash of the new L2 head block.
                let block_hash = self
                    .l2_client
                    .get_block_by_number(block_number.into())
                    .full()
                    .await?
                    .ok_or(ChainOrchestratorError::L2BlockNotFoundInL2Client(block_number))?
                    .header
                    .hash_slow();

                // Cancel the inflight payload building job if the head has changed.
                self.cancel_payload_building_job();

                // Collect transactions of reverted blocks from l2 client.
                let reverted_transactions = self
                    .collect_reverted_txs_in_range(
                        block_number.saturating_add(1),
                        self.engine.fcs().head_block_info().number,
                    )
                    .await?;

                (Some(BlockInfo { number: block_number, hash: block_hash }), reverted_transactions)
            } else {
                (None, Vec::new())
            };

        // If the L1 reorg is before the origin of the inflight payload building job, cancel it.
        if Some(l1_block_number) <
            self.sequencer
                .as_ref()
                .and_then(|s| s.payload_building_job().map(|p| p.l1_origin()))
                .flatten()
        {
            self.cancel_payload_building_job();
        }

        // TODO: Add retry logic
        if l2_head_block_info.is_some() || l2_safe_block_info.is_some() {
            self.engine.update_fcs(l2_head_block_info, l2_safe_block_info, None).await?;
        }

        // Add all reverted transactions to the transaction pool.
        self.reinsert_txs_into_pool(reverted_transactions).await;

        let event = ChainOrchestratorEvent::L1Reorg {
            l1_block_number,
            queue_index,
            l2_head_block_info,
            l2_safe_block_info,
        };

        Ok(Some(event))
    }

    /// Handles a finalized event by updating the chain orchestrator L1 finalized block, returning
    /// the new finalized L2 chain block and the list of finalized batches.
    async fn handle_l1_finalized(
        &mut self,
        block_number: u64,
    ) -> Result<Option<ChainOrchestratorEvent>, ChainOrchestratorError> {
        let (finalized_block_info, triggered_batches) = self
            .database
            .tx_mut(move |tx| async move {
                // Set the latest finalized L1 block in the database.
                tx.set_finalized_l1_block_number(block_number).await?;

                // Finalize consolidated batches up to the finalized L1 block number.
                let finalized_block_info = tx.finalize_consolidated_batches(block_number).await?;

                // Get all unprocessed batches that have been finalized by this L1 block
                // finalization.
                let triggered_batches =
                    tx.fetch_and_update_unprocessed_finalized_batches(block_number).await?;

                Ok::<_, ChainOrchestratorError>((finalized_block_info, triggered_batches))
            })
            .await?;

        if finalized_block_info.is_some() {
            tracing::info!(target: "scroll::chain_orchestrator", ?finalized_block_info, "Updating FCS with new finalized block info from L1 finalization");
            self.engine.update_fcs(None, None, finalized_block_info).await?;
        }

        for batch in &triggered_batches {
            self.derivation_pipeline.push_batch(*batch, BatchStatus::Finalized).await;
        }

        Ok(Some(ChainOrchestratorEvent::L1BlockFinalized(block_number, triggered_batches)))
    }

    /// Handles a batch input by inserting it into the database.
    async fn handle_batch_commit(
        &mut self,
        block_info: BlockInfo,
        batch: BatchCommitData,
    ) -> Result<Option<ChainOrchestratorEvent>, ChainOrchestratorError> {
        let batch_info: BatchInfo = (&batch).into();
        let event = self
            .database
            .tx_mut(move |tx| {
                let batch = batch.clone();
                async move {
                    let prev_batch_index = batch.index - 1;

                    // Perform a consistency check to ensure the previous commit batch exists in the
                    // database.
                    if tx.get_batch_by_index(prev_batch_index).await?.is_none() {
                        return Err(ChainOrchestratorError::BatchCommitGap(batch.index));
                    }

                    let event = ChainOrchestratorEvent::BatchCommitIndexed {
                        batch_info: (&batch).into(),
                        l1_block_number: batch.block_number,
                    };

                    // insert the batch and commit the transaction.
                    tx.insert_batch(batch).await?;

                    // insert the L1 block info.
                    tx.insert_l1_block_info(block_info).await?;

                    Ok::<_, ChainOrchestratorError>(Some(event))
                }
            })
            .await?;

        if self.sync_state.is_synced() {
            self.derivation_pipeline.push_batch(batch_info, BatchStatus::Consolidated).await;
        }

        Ok(event)
    }

    /// Handles a batch finalization event by updating the batch input in the database.
    async fn handle_batch_finalization(
        &mut self,
        batch_index: u64,
        l1_block_info: BlockInfo,
    ) -> Result<Option<ChainOrchestratorEvent>, ChainOrchestratorError> {
        let triggered_batches = self
            .database
            .tx_mut(move |tx| async move {
                // Insert the L1 block info.
                tx.insert_l1_block_info(l1_block_info).await?;

                // finalize all batches up to `batch_index`.
                tx.finalize_batches_up_to_index(batch_index, l1_block_info.number).await?;
                let finalized_block_number = tx.get_finalized_l1_block_number().await?;

                // Get all unprocessed batches that have been finalized by this L1 block
                // finalization.
                let triggered_batches = if finalized_block_number >= l1_block_info.number {
                    tx.fetch_and_update_unprocessed_finalized_batches(finalized_block_number)
                        .await?
                } else {
                    vec![]
                };

                Ok::<_, ChainOrchestratorError>(triggered_batches)
            })
            .await?;

        for batch in &triggered_batches {
            self.derivation_pipeline.push_batch(*batch, BatchStatus::Finalized).await;
        }

        Ok(Some(ChainOrchestratorEvent::BatchFinalizeIndexed { l1_block_info, triggered_batches }))
    }

    /// Handles a batch revert event by updating the database.
    async fn handle_batch_revert(
        &mut self,
        start_index: u64,
        end_index: u64,
        l1_block_info: BlockInfo,
    ) -> Result<Option<ChainOrchestratorEvent>, ChainOrchestratorError> {
        let (safe_block_info, batch_info) = self
            .database
            .tx_mut(move |tx| async move {
                tx.insert_l1_block_info(l1_block_info).await?;
                tx.set_batch_revert_block_number_for_batch_range(
                    start_index,
                    end_index,
                    l1_block_info,
                )
                .await?;

                // handle the case of a batch revert.
                Ok::<_, ChainOrchestratorError>(tx.get_latest_safe_l2_info().await?)
            })
            .await?;

        // Update the forkchoice state to the new safe block.
        if self.sync_state.is_synced() {
            tracing::info!(target: "scroll::chain_orchestrator", ?safe_block_info, "Updating safe head to block after batch revert");
            self.engine.update_fcs(None, Some(safe_block_info), None).await?;
        }

        Ok(Some(ChainOrchestratorEvent::BatchReverted { batch_info, safe_head: safe_block_info }))
    }

    /// Handles an L1 message by inserting it into the database.
    async fn handle_l1_message(
        &self,
        l1_message: TxL1Message,
        l1_block_info: BlockInfo,
    ) -> Result<Option<ChainOrchestratorEvent>, ChainOrchestratorError> {
        let event = ChainOrchestratorEvent::L1MessageCommitted(l1_message.queue_index);
        let queue_hash = compute_l1_message_queue_hash(
            &self.database,
            &l1_message,
            self.config.l1_v2_message_queue_start_index(),
        )
        .await?;
        let l1_message = L1MessageEnvelope::new(l1_message, l1_block_info.number, None, queue_hash);

        // Perform a consistency check to ensure the previous L1 message exists in the database.
        self.database
            .tx_mut(move |tx| {
                let l1_message = l1_message.clone();
                async move {
                    if l1_message.transaction.queue_index > 0 &&
                        tx.get_n_l1_messages(
                            Some(L1MessageKey::from_queue_index(
                                l1_message.transaction.queue_index - 1,
                            )),
                            1,
                        )
                        .await?
                        .is_empty()
                    {
                        return Err(ChainOrchestratorError::L1MessageQueueGap(
                            l1_message.transaction.queue_index,
                        ));
                    }

                    tx.insert_l1_message(l1_message.clone()).await?;
                    tx.insert_l1_block_info(l1_block_info).await?;
                    Ok::<_, ChainOrchestratorError>(())
                }
            })
            .await?;

        Ok(Some(event))
    }

    async fn handle_network_event(
        &mut self,
        event: ScrollNetworkManagerEvent,
    ) -> Result<Option<ChainOrchestratorEvent>, ChainOrchestratorError> {
        match event {
            ScrollNetworkManagerEvent::NewBlock(block_with_peer) => {
                self.notify(ChainOrchestratorEvent::NewBlockReceived(block_with_peer.clone()));
                metered!(Task::L2BlockImport, self, handle_block_from_peer(block_with_peer))
            }
        }
    }

    /// Handles a new block received from a peer.
    async fn handle_block_from_peer(
        &mut self,
        block_with_peer: NewBlockWithPeer,
    ) -> Result<Option<ChainOrchestratorEvent>, ChainOrchestratorError> {
        tracing::debug!(target: "scroll::chain_orchestrator", block_hash = ?block_with_peer.block.header.hash_slow(), block_number = ?block_with_peer.block.number, peer_id = ?block_with_peer.peer_id, "Received new block from peer");

        // Check we are not handling a finalized block.
        if block_with_peer.block.header.number <= self.engine.fcs().finalized_block_info().number {
            self.network
                .handle()
                .block_import_outcome(BlockImportOutcome::finalized_block(block_with_peer.peer_id));
            return Ok(Some(ChainOrchestratorEvent::L2FinalizedBlockReceived(
                block_with_peer.block.header.hash_slow(),
                block_with_peer.peer_id,
            )));
        }

        // Drain any authorization-control update that arrived after this import was selected, so a
        // barrier that just opened is applied before validation. `validate_new_block` then returns
        // the non-penalizing `AuthorizationPending` while the barrier is open, deferring the block
        // (without penalizing the peer) rather than accepting it under a signer that is being
        // revoked and mutating forkchoice/database state.
        self.apply_pending_authorization_control();

        if let Err(err) =
            self.consensus.validate_new_block(&block_with_peer.block, &block_with_peer.signature)
        {
            tracing::error!(target: "scroll::node::manager", ?err, "consensus checks failed on block {:?} from peer {:?}", block_with_peer.block.hash_slow(), block_with_peer.peer_id);
            self.network.handle().block_import_outcome(BlockImportOutcome {
                peer: block_with_peer.peer_id,
                result: Err(err.into()),
            });

            return Ok(Some(ChainOrchestratorEvent::BlockFailedConsensusChecks(
                block_with_peer.block.header.hash_slow(),
                block_with_peer.peer_id,
            )));
        }

        // We optimistically persist the signature upon passing consensus checks.
        let block_hash = block_with_peer.block.header.hash_slow();
        self.database.insert_signature(block_hash, block_with_peer.signature).await?;

        let received_block_number = block_with_peer.block.number;
        let received_block_hash = block_with_peer.block.header.hash_slow();
        let current_head_block_number = self.engine.fcs().head_block_info().number;
        let current_head_block_hash = self.engine.fcs().head_block_info().hash;
        let current_safe_block_number = self.engine.fcs().safe_block_info().number;

        // If the received block number has a block number greater than the current head by more
        // than the optimistic sync threshold, we optimistically sync the chain.
        if received_block_number >
            current_head_block_number + self.config.optimistic_sync_threshold()
        {
            tracing::trace!(target: "scroll::chain_orchestrator", ?received_block_number, ?current_head_block_number, "Received new block from peer with block number greater than current head by more than the optimistic sync threshold");
            let block_info = BlockInfo {
                number: received_block_number,
                hash: block_with_peer.block.header.hash_slow(),
            };
            self.engine.optimistic_sync(block_info).await?;
            // The engine transition succeeded. Cancel synchronously before changing the sync flag,
            // which disables the only branch that polls the payload job.
            self.set_l2_syncing();

            // Purge all L1 message to L2 block mappings as they may be invalid after an
            // optimistic sync.
            self.database.purge_l1_message_to_l2_block_mappings(None).await?;

            return Ok(Some(ChainOrchestratorEvent::OptimisticSync(block_info)));
        }

        // If the block number is greater than the current head we attempt to extend the chain.
        let mut new_headers = if received_block_number > current_head_block_number {
            // Fetch the headers for the received block until we can reconcile it with the current
            // chain head.
            let fetch_count = received_block_number - current_head_block_number;
            let new_headers = if received_block_number > current_head_block_number + 1 {
                tracing::trace!(target: "scroll::chain_orchestrator", ?received_block_hash, ?received_block_number, ?current_head_block_number, fetch_count, "Fetching headers to extend chain");
                self.block_client
                    .get_full_block_range(received_block_hash, fetch_count)
                    .await
                    .into_iter()
                    .rev()
                    .map(|b| b.into_block())
                    .collect()
            } else {
                vec![block_with_peer.block.clone()]
            };

            // If the first header in the new headers has a parent hash that matches the current
            // head hash, we can import the chain.
            if new_headers.first().expect("at least one header exists").parent_hash ==
                current_head_block_hash
            {
                tracing::trace!(target: "scroll::chain_orchestrator", ?received_block_hash, ?received_block_number, "Received block from peer that extends the current head");
                let chain_import = self.import_chain(new_headers, block_with_peer).await?;
                return Ok(Some(ChainOrchestratorEvent::ChainExtended(chain_import)));
            }

            VecDeque::from(new_headers)
        } else {
            // If the block is less than or equal to the current head check if we already have it in
            // the chain.
            let current_chain_block = self
                .l2_client
                .get_block_by_number(received_block_number.into())
                .full()
                .await?
                .ok_or(ChainOrchestratorError::L2BlockNotFoundInL2Client(received_block_number))?;

            if current_chain_block.header.hash_slow() == received_block_hash {
                tracing::info!(target: "scroll::chain_orchestrator", ?received_block_hash, ?received_block_number, "Received block from peer that is already in the chain");
                return Ok(Some(ChainOrchestratorEvent::BlockAlreadyKnown(
                    received_block_hash,
                    block_with_peer.peer_id,
                )));
            }

            // Assert that we are not reorging below the safe head.
            let current_safe_info = self.engine.fcs().safe_block_info();
            if received_block_number <= current_safe_info.number {
                tracing::warn!(target: "scroll::chain_orchestrator", ?received_block_hash, ?received_block_number, current_safe_info = ?self.engine.fcs().safe_block_info(), "Received block from peer that would reorg below the safe head - ignoring");
                return Err(ChainOrchestratorError::L2SafeBlockReorgDetected);
            }

            // Check to assert that we have received a newer chain.
            let current_head = self
                .l2_client
                .get_block_by_number(current_head_block_number.into())
                .full()
                .await?
                .ok_or(ChainOrchestratorError::L2BlockNotFoundInL2Client(
                    current_head_block_number,
                ))?;

            // If the timestamp of the received block is less than or equal to the current head,
            // we ignore it.
            if block_with_peer.block.header.timestamp <= current_head.header.timestamp {
                tracing::debug!(target: "scroll::chain_orchestrator", ?received_block_hash, ?received_block_number, current_head_hash = ?current_head.header.hash_slow(), current_head_number = current_head_block_number, "Received block from peer that is older than the current head - ignoring");
                return Ok(Some(ChainOrchestratorEvent::OldForkReceived {
                    headers: vec![block_with_peer.block.header],
                    peer_id: block_with_peer.peer_id,
                    signature: block_with_peer.signature,
                }));
            }

            // Check if the parent hash of the received block is in the chain.
            let parent_block = self
                .l2_client
                .get_block_by_hash(block_with_peer.block.header.parent_hash)
                .full()
                .await?;
            if let Some(parent_block) = parent_block {
                // If the parent block has a block number equal to or greater than the current safe
                // head then it is safe to reorg.
                if parent_block.header.number >= current_safe_block_number {
                    tracing::debug!(target: "scroll::chain_orchestrator", ?received_block_hash, ?received_block_number, "Received block from peer that extends an earlier part of the chain");
                    let chain_import = self
                        .import_chain(vec![block_with_peer.block.clone()], block_with_peer)
                        .await?;
                    return Ok(Some(ChainOrchestratorEvent::ChainReorged(chain_import)));
                }
                // If the parent block has a block number less than the current safe head then would
                // suggest a reorg of the safe head - reject it.
                tracing::warn!(target: "scroll::chain_orchestrator", ?received_block_hash, ?received_block_number, current_safe_info = ?self.engine.fcs().safe_block_info(), "Received block from peer that would reorg below the safe head - ignoring");
                return Err(ChainOrchestratorError::L2SafeBlockReorgDetected);
            }

            VecDeque::from([block_with_peer.block.clone()])
        };

        // If we reach this point, we have a block that is not in the current chain and does not
        // extend the current head. This implies a reorg. We attempt to reconcile the fork.
        while current_safe_block_number + 1 <
            new_headers.front().expect("at least one header exists").number
        {
            let parent_hash = new_headers.front().expect("at least one header exists").parent_hash;
            let parent_number = new_headers.front().expect("at least one header exists").number - 1;
            let fetch_count = HEADER_FETCH_COUNT.min(parent_number - current_safe_block_number);
            tracing::trace!(target: "scroll::chain_orchestrator", ?received_block_hash, ?received_block_number, ?parent_hash, ?parent_number, %current_safe_block_number, fetch_count, "Fetching headers to find common ancestor for fork");
            let headers: Vec<DogeosBlock> = self
                .block_client
                .get_full_block_range(parent_hash, fetch_count)
                .await
                .into_iter()
                .map(|b| b.into_block())
                .collect();

            let mut index = None;
            for (i, header) in headers.iter().enumerate() {
                let current_block = self
                    .l2_client
                    .get_block_by_number(header.number.into())
                    .full()
                    .await?
                    .ok_or(ChainOrchestratorError::L2BlockNotFoundInL2Client(header.number))?
                    .into_consensus()
                    .map_transactions(|tx| tx.inner.into_inner());

                if header.hash_slow() == current_block.header.hash_slow() {
                    index = Some(i);
                    break;
                }
            }

            if let Some(index) = index {
                tracing::trace!(target: "scroll::chain_orchestrator", ?received_block_hash, ?received_block_number, common_ancestor = ?headers[index].hash_slow(), common_ancestor_number = headers[index].number, "Found common ancestor for fork - reorging to new chain");
                for header in headers.into_iter().take(index) {
                    new_headers.push_front(header);
                }
                let chain_import = self.import_chain(new_headers.into(), block_with_peer).await?;
                return Ok(Some(ChainOrchestratorEvent::ChainReorged(chain_import)));
            };

            // If we did not find a common ancestor, we add all the fetched headers to the front of
            // the deque and continue fetching.
            for header in headers {
                new_headers.push_front(header);
            }
        }

        Err(ChainOrchestratorError::L2SafeBlockReorgDetected)
    }

    /// Imports a chain of headers into the L2 chain.
    async fn import_chain(
        &mut self,
        chain: Vec<DogeosBlock>,
        block_with_peer: NewBlockWithPeer,
    ) -> Result<ChainImport, ChainOrchestratorError> {
        let chain_head_hash = chain.last().expect("at least one header exists").hash_slow();
        let chain_head_number = chain.last().expect("at least one header exists").number;
        tracing::info!(target: "scroll::chain_orchestrator", num_blocks = chain.len(), ?chain_head_hash, ?chain_head_number, "Received chain from peer");

        // If we are in consolidated mode, validate the L1 messages in the new blocks.
        if self.sync_state.is_synced() {
            self.validate_l1_messages(&chain).await?;
        }

        // Validate the new blocks by sending them to the engine.
        for block in &chain {
            let payload = ExecutionPayloadV1::from_block_slow(block);
            let status = self.engine.new_payload(payload).await?;
            tracing::debug!(target: "scroll::chain_orchestrator", block_number = block.number, block_hash = ?block.hash_slow(), ?status, "New payload status from engine");

            if status.is_invalid() {
                tracing::warn!(target: "scroll::chain_orchestrator", block_number = block.number, block_hash = ?block.hash_slow(), ?status, "Received invalid block from peer");
                self.network.handle().block_import_outcome(BlockImportOutcome::invalid_block(
                    block_with_peer.peer_id,
                ));
                return Err(ChainOrchestratorError::InvalidBlock);
            }
        }

        // Update the FCS to the new head.
        let head = BlockInfo { number: chain_head_number, hash: chain_head_hash };
        let result = if self.sync_state.l2().is_syncing() {
            self.engine.optimistic_sync(head).await?
        } else {
            self.engine.update_fcs(Some(head), None, None).await?
        };

        // If the FCS update resulted in an invalid state, we return an error.
        if result.is_invalid() {
            tracing::warn!(target: "scroll::chain_orchestrator", ?chain_head_hash, ?chain_head_number, ?result, "Failed to update FCS after importing new chain from peer");
            return Err(ChainOrchestratorError::InvalidBlock);
        }

        // If we were previously in L2 syncing mode and the FCS update resulted in a valid state, we
        // transition the L2 sync state to synced and consolidate the chain.
        if result.is_valid() && self.sync_state.l2().is_syncing() {
            tracing::info!(target: "scroll::chain_orchestrator", "L2 is now synced");
            self.sync_state.l2_mut().set_synced();

            // If both L1 and L2 are now synced, we transition to consolidated mode by consolidating
            // the chain.
            if self.sync_state.is_synced() {
                self.consolidate_chain().await?;
            }
        }

        // Persist the L1 message to L2 block mappings for reorg awareness, update the l2 head block
        // number and handle the valid block import if we are in a synced state and the
        // result is valid.
        if self.sync_state.is_synced() && result.is_valid() {
            let blocks = chain.iter().map(|block| block.into()).collect::<Vec<_>>();
            self.database
                .tx_mut(move |tx| {
                    let blocks = blocks.clone();
                    async move {
                        tx.update_l1_messages_from_l2_blocks(blocks).await?;
                        tx.set_l2_head_block_number(block_with_peer.block.header.number).await
                    }
                })
                .await?;

            self.network.handle().block_import_outcome(BlockImportOutcome::valid_block(
                block_with_peer.peer_id,
                block_with_peer.block,
                Bytes::copy_from_slice(&block_with_peer.signature.sig_as_bytes()),
            ));
        }

        Ok(ChainImport {
            chain,
            peer_id: block_with_peer.peer_id,
            signature: block_with_peer.signature,
            result,
        })
    }

    /// Consolidates the chain by validating all unsafe blocks from the current safe head to the
    /// current head.
    ///
    /// This involves validating the L1 messages in the blocks against the expected L1 messages
    /// synced from L1.
    async fn consolidate_chain(&mut self) -> Result<(), ChainOrchestratorError> {
        tracing::trace!(target: "scroll::chain_orchestrator", fcs = ?self.engine.fcs(), "Consolidating chain from safe to head");

        let safe_block_number = self.engine.fcs().safe_block_info().number;
        let head_block_number = self.engine.fcs().head_block_info().number;

        if head_block_number == safe_block_number {
            tracing::trace!(target: "scroll::chain_orchestrator", "No unsafe blocks to consolidate");
        } else {
            let block_stream = stream::iter(safe_block_number + 1..=head_block_number)
                .map(|block_number| {
                    let client = self.l2_client.clone();

                    async move {
                        client
                            .get_block_by_number(block_number.into())
                            .full()
                            .await?
                            .ok_or(ChainOrchestratorError::L2BlockNotFoundInL2Client(block_number))
                            .map(|b| {
                                b.into_consensus().map_transactions(|tx| tx.inner.into_inner())
                            })
                    }
                })
                .buffered(BATCH_SIZE);

            let mut block_chunks = block_stream.try_chunks(BATCH_SIZE);

            while let Some(blocks_result) = block_chunks.next().await {
                let blocks_to_validate =
                    blocks_result.map_err(|_| ChainOrchestratorError::InvalidBlock)?;

                if let Err(e) = self.validate_l1_messages(&blocks_to_validate).await {
                    tracing::error!(
                        target: "scroll::chain_orchestrator",
                        error = ?e,
                        "Validation failed — purging all L1→L2 message mappings"
                    );
                    self.database.purge_l1_message_to_l2_block_mappings(None).await?;
                    return Err(e);
                }
                self.database
                    .update_l1_messages_from_l2_blocks(
                        blocks_to_validate.iter().map(|b| b.into()).collect(),
                    )
                    .await?;
            }
        };

        // send a notification to the network that the chain is synced such that it accepts
        // transactions into the transaction pool.
        self.network.handle().inner().update_sync_state(RethSyncState::Idle);

        // Fetch all unprocessed committed batches and push them to the derivation pipeline as
        // consolidated.
        let committed_batches =
            self.database.fetch_and_update_unprocessed_committed_batches().await?;
        for batch_commit in committed_batches {
            self.derivation_pipeline.push_batch(batch_commit, BatchStatus::Consolidated).await;
        }

        self.notify(ChainOrchestratorEvent::ChainConsolidated {
            from: safe_block_number,
            to: head_block_number,
        });

        Ok(())
    }

    /// Validates the L1 messages in the provided blocks against the expected L1 messages synced
    /// from L1.
    async fn validate_l1_messages(
        &self,
        blocks: &[DogeosBlock],
    ) -> Result<(), ChainOrchestratorError> {
        let l1_message_hashes = blocks
            .iter()
            .flat_map(|block| {
                // Get the L1 messages from the block body.
                block
                    .body
                    .transactions()
                    .filter(|&tx| tx.is_l1_message())
                    // The hash for L1 messages is the trie hash of the transaction.
                    .map(|tx| tx.trie_hash())
                    .collect::<Vec<B256>>()
            })
            .collect::<Vec<B256>>();

        // No L1 messages in the blocks, nothing to validate.
        if l1_message_hashes.is_empty() {
            return Ok(());
        }

        let first_block_number =
            blocks.first().expect("at least one block exists because we have l1 messages").number;
        let count = l1_message_hashes.len();
        let mut database_messages = self
            .database
            .get_n_l1_messages(Some(L1MessageKey::block_number(first_block_number)), count)
            .await?
            .into_iter();

        for message_hash in l1_message_hashes {
            // Get the expected L1 message from the database.
            let expected_hash = database_messages
                .next()
                .map(|m| m.transaction.tx_hash())
                .ok_or(ChainOrchestratorError::L1MessageNotFound(L1MessageKey::TransactionHash(
                    message_hash,
                )))
                .inspect_err(|_| {
                    self.notify(ChainOrchestratorEvent::L1MessageNotFoundInDatabase(
                        L1MessageKey::TransactionHash(message_hash),
                    ));
                })?;

            // If the received and expected L1 messages do not match return an error.
            if message_hash != expected_hash {
                self.notify(ChainOrchestratorEvent::L1MessageMismatch {
                    expected: expected_hash,
                    actual: message_hash,
                });
                return Err(ChainOrchestratorError::L1MessageMismatch {
                    expected: expected_hash,
                    actual: message_hash,
                });
            }
        }

        Ok(())
    }
}

/// Computes the queue hash by taking the previous queue hash and performing a 2-to-1 hash with the
/// current transaction hash using keccak. It then applies a mask to the last 32 bits as these bits
/// are used to store the timestamp at which the message was enqueued in the contract. For the first
/// message in the queue, the previous queue hash is zero. If the L1 message queue index is before
/// migration to `L1MessageQueueV2`, the queue hash will be None.
///
/// The solidity contract (`L1MessageQueueV2.sol`) implementation is defined here: <https://github.com/scroll-tech/scroll-contracts/blob/67c1bde19c1d3462abf8c175916a2bb3c89530e4/src/L1/rollup/L1MessageQueueV2.sol#L379-L403>
async fn compute_l1_message_queue_hash(
    database: &Arc<Database>,
    l1_message: &TxL1Message,
    l1_v2_message_queue_start_index: u64,
) -> Result<Option<alloy_primitives::FixedBytes<32>>, ChainOrchestratorError> {
    let queue_hash = if l1_message.queue_index == l1_v2_message_queue_start_index {
        let mut input = B256::default().to_vec();
        input.append(&mut l1_message.tx_hash().to_vec());
        Some(keccak256(input) & L1_MESSAGE_QUEUE_HASH_MASK)
    } else if l1_message.queue_index > l1_v2_message_queue_start_index {
        let index = l1_message.queue_index - 1;
        let mut input = database
            .get_n_l1_messages(Some(L1MessageKey::from_queue_index(index)), 1)
            .await?
            .first()
            .map(|m| m.queue_hash)
            .ok_or(DatabaseError::L1MessageNotFound(L1MessageKey::QueueIndex(index)))?
            .unwrap_or_default()
            .to_vec();

        input.append(&mut l1_message.tx_hash().to_vec());
        Some(keccak256(input) & L1_MESSAGE_QUEUE_HASH_MASK)
    } else {
        None
    };
    Ok(queue_hash)
}

#[cfg(test)]
mod l1_retry_tests {
    use super::{retains_failed_l1_notification, should_process_l1_notification};
    use rollup_node_primitives::BlockInfo;
    use rollup_node_watcher::L1Notification;

    #[test]
    fn only_head_transitions_are_retained_on_failure_in_dynamic_mode() {
        // In dynamic mode, head transitions gate the phase-two barrier close, so a failed one must
        // be retried (retained) to block phase two until the structural transition succeeds.
        assert!(retains_failed_l1_notification(true, &L1Notification::Reorg(7)));
        assert!(retains_failed_l1_notification(
            true,
            &L1Notification::NewBlock(BlockInfo::default())
        ));

        // Everything else is logged and skipped on failure so an unrelated transient error does not
        // stall the L1 stream (and cannot indefinitely block a following barrier close).
        assert!(!retains_failed_l1_notification(true, &L1Notification::Synced));
        assert!(!retains_failed_l1_notification(true, &L1Notification::Finalized(1)));
        assert!(!retains_failed_l1_notification(true, &L1Notification::Processed(1)));
        assert!(!retains_failed_l1_notification(
            true,
            &L1Notification::AuthorizedSigner {
                head: BlockInfo::default(),
                signer: Default::default(),
            }
        ));
    }

    #[test]
    fn static_mode_never_retains_a_failed_notification() {
        // In static/no-op mode there is no phase-two barrier close to gate, so retention is inert
        // for every notification — including head transitions. This keeps the default L1 liveness
        // (a failed handler is logged and skipped, never blocking the stream) unchanged.
        assert!(!retains_failed_l1_notification(false, &L1Notification::Reorg(7)));
        assert!(!retains_failed_l1_notification(
            false,
            &L1Notification::NewBlock(BlockInfo::default())
        ));
        assert!(!retains_failed_l1_notification(false, &L1Notification::Synced));
    }

    #[test]
    fn authorization_phase_two_remains_reachable_during_l2_sync() {
        // The ordinary L1 stream is normally gated on L2 being synced.
        assert!(!should_process_l1_notification(false, false, true, false));
        assert!(should_process_l1_notification(true, false, true, false));

        // An open authorization barrier relaxes only that L2 gate, allowing phase two to close the
        // barrier and unblock network import. Structural ordering gates remain mandatory.
        assert!(should_process_l1_notification(false, true, true, false));
        assert!(!should_process_l1_notification(false, true, false, false));
        assert!(!should_process_l1_notification(false, true, true, true));
    }
}

#[cfg(test)]
mod requested_build_tests {
    use super::{build_block_channel, complete_requested_build_from_payload_result};
    use crate::{BuildBlockOutcome, ChainOrchestratorError};

    #[tokio::test]
    async fn post_admission_payload_error_reaches_correlated_waiter() {
        let (completion, ticket) = build_block_channel();
        let mut completion = Some(completion);
        let result = Err(ChainOrchestratorError::InvalidBlock);

        complete_requested_build_from_payload_result(&mut completion, &result);

        assert!(completion.is_none());
        assert_eq!(
            ticket.wait().await.unwrap(),
            BuildBlockOutcome::Failed("Received an invalid block from peer".to_string())
        );
    }
}

#[cfg(test)]
mod reset_commit_tests {
    use super::{
        commit_reset_generation, reset_decision, signer_result_is_current, ResetDecision,
        StagedReset,
    };
    use crate::{Consensus, SystemContractConsensus};
    use alloy_primitives::Address;
    use rollup_node_primitives::BlockInfo;
    use rollup_node_watcher::L1Notification;
    use scroll_db::UnwindResult;
    use std::sync::Arc;

    fn some_retry() -> Option<Arc<L1Notification>> {
        Some(Arc::new(L1Notification::Reorg(7)))
    }

    fn unwind_result(l1_block_number: u64) -> UnwindResult {
        UnwindResult {
            l1_block_number,
            queue_index: Some(3),
            l2_head_block_number: Some(11),
            l2_safe_block_info: Some(BlockInfo::default()),
        }
    }

    fn some_staged() -> Option<UnwindResult> {
        Some(unwind_result(7))
    }

    #[test]
    fn reset_decision_resumes_same_target_and_rejects_different_targets() {
        // No committed unwind staged: a reset for any target unwinds fresh.
        assert_eq!(reset_decision(&None, 100), ResetDecision::Unwind);

        // A committed unwind staged for target 100: a retry of 100 resumes the tail (no re-unwind).
        let staged = Some(StagedReset { block_number: 100, unwind_result: unwind_result(100) });
        assert_eq!(reset_decision(&staged, 100), ResetDecision::Resume);

        // A *higher* target is rejected: a fresh unwind to 101 could not restore the block-101 data
        // the deeper unwind already deleted, so it would roll the cursor forward over missing data.
        assert_eq!(reset_decision(&staged, 101), ResetDecision::Reject { staged: 100 });

        // A *lower* target is rejected too: a second unwind to 99 could return an empty delta and
        // overwrite the still-unapplied forkchoice target staged for 100.
        assert_eq!(reset_decision(&staged, 99), ResetDecision::Reject { staged: 100 });
    }

    #[test]
    fn stale_generation_signer_results_are_discarded() {
        // A signer result tagged with the current generation is applied.
        assert!(signer_result_is_current(4, 4));

        // A result tagged with an older generation — a block whose signing was requested before a
        // committed reset bumped the generation — is stale and discarded, even though the signer
        // address may still be authorized.
        assert!(!signer_result_is_current(3, 4));

        // Defensive: a tag ahead of the current generation is also treated as not-current.
        assert!(!signer_result_is_current(5, 4));
    }

    #[test]
    fn committed_reset_clears_generation_and_suspends_only_in_dynamic_mode() {
        let signer = Address::new([0x11; 20]);

        // Dynamic mode: a committed reset drops the old watcher generation's retained recovery work
        // AND fails authorization closed (sentinel barrier) for the fresh watcher to later close.
        let mut consensus = SystemContractConsensus::new(signer);
        let mut retry = some_retry();
        let mut staged = some_staged();
        commit_reset_generation(true, &mut retry, &mut staged, &mut consensus);
        assert!(retry.is_none());
        assert!(staged.is_none());
        assert!(consensus.authorization_pending());

        // Static/no-op mode: the retained generation is still dropped, but authorization is left
        // untouched — a static watcher never re-establishes a barrier, so suspending would stall.
        let mut consensus = SystemContractConsensus::new(signer);
        let mut retry = some_retry();
        let mut staged = some_staged();
        commit_reset_generation(false, &mut retry, &mut staged, &mut consensus);
        assert!(retry.is_none());
        assert!(staged.is_none());
        assert!(!consensus.authorization_pending());
    }
}
