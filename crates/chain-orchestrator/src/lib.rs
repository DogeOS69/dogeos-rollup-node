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
    BatchCommitData, BatchInfo, BatchStatus, BlockInfo, ChainImport, L1MessageEnvelope,
    L2BlockInfoWithL1Messages,
};
use rollup_node_providers::L1MessageProvider;
use rollup_node_sequencer::{Sequencer, SequencerEvent};
use rollup_node_signer::{SignatureAsBytes, SignerEvent, SignerHandle};
use rollup_node_watcher::{L1Notification, L1WatcherHandle};
use scroll_db::{
    Database, DatabaseError, DatabaseReadOperations, DatabaseWriteOperations, L1MessageKey,
    UnwindResult,
};
use scroll_derivation_pipeline::DerivationPipeline;
use scroll_engine::{Engine, EngineError, ScrollEngineApi};
use scroll_network::{
    BlockImportOutcome, DogeosNetworkPrimitives, NewBlockWithPeer, ScrollNetwork,
    ScrollNetworkManagerEvent,
};
use std::{collections::VecDeque, sync::Arc, time::Instant, vec};
use tokio::sync::mpsc::{self, UnboundedReceiver};

mod config;
pub use config::ChainOrchestratorConfig;

mod consensus;
pub use consensus::{Consensus, NoopConsensus, SystemContractConsensus};

mod consolidation;

mod derivation;
use derivation::{AttemptStep, DerivationDriver, FatalAttempt, HeldReorgOutcome};

mod event;
pub use event::ChainOrchestratorEvent;

mod error;
pub use error::ChainOrchestratorError;

mod handle;
pub use handle::{ChainOrchestratorCommand, ChainOrchestratorHandle, DatabaseQuery};

mod metrics;
use metrics::{MetricsHandler, Task};

mod sync;
pub use sync::{SyncMode, SyncState};

mod status;
pub use status::{ChainOrchestratorStatus, DerivationStatus, HeldBatchStatus};

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

/// The batch size for batch validation.
#[cfg(not(any(test, feature = "test-utils")))]
const BATCH_SIZE: usize = 100;
#[cfg(any(test, feature = "test-utils"))]
const BATCH_SIZE: usize = 1;

const fn l1_notification_receiver_may_poll(
    l2_synced: bool,
    derivation_pipeline_empty: bool,
    derivation_driver_can_accept_batch: bool,
) -> bool {
    l2_synced && derivation_pipeline_empty && derivation_driver_can_accept_batch
}

/// Upper bound on how many blocks one reverted-transaction collection walks.
///
/// The collection is one serial full-block RPC per block on the single-task run loop, so an
/// unbounded range stalls L1 notifications, gossip, derivation and sequencing for its duration —
/// and the remote source's rewind can request up to `MAX_ANCESTOR_LOOKBACK` blocks. Losing the
/// pool refill below the cap costs only resubmission.
const MAX_REVERTED_TX_COLLECTION_BLOCKS: u64 = 1024;

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
    /// The head that latched L2 sync mode at the peer-import site, re-checked on
    /// every later peer announcement.
    ///
    /// That latch has only one other exit — a LATER import returning VALID — and
    /// on a quiescent chain no later import arrives: peers re-announce blocks
    /// this node already knows, and the remote source short-circuits once its
    /// head is not behind, so nothing re-issues the forkchoice update. The node
    /// then stays internally syncing forever with L1 notifications and
    /// sequencing gated, while the execution node has long since caught up.
    l2_sync_recheck_target: Option<BlockInfo>,
    /// A handle for the [`rollup_node_watcher::L1Watcher`].
    l1_watcher: L1WatcherHandle,
    /// The network manager that manages the scroll p2p network.
    network: ScrollNetwork<N>,
    /// The consensus algorithm used by the rollup node.
    consensus: Box<dyn Consensus + 'static>,
    /// The engine used to communicate with the execution layer.
    engine: Engine<EC>,
    /// The sequencer used to build blocks.
    sequencer: Option<Sequencer<L1MP, ChainSpec>>,
    /// The signer used to sign messages.
    signer: Option<SignerHandle>,
    /// The derivation pipeline used to derive L2 blocks from batches.
    derivation_pipeline: DerivationPipeline,
    /// Owns the single derived result being reconciled or held for Engine sync.
    derivation_driver: DerivationDriver,
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
        l1_watcher: L1WatcherHandle,
        network: ScrollNetwork<N>,
        consensus: Box<dyn Consensus + 'static>,
        engine: Engine<EC>,
        sequencer: Option<Sequencer<L1MP, ChainSpec>>,
        signer: Option<SignerHandle>,
        derivation_pipeline: DerivationPipeline,
    ) -> Result<(Self, ChainOrchestratorHandle<N>), ChainOrchestratorError> {
        let (handle_tx, handle_rx) = mpsc::unbounded_channel();
        let handle = ChainOrchestratorHandle::new(handle_tx);
        Ok((
            Self {
                block_client,
                l2_client: Arc::new(l2_provider),
                database,
                config,
                sync_state: SyncState::default(),
                l2_sync_recheck_target: None,
                l1_watcher,
                network,
                consensus,
                engine,
                sequencer,
                signer,
                derivation_pipeline,
                derivation_driver: DerivationDriver::default(),
                handle_rx,
                event_sender: None,
                metric_handler: MetricsHandler::default(),
            },
            handle,
        ))
    }

    /// Drives the [`ChainOrchestrator`] until shutdown or any held-derivation fail-stop boundary.
    /// Fail-stops (returns `Err`, panicking the critical task) on any
    /// [`ChainOrchestratorError::FatalStateDivergence`] from any select arm,
    /// on any error from an administrative L1 unwind command, and on a fatal
    /// held-derivation boundary.
    pub async fn run_until_shutdown(
        mut self,
        mut shutdown: impl std::future::Future<Output = ()> + Unpin,
    ) -> Result<(), ChainOrchestratorError> {
        loop {
            tokio::select! {
                biased;

                _guard = &mut shutdown => {
                    self.notify(ChainOrchestratorEvent::Shutdown);
                    return Ok(())
                }
                () = self.derivation_driver.wait_for_attempt(), if self.derivation_driver.is_attempt_scheduled() => {
                    let metric = self.metric_handler
                        .get(Task::BatchReconciliation)
                        .expect("metric exists")
                        .clone();
                    let started = Instant::now();
                    let head_before_attempt = *self.engine.fcs().head_block_info();
                    let step = {
                        let mut attempt = Box::pin(self.derivation_driver.run_attempt(
                            &*self.l2_client,
                            &mut self.engine,
                            &self.database,
                        ));
                        tokio::select! {
                            biased;
                            _guard = &mut shutdown => None,
                            step = &mut attempt => Some(step),
                        }
                    };
                    metric.task_duration.record(started.elapsed().as_secs_f64());

                    let Some(step) = step else {
                        self.notify(ChainOrchestratorEvent::Shutdown);
                        return Ok(())
                    };

                    // Any attempt variant may have moved the L2 head before
                    // stopping — a held attempt commits every FCU it applied
                    // before the hold. An in-flight payload building job
                    // (parked while the sequencer arm is gated on derivation
                    // work) would finalize against the pre-attempt head and
                    // reorg the derived chain back out, so key the
                    // cancellation off the observed head, not a per-variant
                    // flag.
                    if *self.engine.fcs().head_block_info() != head_before_attempt {
                        self.cancel_payload_building_job("batch reconciliation moved the L2 head");
                    }

                    match step {
                        AttemptStep::Completed(consolidated) => {
                            for outcome in consolidated.block_outcomes {
                                self.notify(ChainOrchestratorEvent::BlockConsolidated(outcome));
                            }
                            self.notify(ChainOrchestratorEvent::BatchConsolidated(
                                consolidated.batch_outcome,
                            ));
                        }
                        AttemptStep::Held => {}
                        AttemptStep::Fatal(fatal) => {
                            self.log_fatal_attempt(&fatal);
                            return Err(*fatal.error)
                        }
                    }
                }
                Some(command) = self.handle_rx.recv() => {
                    let is_admin_unwind = matches!(
                        &command,
                        ChainOrchestratorCommand::RevertToL1Block(_)
                    );
                    let held_unwind_context = is_admin_unwind
                        .then(|| self.derivation_driver.fatal_context())
                        .flatten();
                    if let Err(err) = self.handle_command(command).await {
                        if matches!(err, ChainOrchestratorError::FatalStateDivergence(_)) {
                            tracing::error!(
                                target: "scroll::chain_orchestrator",
                                ?err,
                                "Fatal state divergence; shutting down so restart re-converges"
                            );
                            return Err(err)
                        }
                        if let Some(context) = held_unwind_context {
                            self.log_fatal_held_operation(context, "administrative L1 unwind", &err);
                            return Err(err)
                        }
                        if is_admin_unwind {
                            // A failed administrative unwind leaves the L1
                            // index indeterminate (possibly half-unwound) AND
                            // the sync gate latched closed: set_syncing() ran,
                            // and only the watcher reset the failure skipped
                            // can re-emit Synced. Logging and running on would
                            // look healthy while never sequencing again —
                            // fail-stop instead.
                            tracing::error!(
                                target: "scroll::chain_orchestrator",
                                ?err,
                                "Administrative L1 unwind failed; shutting down rather than \
                                 running on with a latched-closed sync gate"
                            );
                            return Err(err)
                        }
                        tracing::error!(target: "scroll::chain_orchestrator", ?err, "Error handling command");
                    }
                }
                Some(event) = async {
                    if let Some(event) = self.signer.as_mut() {
                        event.next().await
                    } else {
                        unreachable!()
                    }
                }, if self.signer.is_some() => {
                    let res = self.handle_signer_event(event).await;
                    self.handle_outcome(res)?;
                }
                Some(event) = async {
                    if let Some(seq) = self.sequencer.as_mut() {
                        seq.next().await
                    } else {
                        unreachable!()
                    }
                }, if self.sequencer.is_some() && self.sync_state.is_synced() && !self.has_pending_derivation_work() => {
                    let res = self.handle_sequencer_event(event).await;
                    self.handle_outcome(res)?;
                }
                Some(batch) = self.derivation_pipeline.next(), if self.derivation_driver.can_accept_batch() => {
                    self.derivation_driver.hold_batch(batch);
                }
                Some(event) = self.network.events().next() => {
                    let res = self.handle_network_event(event).await;
                    self.handle_outcome(res)?;
                }
                Some(notification) = self.l1_watcher.l1_notification_receiver().recv(), if l1_notification_receiver_may_poll(
                    self.sync_state.l2().is_synced(),
                    self.derivation_pipeline.is_empty(),
                    self.derivation_driver.can_accept_batch(),
                ) => {
                    let result = self.handle_l1_notification(notification).await;
                    self.handle_outcome(result)?;
                }

            }
        }
    }

    /// Returns whether derivation is queued or one result occupies the held slot.
    const fn has_pending_derivation_work(&self) -> bool {
        !self.derivation_driver.can_accept_batch() || !self.derivation_pipeline.is_empty()
    }

    /// Writes a structured fail-stop record for an operation that made held validity uncertain.
    fn log_fatal_held_operation(
        &self,
        (batch_info, attempt, held_ms): (BatchInfo, u64, u64),
        operation: &'static str,
        error: &ChainOrchestratorError,
    ) {
        self.derivation_driver.record_fatal();
        tracing::error!(
            target: "scroll::chain_orchestrator",
            batch_index = batch_info.index,
            batch_hash = ?batch_info.hash,
            attempt,
            held_ms,
            operation,
            source = %error,
            "Held-batch state is uncertain after L1 mutation; fail-stopping"
        );
    }

    /// Writes the single structured fatal record immediately before the run loop returns `Err`.
    fn log_fatal_attempt(&self, fatal: &FatalAttempt) {
        let (batch_info, attempt, held_ms) =
            self.derivation_driver.fatal_context().expect("a fatal attempt retains its held batch");
        tracing::error!(
            target: "scroll::chain_orchestrator",
            batch_index = batch_info.index,
            batch_hash = ?batch_info.hash,
            attempt,
            held_ms,
            method = fatal.method,
            outcome = fatal.outcome,
            source = %fatal.error,
            "Derived batch reconciliation failed; fail-stopping"
        );
    }

    /// Handles the outcome of an operation, logging errors and notifying event listeners as
    /// appropriate. Only [`ChainOrchestratorError::FatalStateDivergence`] is propagated —
    /// every select arm must `?` this so a fatal divergence raised from any handler
    /// (not just the command arm) actually stops the run loop. (The `ImportBlock`
    /// command arm, which stringifies its reply, extracts the fatal variant before
    /// stringifying for the same reason.)
    // The enum's size is pre-existing (the async handlers returning it are
    // exempt from the lint); boxing it for this one sync pass-through would
    // change every construction site for no runtime benefit.
    #[allow(clippy::result_large_err)]
    fn handle_outcome(
        &self,
        outcome: Result<Option<ChainOrchestratorEvent>, ChainOrchestratorError>,
    ) -> Result<(), ChainOrchestratorError> {
        match outcome {
            Ok(Some(event)) => self.notify(event),
            Err(err) => {
                if matches!(err, ChainOrchestratorError::FatalStateDivergence(_)) {
                    tracing::error!(target: "scroll::chain_orchestrator", ?err, "Fatal state divergence; shutting down so restart re-converges");
                    return Err(err);
                }
                tracing::error!(target: "scroll::chain_orchestrator", ?err, "Encountered error in the chain orchestrator");
            }
            Ok(None) => {}
        }
        Ok(())
    }

    /// Handles an event from the signer.
    async fn handle_signer_event(
        &self,
        event: SignerEvent,
    ) -> Result<Option<ChainOrchestratorEvent>, ChainOrchestratorError> {
        tracing::info!(target: "scroll::chain_orchestrator", ?event, "Handling signer event");
        match event {
            SignerEvent::SignedBlock { block, signature } => {
                let hash = block.hash_slow();
                // Signing is fully asynchronous, so a result can arrive a
                // slot (or more) late while its block is still canonical —
                // that must NOT drop the signature, freeze the persisted
                // head anchor, or stop gossip. Only a block that is no
                // longer on the canonical chain (reorged out) is demoted to
                // a signature-only write (hash-keyed, harmless).
                let head = *self.engine.fcs().head_block_info();
                let canonical = if hash == head.hash {
                    true
                } else if block.header.number < head.number {
                    match self.l2_client.get_block_by_number(block.header.number.into()).await {
                        // Only a returned block with a DIFFERENT hash proves
                        // the signed block was reorged out.
                        Ok(Some(canonical_block)) => canonical_block.header.hash_slow() == hash,
                        // Unknown (transient RPC error or a gap): the signer
                        // result is one-shot, so demoting it here would lose
                        // a possibly-canonical block's anchor and
                        // announcement on a blip. Proceed on the full path —
                        // the anchor write below is made monotone explicitly,
                        // and a stale announcement is harmless gossip.
                        _ => true,
                    }
                } else {
                    // number >= head.number with a differing hash: either the
                    // head was rewound below the signed block, or this is a
                    // same-height sibling the chain moved past. Not canonical
                    // either way.
                    false
                };
                if !canonical {
                    tracing::warn!(
                        target: "scroll::chain_orchestrator",
                        block_number = block.header.number,
                        "Signed block is no longer canonical; keeping its signature, skipping \
                         the head anchor and the announcement"
                    );
                    if let Err(err) = self.database.insert_signature(hash, signature).await {
                        tracing::warn!(
                            target: "scroll::chain_orchestrator",
                            block_number = block.header.number,
                            %err,
                            "Failed to persist the signature of a non-canonical signed block"
                        );
                    }
                    return Ok(None);
                }
                if let Err(err) = self
                    .database
                    .tx_mut(move |tx| async move {
                        // Monotone: a late-but-canonical signer result must
                        // not move the anchor BACKWARDS below a head a
                        // concurrent import already persisted — that value
                        // is restart authority, and a lower write would
                        // un-mark messages consumed by blocks peers already
                        // have.
                        let current = tx.get_l2_head_block_number().await?;
                        if block.header.number > current {
                            tx.set_l2_head_block_number(block.header.number).await?;
                        }
                        tx.insert_signature(hash, signature).await
                    })
                    .await
                {
                    // The signed block is canonical but its head number and
                    // signature could not be persisted — a restart would
                    // rewind past it and peers would never see it (announce
                    // below never runs). Unlike UpdateFcsHead there is no
                    // compensation available here, so it is fatal directly.
                    tracing::error!(
                        target: "scroll::chain_orchestrator",
                        block_number = block.header.number,
                        %err,
                        "Signed-block persistence failed after the engine head advanced"
                    );
                    return Err(ChainOrchestratorError::FatalStateDivergence(
                        "signed block could not be persisted after the engine head advanced; \
                         restart re-converges from the persisted state",
                    ));
                }
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
                    if let Err(err) = self
                        .sequencer
                        .as_mut()
                        .expect("sequencer must be present")
                        .start_payload_building(&mut self.engine)
                        .await
                    {
                        // Close the recording so the failed attempt does not
                        // pollute the next slot's duration sample, and notify
                        // like the BuildBlock path — the identity gates make a
                        // spurious cancellation event harmless. (Metric and
                        // event stream are NOT 1:1 overall: the
                        // no-sequencer BuildBlock arm emits without a
                        // recording ever starting — see metrics.rs.)
                        self.metric_handler.discard_block_building_recording();
                        self.notify(ChainOrchestratorEvent::PayloadBuildingJobCancelled);
                        return Err(err.into());
                    }
                }
            }
            SequencerEvent::PayloadReady(payload_id) => {
                // The job slot is already cleared by the time PayloadReady is
                // yielded, so no later cancel_payload_building_job can notify
                // for a finalization failure — do it here, or waiters would
                // burn their full timeout with the cause in another log
                // target.
                let block = match self
                    .sequencer
                    .as_mut()
                    .expect("sequencer must be present")
                    .finalize_payload_building(payload_id, &mut self.engine)
                    .await
                {
                    Ok(block) => block,
                    Err(err) => {
                        self.metric_handler.discard_block_building_recording();
                        self.notify(ChainOrchestratorEvent::PayloadBuildingJobCancelled);
                        // Every finalization failure is pre-commit (the
                        // sequencer converts and validates the payload BEFORE
                        // its FCU): the head never moved, so this is
                        // recoverable and the next slot rebuilds.
                        return Err(err.into());
                    }
                };

                self.metric_handler.finish_block_building_recording(block.as_ref());

                if let Some(block) = block {
                    let block_info: L2BlockInfoWithL1Messages = (&block).into();
                    // The block is built and head-committed by this point; a
                    // failure below must still emit a terminal outcome event
                    // or waiters would burn their full budget in silence.
                    if let Err(err) = self
                        .database
                        .update_l1_messages_from_l2_blocks(vec![block_info.clone()])
                        .await
                    {
                        self.notify(ChainOrchestratorEvent::PayloadBuildingJobCancelled);
                        // The head is committed but the consumed L1 messages
                        // are not marked: they would be re-selected for the
                        // next block (selection filters on a null L2 block
                        // number) — duplicate L1 messages across consecutive
                        // blocks. Unlike UpdateFcsHead there is no
                        // compensation available here, so it is fatal directly.
                        tracing::error!(
                            target: "scroll::chain_orchestrator",
                            block_number = block_info.block_info.number,
                            %err,
                            "L1-message consumption could not be persisted after the engine \
                             head advanced"
                        );
                        return Err(ChainOrchestratorError::FatalStateDivergence(
                            "built block committed to the engine but its L1-message \
                             consumption could not be persisted; restart re-converges from \
                             the persisted state",
                        ));
                    }
                    if let Err(err) = self
                        .signer
                        .as_mut()
                        .expect("signer must be present")
                        .sign_block(block.clone())
                    {
                        self.notify(ChainOrchestratorEvent::PayloadBuildingJobCancelled);
                        // The block is at the engine head but will never be
                        // signed, persisted, or announced; sequencing on top
                        // of it silently forks this node from its peers.
                        tracing::error!(
                            target: "scroll::chain_orchestrator",
                            block_number = block_info.block_info.number,
                            %err,
                            "Signing failed after the engine head advanced"
                        );
                        return Err(ChainOrchestratorError::FatalStateDivergence(
                            "built block committed to the engine but could not be handed to \
                             the signer; restart re-converges from the persisted state",
                        ));
                    }
                    return Ok(Some(ChainOrchestratorEvent::BlockSequenced(block)));
                }
                return Ok(Some(ChainOrchestratorEvent::BlockBuildingSkipped {
                    head_block_number: self.engine.fcs().head_block_info().number,
                }));
            }
        }

        Ok(None)
    }

    /// Handles a command sent to the chain orchestrator.
    async fn handle_command(
        &mut self,
        command: ChainOrchestratorCommand<N>,
    ) -> Result<(), ChainOrchestratorError> {
        tracing::debug!(target: "scroll::chain_orchestrator", ?command, "Handling command");
        match command {
            ChainOrchestratorCommand::BuildBlock => {
                // Note: a job started while the sequencer select arm is gated
                // (unsynced, or pending derivation work) is parked until the
                // gate reopens — deliberately NOT rejected. Gates reopen (sync
                // completes, derivation drains), after which the job is polled
                // normally, and every job-invalidating transition cancels it
                // observably (the complete emission-site list lives on
                // `ChainOrchestratorEvent::PayloadBuildingJobCancelled`). A held derivation attempt
                // keeps the gate closed without cancelling; the parked job then
                // simply resumes when the hold resolves (its head snapshot check
                // cancels it if the head moved). Rejecting here instead was
                // tried and races startup: a build command can legitimately
                // arrive before the L1-synced notification is processed.
                if let Some(sequencer) = self.sequencer.as_mut() {
                    // Coalesce with an in-flight job instead of silently
                    // replacing it: a replaced job discards engine work and
                    // makes block numbering timing-dependent when the build
                    // timer and manual triggers race (issue #38). The
                    // in-flight job normally emits BlockSequenced or
                    // BlockBuildingSkipped; if it is cancelled instead,
                    // PayloadBuildingJobCancelled is emitted (complete site
                    // list on that event's doc) so waiters can fail fast — they should still
                    // bound their wait (the remote block source does).
                    if sequencer.payload_building_job().is_some() {
                        tracing::debug!(
                            target: "scroll::chain_orchestrator",
                            synced = self.sync_state.is_synced(),
                            pending_derivation = self.has_pending_derivation_work(),
                            "BuildBlock requested while a payload building job is in flight; coalescing with the in-flight job"
                        );
                        self.notify(ChainOrchestratorEvent::BuildBlockCoalesced);
                    } else {
                        self.metric_handler.start_block_building_recording();
                        if let Err(err) = sequencer.start_payload_building(&mut self.engine).await {
                            // A failed start leaves no job and would otherwise
                            // emit nothing, so waiters would burn their full
                            // timeout with the real cause in a different log
                            // target. Notify before propagating.
                            self.metric_handler.discard_block_building_recording();
                            self.notify(ChainOrchestratorEvent::PayloadBuildingJobCancelled);
                            return Err(err.into());
                        }
                    }
                } else {
                    tracing::error!(target: "scroll::chain_orchestrator", "Received BuildBlock command but sequencer is not configured");
                    self.notify(ChainOrchestratorEvent::PayloadBuildingJobCancelled);
                }
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
                    self.derivation_driver.status(self.derivation_pipeline.len()),
                );
                let _ = tx.send(status);
            }
            ChainOrchestratorCommand::NetworkHandle(tx) => {
                let _ = tx.send(self.network.handle().clone());
            }
            ChainOrchestratorCommand::UpdateFcsHead((head, expected_head, sender)) => {
                // Compare-and-swap precondition: a caller that decided this
                // move from an OBSERVED head must not have the decision
                // applied after the head moved — the gap between its status
                // read and this command is a TOCTOU window in which a
                // concurrent revert would otherwise be undone by a
                // now-forward head move.
                if let Some(expected) = expected_head {
                    let current = *self.engine.fcs().head_block_info();
                    if current != expected {
                        let _ = sender.send(Err(format!(
                            "head moved from the observed {expected:?} to {current:?}; \
                             re-derive and retry"
                        )));
                        return Ok(());
                    }
                }
                // Only ever a REWIND, matching the administrative unwind's
                // guard. A forward move purges L1-message mappings only ABOVE
                // the new target and then advances the persisted anchor over
                // blocks whose `update_l1_messages_from_l2_blocks` never ran,
                // leaving those messages unstamped below the anchor. No
                // in-tree caller asks for one — the remote source's rewind
                // guards direction, and this is not exposed on the admin RPC —
                // but the handle is public API.
                let mirror_head = *self.engine.fcs().head_block_info();
                // `>=` with an inequality on the full BlockInfo, not `>`: an
                // equal-height DIFFERENT-hash swap is not a rewind either, and
                // the purge below (`> head.number + 1`) is a no-op for it, so
                // the superseded block's L1 messages would stay stamped consumed
                // forever.
                if head.number >= mirror_head.number && head != mirror_head {
                    let _ = sender.send(Err(format!(
                        "requested head {head:?} is above the current head {mirror_head:?}; \
                         this command only rewinds"
                    )));
                    return Ok(());
                }

                // Collect transactions of reverted blocks from l2 client.
                // Best-effort: a failure must neither abort the update nor
                // drop the responder without a reply.
                let reverted_transactions = match self
                    .collect_reverted_txs_in_range(
                        head.number.saturating_add(1),
                        self.engine.fcs().head_block_info().number,
                    )
                    .await
                {
                    Ok(txs) => txs,
                    Err(err) => {
                        tracing::warn!(
                            target: "scroll::chain_orchestrator",
                            ?head,
                            %err,
                            "Failed to collect reverted transactions for the pool refill; \
                             continuing the head update without them"
                        );
                        Vec::new()
                    }
                };
                let previous_head = *self.engine.fcs().head_block_info();
                let forward = match self.engine.update_fcs_checked(Some(head), None, None).await {
                    Ok(forward) => forward,
                    Err(err) => {
                        let _ = sender.send(Err(format!("forkchoice update failed: {err}")));
                        return Err(err.into());
                    }
                };
                if !forward.is_valid() {
                    // update_fcs_checked commits its mirror only on VALID, so
                    // nothing has been mutated — refuse before purging
                    // mappings, persisting the unapplied head, or cancelling
                    // a valid job. SYNCING (the EL has not adopted the head)
                    // must not proceed either: the rollback below refuses it
                    // for the same reason.
                    // Keep the engine's own verdict diagnosable.
                    tracing::error!(
                        target: "scroll::chain_orchestrator",
                        ?head,
                        ?forward,
                        "Administrative FCS head update was not applied by the engine"
                    );
                    let _ = sender.send(Err(format!(
                        "administrative FCS head update was not applied by the engine: {:?}",
                        forward.payload_status.status
                    )));
                    return Err(ChainOrchestratorError::FcuRejected(
                        "administrative FCS head update was not applied by the engine \
                         (INVALID or SYNCING)",
                    ));
                }

                // The head was moved administratively: an in-flight payload
                // building job still targets the previous head and finalizing
                // it would silently undo the update.
                self.cancel_payload_building_job("administrative FCS head update");

                if let Err(err) = self
                    .database
                    .tx_mut(move |tx| async move {
                        tx.purge_l1_message_to_l2_block_mappings(Some(head.number + 1)).await?;
                        tx.set_l2_head_block_number(head.number).await
                    })
                    .await
                {
                    // The engine head moved but the persisted head/mappings
                    // did not. Left as-is, a restart re-derives the head from
                    // the persisted (old, higher) number and re-canonicalizes
                    // exactly the blocks this command was asked to revert —
                    // so compensate by putting the engine head back, making
                    // engine and persisted state agree on the OLD head.
                    tracing::error!(
                        target: "scroll::chain_orchestrator",
                        ?head,
                        %err,
                        "UpdateFcsHead persistence failed after the engine head moved; \
                         rolling the engine head back to keep state convergent"
                    );
                    // The rollback counts as committed only on VALID:
                    // update_fcs_checked commits its mirror only then, and a
                    // SYNCING response means the EL has not applied the
                    // forkchoice either — treating it as success would skip
                    // the fail-stop while state stays diverged.
                    let rollback_committed =
                        match self.engine.update_fcs_checked(Some(previous_head), None, None).await
                        {
                            Ok(result) if result.is_valid() => true,
                            rollback_outcome => {
                                tracing::error!(
                                    target: "scroll::chain_orchestrator",
                                    ?previous_head,
                                    ?rollback_outcome,
                                    "engine-head rollback after the persistence failure did \
                                     not commit"
                                );
                                false
                            }
                        };
                    if !rollback_committed {
                        // Engine on the new head, database on the old one, and
                        // the compensation failed: running on serves divergent
                        // state, while a restart re-derives the head from the
                        // persisted number and converges. Fail-stop.
                        return Err(ChainOrchestratorError::FatalStateDivergence(
                            "UpdateFcsHead persistence failed and the engine-head rollback \
                             did not commit; restart re-converges on the persisted head",
                        ));
                    }
                    let _ = sender.send(Err(format!("head persistence failed: {err}")));
                    return Err(err.into());
                }

                // Add all reverted transactions to the transaction pool. Done
                // only after persistence succeeds: on the failure path above
                // the head is rolled back, the reverted blocks stay canonical,
                // and their transactions must not re-enter the pool.
                self.reinsert_txs_into_pool(reverted_transactions).await;
                self.notify(ChainOrchestratorEvent::FcsHeadUpdated(head));
                let _ = sender.send(Ok(()));
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
                if self.sequencer.is_some() {
                    // Route the job cancellation through the observable path
                    // (event + metric closure) before disabling; disable()'s
                    // own cancel is then a no-op.
                    self.cancel_payload_building_job("automatic sequencing disabled");
                    self.sequencer.as_mut().expect("checked above").disable();
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
                // Pre-latch validation: refuse a target below the FINALIZED
                // L1 block. NOTE this is an L1-domain check only — it does
                // NOT guarantee the resulting L2 head stays above the
                // engine's finalized L2 block (independently sourced from
                // the EL at startup); that cross-domain case is caught after
                // the unwind and treated as irreconcilable. No UPPER bound is
                // checked: the latest-L1 marker is not advanced by batch
                // commits, so it would refuse legitimate reverts (and the
                // unwind itself moves the marker to the target). Errors
                // after the latch below deliberately fail-stop: the L1 index
                // may be half-unwound and the sync gate can only be reopened
                // by the watcher reset at the end.
                let finalized_l1 = match self
                    .database
                    .tx(|tx| async move { tx.get_finalized_l1_block_number().await })
                    .await
                {
                    Ok(finalized) => finalized,
                    Err(err) => {
                        // NOTE: this arm catches connection-level errors only;
                        // a missing metadata row panics inside the DB layer's
                        // own expect and never reaches here.
                        tracing::warn!(
                            target: "scroll::chain_orchestrator",
                            block_number,
                            %err,
                            "Refusing administrative L1 unwind: could not read the \
                             finalized L1 block"
                        );
                        let _ = tx.send(false);
                        return Ok(());
                    }
                };
                if block_number < finalized_l1 {
                    tracing::warn!(
                        target: "scroll::chain_orchestrator",
                        block_number,
                        finalized_l1,
                        "Refusing administrative L1 unwind below the finalized L1 block"
                    );
                    let _ = tx.send(false);
                    return Ok(());
                }
                self.sync_state.l1_mut().set_syncing();
                // The gate is now closed for the whole administrative re-scan:
                // a parked job could not be polled to completion, later
                // BuildBlocks would coalesce into it with no outcome ever
                // arriving, and (for a job with an L1 origin above the unwind
                // point) the unwind deletes L1 messages it depends on. Cancel
                // unconditionally before any fallible work.
                self.cancel_payload_building_job("administrative L1 unwind");
                self.derivation_driver.cancel_attempt();
                let (_unwind_result, held_outcome) =
                    self.unwind_and_revalidate_held_batch(block_number).await?;

                // The unwind may rewind the persisted L2 head and/or the safe
                // head. Issue ONE combined FCU preserving head >= safe >=
                // finalized: a head-only update can trip HeadBelowSafe when
                // the unwind also removed the batch that made the current
                // safe head safe — and the database unwind is already
                // committed, so an avoidable FCS-order error here would
                // fail-stop the node for nothing. Any genuine error still
                // fail-stops via the admin-unwind policy.
                //
                // The FCU targets are derived from ABSOLUTE persisted state
                // compared against the engine mirror — never from what this
                // particular unwind deleted. The unwind is idempotent in its
                // effects but not in its deltas: after a retryable refusal
                // below, the re-issued revert deletes nothing, and a
                // delta-derived FCU would be skipped entirely while the
                // engine still disagrees — the very retry the refusal asked
                // for would silently no-op and reply true.
                let finalized = *self.engine.fcs().finalized_block_info();
                let mirror_head = *self.engine.fcs().head_block_info();
                let mirror_safe = *self.engine.fcs().safe_block_info();
                // A failed read is RETRYABLE, not divergence — the same
                // rationale as the head lookup below: the unwind is durably
                // committed and nothing has been issued to the engine yet,
                // so momentary database contention must refuse, not
                // fail-stop.
                let (persisted_head_number, persisted_safe) = match (
                    self.database.get_l2_head_block_number().await,
                    self.database.get_latest_safe_l2_info().await,
                ) {
                    (Ok(head_number), Ok((safe, _))) => (head_number, safe),
                    outcome => {
                        tracing::warn!(
                            target: "scroll::chain_orchestrator",
                            ?outcome,
                            "Administrative unwind could not read the persisted head/safe; \
                             refusing (retryable)"
                        );
                        self.l1_watcher.revert_to_l1_block(block_number);
                        if matches!(held_outcome, HeldReorgOutcome::Survived { .. }) {
                            self.derivation_driver.schedule_fresh_reconciliation();
                        }
                        let _ = tx.send(false);
                        return Ok(());
                    }
                };
                // Symmetric with handle_l1_reorg: a persisted head below the
                // FINALIZED L2 block is irreconcilable once the unwind is
                // durable (the L1-domain pre-latch check cannot rule this
                // out — the engine's finalized L2 block is independently
                // sourced from the EL at startup).
                if persisted_head_number < finalized.number {
                    tracing::error!(
                        target: "scroll::chain_orchestrator",
                        persisted_head_number,
                        ?finalized,
                        "Administrative unwind left the persisted L2 head below the \
                         finalized block"
                    );
                    return Err(ChainOrchestratorError::FatalStateDivergence(
                        "administrative unwind rewound the L2 head below the finalized \
                         block; the persisted state is irreconcilable without manual \
                         intervention",
                    ));
                }
                // Only ever a REWIND. Gating on `==` would make an anchor ABOVE
                // the mirror head issue a FORWARD forkchoice move — the
                // opposite of what this command means and of what its log line
                // claims — so refuse that shape and leave the head alone; the
                // safe clamp below then floors on the mirror head.
                let new_head = if persisted_head_number >= mirror_head.number {
                    if persisted_head_number > mirror_head.number {
                        tracing::warn!(
                            target: "scroll::chain_orchestrator",
                            persisted_head_number,
                            ?mirror_head,
                            "Administrative unwind left the persisted L2 head ABOVE the \
                             engine's head mirror; leaving the engine head untouched rather \
                             than moving it forward"
                        );
                    }
                    None
                } else {
                    // A failed lookup is RETRYABLE, not divergence: reset the
                    // watcher (the database is consistently unwound to the
                    // target, so re-scanning from it is correct), reply false
                    // so the operator re-issues, and do not ride the
                    // admin-unwind fail-stop.
                    let head_block = match self
                        .l2_client
                        .get_block_by_number(persisted_head_number.into())
                        .await
                    {
                        Ok(Some(block)) => block,
                        outcome => {
                            tracing::warn!(
                                target: "scroll::chain_orchestrator",
                                persisted_head_number,
                                ?outcome,
                                "Administrative unwind could not resolve the new L2 head; \
                                 refusing (retryable)"
                            );
                            self.l1_watcher.revert_to_l1_block(block_number);
                            if matches!(held_outcome, HeldReorgOutcome::Survived { .. }) {
                                self.derivation_driver.schedule_fresh_reconciliation();
                            }
                            let _ = tx.send(false);
                            return Ok(());
                        }
                    };
                    Some(BlockInfo {
                        number: persisted_head_number,
                        hash: head_block.header.hash_slow(),
                    })
                };
                // New safe target: the persisted value floored at finalized,
                // dragged down to the effective head (a safe target above the
                // head waits for the head to catch up rather than tripping a
                // local FCS refusal), and issued only when the mirror
                // disagrees.
                let effective_head = new_head.unwrap_or(mirror_head);
                let mut safe_target = if persisted_safe.number >= finalized.number {
                    persisted_safe
                } else {
                    finalized
                };
                if safe_target.number > effective_head.number {
                    safe_target = effective_head;
                }
                // Full-BlockInfo comparison: a same-height safe marker with
                // a different hash is still divergence and must be reissued
                // (the head comparison above is number-only of necessity —
                // the persisted head is a bare number).
                let new_safe = (safe_target != mirror_safe).then_some(safe_target);
                if new_head.is_some() || new_safe.is_some() {
                    // Best-effort (see handle_l1_reorg): a collection failure
                    // must not abort the unwind — and on this admin path it
                    // would even fail-stop the node.
                    let reverted_transactions = if let Some(head) = new_head {
                        match self
                            .collect_reverted_txs_in_range(
                                head.number.saturating_add(1),
                                self.engine.fcs().head_block_info().number,
                            )
                            .await
                        {
                            Ok(txs) => txs,
                            Err(err) => {
                                tracing::warn!(
                                    target: "scroll::chain_orchestrator",
                                    %err,
                                    "Failed to collect reverted transactions for the pool \
                                     refill; continuing the unwind without them"
                                );
                                Vec::new()
                            }
                        }
                    } else {
                        Vec::new()
                    };
                    // Every other head-rewind site pairs the move with this
                    // purge (`UpdateFcsHead`, startup repair, and `unwind`
                    // itself when it removed rows). Since the head target is no
                    // longer derived from the unwind's delta, this path rewinds
                    // in cases `unwind` did not purge for — and a message left
                    // stamped with a rewound block number is skipped by
                    // `get_n_messages(NotIncluded(..))`, which filters on
                    // `l2_block_number IS NULL`. The next build then takes the
                    // FOLLOWING queue index, and the freed one lands a block
                    // later: an out-of-order L1 message queue that
                    // `validate_l1_messages` rejects on every peer.
                    if let Some(head) = new_head {
                        // Refuse, do not fail-stop: the two database reads above
                        // this arm deliberately reply `false` and return Ok on
                        // error, because momentary contention must not kill the
                        // node. Propagating here would make this one write the
                        // exception. Nothing durable has changed yet — the FCU
                        // has not been issued — so the operator re-issues.
                        if let Err(err) = self
                            .database
                            .tx_mut(move |tx| async move {
                                tx.purge_l1_message_to_l2_block_mappings(Some(head.number + 1))
                                    .await
                            })
                            .await
                        {
                            tracing::warn!(
                                target: "scroll::chain_orchestrator",
                                ?head,
                                %err,
                                "Could not purge L1 message mappings for the administrative \
                                 unwind; refusing (retryable)"
                            );
                            self.l1_watcher.revert_to_l1_block(block_number);
                            if matches!(held_outcome, HeldReorgOutcome::Survived { .. }) {
                                self.derivation_driver.schedule_fresh_reconciliation();
                            }
                            let _ = tx.send(false);
                            return Ok(());
                        }
                    }
                    let result = self.engine.update_fcs_checked(new_head, new_safe, None).await?;
                    if result.is_invalid() {
                        // INVALID after a durable unwind: genuine divergence,
                        // fail-stops via the admin-unwind policy.
                        tracing::error!(
                            target: "scroll::chain_orchestrator",
                            ?new_head,
                            ?new_safe,
                            ?result,
                            "Administrative unwind FCU rejected as INVALID by the engine"
                        );
                        return Err(ChainOrchestratorError::FcuRejected(
                            "administrative unwind forkchoice update rejected as INVALID \
                             by the engine",
                        ));
                    }
                    if !result.is_valid() {
                        // SYNCING commits nothing — RETRYABLE, not divergence:
                        // an operator RPC must not panic the node on a
                        // routine engine response. Reset the watcher so the
                        // sync gate is not left latched and reply false.
                        tracing::warn!(
                            target: "scroll::chain_orchestrator",
                            ?new_head,
                            ?new_safe,
                            "Administrative unwind FCU returned SYNCING; refusing (retryable)"
                        );
                        self.l1_watcher.revert_to_l1_block(block_number);
                        if matches!(held_outcome, HeldReorgOutcome::Survived { .. }) {
                            self.derivation_driver.schedule_fresh_reconciliation();
                        }
                        let _ = tx.send(false);
                        return Ok(());
                    }
                    self.reinsert_txs_into_pool(reverted_transactions).await;
                }

                // Revert the L1 watcher to the specified block.
                self.l1_watcher.revert_to_l1_block(block_number);

                if matches!(held_outcome, HeldReorgOutcome::Survived { .. }) {
                    self.derivation_driver.schedule_fresh_reconciliation();
                }

                self.notify(ChainOrchestratorEvent::UnwoundToL1Block(block_number));
                let _ = tx.send(true);
            }
            ChainOrchestratorCommand::ImportBlock { block_with_peer, response } => {
                let result =
                    self.import_chain(vec![block_with_peer.block.clone()], block_with_peer).await;
                match result {
                    // A fatal divergence must reach the run loop: stringifying
                    // it into the reply would downgrade head-committed-but-
                    // unpersisted state to a mere import rejection.
                    Err(ChainOrchestratorError::FatalStateDivergence(msg)) => {
                        let _ = response.send(Err(format!("fatal state divergence: {msg}")));
                        return Err(ChainOrchestratorError::FatalStateDivergence(msg));
                    }
                    other => {
                        let _ = response.send(other.map_err(|e| e.to_string()));
                    }
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

    /// Cancels the in-flight payload building job, if any: discards its
    /// metrics recording (a cancelled job's elapsed time is not a build
    /// latency and must not pollute the duration histograms), counts the
    /// cancellation, and notifies waiters so they can fail fast instead of
    /// burning their full wait timeout. Call wherever the job's inputs are
    /// invalidated — a head move; an L1 unwind (unconditionally for an
    /// administrative revert, and on an L1 reorg when the job MAY carry L1
    /// messages or the head moved); or sequencing being disabled. One caller
    /// is deliberately NOT an input invalidation: the re-enter-L2-sync path
    /// after an unadopted import head cancels a still-valid job purely so its
    /// waiters are not stranded behind the closed sync gate.
    fn cancel_payload_building_job(&mut self, reason: &'static str) {
        let Some(sequencer) = self.sequencer.as_mut() else { return };
        let Some(job) = sequencer.payload_building_job() else { return };
        let l1_origin = job.l1_origin();
        sequencer.cancel_payload_building_job();
        tracing::warn!(
            target: "scroll::chain_orchestrator",
            reason,
            ?l1_origin,
            "Cancelled in-flight payload building job"
        );
        self.metric_handler.discard_block_building_recording();
        self.notify(ChainOrchestratorEvent::PayloadBuildingJobCancelled);
    }

    /// Atomically unwinds and decides only the owned held result. Pipeline results that have not
    /// yet yielded remain the separately tracked issue #32 and are outside this branch's scope.
    async fn unwind_and_revalidate_held_batch(
        &mut self,
        ancestor: u64,
    ) -> Result<(UnwindResult, HeldReorgOutcome), ChainOrchestratorError> {
        let (unwind_result, outcome) =
            self.derivation_driver.unwind_and_revalidate(&self.database, ancestor).await?;
        match outcome {
            HeldReorgOutcome::NoHeldBatch => {}
            HeldReorgOutcome::Invalidated { batch_info, reason } => tracing::info!(
                target: "scroll::chain_orchestrator",
                batch_index = batch_info.index,
                batch_hash = ?batch_info.hash,
                ancestor,
                reason,
                "Invalidated held derived batch after L1 unwind"
            ),
            HeldReorgOutcome::Survived { batch_info } => tracing::info!(
                target: "scroll::chain_orchestrator",
                batch_index = batch_info.index,
                batch_hash = ?batch_info.hash,
                ancestor,
                "Held derived batch survived L1 unwind; awaiting post-unwind repair"
            ),
        }

        Ok((unwind_result, outcome))
    }

    /// Meters L1 reorg handling.
    async fn handle_l1_reorg_and_revalidate(
        &mut self,
        block_number: u64,
    ) -> Result<Option<ChainOrchestratorEvent>, ChainOrchestratorError> {
        metered!(Task::L1Reorg, self, handle_l1_reorg(block_number))
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
                self.handle_l1_reorg_and_revalidate(*block_number).await
            }
            L1Notification::Consensus(update) => {
                self.consensus.update_config(update);
                Ok(None)
            }
            L1Notification::NewBlock(block_info) => self.handle_l1_new_block(*block_info).await,
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
                    // A consolidation failure that reaches this arm has
                    // already purged the L1-message mappings (validation
                    // failure) or failed a database write, and the sync
                    // state is flipped: running on would sequence nothing
                    // (pool never opens) while reporting healthy — nothing
                    // re-runs consolidation. Transient fetch failures do NOT
                    // get here: the retry wrapper handles those in place.
                    if let Err(err) = self.consolidate_chain_with_retry().await {
                        tracing::error!(
                            target: "scroll::chain_orchestrator",
                            %err,
                            "Chain consolidation failed after the node was marked synced"
                        );
                        return Err(ChainOrchestratorError::FatalStateDivergence(
                            "chain consolidation failed after the node was marked synced; \
                             restart re-consolidates from the persisted state",
                        ));
                    }
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
    ///
    /// Best-effort per block, and bounded. A failed block is skipped rather than discarding
    /// everything collected so far: every caller already treats the result as advisory (a pool
    /// refill), and losing an entire unwind's user transactions to one transport blip on the last
    /// block — under a `warn!` that reads like "there were none" — is the worse outcome.
    ///
    /// The range is capped at [`MAX_REVERTED_TX_COLLECTION_BLOCKS`] nearest the head. This runs
    /// inside `handle_command` on the single-task run loop, one full-block RPC per block, while no
    /// L1 notification, gossip message, derivation step or sequencer slot is polled — and the
    /// remote source's rewind can drive it over up to `MAX_ANCESTOR_LOOKBACK` blocks on any remote
    /// reorg. Truncation is logged, and it costs only pool refill: the transactions are still in
    /// their reverted blocks, and clients resubmit.
    async fn collect_reverted_txs_in_range(
        &self,
        from: u64,
        to: u64,
    ) -> Result<Vec<ScrollTxEnvelope>, ChainOrchestratorError> {
        if from > to {
            return Ok(Vec::new());
        }
        let span = to.saturating_sub(from).saturating_add(1);
        let from = if span > MAX_REVERTED_TX_COLLECTION_BLOCKS {
            let capped = to.saturating_sub(MAX_REVERTED_TX_COLLECTION_BLOCKS - 1);
            tracing::warn!(
                target: "scroll::chain_orchestrator",
                requested_from = from,
                from = capped,
                to,
                "Reverted-transaction collection truncated to the blocks nearest the head;                  transactions below it are not refilled into the pool"
            );
            capped
        } else {
            from
        };

        let mut reverted_transactions: Vec<ScrollTxEnvelope> = Vec::new();
        let mut failed: Vec<u64> = Vec::new();
        for number in from..=to {
            let block = match self.l2_client.get_block_by_number(number.into()).full().await {
                Ok(Some(block)) => block,
                outcome => {
                    tracing::debug!(
                        target: "scroll::chain_orchestrator",
                        number,
                        ?outcome,
                        "Could not fetch a reverted block for the pool refill; skipping it"
                    );
                    failed.push(number);
                    continue;
                }
            };

            let block = block.into_consensus().map_transactions(|tx| tx.inner.into_inner());
            reverted_transactions.extend(
                block.into_body().transactions.into_iter().filter(|tx| !tx.is_l1_message()),
            );
        }
        if !failed.is_empty() {
            tracing::error!(
                target: "scroll::chain_orchestrator",
                failed_blocks = failed.len(),
                first_failed = failed.first(),
                last_failed = failed.last(),
                collected = reverted_transactions.len(),
                "Some reverted blocks could not be read; their transactions are not refilled                  into the pool"
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
        self.derivation_driver.cancel_attempt();
        // On failure the unwind transaction rolled back (rows untouched) —
        // but the NOTIFICATION is consumed and the watcher never re-emits a
        // reorg: it resets and re-scans forward, delivering the replacement
        // chain's blocks while our L1 index still describes the reorged-out
        // one (duplicate batch indices, queue gaps, forever). Unreplayable:
        // stop, rather than reschedule into a permanently inconsistent index.
        let (unwind_result, held_outcome) =
            match self.unwind_and_revalidate_held_batch(block_number).await {
                Ok(result) => result,
                Err(err) => {
                    tracing::error!(
                        target: "scroll::chain_orchestrator",
                        block_number,
                        %err,
                        "L1-reorg unwind failed with the notification consumed"
                    );
                    return Err(ChainOrchestratorError::FatalStateDivergence(
                        "L1-reorg unwind failed and the reorg notification cannot be \
                         replayed; restart re-processes L1 from a consistent index",
                    ));
                }
            };
        let UnwindResult { l1_block_number, queue_index, l2_head_block_number, l2_safe_block_info } =
            unwind_result;

        let (l2_head_block_info, reverted_transactions) =
            if let Some(block_number) = l2_head_block_number {
                // FINALIZED L2 state is irreversible, and the unwind above
                // has already durably persisted this head and purged the
                // mappings above it — clamping only the FCU target (as an
                // earlier revision did) would leave a divergence that even a
                // restart cannot detect (the startup repair loop only runs
                // while persisted head > finalized). Irreconcilable: stop.
                let finalized_floor = self.engine.fcs().finalized_block_info().number;
                if block_number < finalized_floor {
                    tracing::error!(
                        target: "scroll::chain_orchestrator",
                        block_number,
                        finalized_floor,
                        "L1 reorg rewound the L2 head below the finalized block"
                    );
                    return Err(ChainOrchestratorError::FatalStateDivergence(
                        "L1 reorg rewound the L2 head below the finalized block; the \
                         persisted state is irreconcilable without manual intervention",
                    ));
                }
                // The unwind above is already committed (persisted head
                // rewound, mappings purged). A failure between here and the
                // FCU would leave the engine on the old chain with the
                // notification consumed and never retried — strictly worse
                // than a rejected FCU, so both reads share its fatal policy.
                let block_hash =
                    match self.l2_client.get_block_by_number(block_number.into()).full().await {
                        Ok(Some(block)) => block.header.hash_slow(),
                        outcome => {
                            tracing::error!(
                                target: "scroll::chain_orchestrator",
                                block_number,
                                ?outcome,
                                "Post-unwind L2 head lookup failed; engine and database now diverge"
                            );
                            return Err(ChainOrchestratorError::FatalStateDivergence(
                                "post-unwind L2 head lookup failed before the reorg FCU; \
                             restart re-converges from the persisted state",
                            ));
                        }
                    };

                // Collect transactions of reverted blocks from l2 client.
                // BEST-EFFORT: the only consumer is the pool refill, which
                // already tolerates per-tx failure with a warn. One transport
                // blip on one of N per-block round-trips must not abort the
                // unwind (let alone fail-stop) — the FCU below is what
                // converges state, and it must still run.
                let reverted_transactions = match self
                    .collect_reverted_txs_in_range(
                        block_number.saturating_add(1),
                        self.engine.fcs().head_block_info().number,
                    )
                    .await
                {
                    Ok(txs) => txs,
                    Err(err) => {
                        tracing::warn!(
                            target: "scroll::chain_orchestrator",
                            block_number,
                            %err,
                            "Failed to collect reverted transactions for the pool refill; \
                             continuing the unwind without them"
                        );
                        Vec::new()
                    }
                };

                (Some(BlockInfo { number: block_number, hash: block_hash }), reverted_transactions)
            } else {
                (None, Vec::new())
            };

        // If the inflight payload building job MAY carry L1 messages, cancel it: the unwind
        // may have deleted messages the job depends on. `l1_origin` is set from the first
        // CANDIDATE message before the gas filter (so this can fire for a job that included
        // none — conservative), and it records only the first message's L1 block, so a range
        // comparison against it could miss messages from later L1 blocks in the same payload.
        if self
            .sequencer
            .as_ref()
            .and_then(|s| s.payload_building_job().map(|p| p.l1_origin()))
            .flatten()
            .is_some()
        {
            self.cancel_payload_building_job("L1 reorg while the job may carry L1 messages");
        }

        let finalized = *self.engine.fcs().finalized_block_info();
        // The safe target must satisfy finalized <= safe <= head before the
        // FCU, or fcs.update() refuses LOCALLY after the unwind is already
        // durable. A below-finalized head is already fatal above, so flooring
        // safe at finalized cannot construct safe < finalized. The head drag
        // uses the EFFECTIVE head — the rewound head when the reorg moved it,
        // otherwise the current mirror head, which an earlier administrative
        // UpdateFcsHead can have left BELOW the DB-absolute safe (the unwind
        // never deletes l2_block rows). Dragging only in the head-rewound
        // branch (the previous behaviour) left the head-not-rewound shape
        // (safe = Some above the mirror head, head = None) to trip
        // HeadBelowSafe and crash-loop the node on a routine reorg.
        let effective_head =
            l2_head_block_info.unwrap_or_else(|| *self.engine.fcs().head_block_info());
        let l2_safe_block_info = match l2_safe_block_info {
            Some(safe) => {
                let floored = if safe.number >= finalized.number {
                    safe
                } else {
                    // Same invariant violation the batch-revert twin reports at
                    // error!: the database's post-unwind safe head sits below
                    // the engine's finalized block, which is an L1-side problem
                    // and must not be silently absorbed.
                    tracing::error!(
                        target: "scroll::chain_orchestrator",
                        ?safe,
                        ?finalized,
                        "L1 reorg produced a safe head below the finalized block; flooring it"
                    );
                    finalized
                };
                Some(if floored.number > effective_head.number { effective_head } else { floored })
            }
            None if self.engine.fcs().safe_block_info().number > effective_head.number => {
                Some(effective_head)
            }
            None => None,
        };

        if l2_head_block_info.is_some() || l2_safe_block_info.is_some() {
            // The L1 database is already unwound by this point: an FCU that
            // does not apply leaves the engine on the reorged-out chain, no
            // L1Reorg event emitted, reverted transactions not reinserted.
            match self.engine.update_fcs(l2_head_block_info, l2_safe_block_info, None).await {
                // This is the UNCHECKED call: its mirror commits on any
                // non-INVALID result, so on SYNCING the engine mirror and the
                // database AGREE (the EL converges as it catches up) — that
                // must not fail-stop.
                Ok(result) if !result.is_invalid() => {
                    if !result.is_valid() {
                        tracing::warn!(
                            target: "scroll::chain_orchestrator",
                            ?l2_head_block_info,
                            ?l2_safe_block_info,
                            "L1-reorg FCU SYNCING; mirror committed, EL will converge"
                        );
                    }
                }
                Ok(result) => {
                    // INVALID: the one response that does NOT commit the
                    // mirror while the unwind is already durable.
                    tracing::error!(
                        target: "scroll::chain_orchestrator",
                        ?l2_head_block_info,
                        ?l2_safe_block_info,
                        ?result,
                        "L1-reorg FCU rejected as INVALID AFTER the L1 database unwind; \
                         engine and database now diverge"
                    );
                    return Err(ChainOrchestratorError::FatalStateDivergence(
                        "post-unwind L1-reorg forkchoice update rejected as INVALID; \
                         restart re-converges from the persisted state",
                    ));
                }
                Err(EngineError::FcsError(err)) => {
                    // Local pre-flight refusal — but the DB unwind is already
                    // durable, so engine and database HAVE diverged (nothing
                    // was sent to the engine, yet nothing will re-send it:
                    // the notification is consumed). The clamping above makes
                    // every benign refusal unconstructible, so reaching this
                    // arm means genuinely inconsistent durable state.
                    tracing::error!(
                        target: "scroll::chain_orchestrator",
                        ?l2_head_block_info,
                        ?l2_safe_block_info,
                        %err,
                        "Post-unwind L1-reorg FCU refused by local forkchoice validation \
                         with the unwind already committed"
                    );
                    return Err(ChainOrchestratorError::FatalStateDivergence(
                        "post-unwind L1-reorg forkchoice targets failed local validation; \
                         restart re-converges from the persisted state",
                    ));
                }
                Err(err) => {
                    // Transport error with the unwind already committed and
                    // no FCU applied: genuine divergence.
                    tracing::error!(
                        target: "scroll::chain_orchestrator",
                        ?l2_head_block_info,
                        ?l2_safe_block_info,
                        %err,
                        "L1-reorg FCU failed AFTER the L1 database unwind; engine and \
                         database now diverge"
                    );
                    return Err(ChainOrchestratorError::FatalStateDivergence(
                        "post-unwind L1-reorg forkchoice update failed; restart \
                         re-converges from the persisted state",
                    ));
                }
            }
        }

        // Cancel the inflight payload building job if the head has changed —
        // after the FCU (a job that may carry L1 messages was already
        // cancelled above), so the cancel and the head move stay adjacent;
        // the single-task run loop means the job cannot complete in between.
        if l2_head_block_info.is_some() {
            self.cancel_payload_building_job("L1 reorg moved the L2 head");
        }

        // Add all reverted transactions to the transaction pool.
        self.reinsert_txs_into_pool(reverted_transactions).await;

        if matches!(held_outcome, HeldReorgOutcome::Survived { .. }) {
            self.derivation_driver.schedule_fresh_reconciliation();
        }

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

        if let Some(finalized_block_info) = finalized_block_info {
            // Deliberately UNCHECKED: SYNCING is routine while the EL catches
            // up, and the mirror-committed value rides every later FCU until
            // the EL adopts it. Failures here are RETRYABLE, not fatal: the
            // marker is recomputed from the database (already-`Finalized`
            // batches included) on every finalized notification — and on the
            // startup replay — so a lost FCU is reissued whenever the mirror
            // is observed behind. Only INVALID (never committed; the engine
            // actively rejects a block it validated at consolidation) is a
            // fatal divergence.
            let mirror_finalized = *self.engine.fcs().finalized_block_info();
            let mirror_safe = self.engine.fcs().safe_block_info().number;
            if finalized_block_info.number <= mirror_finalized.number {
                // The DB-derived marker and the EL-seeded mirror are
                // independently sourced: the same height with a different
                // hash is divergence at the finality boundary — the class
                // this handler fail-stops on everywhere else.
                if finalized_block_info.number == mirror_finalized.number &&
                    finalized_block_info.hash != mirror_finalized.hash
                {
                    tracing::error!(
                        target: "scroll::chain_orchestrator",
                        ?finalized_block_info,
                        ?mirror_finalized,
                        "Database and engine disagree on the finalized block at the same \
                         height"
                    );
                    return Err(ChainOrchestratorError::FatalStateDivergence(
                        "database and engine disagree on the finalized block at the same \
                         height",
                    ));
                }
                tracing::trace!(
                    target: "scroll::chain_orchestrator",
                    ?finalized_block_info,
                    ?mirror_finalized,
                    "Finalized marker already committed to the engine mirror; skipping FCU"
                );
            } else if finalized_block_info.number > self.engine.fcs().head_block_info().number {
                // The EL's markers sit below database finality AND its head
                // does not even cover the marker (a deep EL-side rollback):
                // nothing can be raised safely from here — the startup
                // head-repair path owns this shape. Retry on the next
                // finalized notification.
                tracing::error!(
                    target: "scroll::chain_orchestrator",
                    ?finalized_block_info,
                    mirror_head = self.engine.fcs().head_block_info().number,
                    "Finalized marker exceeds the engine's head mirror; deferring the marker FCU"
                );
            } else {
                // When the marker also exceeds the SAFE mirror (an EL-side
                // marker rollback with the head still covering the marker),
                // raise safe TOGETHER with finalized: a finalized block is
                // safe by construction, and deferring would stall forever on
                // a quiescent chain — already-`Finalized` batches are never
                // re-derived, so nothing else would ever advance safe and
                // the engine would keep accepting reorgs of blocks the
                // database holds finalized.
                let safe_arg =
                    (finalized_block_info.number > mirror_safe).then_some(finalized_block_info);
                tracing::info!(target: "scroll::chain_orchestrator", ?finalized_block_info, ?safe_arg, "Updating FCS with new finalized block info from L1 finalization");
                match self.engine.update_fcs(None, safe_arg, Some(finalized_block_info)).await {
                    Ok(result) if result.is_invalid() => {
                        // INVALID does not commit the mirror, and no retry
                        // changes the engine's verdict on a block it already
                        // validated — the same divergence class the head
                        // updates treat as fatal.
                        tracing::error!(
                            target: "scroll::chain_orchestrator",
                            ?finalized_block_info,
                            ?result,
                            "Finalized-head FCU rejected as INVALID by the engine"
                        );
                        return Err(ChainOrchestratorError::FatalStateDivergence(
                            "finalized-head forkchoice update rejected as INVALID for a \
                             previously consolidated block",
                        ));
                    }
                    Ok(_) => {}
                    Err(err) => {
                        // The mirror is untouched on any error (local
                        // validation and transport alike), so the next
                        // finalized notification recomputes the marker and
                        // retries.
                        tracing::error!(
                            target: "scroll::chain_orchestrator",
                            ?finalized_block_info,
                            %err,
                            "Finalized-head FCU failed; the marker will be reissued on a later \
                             finalized notification"
                        );
                    }
                }
            }
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

        // Update the forkchoice state to the new safe block. The emitted
        // event carries what was actually ISSUED to the engine on the synced
        // path (the clamped target), and the database's post-revert safe
        // head when not synced (no FCU is issued then).
        let mut effective_safe_head = safe_block_info;
        if self.sync_state.is_synced() {
            // Floor the target at the engine's finalized block (mirrors the
            // L1-reorg and admin-unwind sites): the revert range is not
            // guaranteed to exclude finalized batches, so an unclamped
            // below-finalized target would trip a LOCAL SafeBelowFinalized
            // refusal — and the fatal arms below would fail-stop every
            // synced node on the same L1 event. A clamp that fires indicates
            // an L1-side violation: log it loudly and keep the fatal arms
            // for transport errors and INVALID — the only constructible
            // failures once BOTH clamps (the finalized floor here and the
            // head drag below) are applied.
            let finalized = *self.engine.fcs().finalized_block_info();
            let safe_target = if safe_block_info.number >= finalized.number {
                safe_block_info
            } else {
                tracing::error!(
                    target: "scroll::chain_orchestrator",
                    ?safe_block_info,
                    ?finalized,
                    "Batch revert produced a safe target below the finalized block; \
                     flooring at finalized"
                );
                finalized
            };
            // Preserve head >= safe (mirrors the L1-reorg and admin-unwind
            // sites): the DB-absolute safe can exceed the mirror head after
            // an administrative head update (which does not delete l2_block
            // rows), and an undragged target would trip a LOCAL
            // HeadBelowSafe refusal that the fatal arms below turn into a
            // node-killing fail-stop on a routine L1 event.
            let mirror_head = *self.engine.fcs().head_block_info();
            let safe_target =
                if safe_target.number > mirror_head.number { mirror_head } else { safe_target };
            effective_safe_head = safe_target;
            tracing::info!(target: "scroll::chain_orchestrator", ?safe_target, "Updating safe head to block after batch revert");
            // Deliberately UNCHECKED (see the finalized-head FCU in the L1
            // finalization handler): the batch-revert database mutation is
            // already committed, so a checked refusal on SYNCING would leave
            // the old safe head standing with no retry while imports that
            // must reorg below it get refused indefinitely.
            let result = match self.engine.update_fcs(None, Some(safe_target), None).await {
                Ok(result) => result,
                Err(err) => {
                    // See the finalized-head FCU: the revert is durable and
                    // no retry path exists.
                    tracing::error!(
                        target: "scroll::chain_orchestrator",
                        ?safe_target,
                        %err,
                        "Post-batch-revert safe-head FCU failed after the revert was \
                         committed"
                    );
                    return Err(ChainOrchestratorError::FatalStateDivergence(
                        "post-batch-revert safe-head forkchoice update failed; restart \
                         re-converges from the persisted state",
                    ));
                }
            };
            if result.is_invalid() {
                // See the finalized-head FCU: the revert is already durable
                // and INVALID left the mirror on the old (higher) safe head —
                // imports that must reorg below it would be refused forever.
                tracing::error!(
                    target: "scroll::chain_orchestrator",
                    ?safe_target,
                    ?result,
                    "Post-batch-revert safe-head FCU rejected as INVALID by the engine"
                );
                return Err(ChainOrchestratorError::FatalStateDivergence(
                    "post-batch-revert safe-head forkchoice update rejected as INVALID; \
                     restart re-converges from the persisted state",
                ));
            }
        }

        Ok(Some(ChainOrchestratorEvent::BatchReverted {
            batch_info,
            safe_head: effective_safe_head,
        }))
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

    /// Re-issues the forkchoice update for the head that latched L2 sync mode, and leaves sync
    /// mode when the execution node has adopted it.
    ///
    /// See [`Self::l2_sync_recheck_target`]: without this, a node that received SYNCING for a head
    /// it already knows stays gated until some NEWER block arrives, which never happens on a
    /// quiescent chain.
    async fn recheck_l2_sync_target(&mut self) -> Result<(), ChainOrchestratorError> {
        if !self.sync_state.l2().is_syncing() {
            return Ok(());
        }
        let Some(target) = self.l2_sync_recheck_target else { return Ok(()) };

        // Checked, like the site that latched: the mirror must not advance to a head the engine
        // has not adopted. A transport error is not fatal here — this is an opportunistic probe,
        // and the next announcement retries it.
        let result = match self.engine.update_fcs_checked(Some(target), None, None).await {
            Ok(result) => result,
            Err(err) => {
                tracing::debug!(
                    target: "scroll::chain_orchestrator",
                    ?target,
                    %err,
                    "L2 sync re-check could not reach the engine; will retry"
                );
                return Ok(());
            }
        };
        if result.is_invalid() {
            // The engine actively rejected the latched head: it is not coming back, so stop
            // re-issuing it. Sync mode stays closed until a real import reopens it.
            tracing::warn!(
                target: "scroll::chain_orchestrator",
                ?target,
                "Latched L2 sync target rejected as INVALID; dropping the re-check"
            );
            self.l2_sync_recheck_target = None;
            return Ok(());
        }
        if !result.is_valid() {
            return Ok(());
        }

        tracing::info!(
            target: "scroll::chain_orchestrator",
            ?target,
            "Execution node adopted the latched head; L2 is now synced"
        );
        self.l2_sync_recheck_target = None;
        self.sync_state.l2_mut().set_synced();
        if self.sync_state.is_synced() {
            self.consolidate_chain_with_retry().await?;
        }
        Ok(())
    }

    /// Handles a new block received from a peer.
    async fn handle_block_from_peer(
        &mut self,
        block_with_peer: NewBlockWithPeer,
    ) -> Result<Option<ChainOrchestratorEvent>, ChainOrchestratorError> {
        tracing::debug!(target: "scroll::chain_orchestrator", block_hash = ?block_with_peer.block.header.hash_slow(), block_number = ?block_with_peer.block.number, peer_id = ?block_with_peer.peer_id, "Received new block from peer");

        // Before any short-circuit: an announcement of a block this node ALREADY
        // knows is the only signal a quiescent chain still produces, and it is
        // exactly what the latched sync state needs to notice the engine caught
        // up. A no-op unless the import site latched.
        self.recheck_l2_sync_target().await?;

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
            let result = self.engine.optimistic_sync(block_info).await?;
            if result.is_invalid() {
                // The engine rejected the target and kept the old head: a
                // rejection must be a TRUE no-op. Closing the gate or purging
                // mappings here would strand a surviving job behind a gate
                // that never reopens (every later BuildBlock coalescing into
                // it with only non-terminal events) while nothing was synced.
                tracing::warn!(
                    target: "scroll::chain_orchestrator",
                    ?block_info,
                    "Optimistic sync target rejected as INVALID by the engine; ignoring \
                     the peer block"
                );
                return Ok(None);
            }
            // Cancel any in-flight payload building job now that the head
            // jumped far ahead — after the FCU so a transient engine error
            // (head unchanged) does not destroy a valid job. Mirrors the
            // ordering at the other head-moving sites; the single-task run
            // loop means the job cannot complete in between. Cancelling
            // matters even though a parked job could not complete anyway —
            // set_syncing() closes the sequencer arm's gate, and the path
            // back to synced cancels first — because leaving it in the slot
            // would make every BuildBlock during the sync window coalesce
            // into a job that can never emit an outcome.
            self.cancel_payload_building_job("optimistic sync moved the head");
            self.sync_state.l2_mut().set_syncing();

            // Purge all L1 message to L2 block mappings as they may be invalid after an
            // optimistic sync. The head is already committed hundreds of
            // blocks ahead: failing to purge leaves messages marked consumed
            // by vanished blocks, silently excluded from selection.
            if let Err(err) = self.database.purge_l1_message_to_l2_block_mappings(None).await {
                tracing::error!(
                    target: "scroll::chain_orchestrator",
                    %err,
                    "Mapping purge failed after the optimistic-sync head committed"
                );
                return Err(ChainOrchestratorError::FatalStateDivergence(
                    "L1-message mapping purge failed after optimistic sync; restart \
                     re-converges from the persisted state",
                ));
            }

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
        let was_syncing = self.sync_state.l2().is_syncing();
        let result = if was_syncing {
            self.engine.optimistic_sync(head).await?
        } else {
            // Checked: on a synced node the mirror must not advance to a head
            // the EL has not adopted — plain update_fcs commits on SYNCING
            // too, which would freeze the persisted anchor while the mirror
            // climbs and stall a sequencer at MissingPayloadId behind a
            // healthy-looking status.
            self.engine.update_fcs_checked(Some(head), None, None).await?
        };

        // If the FCS update resulted in an invalid state, we return an error.
        if result.is_invalid() {
            tracing::warn!(target: "scroll::chain_orchestrator", ?chain_head_hash, ?chain_head_number, ?result, "Failed to update FCS after importing new chain from peer");
            return Err(ChainOrchestratorError::InvalidBlock);
        }
        if !was_syncing && !result.is_valid() {
            // SYNCING from a synced node's own EL: it lost recent ancestors
            // (e.g. an EL restart dropping unpersisted blocks). The checked
            // FCU left the mirror untouched; re-enter the L2 sync path that
            // exists for exactly this instead of pretending the head moved.
            tracing::warn!(
                target: "scroll::chain_orchestrator",
                ?head,
                "EL has not adopted the imported head; re-entering L2 sync"
            );
            // Cancel any in-flight job before closing the gate: a parked
            // job could never be polled again and every later BuildBlock
            // would coalesce into it with no terminal event (mirrors the
            // optimistic-sync site). The mirror is untouched, so the job is
            // not stale — this is about not stranding waiters.
            self.cancel_payload_building_job("EL has not adopted the imported head");
            self.sync_state.l2_mut().set_syncing();
            // Remember what to re-check: the EL may catch up to this exact head
            // without any newer block ever arriving.
            self.l2_sync_recheck_target = Some(head);
            return Err(ChainOrchestratorError::FcuRejected(
                "peer chain import forkchoice update was not applied by the engine",
            ));
        }

        // The head moved (on the optimistic branch a SYNCING result still
        // commits the mirror): cancel any in-flight payload building job
        // unconditionally once the import was not rejected. Its attributes
        // were fixed against the pre-import head, and finalizing it now could
        // reorg the imported chain back out via a stale side chain (mirrors
        // the cancellation in handle_l1_reorg). Done after the invalid-check
        // so a rejected import does not discard a valid job; that is safe
        // because the job future is polled only by the run_until_shutdown
        // select arm, which is blocked while this handler runs — the job
        // cannot complete in between.
        self.cancel_payload_building_job("chain import moved the head");

        // If we were previously in L2 syncing mode and the FCS update resulted in a valid state, we
        // transition the L2 sync state to synced and consolidate the chain.
        if result.is_valid() && self.sync_state.l2().is_syncing() {
            tracing::info!(target: "scroll::chain_orchestrator", "L2 is now synced");
            self.l2_sync_recheck_target = None;
            self.sync_state.l2_mut().set_synced();

            // If both L1 and L2 are now synced, we transition to consolidated mode by consolidating
            // the chain.
            if self.sync_state.is_synced() {
                // See the Synced-notification arm: a failure here is not
                // survivable-in-place; transient fetch failures are retried
                // in place by the wrapper and never reach the fatal arm.
                if let Err(err) = self.consolidate_chain_with_retry().await {
                    tracing::error!(
                        target: "scroll::chain_orchestrator",
                        %err,
                        "Chain consolidation failed after the node was marked synced"
                    );
                    return Err(ChainOrchestratorError::FatalStateDivergence(
                        "chain consolidation failed after the node was marked synced; \
                         restart re-consolidates from the persisted state",
                    ));
                }
            }
        }

        // Persist the L1 message to L2 block mappings for reorg awareness, update the l2 head block
        // number and handle the valid block import if we are in a synced state and the
        // result is valid.
        if self.sync_state.is_synced() && result.is_valid() {
            let blocks = chain.iter().map(|block| block.into()).collect::<Vec<_>>();
            if let Err(err) = self
                .database
                .tx_mut(move |tx| {
                    let blocks = blocks.clone();
                    async move {
                        tx.update_l1_messages_from_l2_blocks(blocks).await?;
                        tx.set_l2_head_block_number(block_with_peer.block.header.number).await
                    }
                })
                .await
            {
                // The FCU above already committed the head: unmarked L1
                // messages get re-selected for the next block and a frozen
                // anchor rewinds a restart past adopted blocks — the same
                // divergence the sequencing path treats as fatal.
                tracing::error!(
                    target: "scroll::chain_orchestrator",
                    block_number = block_with_peer.block.header.number,
                    %err,
                    "Post-import persistence failed after the engine head moved"
                );
                return Err(ChainOrchestratorError::FatalStateDivergence(
                    "imported chain committed to the engine but could not be persisted; \
                     restart re-converges from the persisted state",
                ));
            }

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

    /// Runs [`Self::consolidate_chain`], retrying fetch-shaped failures
    /// (transport error, block temporarily missing from the L2 client) a
    /// bounded number of times: a single RPC timeout must not ride the
    /// callers' fatal escalation and fail-stop the node, while a persistent
    /// failure still surfaces with its real cause. Retrying in place (rather
    /// than re-entering L2 sync) matters for the lone-sequencer topology,
    /// where nothing external would ever flip the L2 sync state back.
    async fn consolidate_chain_with_retry(&mut self) -> Result<(), ChainOrchestratorError> {
        const ATTEMPTS: u32 = 3;
        let mut attempt = 1;
        loop {
            match metered!(Task::ChainConsolidation, self, consolidate_chain()) {
                Err(err @ ChainOrchestratorError::ConsolidationFetchFailed(_))
                    if attempt < ATTEMPTS =>
                {
                    tracing::warn!(
                        target: "scroll::chain_orchestrator",
                        %err,
                        attempt,
                        "Consolidation block fetch failed; retrying in place"
                    );
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                    attempt += 1;
                }
                other => return other,
            }
        }
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
                let blocks_to_validate = blocks_result.map_err(|e| {
                    // Preserve the real cause (RPC timeout, missing block)
                    // instead of collapsing it into InvalidBlock — the
                    // callers' fatal policy must not fire on a transient.
                    ChainOrchestratorError::ConsolidationFetchFailed(Box::new(e.1))
                })?;

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
mod run_loop_policy_tests {
    use super::*;
    use alloy_primitives::{Address, Bloom, Bytes, U256};
    use alloy_provider::ProviderBuilder;
    use alloy_rpc_types_engine::{
        ExecutionPayloadV1, ForkchoiceUpdated, PayloadId, PayloadStatus, PayloadStatusEnum,
    };
    use alloy_transport::mock::Asserter;
    use dogeos_chainspec::{DogeosChainSpec, DOGEOS_DEV};
    use dogeos_reth_consensus::DogeosConsensus;
    use dogeos_reth_engine::ScrollPayloadAttributes;
    use reth_network_api::noop::NoopNetwork;
    use reth_network_p2p::NoopFullBlockClient;
    use rollup_node_primitives::{BatchCommitData, L1MessageEnvelope};
    use rollup_node_providers::{test_utils::MockL1Provider, ScrollRootProvider};
    use scroll_db::test_utils::{seeded_test_genesis, setup_test_db};
    use scroll_derivation_pipeline::{BatchDerivationResult, DerivedAttributes};
    use scroll_engine::{
        test_utils::{ScriptedEngineClient, ScriptedResponse},
        ForkchoiceState,
    };
    use scroll_network::{NetworkHandleMessage, ScrollNetworkHandle};
    use std::time::Duration;
    use tokio::time;

    const SAFE: u64 = 100;
    const TEST_L1_NOTIFICATION_CAPACITY: usize = 16;

    type TestNetwork = NoopNetwork<DogeosNetworkPrimitives>;
    type TestL1Provider = MockL1Provider<Arc<Database>>;
    type TestOrchestrator = ChainOrchestrator<
        TestNetwork,
        DogeosChainSpec,
        TestL1Provider,
        ScrollRootProvider,
        ScriptedEngineClient,
    >;

    fn info(number: u64, tag: u8) -> BlockInfo {
        BlockInfo { number, hash: B256::repeat_byte(tag) }
    }

    fn fcu(status: PayloadStatusEnum, payload_id: Option<PayloadId>) -> ForkchoiceUpdated {
        ForkchoiceUpdated {
            payload_status: PayloadStatus { status, latest_valid_hash: None },
            payload_id,
        }
    }

    fn payload(number: u64) -> ExecutionPayloadV1 {
        ExecutionPayloadV1 {
            parent_hash: B256::ZERO,
            fee_recipient: Address::ZERO,
            state_root: B256::ZERO,
            receipts_root: B256::ZERO,
            logs_bloom: Bloom::default(),
            prev_randao: B256::ZERO,
            block_number: number,
            gas_limit: 0,
            gas_used: 0,
            timestamp: 0,
            extra_data: Bytes::new(),
            base_fee_per_gas: U256::ZERO,
            block_hash: B256::repeat_byte(0x22),
            transactions: vec![],
        }
    }

    fn script_syncing_hold_then_success(client: &ScriptedEngineClient) {
        client
            .push_fork_choice_updated(ScriptedResponse::Ok(fcu(PayloadStatusEnum::Syncing, None)));
        client.push_fork_choice_updated(ScriptedResponse::Ok(fcu(
            PayloadStatusEnum::Valid,
            Some(PayloadId::new([7; 8])),
        )));
        client.push_get_payload(ScriptedResponse::Ok(payload(SAFE + 1)));
        client.push_new_payload(ScriptedResponse::Ok(PayloadStatus {
            status: PayloadStatusEnum::Valid,
            latest_valid_hash: None,
        }));
        client.push_fork_choice_updated(ScriptedResponse::Ok(fcu(PayloadStatusEnum::Valid, None)));
    }

    async fn test_scroll_network() -> ScrollNetwork<NoopNetwork<DogeosNetworkPrimitives>> {
        let (to_manager_tx, mut from_handle_rx) = mpsc::unbounded_channel();
        let handle =
            ScrollNetworkHandle::new(to_manager_tx, NoopNetwork::<DogeosNetworkPrimitives>::new());
        tokio::spawn(async move {
            let events = EventSender::new(16);
            while let Some(message) = from_handle_rx.recv().await {
                if let NetworkHandleMessage::EventListener(response) = message {
                    let _ = response.send(events.new_listener());
                }
            }
        });
        handle.into_scroll_network().await
    }

    async fn test_orchestrator(
        database: Arc<Database>,
        engine_client: Arc<ScriptedEngineClient>,
        asserter: Asserter,
        derived: BatchDerivationResult,
    ) -> (TestOrchestrator, ChainOrchestratorHandle<TestNetwork>, mpsc::Sender<Arc<L1Notification>>)
    {
        test_orchestrator_with_fcs(
            database,
            engine_client,
            asserter,
            derived,
            ForkchoiceState::new(info(SAFE, 0x11), info(SAFE, 0x11), info(0, 0)),
        )
        .await
    }

    /// [`test_orchestrator`] with an explicit initial forkchoice state, for
    /// tests that need a non-genesis finalized block.
    async fn test_orchestrator_with_fcs(
        database: Arc<Database>,
        engine_client: Arc<ScriptedEngineClient>,
        asserter: Asserter,
        derived: BatchDerivationResult,
        fcs: ForkchoiceState,
    ) -> (TestOrchestrator, ChainOrchestratorHandle<TestNetwork>, mpsc::Sender<Arc<L1Notification>>)
    {
        let engine = Engine::new(engine_client, fcs);
        let l2_provider =
            ProviderBuilder::<_, _, Scroll>::default().connect_mocked_client(asserter);
        let l1_provider = MockL1Provider { db: database.clone(), blobs: Default::default() };
        let derivation_pipeline =
            DerivationPipeline::new(l1_provider.clone(), database.clone(), 0).await;

        let (watcher_command_tx, _watcher_command_rx) = mpsc::unbounded_channel();
        let (notification_tx, notification_rx) = mpsc::channel(TEST_L1_NOTIFICATION_CAPACITY);
        let l1_watcher = L1WatcherHandle::new(watcher_command_tx, notification_rx);
        let block_client = Arc::new(FullBlockClient::new(
            NoopFullBlockClient::<DogeosNetworkPrimitives>::default(),
            Arc::new(DogeosConsensus),
        ));
        let config = ChainOrchestratorConfig::<DogeosChainSpec>::new(DOGEOS_DEV.clone(), 1, 0);
        let (mut orchestrator, handle) = ChainOrchestrator::new(
            database,
            config,
            block_client,
            l2_provider,
            l1_watcher,
            test_scroll_network().await,
            Box::new(NoopConsensus),
            engine,
            None::<Sequencer<TestL1Provider, DogeosChainSpec>>,
            None,
            derivation_pipeline,
        )
        .await
        .unwrap();
        orchestrator.sync_state.l1_mut().set_synced();
        orchestrator.derivation_driver.hold_batch(derived);

        (orchestrator, handle, notification_tx)
    }

    #[test]
    fn l1_notification_receiver_requires_synced_idle_derivation() {
        for (l2_synced, pipeline_empty, can_accept_batch, expected) in [
            (false, false, false, false),
            (false, false, true, false),
            (false, true, false, false),
            (false, true, true, false),
            (true, false, false, false),
            (true, false, true, false),
            (true, true, false, false),
            (true, true, true, true),
        ] {
            assert_eq!(
                l1_notification_receiver_may_poll(l2_synced, pipeline_empty, can_accept_batch),
                expected,
                "l2_synced={l2_synced}, pipeline_empty={pipeline_empty}, \
                 can_accept_batch={can_accept_batch}"
            );
        }
    }

    #[tokio::test]
    async fn notification_waits_until_held_slot_and_pipeline_are_empty() {
        let database = Arc::new(setup_test_db().await);
        database.insert_genesis_block(B256::ZERO).await.unwrap();
        // The dev migration seeded the Scroll dev genesis row beside the
        // test genesis; drop it so height-0 queries are deterministic.
        database.delete_mismatched_genesis_blocks(B256::ZERO).await.unwrap();
        database.set_processed_l1_block_number(5).await.unwrap();
        let batch_info = BatchInfo::new(1, B256::repeat_byte(1));
        database
            .insert_batch(BatchCommitData {
                hash: batch_info.hash,
                index: batch_info.index,
                block_number: 1,
                block_timestamp: 0,
                calldata: Arc::new(Bytes::new()),
                blob_versioned_hash: None,
                finalized_block_number: None,
                reverted_block_number: None,
            })
            .await
            .unwrap();
        database.update_batch_status(batch_info.hash, BatchStatus::Processing).await.unwrap();

        let engine_client = Arc::new(ScriptedEngineClient::new());
        script_syncing_hold_then_success(&engine_client);

        let asserter = Asserter::new();
        asserter.push_success(&Option::<()>::None);
        asserter.push_success(&Option::<()>::None);
        let (mut orchestrator, handle, notification_tx) = test_orchestrator(
            database.clone(),
            engine_client.clone(),
            asserter,
            BatchDerivationResult {
                attributes: vec![DerivedAttributes {
                    block_number: SAFE + 1,
                    attributes: ScrollPayloadAttributes::default(),
                }],
                batch_info,
                skipped_l1_messages: vec![],
                target_status: BatchStatus::Consolidated,
            },
        )
        .await;
        let mut events = orchestrator.event_listener();

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let run_task = tokio::spawn(orchestrator.run_until_shutdown(Box::pin(async move {
            let _ = shutdown_rx.await;
        })));

        time::timeout(Duration::from_secs(2), async {
            loop {
                let status = handle.status().await.unwrap();
                if matches!(
                    status.derivation,
                    DerivationStatus::Held(HeldBatchStatus { attempts_started: 1, .. })
                ) {
                    break
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("first Engine SYNCING response must hold the batch");

        notification_tx.send(Arc::new(L1Notification::Processed(10))).await.unwrap();
        time::sleep(Duration::from_millis(50)).await;
        assert_eq!(notification_tx.capacity(), TEST_L1_NOTIFICATION_CAPACITY - 1);
        assert_eq!(
            database.get_processed_l1_block_number().await.unwrap(),
            5,
            "the watcher channel must retain the notification while derivation is held"
        );

        time::timeout(Duration::from_secs(5), async {
            loop {
                if matches!(
                    events.next().await,
                    Some(ChainOrchestratorEvent::BatchConsolidated(outcome))
                        if outcome.batch_info == batch_info
                ) {
                    break
                }
            }
        })
        .await
        .expect("the held batch must consolidate");

        time::timeout(Duration::from_secs(2), async {
            while database.get_processed_l1_block_number().await.unwrap() != 10 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the notification must apply once derivation is idle");
        assert_eq!(notification_tx.capacity(), TEST_L1_NOTIFICATION_CAPACITY);
        assert!(matches!(handle.status().await.unwrap().derivation, DerivationStatus::Idle));
        assert_eq!(engine_client.fork_choice_updated_calls(), 3);

        let _ = shutdown_tx.send(());
        assert!(run_task.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn queued_reorg_runs_after_consolidation_then_unwinds_it() {
        let database = Arc::new(setup_test_db().await);
        database.insert_genesis_block(B256::ZERO).await.unwrap();
        // The dev migration seeded the Scroll dev genesis row beside the
        // test genesis; drop it so height-0 queries are deterministic.
        database.delete_mismatched_genesis_blocks(B256::ZERO).await.unwrap();
        let batch_info = BatchInfo::new(1, B256::repeat_byte(1));
        database
            .insert_batch(BatchCommitData {
                hash: batch_info.hash,
                index: batch_info.index,
                block_number: 10,
                block_timestamp: 0,
                calldata: Arc::new(Bytes::new()),
                blob_versioned_hash: None,
                finalized_block_number: None,
                reverted_block_number: None,
            })
            .await
            .unwrap();
        database.update_batch_status(batch_info.hash, BatchStatus::Processing).await.unwrap();

        let engine_client = Arc::new(ScriptedEngineClient::new());
        script_syncing_hold_then_success(&engine_client);
        engine_client
            .push_fork_choice_updated(ScriptedResponse::Ok(fcu(PayloadStatusEnum::Valid, None)));
        let asserter = Asserter::new();
        asserter.push_success(&Option::<()>::None);
        asserter.push_success(&Option::<()>::None);
        let (mut orchestrator, _handle, notification_tx) = test_orchestrator(
            database.clone(),
            engine_client.clone(),
            asserter,
            BatchDerivationResult {
                attributes: vec![DerivedAttributes {
                    block_number: SAFE + 1,
                    attributes: ScrollPayloadAttributes::default(),
                }],
                batch_info,
                skipped_l1_messages: vec![],
                target_status: BatchStatus::Consolidated,
            },
        )
        .await;
        let mut events = orchestrator.event_listener();

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let run_task = tokio::spawn(orchestrator.run_until_shutdown(Box::pin(async move {
            let _ = shutdown_rx.await;
        })));

        time::timeout(Duration::from_secs(2), async {
            while engine_client.fork_choice_updated_calls() < 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("first Engine SYNCING response must hold the batch");

        notification_tx.send(Arc::new(L1Notification::Reorg(5))).await.unwrap();
        time::sleep(Duration::from_millis(50)).await;
        assert_eq!(notification_tx.capacity(), TEST_L1_NOTIFICATION_CAPACITY - 1);
        assert_eq!(
            database.get_batch_status_by_hash(batch_info.hash).await.unwrap(),
            Some(BatchStatus::Processing),
            "the queued reorg must not preempt the held batch"
        );

        let observed = time::timeout(Duration::from_secs(5), async {
            let mut observed = Vec::new();
            while observed.len() < 2 {
                match events.next().await {
                    Some(ChainOrchestratorEvent::BatchConsolidated(outcome))
                        if outcome.batch_info == batch_info =>
                    {
                        observed.push("consolidated");
                    }
                    Some(ChainOrchestratorEvent::L1Reorg { l1_block_number: 5, .. }) => {
                        observed.push("reorg")
                    }
                    _ => {}
                }
            }
            observed
        })
        .await
        .expect("consolidation and the queued reorg must both complete");
        assert_eq!(observed, ["consolidated", "reorg"]);
        assert_eq!(database.get_latest_l1_block_number().await.unwrap(), 5);
        assert!(database.get_batch_by_index(batch_info.index).await.unwrap().is_none());
        assert_eq!(engine_client.fork_choice_updated_calls(), 4);
        assert_eq!(engine_client.get_payload_calls(), 1);
        assert_eq!(engine_client.new_payload_calls(), 1);

        let _ = shutdown_tx.send(());
        assert!(run_task.await.unwrap().is_ok());
    }

    /// Pins the pass-43 safe-target clamp — the `None if mirror_safe >
    /// effective_head` arm. Every other reorg/unwind test runs with
    /// head == safe, where every branch of that `match` is a no-op: delete the
    /// whole thing and they stay green. The shape that matters is head BELOW
    /// safe, which an earlier administrative `UpdateFcsHead` leaves behind. A
    /// routine L1 reorg that rewinds nothing must still drag the safe target
    /// down to the engine's head; without the drag the safe stays above the
    /// head, `fcs.update()` refuses LOCALLY with `HeadBelowSafe` after the
    /// unwind is already durable, and the node crash-loops on every reorg.
    #[tokio::test]
    async fn l1_reorg_drags_the_safe_target_down_to_a_lower_head() {
        let database = Arc::new(setup_test_db().await);
        database.insert_genesis_block(B256::ZERO).await.unwrap();
        database.delete_mismatched_genesis_blocks(B256::ZERO).await.unwrap();

        // No batches: the reorg unwinds nothing, so BOTH targets arrive as
        // `None` and only the mirror-safe-vs-head comparison can produce one.
        // Nothing derives either, so the mirror still holds the constructed
        // head when the reorg is handled.
        let engine_client = Arc::new(ScriptedEngineClient::new());
        engine_client
            .push_fork_choice_updated(ScriptedResponse::Ok(fcu(PayloadStatusEnum::Valid, None)));
        let asserter = Asserter::new();
        asserter.push_success(&Option::<()>::None);
        asserter.push_success(&Option::<()>::None);
        let (orchestrator, _handle, notification_tx) = test_orchestrator_with_fcs(
            database.clone(),
            engine_client.clone(),
            asserter,
            BatchDerivationResult {
                attributes: vec![],
                batch_info: BatchInfo::new(1, B256::repeat_byte(1)),
                skipped_l1_messages: vec![],
                target_status: BatchStatus::Consolidated,
            },
            // Head (50) BELOW safe (100) — the shape an earlier administrative
            // UpdateFcsHead leaves: the unwind never deletes l2_block rows, so
            // the database still knows the higher safe.
            ForkchoiceState::new(info(50, 0x50), info(SAFE, 0x11), info(0, 0x00)),
        )
        .await;

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let run_task = tokio::spawn(orchestrator.run_until_shutdown(Box::pin(async move {
            let _ = shutdown_rx.await;
        })));

        notification_tx.send(Arc::new(L1Notification::Reorg(5))).await.unwrap();
        time::timeout(Duration::from_secs(5), async {
            while engine_client.fork_choice_updated_calls() < 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the reorg must issue an FCU carrying the dragged safe target");

        assert_eq!(
            engine_client.fork_choice_updated_states()[0].safe_block_hash,
            B256::repeat_byte(0x50),
            "the safe target must be dragged down to the engine's head, not left at the \
             database's higher safe"
        );

        // The run loop must survive: an undragged safe comes back from the
        // local pre-flight as FcsError(HeadBelowSafe) -> FatalStateDivergence.
        let _ = shutdown_tx.send(());
        assert!(run_task.await.unwrap().is_ok(), "a routine reorg must not terminate the run loop");
    }

    /// Pins pass-33 C1: the post-unwind L1-reorg FCU is issued through the
    /// UNCHECKED `update_fcs`, whose mirror commits on SYNCING — engine and
    /// database agree, so a routine SYNCING answer must NOT fail-stop the
    /// node (an earlier revision killed it here).
    #[tokio::test]
    async fn l1_reorg_post_unwind_fcu_syncing_continues() {
        let database = Arc::new(setup_test_db().await);
        database.insert_genesis_block(B256::ZERO).await.unwrap();
        // The dev migration seeded the Scroll dev genesis row beside the
        // test genesis; drop it so height-0 queries are deterministic.
        database.delete_mismatched_genesis_blocks(B256::ZERO).await.unwrap();
        let batch_info = BatchInfo::new(1, B256::repeat_byte(1));
        database
            .insert_batch(BatchCommitData {
                hash: batch_info.hash,
                index: batch_info.index,
                block_number: 10,
                block_timestamp: 0,
                calldata: Arc::new(Bytes::new()),
                blob_versioned_hash: None,
                finalized_block_number: None,
                reverted_block_number: None,
            })
            .await
            .unwrap();
        database.update_batch_status(batch_info.hash, BatchStatus::Processing).await.unwrap();

        let engine_client = Arc::new(ScriptedEngineClient::new());
        script_syncing_hold_then_success(&engine_client);
        engine_client
            .push_fork_choice_updated(ScriptedResponse::Ok(fcu(PayloadStatusEnum::Syncing, None)));
        let asserter = Asserter::new();
        asserter.push_success(&Option::<()>::None);
        asserter.push_success(&Option::<()>::None);
        let (mut orchestrator, _handle, notification_tx) = test_orchestrator(
            database.clone(),
            engine_client.clone(),
            asserter,
            BatchDerivationResult {
                attributes: vec![DerivedAttributes {
                    block_number: SAFE + 1,
                    attributes: ScrollPayloadAttributes::default(),
                }],
                batch_info,
                skipped_l1_messages: vec![],
                target_status: BatchStatus::Consolidated,
            },
        )
        .await;
        let mut events = orchestrator.event_listener();

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let run_task = tokio::spawn(orchestrator.run_until_shutdown(Box::pin(async move {
            let _ = shutdown_rx.await;
        })));

        time::timeout(Duration::from_secs(2), async {
            while engine_client.fork_choice_updated_calls() < 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("first Engine SYNCING response must hold the batch");

        notification_tx.send(Arc::new(L1Notification::Reorg(5))).await.unwrap();

        // The reorg completes despite the SYNCING FCU: the event is emitted
        // and the run loop stays alive.
        time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(ChainOrchestratorEvent::L1Reorg { l1_block_number: 5, .. }) =
                    events.next().await
                {
                    break;
                }
            }
        })
        .await
        .expect("the reorg must complete on a SYNCING post-unwind FCU");
        assert_eq!(database.get_latest_l1_block_number().await.unwrap(), 5);

        let _ = shutdown_tx.send(());
        assert!(run_task.await.unwrap().is_ok(), "SYNCING must not fail-stop the run loop");
    }

    /// The INVALID counterpart: the mirror did NOT commit while the unwind
    /// is durable — the run loop must terminate with a fatal divergence.
    #[tokio::test]
    async fn l1_reorg_post_unwind_fcu_invalid_fail_stops() {
        let database = Arc::new(setup_test_db().await);
        database.insert_genesis_block(B256::ZERO).await.unwrap();
        // The dev migration seeded the Scroll dev genesis row beside the
        // test genesis; drop it so height-0 queries are deterministic.
        database.delete_mismatched_genesis_blocks(B256::ZERO).await.unwrap();
        let batch_info = BatchInfo::new(1, B256::repeat_byte(1));
        database
            .insert_batch(BatchCommitData {
                hash: batch_info.hash,
                index: batch_info.index,
                block_number: 10,
                block_timestamp: 0,
                calldata: Arc::new(Bytes::new()),
                blob_versioned_hash: None,
                finalized_block_number: None,
                reverted_block_number: None,
            })
            .await
            .unwrap();
        database.update_batch_status(batch_info.hash, BatchStatus::Processing).await.unwrap();

        let engine_client = Arc::new(ScriptedEngineClient::new());
        script_syncing_hold_then_success(&engine_client);
        engine_client.push_fork_choice_updated(ScriptedResponse::Ok(fcu(
            PayloadStatusEnum::Invalid { validation_error: "scripted".to_string() },
            None,
        )));
        let asserter = Asserter::new();
        asserter.push_success(&Option::<()>::None);
        asserter.push_success(&Option::<()>::None);
        let (orchestrator, _handle, notification_tx) = test_orchestrator(
            database.clone(),
            engine_client.clone(),
            asserter,
            BatchDerivationResult {
                attributes: vec![DerivedAttributes {
                    block_number: SAFE + 1,
                    attributes: ScrollPayloadAttributes::default(),
                }],
                batch_info,
                skipped_l1_messages: vec![],
                target_status: BatchStatus::Consolidated,
            },
        )
        .await;

        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let run_task = tokio::spawn(orchestrator.run_until_shutdown(Box::pin(async move {
            let _ = shutdown_rx.await;
        })));

        time::timeout(Duration::from_secs(2), async {
            while engine_client.fork_choice_updated_calls() < 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("first Engine SYNCING response must hold the batch");

        notification_tx.send(Arc::new(L1Notification::Reorg(5))).await.unwrap();

        let result = time::timeout(Duration::from_secs(5), run_task)
            .await
            .expect("an INVALID post-unwind FCU must terminate the run loop")
            .unwrap();
        assert!(
            matches!(result, Err(ChainOrchestratorError::FatalStateDivergence(_))),
            "expected FatalStateDivergence, got {result:?}"
        );
    }

    /// A batch already `Finalized` in the database (the marker FCU was lost
    /// to a crash or transport error) must have its finalized marker
    /// recomputed and reissued when a finalized notification replays — and a
    /// second replay must be an idempotent skip once the mirror caught up.
    #[tokio::test]
    async fn l1_finalization_replay_reissues_marker_fcu_once() {
        let database = Arc::new(setup_test_db().await);
        database.insert_genesis_block(B256::ZERO).await.unwrap();
        // The dev migration seeded the Scroll dev genesis row beside the
        // test genesis; drop it so height-0 queries are deterministic.
        database.delete_mismatched_genesis_blocks(B256::ZERO).await.unwrap();
        let batch_info = BatchInfo::new(1, B256::repeat_byte(1));
        database
            .insert_batch(BatchCommitData {
                hash: batch_info.hash,
                index: batch_info.index,
                block_number: 10,
                block_timestamp: 0,
                calldata: Arc::new(Bytes::new()),
                blob_versioned_hash: None,
                finalized_block_number: None,
                reverted_block_number: None,
            })
            .await
            .unwrap();
        database.update_batch_status(batch_info.hash, BatchStatus::Processing).await.unwrap();

        // The crash-window state: a batch whose rows are already `Finalized`
        // (database committed) while the engine's finalized mirror is still
        // at genesis (the marker FCU never landed).
        let finalized_batch = BatchInfo::new(2, B256::repeat_byte(2));
        database
            .insert_batch(BatchCommitData {
                hash: finalized_batch.hash,
                index: finalized_batch.index,
                block_number: 6,
                block_timestamp: 0,
                calldata: Arc::new(Bytes::new()),
                blob_versioned_hash: None,
                finalized_block_number: Some(6),
                reverted_block_number: None,
            })
            .await
            .unwrap();
        // Height 50 sits below the initial safe head: the phantom block must
        // not participate in the live safe-chain reconciliation (a block AT
        // the safe height with a different hash would).
        database.insert_blocks(vec![info(50, 0x22)], finalized_batch).await.unwrap();
        // insert_batch deliberately discards the struct's
        // finalized_block_number; set it through the real finalization API.
        database.finalize_batches_up_to_index(2, 6).await.unwrap();
        database.update_batch_status(finalized_batch.hash, BatchStatus::Finalized).await.unwrap();

        let engine_client = Arc::new(ScriptedEngineClient::new());
        script_syncing_hold_then_success(&engine_client);
        // One extra Valid FCU for the reissued finalized marker; the second
        // finalized notification must not consume anything.
        engine_client
            .push_fork_choice_updated(ScriptedResponse::Ok(fcu(PayloadStatusEnum::Valid, None)));
        let asserter = Asserter::new();
        asserter.push_success(&Option::<()>::None);
        asserter.push_success(&Option::<()>::None);
        let (mut orchestrator, _handle, notification_tx) = test_orchestrator(
            database.clone(),
            engine_client.clone(),
            asserter,
            BatchDerivationResult {
                attributes: vec![DerivedAttributes {
                    block_number: SAFE + 1,
                    attributes: ScrollPayloadAttributes::default(),
                }],
                batch_info,
                skipped_l1_messages: vec![],
                target_status: BatchStatus::Consolidated,
            },
        )
        .await;
        let mut events = orchestrator.event_listener();

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let run_task = tokio::spawn(orchestrator.run_until_shutdown(Box::pin(async move {
            let _ = shutdown_rx.await;
        })));

        time::timeout(Duration::from_secs(2), async {
            while engine_client.fork_choice_updated_calls() < 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("first Engine SYNCING response must hold the batch");

        notification_tx.send(Arc::new(L1Notification::Finalized(6))).await.unwrap();
        time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(ChainOrchestratorEvent::L1BlockFinalized(6, _)) = events.next().await {
                    break;
                }
            }
        })
        .await
        .expect("the first finalized notification must complete");
        // Consolidation consumed 3 FCUs (Syncing hold, attributes, head
        // commit); the replayed marker is the fourth.
        assert_eq!(
            engine_client.fork_choice_updated_calls(),
            4,
            "the replayed finalized marker must be reissued through one FCU"
        );
        assert_eq!(
            engine_client.fork_choice_updated_states()[3].finalized_block_hash,
            B256::repeat_byte(0x22),
            "the reissued marker must carry the database-derived finalized hash"
        );

        notification_tx.send(Arc::new(L1Notification::Finalized(7))).await.unwrap();
        time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(ChainOrchestratorEvent::L1BlockFinalized(7, _)) = events.next().await {
                    break;
                }
            }
        })
        .await
        .expect("the second finalized notification must complete");
        assert_eq!(
            engine_client.fork_choice_updated_calls(),
            4,
            "a caught-up mirror must skip the marker FCU"
        );

        let _ = shutdown_tx.send(());
        assert!(run_task.await.unwrap().is_ok());
    }

    /// Pins the pass-37 policy at the L1-reorg site: an unwind that rewinds
    /// the L2 head below the engine's FINALIZED block is irreconcilable —
    /// the run loop must fail-stop, not clamp.
    #[tokio::test]
    async fn l1_reorg_head_below_finalized_fail_stops() {
        let database = Arc::new(setup_test_db().await);
        database.insert_genesis_block(B256::ZERO).await.unwrap();
        // The dev migration seeded the Scroll dev genesis row beside the
        // test genesis; drop it so height-0 queries are deterministic.
        database.delete_mismatched_genesis_blocks(B256::ZERO).await.unwrap();
        let batch_info = BatchInfo::new(1, B256::repeat_byte(1));
        database
            .insert_batch(BatchCommitData {
                hash: batch_info.hash,
                index: batch_info.index,
                block_number: 10,
                block_timestamp: 0,
                calldata: Arc::new(Bytes::new()),
                blob_versioned_hash: None,
                finalized_block_number: None,
                reverted_block_number: None,
            })
            .await
            .unwrap();
        database.update_batch_status(batch_info.hash, BatchStatus::Processing).await.unwrap();
        // An L1 message above the reorg point, executed in an L2 block below
        // the engine's finalized block: the reorg unwind rewinds the head
        // below finality.
        database
            .insert_l1_message(L1MessageEnvelope::new(Default::default(), 6, Some(1), None))
            .await
            .unwrap();

        let engine_client = Arc::new(ScriptedEngineClient::new());
        script_syncing_hold_then_success(&engine_client);
        let asserter = Asserter::new();
        asserter.push_success(&Option::<()>::None);
        asserter.push_success(&Option::<()>::None);
        let (orchestrator, _handle, notification_tx) = test_orchestrator_with_fcs(
            database.clone(),
            engine_client.clone(),
            asserter,
            BatchDerivationResult {
                attributes: vec![DerivedAttributes {
                    block_number: SAFE + 1,
                    attributes: ScrollPayloadAttributes::default(),
                }],
                batch_info,
                skipped_l1_messages: vec![],
                target_status: BatchStatus::Consolidated,
            },
            ForkchoiceState::new(info(SAFE, 0x11), info(SAFE, 0x11), info(2, 0x0f)),
        )
        .await;

        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let run_task = tokio::spawn(orchestrator.run_until_shutdown(Box::pin(async move {
            let _ = shutdown_rx.await;
        })));

        time::timeout(Duration::from_secs(2), async {
            while engine_client.fork_choice_updated_calls() < 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("first Engine SYNCING response must hold the batch");

        notification_tx.send(Arc::new(L1Notification::Reorg(5))).await.unwrap();

        let result = time::timeout(Duration::from_secs(5), run_task)
            .await
            .expect("a below-finalized reorg unwind must terminate the run loop")
            .unwrap();
        assert!(
            matches!(result, Err(ChainOrchestratorError::FatalStateDivergence(_))),
            "expected FatalStateDivergence, got {result:?}"
        );
    }

    /// The administrative-unwind counterpart: a persisted head below the
    /// engine's finalized block fail-stops the run loop.
    #[tokio::test]
    async fn admin_unwind_persisted_head_below_finalized_fail_stops() {
        let database = Arc::new(setup_test_db().await);
        database.insert_genesis_block(B256::ZERO).await.unwrap();
        // The dev migration seeded the Scroll dev genesis row beside the
        // test genesis; drop it so height-0 queries are deterministic.
        database.delete_mismatched_genesis_blocks(B256::ZERO).await.unwrap();
        let batch_info = BatchInfo::new(1, B256::repeat_byte(1));
        database
            .insert_batch(BatchCommitData {
                hash: batch_info.hash,
                index: batch_info.index,
                block_number: 10,
                block_timestamp: 0,
                calldata: Arc::new(Bytes::new()),
                blob_versioned_hash: None,
                finalized_block_number: None,
                reverted_block_number: None,
            })
            .await
            .unwrap();
        database.update_batch_status(batch_info.hash, BatchStatus::Processing).await.unwrap();
        database.set_finalized_l1_block_number(5).await.unwrap();
        database.set_l2_head_block_number(1).await.unwrap();

        let engine_client = Arc::new(ScriptedEngineClient::new());
        engine_client
            .push_fork_choice_updated(ScriptedResponse::Ok(fcu(PayloadStatusEnum::Syncing, None)));
        let asserter = Asserter::new();
        asserter.push_success(&Option::<()>::None);
        asserter.push_success(&Option::<()>::None);
        let (orchestrator, handle, _notification_tx) = test_orchestrator_with_fcs(
            database.clone(),
            engine_client.clone(),
            asserter,
            BatchDerivationResult {
                attributes: vec![DerivedAttributes {
                    block_number: SAFE + 1,
                    attributes: ScrollPayloadAttributes::default(),
                }],
                batch_info,
                skipped_l1_messages: vec![],
                target_status: BatchStatus::Consolidated,
            },
            ForkchoiceState::new(info(SAFE, 0x11), info(SAFE, 0x11), info(2, 0x0f)),
        )
        .await;

        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let run_task = tokio::spawn(orchestrator.run_until_shutdown(Box::pin(async move {
            let _ = shutdown_rx.await;
        })));

        time::timeout(Duration::from_secs(2), async {
            while engine_client.fork_choice_updated_calls() < 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("first Engine SYNCING response must hold the batch");

        // The reply channel is dropped by the fail-stop; ignore the RecvError.
        tokio::spawn(async move {
            let _ = handle.revert_to_l1_block(10).await;
        });

        let result = time::timeout(Duration::from_secs(5), run_task)
            .await
            .expect("a below-finalized persisted head must terminate the run loop")
            .unwrap();
        assert!(
            matches!(result, Err(ChainOrchestratorError::FatalStateDivergence(_))),
            "expected FatalStateDivergence, got {result:?}"
        );
    }

    /// Pins pass-39 C1: the administrative unwind derives its FCU targets
    /// from ABSOLUTE persisted state, so a retryable refusal stays a refusal
    /// on every retry until the divergence is actually gone — the delta-
    /// derived version replied `true` on the retry while issuing no FCU at
    /// all.
    #[tokio::test]
    async fn admin_unwind_refusal_is_sticky_until_state_converges() {
        let database = Arc::new(setup_test_db().await);
        database.insert_genesis_block(B256::ZERO).await.unwrap();
        // The dev migration seeded the Scroll dev genesis row beside the
        // test genesis; drop it so height-0 queries are deterministic.
        database.delete_mismatched_genesis_blocks(B256::ZERO).await.unwrap();
        let batch_info = BatchInfo::new(1, B256::repeat_byte(1));
        database
            .insert_batch(BatchCommitData {
                hash: batch_info.hash,
                index: batch_info.index,
                block_number: 10,
                block_timestamp: 0,
                calldata: Arc::new(Bytes::new()),
                blob_versioned_hash: None,
                finalized_block_number: None,
                reverted_block_number: None,
            })
            .await
            .unwrap();
        database.update_batch_status(batch_info.hash, BatchStatus::Processing).await.unwrap();
        database.set_finalized_l1_block_number(5).await.unwrap();

        let engine_client = Arc::new(ScriptedEngineClient::new());
        script_syncing_hold_then_success(&engine_client);
        let asserter = Asserter::new();
        asserter.push_success(&Option::<()>::None);
        asserter.push_success(&Option::<()>::None);
        // Two head-lookup refusals: the persisted head diverges from the
        // mirror and the L2 client cannot resolve the block.
        asserter.push_success(&Option::<()>::None);
        asserter.push_success(&Option::<()>::None);
        let (mut orchestrator, handle, notification_tx) = test_orchestrator(
            database.clone(),
            engine_client.clone(),
            asserter,
            BatchDerivationResult {
                attributes: vec![DerivedAttributes {
                    block_number: SAFE + 1,
                    attributes: ScrollPayloadAttributes::default(),
                }],
                batch_info,
                skipped_l1_messages: vec![],
                target_status: BatchStatus::Consolidated,
            },
        )
        .await;
        let mut events = orchestrator.event_listener();

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let run_task = tokio::spawn(orchestrator.run_until_shutdown(Box::pin(async move {
            let _ = shutdown_rx.await;
        })));

        time::timeout(Duration::from_secs(2), async {
            while engine_client.fork_choice_updated_calls() < 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("first Engine SYNCING response must hold the batch");

        // Drive the held batch to full consolidation so no scripted
        // responses remain queued for it.
        notification_tx.send(Arc::new(L1Notification::Finalized(1))).await.unwrap();
        time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(ChainOrchestratorEvent::L1BlockFinalized(1, _)) = events.next().await {
                    break;
                }
            }
        })
        .await
        .expect("consolidation must complete");
        assert_eq!(engine_client.fork_choice_updated_calls(), 3);

        // Diverge the persisted head below the mirror (the crash-window /
        // half-completed-unwind state C1 describes).
        database.set_l2_head_block_number(SAFE).await.unwrap();

        // First revert: the head lookup fails (mocked None) — retryable
        // refusal, no FCU issued.
        let replied = handle.revert_to_l1_block(10).await.unwrap();
        assert!(!replied, "an unresolvable head must be a refusal");
        assert_eq!(engine_client.fork_choice_updated_calls(), 3, "no FCU on a refusal");

        // The re-issued revert deletes nothing, but the divergence is still
        // there: the retry must REFUSE again, not silently reply true.
        let replied = handle.revert_to_l1_block(10).await.unwrap();
        assert!(!replied, "the retry must stay a refusal while the mirror disagrees");
        assert_eq!(engine_client.fork_choice_updated_calls(), 3);

        // Converge the persisted state; the next revert is a clean no-op
        // success without any FCU.
        database.set_l2_head_block_number(SAFE + 1).await.unwrap();
        let replied = handle.revert_to_l1_block(10).await.unwrap();
        assert!(replied, "a converged revert must succeed");
        assert_eq!(engine_client.fork_choice_updated_calls(), 3, "converged state needs no FCU");

        let _ = shutdown_tx.send(());
        assert!(run_task.await.unwrap().is_ok());
    }

    /// Direct database pin for the pass-38 marker semantics: the finalized
    /// marker is recomputed over already-`Finalized` batches (the replay
    /// path) while only `Consolidated` rows have their status transitioned.
    #[tokio::test]
    async fn finalize_consolidated_batches_recomputes_over_finalized_rows() {
        let database = Arc::new(setup_test_db().await);
        database.insert_genesis_block(B256::ZERO).await.unwrap();
        // The dev migration seeded the Scroll dev genesis row beside the
        // test genesis; drop it so height-0 queries are deterministic.
        database.delete_mismatched_genesis_blocks(B256::ZERO).await.unwrap();
        // The genesis batch is marker-eligible, so the marker floor is the
        // genesis block even before any real batch finalizes (harmless in
        // production: it can never exceed the engine's finalized mirror).
        assert_eq!(
            database.finalize_consolidated_batches(10).await.unwrap(),
            Some(info(0, 0x00)),
            "the genesis batch is the initial marker floor"
        );

        let batch_info = BatchInfo::new(1, B256::repeat_byte(1));
        database
            .insert_batch(BatchCommitData {
                hash: batch_info.hash,
                index: batch_info.index,
                block_number: 6,
                block_timestamp: 0,
                calldata: Arc::new(Bytes::new()),
                blob_versioned_hash: None,
                finalized_block_number: Some(6),
                reverted_block_number: None,
            })
            .await
            .unwrap();
        database.insert_blocks(vec![info(3, 0x33)], batch_info).await.unwrap();
        // insert_batch deliberately discards the struct's
        // finalized_block_number; set it through the real finalization API.
        database.finalize_batches_up_to_index(1, 6).await.unwrap();
        database.update_batch_status(batch_info.hash, BatchStatus::Consolidated).await.unwrap();

        assert_eq!(
            database.finalize_consolidated_batches(10).await.unwrap(),
            Some(info(3, 0x33)),
            "a consolidated batch transitions and yields the marker"
        );
        assert_eq!(
            database.finalize_consolidated_batches(10).await.unwrap(),
            Some(info(3, 0x33)),
            "an already-finalized batch must still yield the marker on replay"
        );
        assert_eq!(
            database.finalize_consolidated_batches(5).await.unwrap(),
            Some(info(0, 0x00)),
            "batches above the finalized L1 bound are not eligible; the floor remains"
        );
    }

    /// The static dev migration seeds the Scroll dev genesis row; a custom
    /// chain's real genesis is inserted separately and CANNOT overwrite it
    /// (the insert's conflict key includes the hash). The startup
    /// reconciliation must delete the mismatched row so highest-block
    /// queries — the safe head among them — are deterministic.
    #[tokio::test]
    async fn mismatched_genesis_rows_are_reconciled() {
        let database = Arc::new(setup_test_db().await);
        // Two rows now sit at height 0: the migration's Scroll dev genesis
        // and this "custom chain" genesis.
        database.insert_genesis_block(B256::ZERO).await.unwrap();
        assert_eq!(
            database.delete_mismatched_genesis_blocks(B256::ZERO).await.unwrap(),
            1,
            "exactly the migration-seeded row must be removed"
        );
        let (safe, _) = database.get_latest_safe_l2_info().await.unwrap();
        assert_eq!(safe, info(0, 0x00), "the chain's genesis must be the safe fallback");
        assert_eq!(
            database.delete_mismatched_genesis_blocks(B256::ZERO).await.unwrap(),
            0,
            "reconciliation is idempotent"
        );
    }

    /// Pins the pass-44 P1: a POPULATED database written by the older
    /// custom-chain startup carries BOTH height-0 rows (that startup inserted
    /// the real genesis without removing the migration's). Upgrading such a
    /// node must drop the legacy duplicate and start, not abort as a
    /// wrong-chain database — `get_l2_block_info_by_number(0)` is unordered,
    /// so deciding on it would brick the upgrade nondeterministically.
    #[tokio::test]
    async fn populated_database_reconciles_a_legacy_genesis_duplicate() {
        let database = Arc::new(setup_test_db().await);
        // Both height-0 rows, exactly as the older startup left them, under
        // history that has already advanced past genesis.
        database.insert_genesis_block(B256::ZERO).await.unwrap();
        database.set_l2_head_block_number(7).await.unwrap();

        assert_eq!(
            database.reconcile_genesis_block(B256::ZERO, seeded_test_genesis()).await.unwrap(),
            1,
            "the legacy seeded row must be dropped, not treated as another chain's genesis"
        );
        assert_eq!(
            database.get_l2_block_info_by_number(0).await.unwrap(),
            Some(info(0, 0x00)),
            "only this chain's genesis may remain at height 0"
        );
        assert_eq!(
            database.reconcile_genesis_block(B256::ZERO, seeded_test_genesis()).await.unwrap(),
            0,
            "reconciliation is idempotent"
        );

        // With this chain's genesis absent AND the stored row not the seed, the
        // database really does belong to another chain: still a fail-stop.
        assert!(matches!(
            database.reconcile_genesis_block(B256::repeat_byte(0xAB), seeded_test_genesis()).await,
            Err(DatabaseError::GenesisMismatch { .. })
        ));
    }

    /// The migration's seeded genesis is NOT the chain spec's genesis — the dev
    /// migration hardcodes upstream Scroll's dev genesis while the dev chain
    /// spec computes its own (pinned by
    /// `genesis_seed_pairing_holds_for_shipped_chain_specs`).
    /// A database written before the genesis reconciliation existed therefore
    /// carries ONLY that seed at height 0, so treating an unrecognised height-0
    /// row as another chain's data would fail every such node at startup on
    /// upgrade. The seed must be reconciled in place instead.
    #[tokio::test]
    async fn populated_database_reconciles_a_migration_seeded_genesis() {
        let database = Arc::new(setup_test_db().await);
        // Exactly what the older binary left: the migration's seed alone,
        // under history that has already advanced past genesis.
        database.set_l2_head_block_number(7).await.unwrap();
        assert_eq!(
            database.get_l2_block_info_by_number(0).await.unwrap().map(|b| b.hash),
            Some(seeded_test_genesis()),
            "the fixture must start from the seeded row alone"
        );

        assert_eq!(
            database.reconcile_genesis_block(B256::ZERO, seeded_test_genesis()).await.unwrap(),
            1,
            "the seeded row must be replaced, not rejected as another chain's genesis"
        );
        assert_eq!(
            database.get_l2_block_info_by_number(0).await.unwrap(),
            Some(info(0, 0x00)),
            "this chain's genesis must be restored in the seed's place"
        );
        assert_eq!(
            database.reconcile_genesis_block(B256::ZERO, seeded_test_genesis()).await.unwrap(),
            0,
            "reconciliation is idempotent"
        );
    }

    /// The fourth `reconcile_genesis_block` outcome: a populated database with
    /// no height-0 row at all. Pass-45 m2 replaced a silent `Ok(0)` here
    /// precisely because returning success let startup run on into
    /// `get_latest_safe_l2_info`'s "there should always be at least the genesis
    /// block" expectation — an opaque panic, or a silently wrong safe baseline.
    #[tokio::test]
    async fn populated_database_without_a_genesis_row_is_reported_missing() {
        let database = Arc::new(setup_test_db().await);
        // Drop every height-0 row, including the migration's seed, then claim
        // history above it — a truncated or corrupt database.
        database.delete_mismatched_genesis_blocks(B256::repeat_byte(0xCD)).await.unwrap();
        database.set_l2_head_block_number(7).await.unwrap();
        assert_eq!(database.get_l2_block_info_by_number(0).await.unwrap(), None);

        assert!(
            matches!(
                database.reconcile_genesis_block(B256::ZERO, seeded_test_genesis()).await,
                Err(DatabaseError::GenesisMissing { .. })
            ),
            "a populated database with no genesis row must be reported, not silently accepted"
        );
        // Load-bearing ordering: the check runs BEFORE
        // delete_mismatched_genesis_blocks, so the failure leaves no writes.
        assert_eq!(database.get_l2_head_block_number().await.unwrap(), 7);
    }

    /// Pins the pass-44 P1: an L1 reorg that removes a batch revert restores
    /// each batch to the status its own rows can prove. A batch that had not
    /// been derived when the revert landed carries no L2 blocks and must
    /// return to `Committed` — the only status derivation ever picks up.
    /// Restoring it as `Consolidated` let `finalize_consolidated_batches` mark
    /// it `Finalized` while its blocks were never derived, silently dropping
    /// it from the finalized chain.
    #[tokio::test]
    async fn reverted_batches_restore_to_a_derivable_status() {
        let database = Arc::new(setup_test_db().await);
        database.insert_genesis_block(B256::ZERO).await.unwrap();
        database.delete_mismatched_genesis_blocks(B256::ZERO).await.unwrap();

        // A derived batch (its L2 block is on record) and a committed-only
        // batch (never derived, no L2 rows), both reverted at L1 block 20.
        let derived = BatchInfo::new(1, B256::repeat_byte(1));
        let blockless = BatchInfo::new(2, B256::repeat_byte(2));
        for batch in [derived, blockless] {
            database
                .insert_batch(BatchCommitData {
                    hash: batch.hash,
                    index: batch.index,
                    block_number: 6,
                    block_timestamp: 0,
                    calldata: Arc::new(Bytes::new()),
                    blob_versioned_hash: None,
                    finalized_block_number: Some(6),
                    reverted_block_number: None,
                })
                .await
                .unwrap();
        }
        database.insert_blocks(vec![info(3, 0x33)], derived).await.unwrap();
        // insert_batch deliberately discards the struct's
        // finalized_block_number; set it through the real finalization API.
        database.finalize_batches_up_to_index(2, 6).await.unwrap();
        database.set_batch_revert_block_number_for_batch_range(1, 2, info(20, 0x20)).await.unwrap();

        // The reorg removes the revert.
        assert_eq!(database.delete_batch_revert_gt_block_number(10).await.unwrap(), 2);
        assert_eq!(
            database.get_batch_status_by_hash(derived.hash).await.unwrap(),
            Some(BatchStatus::Consolidated),
            "a batch whose L2 blocks are on record returns to Consolidated"
        );
        assert_eq!(
            database.get_batch_status_by_hash(blockless.hash).await.unwrap(),
            Some(BatchStatus::Committed),
            "a batch with no L2 blocks returns to the derivation queue"
        );

        // Finalization must not swallow the underived batch...
        assert_eq!(
            database.finalize_consolidated_batches(10).await.unwrap(),
            Some(info(3, 0x33)),
            "the derived batch still yields the marker"
        );
        assert_eq!(
            database.get_batch_status_by_hash(blockless.hash).await.unwrap(),
            Some(BatchStatus::Committed),
            "the underived batch must NOT be marked Finalized"
        );
        // ...and the same finalization pass must queue it for derivation.
        assert_eq!(
            database.fetch_and_update_unprocessed_finalized_batches(10).await.unwrap(),
            vec![blockless],
            "the restored batch is picked up for derivation"
        );
    }

    /// Pins the pass-40 M2 check: a DB-derived finalized marker at the SAME
    /// height as the engine mirror but with a DIFFERENT hash is divergence
    /// at the finality boundary and must fail-stop, not trace-skip.
    #[tokio::test]
    async fn l1_finalization_marker_hash_divergence_fail_stops() {
        let database = Arc::new(setup_test_db().await);
        database.insert_genesis_block(B256::ZERO).await.unwrap();
        // The dev migration seeded the Scroll dev genesis row beside the
        // test genesis; drop it so height-0 queries are deterministic.
        database.delete_mismatched_genesis_blocks(B256::ZERO).await.unwrap();
        let batch_info = BatchInfo::new(1, B256::repeat_byte(1));
        database
            .insert_batch(BatchCommitData {
                hash: batch_info.hash,
                index: batch_info.index,
                block_number: 10,
                block_timestamp: 0,
                calldata: Arc::new(Bytes::new()),
                blob_versioned_hash: None,
                finalized_block_number: None,
                reverted_block_number: None,
            })
            .await
            .unwrap();
        database.update_batch_status(batch_info.hash, BatchStatus::Processing).await.unwrap();
        let finalized_batch = BatchInfo::new(2, B256::repeat_byte(2));
        database
            .insert_batch(BatchCommitData {
                hash: finalized_batch.hash,
                index: finalized_batch.index,
                block_number: 6,
                block_timestamp: 0,
                calldata: Arc::new(Bytes::new()),
                blob_versioned_hash: None,
                finalized_block_number: Some(6),
                reverted_block_number: None,
            })
            .await
            .unwrap();
        database.insert_blocks(vec![info(50, 0x22)], finalized_batch).await.unwrap();
        // insert_batch deliberately discards the struct's
        // finalized_block_number; set it through the real finalization API.
        database.finalize_batches_up_to_index(2, 6).await.unwrap();
        database.update_batch_status(finalized_batch.hash, BatchStatus::Finalized).await.unwrap();

        // No FCU is scripted: the empty held batch consolidates without one,
        // and the marker path fail-stops on the hash mismatch BEFORE issuing.
        let engine_client = Arc::new(ScriptedEngineClient::new());
        let asserter = Asserter::new();
        asserter.push_success(&Option::<()>::None);
        asserter.push_success(&Option::<()>::None);
        // The engine mirror's finalized block sits at the marker's height
        // (50) with a DIFFERENT hash.
        let (orchestrator, _handle, notification_tx) = test_orchestrator_with_fcs(
            database.clone(),
            engine_client.clone(),
            asserter,
            BatchDerivationResult {
                // Empty attributes: the held batch consolidates without
                // contributing an L2 block, so the finalized_batch's
                // block(50) is the sole marker-eligible block and the marker
                // resolves to exactly the mirror's finalized height.
                attributes: vec![],
                batch_info,
                skipped_l1_messages: vec![],
                target_status: BatchStatus::Consolidated,
            },
            ForkchoiceState::new(info(SAFE, 0x11), info(SAFE, 0x11), info(50, 0x0f)),
        )
        .await;

        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let run_task = tokio::spawn(orchestrator.run_until_shutdown(Box::pin(async move {
            let _ = shutdown_rx.await;
        })));

        notification_tx.send(Arc::new(L1Notification::Finalized(6))).await.unwrap();

        let result = time::timeout(Duration::from_secs(5), run_task)
            .await
            .expect("a same-height different-hash marker must terminate the run loop")
            .unwrap();
        assert!(
            matches!(result, Err(ChainOrchestratorError::FatalStateDivergence(_))),
            "expected FatalStateDivergence, got {result:?}"
        );
    }

    /// Pins the admin revert's safe-FCU branches: a SYNCING answer is a
    /// retryable refusal (reply false, mirror uncommitted), the retry
    /// reissues the SAME absolute target and converges on VALID, and a
    /// converged third call needs no FCU. Also asserts WHAT was issued: the
    /// genesis safe target the persisted state dictates.
    #[tokio::test]
    async fn admin_unwind_syncing_safe_fcu_refuses_then_converges() {
        let database = Arc::new(setup_test_db().await);
        database.insert_genesis_block(B256::ZERO).await.unwrap();
        // The dev migration seeded the Scroll dev genesis row beside the
        // test genesis; drop it so height-0 queries are deterministic.
        database.delete_mismatched_genesis_blocks(B256::ZERO).await.unwrap();
        let batch_info = BatchInfo::new(1, B256::repeat_byte(1));
        database
            .insert_batch(BatchCommitData {
                hash: batch_info.hash,
                index: batch_info.index,
                block_number: 10,
                block_timestamp: 0,
                calldata: Arc::new(Bytes::new()),
                blob_versioned_hash: None,
                finalized_block_number: None,
                reverted_block_number: None,
            })
            .await
            .unwrap();
        database.update_batch_status(batch_info.hash, BatchStatus::Processing).await.unwrap();
        // Revert target 0 sits below batch 1's commit block, so the unwind
        // discards the held batch (no reconciliation retry can race the
        // scripted FCU queue) and the absolute safe falls to genesis.
        database.set_l2_head_block_number(SAFE).await.unwrap();

        let engine_client = Arc::new(ScriptedEngineClient::new());
        engine_client
            .push_fork_choice_updated(ScriptedResponse::Ok(fcu(PayloadStatusEnum::Syncing, None)));
        engine_client
            .push_fork_choice_updated(ScriptedResponse::Ok(fcu(PayloadStatusEnum::Syncing, None)));
        let asserter = Asserter::new();
        asserter.push_success(&Option::<()>::None);
        asserter.push_success(&Option::<()>::None);
        let (orchestrator, handle, _notification_tx) = test_orchestrator(
            database.clone(),
            engine_client.clone(),
            asserter,
            BatchDerivationResult {
                attributes: vec![DerivedAttributes {
                    block_number: SAFE + 1,
                    attributes: ScrollPayloadAttributes::default(),
                }],
                batch_info,
                skipped_l1_messages: vec![],
                target_status: BatchStatus::Consolidated,
            },
        )
        .await;

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let run_task = tokio::spawn(orchestrator.run_until_shutdown(Box::pin(async move {
            let _ = shutdown_rx.await;
        })));

        time::timeout(Duration::from_secs(2), async {
            while engine_client.fork_choice_updated_calls() < 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("first Engine SYNCING response must hold the batch");

        // SYNCING on the safe FCU: retryable refusal, mirror uncommitted.
        let replied = handle.revert_to_l1_block(0).await.unwrap();
        assert!(!replied, "SYNCING must be a retryable refusal");
        assert_eq!(engine_client.fork_choice_updated_calls(), 2);
        let issued = engine_client.fork_choice_updated_states();
        assert_eq!(
            issued[1].safe_block_hash,
            B256::ZERO,
            "the refused FCU must carry the persisted (genesis) safe target"
        );
        assert_eq!(
            issued[1].head_block_hash,
            B256::repeat_byte(0x11),
            "the head must not move on a safe-only divergence"
        );

        // The retry reissues the SAME absolute target and converges.
        engine_client
            .push_fork_choice_updated(ScriptedResponse::Ok(fcu(PayloadStatusEnum::Valid, None)));
        let replied = handle.revert_to_l1_block(0).await.unwrap();
        assert!(replied, "a VALID retry must succeed");
        assert_eq!(engine_client.fork_choice_updated_calls(), 3);
        assert_eq!(engine_client.fork_choice_updated_states()[2].safe_block_hash, B256::ZERO);

        // Converged: no further FCU.
        let replied = handle.revert_to_l1_block(0).await.unwrap();
        assert!(replied, "a converged revert is a clean no-op success");
        assert_eq!(engine_client.fork_choice_updated_calls(), 3);

        let _ = shutdown_tx.send(());
        assert!(run_task.await.unwrap().is_ok());
    }

    /// The INVALID counterpart: the engine actively rejects the safe target
    /// after a durable unwind — the run loop must fail-stop via `FcuRejected`.
    #[tokio::test]
    async fn admin_unwind_invalid_safe_fcu_fail_stops() {
        let database = Arc::new(setup_test_db().await);
        database.insert_genesis_block(B256::ZERO).await.unwrap();
        // The dev migration seeded the Scroll dev genesis row beside the
        // test genesis; drop it so height-0 queries are deterministic.
        database.delete_mismatched_genesis_blocks(B256::ZERO).await.unwrap();
        let batch_info = BatchInfo::new(1, B256::repeat_byte(1));
        database
            .insert_batch(BatchCommitData {
                hash: batch_info.hash,
                index: batch_info.index,
                block_number: 10,
                block_timestamp: 0,
                calldata: Arc::new(Bytes::new()),
                blob_versioned_hash: None,
                finalized_block_number: None,
                reverted_block_number: None,
            })
            .await
            .unwrap();
        database.update_batch_status(batch_info.hash, BatchStatus::Processing).await.unwrap();
        database.set_l2_head_block_number(SAFE).await.unwrap();

        let engine_client = Arc::new(ScriptedEngineClient::new());
        engine_client
            .push_fork_choice_updated(ScriptedResponse::Ok(fcu(PayloadStatusEnum::Syncing, None)));
        engine_client.push_fork_choice_updated(ScriptedResponse::Ok(fcu(
            PayloadStatusEnum::Invalid { validation_error: "scripted".to_string() },
            None,
        )));
        let asserter = Asserter::new();
        asserter.push_success(&Option::<()>::None);
        asserter.push_success(&Option::<()>::None);
        let (orchestrator, handle, _notification_tx) = test_orchestrator(
            database.clone(),
            engine_client.clone(),
            asserter,
            BatchDerivationResult {
                attributes: vec![DerivedAttributes {
                    block_number: SAFE + 1,
                    attributes: ScrollPayloadAttributes::default(),
                }],
                batch_info,
                skipped_l1_messages: vec![],
                target_status: BatchStatus::Consolidated,
            },
        )
        .await;

        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let run_task = tokio::spawn(orchestrator.run_until_shutdown(Box::pin(async move {
            let _ = shutdown_rx.await;
        })));

        time::timeout(Duration::from_secs(2), async {
            while engine_client.fork_choice_updated_calls() < 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("first Engine SYNCING response must hold the batch");

        // The reply channel is dropped by the fail-stop; ignore the error.
        tokio::spawn(async move {
            let _ = handle.revert_to_l1_block(0).await;
        });

        let result = time::timeout(Duration::from_secs(5), run_task)
            .await
            .expect("an INVALID admin safe FCU must terminate the run loop")
            .unwrap();
        assert!(
            matches!(result, Err(ChainOrchestratorError::FcuRejected(_))),
            "expected FcuRejected, got {result:?}"
        );
    }

    /// Pins the pass-42 compare-and-swap precondition on the administrative
    /// head update: a stale observed head refuses without touching the
    /// engine, a matching one proceeds.
    #[tokio::test]
    async fn update_fcs_head_precondition_rejects_moved_head() {
        let database = Arc::new(setup_test_db().await);
        database.insert_genesis_block(B256::ZERO).await.unwrap();
        // The dev migration seeded the Scroll dev genesis row beside the
        // test genesis; drop it so height-0 queries are deterministic.
        database.delete_mismatched_genesis_blocks(B256::ZERO).await.unwrap();
        let batch_info = BatchInfo::new(1, B256::repeat_byte(1));
        database
            .insert_batch(BatchCommitData {
                hash: batch_info.hash,
                index: batch_info.index,
                block_number: 10,
                block_timestamp: 0,
                calldata: Arc::new(Bytes::new()),
                blob_versioned_hash: None,
                finalized_block_number: None,
                reverted_block_number: None,
            })
            .await
            .unwrap();
        database.update_batch_status(batch_info.hash, BatchStatus::Processing).await.unwrap();
        let engine_client = Arc::new(ScriptedEngineClient::new());
        engine_client
            .push_fork_choice_updated(ScriptedResponse::Ok(fcu(PayloadStatusEnum::Syncing, None)));
        let asserter = Asserter::new();
        asserter.push_success(&Option::<()>::None);
        asserter.push_success(&Option::<()>::None);
        let (orchestrator, handle, _notification_tx) = test_orchestrator_with_fcs(
            database.clone(),
            engine_client.clone(),
            asserter,
            BatchDerivationResult {
                attributes: vec![DerivedAttributes {
                    block_number: SAFE + 1,
                    attributes: ScrollPayloadAttributes::default(),
                }],
                batch_info,
                skipped_l1_messages: vec![],
                target_status: BatchStatus::Consolidated,
            },
            // Head ABOVE safe, so a rewind target can still sit at or above the
            // safe mirror: with head == safe there is no target that is both a
            // rewind and not a HeadBelowSafe refusal.
            ForkchoiceState::new(info(SAFE + 10, 0x11), info(SAFE, 0x11), info(0, 0x00)),
        )
        .await;

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let run_task = tokio::spawn(orchestrator.run_until_shutdown(Box::pin(async move {
            let _ = shutdown_rx.await;
        })));

        time::timeout(Duration::from_secs(2), async {
            while engine_client.fork_choice_updated_calls() < 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("first Engine SYNCING response must hold the batch");

        // Stale observed head: refused before any FCU.
        let refusal = handle
            .update_fcs_head_if_unmoved(info(50, 0x22), Some(info(99, 0x33)))
            .await
            .unwrap()
            .expect_err("a moved head must refuse the update");
        assert!(refusal.contains("head moved"), "unexpected refusal text: {refusal}");
        assert_eq!(engine_client.fork_choice_updated_calls(), 1, "no FCU on a CAS refusal");

        // A FORWARD target is refused whatever the CAS says: this command only
        // rewinds, and moving forward would purge mappings only above the
        // target while advancing the anchor over blocks whose L1 messages were
        // never stamped.
        let refusal = handle
            .update_fcs_head_if_unmoved(info(SAFE + 20, 0x22), Some(info(SAFE + 10, 0x11)))
            .await
            .unwrap()
            .expect_err("a forward head move must refuse");
        assert!(refusal.contains("only rewinds"), "unexpected refusal text: {refusal}");
        assert_eq!(engine_client.fork_choice_updated_calls(), 1, "no FCU on a direction refusal");

        // Matching observed head, rewinding to a target still at or above the
        // safe mirror: proceeds through the checked FCU.
        engine_client
            .push_fork_choice_updated(ScriptedResponse::Ok(fcu(PayloadStatusEnum::Valid, None)));
        handle
            .update_fcs_head_if_unmoved(info(SAFE + 5, 0x22), Some(info(SAFE + 10, 0x11)))
            .await
            .unwrap()
            .expect("a matching observed head must proceed");
        assert_eq!(engine_client.fork_choice_updated_calls(), 2);
        assert_eq!(
            engine_client.fork_choice_updated_states()[1].head_block_hash,
            B256::repeat_byte(0x22)
        );

        let _ = shutdown_tx.send(());
        assert!(run_task.await.unwrap().is_ok());
    }

    /// Pins the pass-42 joint raise: after an EL-side marker rollback (safe
    /// and finalized mirrors below database finality, head still covering
    /// the marker), a replayed finalized notification raises safe TOGETHER
    /// with finalized — deferring would stall forever on a quiescent chain.
    #[tokio::test]
    async fn l1_finalization_joint_safe_raise_after_marker_regression() {
        let database = Arc::new(setup_test_db().await);
        database.insert_genesis_block(B256::ZERO).await.unwrap();
        database.delete_mismatched_genesis_blocks(B256::ZERO).await.unwrap();
        let batch_info = BatchInfo::new(1, B256::repeat_byte(1));
        database
            .insert_batch(BatchCommitData {
                hash: batch_info.hash,
                index: batch_info.index,
                block_number: 1,
                block_timestamp: 0,
                calldata: Arc::new(Bytes::new()),
                blob_versioned_hash: None,
                finalized_block_number: None,
                reverted_block_number: None,
            })
            .await
            .unwrap();
        database.update_batch_status(batch_info.hash, BatchStatus::Processing).await.unwrap();
        let finalized_batch = BatchInfo::new(2, B256::repeat_byte(2));
        database
            .insert_batch(BatchCommitData {
                hash: finalized_batch.hash,
                index: finalized_batch.index,
                block_number: 6,
                block_timestamp: 0,
                calldata: Arc::new(Bytes::new()),
                blob_versioned_hash: None,
                finalized_block_number: Some(6),
                reverted_block_number: None,
            })
            .await
            .unwrap();
        database.insert_blocks(vec![info(50, 0x22)], finalized_batch).await.unwrap();
        database.finalize_batches_up_to_index(2, 6).await.unwrap();
        database.update_batch_status(finalized_batch.hash, BatchStatus::Finalized).await.unwrap();

        let engine_client = Arc::new(ScriptedEngineClient::new());
        // The held batch consolidates trivially (no attributes), so the only
        // scripted FCU is the joint marker raise.
        engine_client
            .push_fork_choice_updated(ScriptedResponse::Ok(fcu(PayloadStatusEnum::Valid, None)));
        let asserter = Asserter::new();
        asserter.push_success(&Option::<()>::None);
        asserter.push_success(&Option::<()>::None);
        // Safe and finalized mirrors regressed below the marker (50); the
        // head still covers it.
        let (mut orchestrator, _handle, notification_tx) = test_orchestrator_with_fcs(
            database.clone(),
            engine_client.clone(),
            asserter,
            BatchDerivationResult {
                attributes: vec![],
                batch_info,
                skipped_l1_messages: vec![],
                target_status: BatchStatus::Consolidated,
            },
            ForkchoiceState::new(info(SAFE, 0x11), info(45, 0x0d), info(40, 0x0e)),
        )
        .await;
        let mut events = orchestrator.event_listener();

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let run_task = tokio::spawn(orchestrator.run_until_shutdown(Box::pin(async move {
            let _ = shutdown_rx.await;
        })));

        notification_tx.send(Arc::new(L1Notification::Finalized(6))).await.unwrap();
        time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(ChainOrchestratorEvent::L1BlockFinalized(6, _)) = events.next().await {
                    break;
                }
            }
        })
        .await
        .expect("the finalized notification must complete");

        assert_eq!(engine_client.fork_choice_updated_calls(), 1);
        let issued = engine_client.fork_choice_updated_states();
        assert_eq!(
            issued[0].finalized_block_hash,
            B256::repeat_byte(0x22),
            "the marker must be raised"
        );
        assert_eq!(
            issued[0].safe_block_hash,
            B256::repeat_byte(0x22),
            "safe must ride along with the replayed marker"
        );
        assert_eq!(issued[0].head_block_hash, B256::repeat_byte(0x11), "the head must not move");

        // A second notification is an idempotent skip.
        notification_tx.send(Arc::new(L1Notification::Finalized(7))).await.unwrap();
        time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(ChainOrchestratorEvent::L1BlockFinalized(7, _)) = events.next().await {
                    break;
                }
            }
        })
        .await
        .expect("the second finalized notification must complete");
        assert_eq!(engine_client.fork_choice_updated_calls(), 1);

        let _ = shutdown_tx.send(());
        assert!(run_task.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn administrative_post_unwind_engine_failure_fail_stops_without_retry() {
        let database = Arc::new(setup_test_db().await);
        database.insert_genesis_block(B256::ZERO).await.unwrap();
        // The dev migration seeded the Scroll dev genesis row beside the
        // test genesis; drop it so height-0 queries are deterministic.
        database.delete_mismatched_genesis_blocks(B256::ZERO).await.unwrap();
        let batch_info = BatchInfo::new(1, B256::repeat_byte(1));
        database
            .insert_batch(BatchCommitData {
                hash: batch_info.hash,
                index: batch_info.index,
                block_number: 1,
                block_timestamp: 0,
                calldata: Arc::new(Bytes::new()),
                blob_versioned_hash: None,
                finalized_block_number: None,
                reverted_block_number: None,
            })
            .await
            .unwrap();
        database.update_batch_status(batch_info.hash, BatchStatus::Processing).await.unwrap();
        database
            .insert_batch(BatchCommitData {
                hash: B256::repeat_byte(2),
                index: 2,
                block_number: 10,
                block_timestamp: 0,
                calldata: Arc::new(Bytes::new()),
                blob_versioned_hash: None,
                finalized_block_number: None,
                reverted_block_number: None,
            })
            .await
            .unwrap();
        // Align the persisted head with the engine mirror so the absolute
        // derivation skips the head lookup and the divergence is safe-only:
        // deleting batch 2 leaves the persisted safe at genesis, below the
        // mirror's safe — the FCU under test.
        database.set_l2_head_block_number(SAFE).await.unwrap();
        let engine_client = Arc::new(ScriptedEngineClient::new());
        engine_client
            .push_fork_choice_updated(ScriptedResponse::Ok(fcu(PayloadStatusEnum::Syncing, None)));
        engine_client.push_fork_choice_updated(ScriptedResponse::TransportFailure);
        let asserter = Asserter::new();
        asserter.push_success(&Option::<()>::None);
        let (orchestrator, handle, _notification_tx) = test_orchestrator(
            database.clone(),
            engine_client.clone(),
            asserter,
            BatchDerivationResult {
                attributes: vec![DerivedAttributes {
                    block_number: SAFE + 1,
                    attributes: ScrollPayloadAttributes::default(),
                }],
                batch_info,
                skipped_l1_messages: vec![],
                target_status: BatchStatus::Consolidated,
            },
        )
        .await;

        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let run_task = tokio::spawn(orchestrator.run_until_shutdown(Box::pin(async move {
            let _ = shutdown_rx.await;
        })));
        time::timeout(Duration::from_secs(2), async {
            loop {
                let status = handle.status().await.unwrap();
                if matches!(
                    status.derivation,
                    DerivationStatus::Held(HeldBatchStatus { attempts_started: 1, .. })
                ) {
                    break
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("first Engine SYNCING response must hold the batch");

        let revert_task = tokio::spawn(async move { handle.revert_to_l1_block(5).await });
        let result = time::timeout(Duration::from_secs(2), run_task)
            .await
            .expect("post-unwind Engine failure must terminate the run loop")
            .unwrap();
        assert!(revert_task.await.unwrap().is_err());
        assert!(matches!(result, Err(ChainOrchestratorError::EngineError(_))));
        assert_eq!(engine_client.fork_choice_updated_calls(), 2);
        assert_eq!(
            database.get_batch_status_by_hash(batch_info.hash).await.unwrap(),
            Some(BatchStatus::Processing),
            "the surviving held row must remain for restart recovery"
        );
        assert!(database.get_batch_by_index(2).await.unwrap().is_none());
    }
}
