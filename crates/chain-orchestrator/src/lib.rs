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
use scroll_engine::{Engine, ScrollEngineApi};
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
    /// (not just the command arm) actually stops the run loop.
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
                if let Err(err) = self
                    .database
                    .tx_mut(move |tx| async move {
                        tx.set_l2_head_block_number(block.header.number).await?;
                        tx.insert_signature(hash, signature).await
                    })
                    .await
                {
                    // The signed block sits at the engine head but its head
                    // number and signature could not be persisted — a restart
                    // would rewind past it and peers would never see it
                    // (announce below never runs). Same condition
                    // UpdateFcsHead treats as fatal.
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
                        // spurious cancellation event harmless, and symmetry
                        // keeps the metric and the event stream 1:1.
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
                        // blocks. Same condition UpdateFcsHead treats as
                        // fatal.
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
                // normally, and the job-invalidating transitions
                // (optimistic sync, L1 reorg/unwind, sequencing disabled,
                // chain import, batch reconciliation head moves) cancel it
                // observably. A held derivation attempt keeps the
                // gate closed without cancelling; the parked job then simply
                // resumes when the hold resolves (its head snapshot check
                // cancels it if the head moved). Rejecting here instead was
                // tried and races startup: a build command can legitimately
                // arrive before the L1-synced notification is processed.
                if let Some(sequencer) = self.sequencer.as_mut() {
                    // Coalesce with an in-flight job instead of silently
                    // replacing it: a replaced job discards engine work and
                    // makes block numbering timing-dependent when the build
                    // timer and manual triggers race (issue #38). The
                    // in-flight job normally emits BlockSequenced or
                    // BlockBuildingSkipped; if it is cancelled instead (L1
                    // reorg, chain import, administrative FCS head update, or
                    // sequencing disabled), PayloadBuildingJobCancelled is
                    // emitted so waiters can fail fast — they should still
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
            ChainOrchestratorCommand::UpdateFcsHead((head, sender)) => {
                // Collect transactions of reverted blocks from l2 client.
                let reverted_transactions = self
                    .collect_reverted_txs_in_range(
                        head.number.saturating_add(1),
                        self.engine.fcs().head_block_info().number,
                    )
                    .await?;
                let previous_head = *self.engine.fcs().head_block_info();
                let forward = self.engine.update_fcs_checked(Some(head), None, None).await?;
                if !forward.is_valid() {
                    // update_fcs_checked commits its mirror only on VALID, so
                    // nothing has been mutated — refuse before purging
                    // mappings, persisting the unapplied head, or cancelling
                    // a valid job. SYNCING (the EL has not adopted the head)
                    // must not proceed either: the rollback below refuses it
                    // for the same reason.
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
                    return Err(err.into());
                }

                // Add all reverted transactions to the transaction pool. Done
                // only after persistence succeeds: on the failure path above
                // the head is rolled back, the reverted blocks stay canonical,
                // and their transactions must not re-enter the pool.
                self.reinsert_txs_into_pool(reverted_transactions).await;
                self.notify(ChainOrchestratorEvent::FcsHeadUpdated(head));
                let _ = sender.send(());
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
                self.sync_state.l1_mut().set_syncing();
                // The gate is now closed for the whole administrative re-scan:
                // a parked job could not be polled to completion, later
                // BuildBlocks would coalesce into it with no outcome ever
                // arriving, and (for a job with an L1 origin above the unwind
                // point) the unwind deletes L1 messages it depends on. Cancel
                // unconditionally before any fallible work.
                self.cancel_payload_building_job("administrative L1 unwind");
                self.derivation_driver.cancel_attempt();
                let (unwind_result, held_outcome) =
                    self.unwind_and_revalidate_held_batch(block_number).await?;

                // Check if the unwind impacts the fcs safe head.
                if let Some(block_info) = unwind_result.l2_safe_block_info {
                    // If the new safe head is above the current finalized head, update the fcs safe
                    // head to the new safe head; otherwise move it to the finalized head.
                    let target =
                        if block_info.number >= self.engine.fcs().finalized_block_info().number {
                            block_info
                        } else {
                            *self.engine.fcs().finalized_block_info()
                        };
                    let result = self.engine.update_fcs(None, Some(target), None).await?;
                    if result.is_invalid() {
                        // Replying success while the engine refused the safe
                        // head would misreport the unwind; the run loop
                        // fail-stops on any error from this command.
                        return Err(ChainOrchestratorError::FcuRejected(
                            "administrative unwind safe-head update rejected as INVALID by \
                             the engine",
                        ));
                    }
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
                let result = self
                    .import_chain(vec![block_with_peer.block.clone()], block_with_peer)
                    .await
                    .map_err(|e| e.to_string());
                let _ = response.send(result);
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
    /// administrative revert, and on an L1 reorg when the job carries L1
    /// messages or the head moved); or sequencing being disabled.
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
        self.derivation_driver.cancel_attempt();
        let (unwind_result, held_outcome) =
            self.unwind_and_revalidate_held_batch(block_number).await?;
        let UnwindResult { l1_block_number, queue_index, l2_head_block_number, l2_safe_block_info } =
            unwind_result;

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

        // If the inflight payload building job carries any L1 messages, cancel it: the unwind
        // may have deleted messages the job depends on, and `l1_origin` records only the FIRST
        // included message's L1 block, so a range comparison against it can miss messages from
        // later L1 blocks in the same payload.
        if self
            .sequencer
            .as_ref()
            .and_then(|s| s.payload_building_job().map(|p| p.l1_origin()))
            .flatten()
            .is_some()
        {
            self.cancel_payload_building_job("L1 reorg while the job carries L1 messages");
        }

        // TODO: Add retry logic
        if l2_head_block_info.is_some() || l2_safe_block_info.is_some() {
            // The L1 database is already unwound by this point: an FCU that
            // does not apply leaves the engine on the reorged-out chain, no
            // L1Reorg event emitted, reverted transactions not reinserted.
            match self.engine.update_fcs(l2_head_block_info, l2_safe_block_info, None).await {
                Ok(result) if !result.is_invalid() => {}
                Ok(_) => {
                    tracing::error!(
                        target: "scroll::chain_orchestrator",
                        ?l2_head_block_info,
                        ?l2_safe_block_info,
                        "L1-reorg FCU rejected as INVALID AFTER the L1 database unwind; \
                         engine and database now diverge"
                    );
                    // No in-process compensation exists here; a restart
                    // re-derives from the (unwound) database and converges.
                    return Err(ChainOrchestratorError::FatalStateDivergence(
                        "post-unwind L1-reorg forkchoice update rejected as INVALID; \
                         restart re-converges from the persisted state",
                    ));
                }
                Err(err) => {
                    // Same divergence as the INVALID arm above — the L1 unwind
                    // is already committed — and a transport error (engine
                    // restart, socket reset) is the more common shape in
                    // production. One policy for both.
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
        // after the FCU so a failed update does not destroy a valid job (a
        // job carrying L1 messages was already cancelled above). The
        // single-task run loop means the job cannot complete in between.
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
            // Cancel any in-flight payload building job now that the head
            // jumped far ahead — after the FCU, and only when the engine
            // accepted it, so a transient engine error or an INVALID result
            // (head unchanged in both cases) does not destroy a valid job.
            // Mirrors the ordering at the other head-moving sites; the
            // single-task run loop means the job cannot complete in between.
            // Cancelling matters even though a parked job could not complete
            // anyway — set_syncing() closes the sequencer arm's gate, and the
            // path back to synced cancels first — because leaving it in the
            // slot would make every BuildBlock during the sync window
            // coalesce into a job that can never emit an outcome.
            if !result.is_invalid() {
                self.cancel_payload_building_job("optimistic sync moved the head");
            }
            self.sync_state.l2_mut().set_syncing();

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

        // The head may have moved (and on a SYNCING result the engine's view
        // is unknown): cancel any in-flight payload building job
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
    use rollup_node_primitives::BatchCommitData;
    use rollup_node_providers::{test_utils::MockL1Provider, ScrollRootProvider};
    use scroll_db::test_utils::setup_test_db;
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
        let engine = Engine::new(
            engine_client,
            ForkchoiceState::new(info(SAFE, 0x11), info(SAFE, 0x11), info(0, 0)),
        );
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

    #[tokio::test]
    async fn administrative_post_unwind_engine_failure_fail_stops_without_retry() {
        let database = Arc::new(setup_test_db().await);
        database.insert_genesis_block(B256::ZERO).await.unwrap();
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
