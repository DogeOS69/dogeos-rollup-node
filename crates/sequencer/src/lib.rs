//! This library contains the sequencer, which is responsible for sequencing transactions and
//! producing new blocks.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use alloy_eips::eip2718::Encodable2718;
use alloy_primitives::U256;
use alloy_rpc_types_engine::{ExecutionData, PayloadAttributes, PayloadId};
use dogeos_hardforks::DogeosHardforks;
use dogeos_reth_engine::{BlockDataHint, ScrollPayloadAttributes};
use dogeos_reth_primitives::{DogeosBlock, ScrollTransactionSigned};
use futures::{task::AtomicWaker, Stream};
use rollup_node_primitives::{BlockInfo, DEFAULT_BLOCK_DIFFICULTY};
use rollup_node_providers::{L1MessageProvider, L1ProviderError};
use scroll_engine::{Engine, ScrollEngineApi};
use tokio::time::Interval;

mod config;
pub use config::{L1MessageInclusionMode, PayloadBuildingConfig, SequencerConfig};

mod error;
pub use error::SequencerError;

mod event;
pub use event::SequencerEvent;

mod metrics;
pub use metrics::SequencerMetrics;

/// A type alias for the payload building job future.
pub type PayloadBuildingJobFuture = Pin<Box<dyn Future<Output = PayloadId> + Send + Sync>>;

/// The sequencer is responsible for sequencing transactions and producing new blocks.
pub struct Sequencer<P, CS> {
    /// A reference to the provider.
    provider: Arc<P>,
    /// The configuration for the sequencer.
    config: SequencerConfig<CS>,
    /// The interval trigger for building a new block.
    trigger: Option<Interval>,
    /// The inflight payload building job
    payload_building_job: Option<PayloadBuildingJob>,
    /// Recovery for failed L1 message candidates on the current parent.
    l1_message_recovery: L1MessageBuildRecovery,
    /// The sequencer metrics.
    metrics: SequencerMetrics,
    /// A waker to notify when the Sequencer should be polled.
    waker: AtomicWaker,
}

impl<P, CS> Sequencer<P, CS>
where
    P: L1MessageProvider + Unpin + Send + Sync + 'static,
    CS: DogeosHardforks,
{
    /// Creates a new sequencer.
    pub fn new(provider: Arc<P>, config: SequencerConfig<CS>) -> Self {
        Self {
            provider,
            trigger: config.auto_start.then(|| delayed_interval(config.block_time)),
            config,
            payload_building_job: None,
            l1_message_recovery: L1MessageBuildRecovery::default(),
            metrics: SequencerMetrics::default(),
            waker: AtomicWaker::new(),
        }
    }

    /// Returns a reference to the payload building job.
    pub const fn payload_building_job(&self) -> Option<&PayloadBuildingJob> {
        self.payload_building_job.as_ref()
    }

    /// Cancels the current payload building job, if any.
    pub fn cancel_payload_building_job(&mut self) {
        self.payload_building_job = None;
        self.l1_message_recovery.cancel_request();
    }

    /// Enables the sequencer.
    pub fn enable(&mut self) {
        if self.trigger.is_none() {
            self.trigger = Some(delayed_interval(self.config.block_time));
        }
    }

    /// Disables the sequencer.
    pub fn disable(&mut self) {
        self.trigger = None;
        self.cancel_payload_building_job();
    }

    /// Creates a new block using the pending transactions from the message queue and
    /// the transaction pool.
    pub async fn start_payload_building<EC: ScrollEngineApi + Sync + Send + 'static>(
        &mut self,
        engine: &mut Engine<EC>,
    ) -> Result<(), SequencerError> {
        tracing::info!(target: "rollup_node::sequencer", "New payload attributes request received.");
        let now = Instant::now();

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time can't go backwards")
            .as_secs();
        let payload_attributes = PayloadAttributes {
            timestamp,
            suggested_fee_recipient: self.config.fee_recipient,
            parent_beacon_block_root: None,
            prev_randao: Default::default(),
            withdrawals: None,
        };

        let parent = *engine.fcs().head_block_info();
        let max_l1_messages = self
            .l1_message_recovery
            .max_messages(parent, self.config.payload_building_config.max_l1_messages_per_block);
        let mut l1_messages = vec![];
        let mut cumulative_gas_used = 0;

        // Collect L1 messages to include in payload.
        let db_l1_messages = self
            .provider
            .get_n_messages(
                self.config.payload_building_config.l1_message_inclusion_mode.into(),
                max_l1_messages,
            )
            .await
            .map_err(Into::<L1ProviderError>::into)?;

        let l1_origin = db_l1_messages.first().map(|msg| msg.l1_block_number);
        for msg in db_l1_messages {
            // TODO (greg): we only check the DA limit on the execution node side. We should also
            // check it here.
            let fits_in_block = msg.transaction.gas_limit + cumulative_gas_used <=
                self.config.payload_building_config.block_gas_limit;
            if !fits_in_block {
                break;
            }

            cumulative_gas_used += msg.transaction.gas_limit;
            l1_messages.push(msg.transaction.encoded_2718().into());
        }

        let l1_message_count = l1_messages.len() as u64;
        let payload_attributes = ScrollPayloadAttributes {
            payload_attributes,
            transactions: (!l1_messages.is_empty()).then_some(l1_messages),
            no_tx_pool: false,
            block_data_hint: BlockDataHint {
                difficulty: Some(DEFAULT_BLOCK_DIFFICULTY),
                ..Default::default()
            },
            // If setting the gas limit to None, the Reth payload builder will use the gas limit
            // passed via the `builder.gaslimit` CLI arg.
            gas_limit: None,
        };

        self.metrics.payload_attributes_building_duration.record(now.elapsed().as_secs_f64());

        // Request the engine to build a new payload.
        let fcu = engine.build_payload(None, payload_attributes).await?;
        let payload_id = fcu.payload_id.ok_or(SequencerError::MissingPayloadId)?;
        self.l1_message_recovery.record_request(payload_id, parent, l1_message_count);

        // Create a job that will wait for the configured duration before marking the payload as
        // ready.
        let payload_building_duration = self.config.payload_building_duration;
        self.payload_building_job = Some(PayloadBuildingJob {
            parent,
            l1_origin,
            future: Box::pin(async move {
                // wait the configured duration for the execution node to build the payload.
                tokio::time::sleep(tokio::time::Duration::from_millis(payload_building_duration))
                    .await;
                payload_id
            }),
        });

        self.waker.wake();

        Ok(())
    }

    /// Handles a new payload by fetching it from the engine and updating the FCS head.
    pub async fn finalize_payload_building<EC: ScrollEngineApi + Sync + Send + 'static>(
        &mut self,
        payload_id: PayloadId,
        engine: &mut Engine<EC>,
    ) -> Result<Option<DogeosBlock>, SequencerError> {
        let result = engine.get_payload(payload_id).await;
        if let Some(limit) = self.l1_message_recovery.record_result(
            payload_id,
            *engine.fcs().head_block_info(),
            result.is_ok(),
        ) {
            // Reth can discard the original build error before returning MissingPayload. This
            // recovers from candidate failures generally; it does not classify an L1 message as
            // invalid or prove code overflow. Retry only on a later slot, with the same queue head.
            tracing::warn!(target: "rollup_node::sequencer", ?payload_id, max_l1_messages = limit,
                "Payload retrieval failed; reducing the L1 message prefix for the next attempt");
        }
        let payload = result?;

        if payload.transactions.is_empty() && !self.config.allow_empty_blocks {
            tracing::trace!(target: "rollup_node::sequencer", "Built empty payload with id {payload_id:?}, discarding payload.");
            Ok(None)
        } else {
            tracing::info!(target: "rollup_node::sequencer", "Built payload with id {payload_id:?}, hash: {:#x}, number: {} containing {} transactions.", payload.block_hash, payload.block_number, payload.transactions.len());
            let block_info = BlockInfo { hash: payload.block_hash, number: payload.block_number };
            engine.update_fcs(Some(block_info), None, None).await?;
            let expected_hash = payload.block_hash;
            let ExecutionData { payload, sidecar } =
                ExecutionData { payload: payload.into(), sidecar: Default::default() };
            let mut block: DogeosBlock = payload
                .try_into_block_with_sidecar::<ScrollTransactionSigned>(&sidecar)
                .map_err(|_| SequencerError::PayloadError)?;
            block.header.difficulty = U256::ONE;
            if block.hash_slow() != expected_hash {
                return Err(SequencerError::PayloadError)
            }
            Ok(Some(block))
        }
    }
}

/// Keeps retry limits local to one parent and one outstanding Engine request.
#[derive(Debug, Default)]
struct L1MessageBuildRecovery {
    limit: Option<(BlockInfo, u64)>,
    request: Option<(PayloadId, BlockInfo, u64)>,
}

impl L1MessageBuildRecovery {
    fn max_messages(&mut self, parent: BlockInfo, configured: u64) -> u64 {
        self.reset_for_parent(parent);
        self.limit.map_or(configured, |(_, limit)| configured.min(limit))
    }

    fn reset_for_parent(&mut self, parent: BlockInfo) {
        if self.limit.is_some_and(|(limit_parent, _)| limit_parent != parent) {
            self.limit = None;
        }
    }

    const fn record_request(
        &mut self,
        payload_id: PayloadId,
        parent: BlockInfo,
        message_count: u64,
    ) {
        self.request = Some((payload_id, parent, message_count));
    }

    const fn cancel_request(&mut self) {
        self.request = None;
    }

    /// Returns a reduced prefix only for the matching request on the current parent.
    fn record_result(
        &mut self,
        payload_id: PayloadId,
        parent: BlockInfo,
        succeeded: bool,
    ) -> Option<u64> {
        self.reset_for_parent(parent);
        let (request_id, request_parent, count) = self.request?;
        if request_id != payload_id {
            return None;
        }
        self.request = None;
        if request_parent != parent {
            return None;
        }
        if succeeded {
            self.limit = None;
        } else if count > 1 {
            let limit = count / 2;
            self.limit = Some((parent, limit));
            return Some(limit);
        }
        // A single oversized message remains at the head and the error is returned to the caller.
        // Reducing to zero would skip a required message and falsely appear to make progress.
        None
    }
}

/// A job that builds a new payload.
pub struct PayloadBuildingJob {
    /// The L2 head on which the payload was requested.
    parent: BlockInfo,
    /// The L1 origin block number of the first included L1 message, if any.
    l1_origin: Option<u64>,
    /// The future that resolves to the payload ID once the job is complete.
    future: PayloadBuildingJobFuture,
}

impl fmt::Debug for PayloadBuildingJob {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PayloadBuildingJob")
            .field("parent", &self.parent)
            .field("l1_origin", &self.l1_origin)
            .field("future", &"PayloadBuildingJobFuture")
            .finish()
    }
}

impl PayloadBuildingJob {
    /// Returns the L2 parent of this payload job.
    pub const fn parent(&self) -> BlockInfo {
        self.parent
    }

    /// Returns the L1 origin block number of the first included L1 message, if any.
    pub const fn l1_origin(&self) -> Option<u64> {
        self.l1_origin
    }
}

/// A stream that produces payload attributes.
impl<SMP, CS> Stream for Sequencer<SMP, CS> {
    type Item = SequencerEvent;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        this.waker.register(cx.waker());

        // If there is an inflight payload building job, poll it.
        if let Some(payload_building_job) = this.payload_building_job.as_mut() {
            match payload_building_job.future.as_mut().poll(cx) {
                Poll::Ready(payload_id) => {
                    this.payload_building_job = None;
                    return Poll::Ready(Some(SequencerEvent::PayloadReady(payload_id)));
                }
                Poll::Pending => {}
            }
        }

        // Poll the trigger to see if it's time to build a new block.
        if let Some(trigger) = this.trigger.as_mut() {
            match trigger.poll_tick(cx) {
                Poll::Ready(_) => {
                    // If there's no inflight job, emit a new slot event.
                    if this.payload_building_job.is_none() {
                        return Poll::Ready(Some(SequencerEvent::NewSlot));
                    };
                    tracing::trace!(target: "rollup_node::sequencer", "Payload building job already in progress, skipping slot.");
                }
                Poll::Pending => {}
            }
        }

        Poll::Pending
    }
}

impl<SMP, CS: fmt::Debug> fmt::Debug for Sequencer<SMP, CS> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sequencer")
            .field("provider", &"SequencerMessageProvider")
            .field("config", &self.config)
            .field("payload_building_job", &"PayloadBuildingJob")
            .finish()
    }
}

/// Creates a delayed interval that will not skip ticks if the interval is missed but will delay
/// the next tick until the interval has passed.
fn delayed_interval(interval: u64) -> Interval {
    let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(interval));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    interval
}

#[cfg(test)]
mod recovery_tests {
    use super::*;
    use alloy_primitives::B256;

    fn parent(byte: u8) -> BlockInfo {
        BlockInfo { hash: B256::repeat_byte(byte), number: u64::from(byte) }
    }

    fn payload(byte: u8) -> PayloadId {
        PayloadId::new([byte; 8])
    }

    #[tokio::test]
    async fn sequencer_returns_build_error_then_requests_a_smaller_queue_prefix() {
        use alloy_consensus::{
            proofs::calculate_transaction_root, Block, BlockBody, Header, Sealable,
        };
        use alloy_primitives::Address;
        use alloy_rpc_types_engine::{ExecutionPayloadV1, ForkchoiceUpdated, PayloadStatusEnum};
        use dogeos_chainspec::DOGEOS_DEV;
        use dogeos_protocol_types::{ScrollTxEnvelope, TxL1Message};
        use futures::StreamExt;
        use rollup_node_primitives::L1MessageEnvelope;
        use scroll_db::{test_utils::setup_test_db, DatabaseWriteOperations};
        use scroll_engine::{
            test_utils::{ScriptedEngineClient, ScriptedResponse},
            ForkchoiceState,
        };

        let database = Arc::new(setup_test_db().await);
        database.set_latest_l1_block_number(1).await.unwrap();
        let mut messages = Vec::new();
        for queue_index in 0..2 {
            let transaction = TxL1Message { queue_index, gas_limit: 21_000, ..Default::default() };
            database
                .insert_l1_message(L1MessageEnvelope {
                    transaction: transaction.clone(),
                    l1_block_number: 1,
                    l2_block_number: None,
                    queue_hash: None,
                })
                .await
                .unwrap();
            messages.push(transaction);
        }
        let head = parent(1);
        let client = Arc::new(ScriptedEngineClient::new());
        let mut engine = Engine::new(client.clone(), ForkchoiceState::from_block_info(head));
        let mut sequencer = Sequencer::new(
            database.clone(),
            SequencerConfig {
                chain_spec: DOGEOS_DEV.clone(),
                fee_recipient: Address::ZERO,
                auto_start: false,
                payload_building_config: PayloadBuildingConfig {
                    block_gas_limit: 1_000_000,
                    max_l1_messages_per_block: 8,
                    l1_message_inclusion_mode: L1MessageInclusionMode::BlockDepth(0),
                },
                block_time: 1,
                payload_building_duration: 0,
                allow_empty_blocks: false,
            },
        );
        client.push_fork_choice_updated(ScriptedResponse::Ok(
            ForkchoiceUpdated::from_status(PayloadStatusEnum::Valid).with_payload_id(payload(1)),
        ));
        client.push_get_payload(ScriptedResponse::TransportFailure);
        sequencer.start_payload_building(&mut engine).await.unwrap();
        assert_eq!(sequencer.l1_message_recovery.request.unwrap().2, 2);
        assert!(
            matches!(sequencer.next().await, Some(SequencerEvent::PayloadReady(id)) if id == payload(1))
        );
        assert!(matches!(
            sequencer.finalize_payload_building(payload(1), &mut engine).await,
            Err(SequencerError::EngineError(_))
        ));
        // The error was returned without an immediate retry or any forkchoice update.
        assert_eq!(client.fork_choice_updated_calls(), 1);
        assert_eq!(*engine.fcs().head_block_info(), head);
        assert!(sequencer.payload_building_job().is_none());

        let transactions = vec![ScrollTxEnvelope::L1Message(messages[0].clone().seal_slow())];
        let built = Block {
            header: Header {
                parent_hash: head.hash,
                number: head.number + 1,
                timestamp: 2,
                gas_limit: 1_000_000,
                base_fee_per_gas: Some(0),
                difficulty: U256::ONE,
                transactions_root: calculate_transaction_root(&transactions),
                ..Default::default()
            },
            body: BlockBody { transactions, ..Default::default() },
        };
        client.push_fork_choice_updated(ScriptedResponse::Ok(
            ForkchoiceUpdated::from_status(PayloadStatusEnum::Valid).with_payload_id(payload(2)),
        ));
        client.push_get_payload(ScriptedResponse::Ok(ExecutionPayloadV1::from_block_slow(&built)));
        client.push_fork_choice_updated(ScriptedResponse::Ok(ForkchoiceUpdated::from_status(
            PayloadStatusEnum::Valid,
        )));
        sequencer.start_payload_building(&mut engine).await.unwrap();
        assert_eq!(sequencer.l1_message_recovery.request.unwrap().2, 1);
        assert!(
            matches!(sequencer.next().await, Some(SequencerEvent::PayloadReady(id)) if id == payload(2))
        );
        let block =
            sequencer.finalize_payload_building(payload(2), &mut engine).await.unwrap().unwrap();
        assert_eq!(block.body.transactions.len(), 1);
        assert_eq!(block.body.transactions, built.body.transactions);
        assert!(sequencer.l1_message_recovery.limit.is_none());
        // Only the included first message is consumed. The second remains at the queue head.
        database.update_l1_messages_from_l2_blocks(vec![(&block).into()]).await.unwrap();
        let remaining =
            database.get_n_messages(L1MessageInclusionMode::BlockDepth(0).into(), 8).await.unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].transaction.queue_index, 1);
    }

    #[test]
    fn failed_two_message_candidate_retries_one_and_success_restores_limit() {
        let mut recovery = L1MessageBuildRecovery::default();
        let head = parent(1);
        assert_eq!(recovery.max_messages(head, 8), 8);
        // These stand for two messages which fit separately but whose code union is too large.
        recovery.record_request(payload(1), head, 2);
        assert_eq!(recovery.record_result(payload(1), head, false), Some(1));
        assert_eq!(recovery.max_messages(head, 8), 1);
        recovery.record_request(payload(2), head, 1);
        assert_eq!(recovery.record_result(payload(2), head, true), None);
        assert_eq!(recovery.max_messages(head, 8), 8);
    }

    #[test]
    fn retries_halve_actual_prefix_and_never_drop_the_single_head_message() {
        let mut recovery = L1MessageBuildRecovery::default();
        let head = parent(1);
        for (attempt, count, expected) in [(1, 7, 3), (2, 3, 1)] {
            recovery.record_request(payload(attempt), head, count);
            assert_eq!(recovery.record_result(payload(attempt), head, false), Some(expected));
            assert_eq!(recovery.max_messages(head, 100), expected);
        }
        recovery.record_request(payload(3), head, 1);
        assert_eq!(recovery.record_result(payload(3), head, false), None);
        assert_eq!(recovery.max_messages(head, 100), 1);
    }

    #[test]
    fn parent_change_resets_limit_and_stale_results_cannot_change_new_parent() {
        let mut recovery = L1MessageBuildRecovery::default();
        recovery.record_request(payload(1), parent(1), 4);
        assert_eq!(recovery.record_result(payload(1), parent(1), false), Some(2));
        recovery.record_request(payload(2), parent(1), 2);
        assert_eq!(recovery.max_messages(parent(2), 8), 8);
        assert_eq!(recovery.record_result(payload(2), parent(2), false), None);
        assert_eq!(recovery.max_messages(parent(2), 8), 8);
    }

    #[test]
    fn cancelled_and_superseded_requests_do_not_change_recovery() {
        let mut recovery = L1MessageBuildRecovery::default();
        let head = parent(1);
        recovery.record_request(payload(1), head, 4);
        recovery.cancel_request();
        assert_eq!(recovery.record_result(payload(1), head, false), None);
        assert_eq!(recovery.max_messages(head, 8), 8);
        recovery.record_request(payload(2), head, 4);
        recovery.record_request(payload(3), head, 4);
        assert_eq!(recovery.record_result(payload(2), head, false), None);
        assert_eq!(recovery.record_result(payload(3), head, false), Some(2));
        recovery.record_request(payload(4), head, 2);
        assert_eq!(recovery.record_result(payload(3), head, true), None);
        assert_eq!(recovery.max_messages(head, 8), 2);
    }

    #[test]
    fn no_l1_failure_does_not_introduce_a_retry_limit() {
        let mut recovery = L1MessageBuildRecovery::default();
        let head = parent(1);
        recovery.record_request(payload(1), head, 0);
        assert_eq!(recovery.record_result(payload(1), head, false), None);
        assert_eq!(recovery.max_messages(head, 8), 8);
    }
}
