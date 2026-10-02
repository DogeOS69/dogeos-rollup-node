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

        let mut l1_messages = vec![];
        let mut cumulative_gas_used = 0;

        // Collect L1 messages to include in payload.
        let db_l1_messages = self
            .provider
            .get_n_messages(
                self.config.payload_building_config.l1_message_inclusion_mode.into(),
                self.config.payload_building_config.max_l1_messages_per_block,
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
        let parent = *engine.fcs().head_block_info();
        let fcu = engine.build_payload(None, payload_attributes).await?;
        let payload_id = fcu.payload_id.ok_or(SequencerError::MissingPayloadId)?;

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

    /// Handles a new payload by fetching it from the engine, validating it and committing it as
    /// the FCS head.
    ///
    /// Returns `Ok(Some(block))` once the engine confirmed the block as its head with `VALID`,
    /// and `Ok(None)` when the payload is empty and empty blocks are disabled. Fails with
    /// [`SequencerError::PayloadError`] when the payload does not convert into a block or its hash
    /// does not match the reconstructed header after applying [`DEFAULT_BLOCK_DIFFICULTY`], with
    /// [`SequencerError::FcuNotValid`] when the engine answered anything but `VALID`, and with the
    /// engine error when a call itself failed. On every `Ok(None)` and error path the local FCS
    /// mirror is left unchanged.
    pub async fn finalize_payload_building<EC: ScrollEngineApi + Sync + Send + 'static>(
        &mut self,
        payload_id: PayloadId,
        engine: &mut Engine<EC>,
    ) -> Result<Option<DogeosBlock>, SequencerError> {
        let payload = engine.get_payload(payload_id).await?;

        if payload.transactions.is_empty() && !self.config.allow_empty_blocks {
            tracing::trace!(target: "rollup_node::sequencer", "Built empty payload with id {payload_id:?}, discarding payload.");
            Ok(None)
        } else {
            tracing::info!(target: "rollup_node::sequencer", "Built payload with id {payload_id:?}, hash: {:#x}, number: {} containing {} transactions.", payload.block_hash, payload.block_number, payload.transactions.len());
            let block_info = BlockInfo { hash: payload.block_hash, number: payload.block_number };
            let expected_hash = payload.block_hash;
            // Convert and validate, including the difficulty used by `block_data_hint`, before
            // committing the head so a deterministic local failure cannot leave it advanced.
            let ExecutionData { payload, sidecar } =
                ExecutionData { payload: payload.into(), sidecar: Default::default() };
            let mut block: DogeosBlock = payload
                .try_into_block_with_sidecar::<ScrollTransactionSigned>(&sidecar)
                .map_err(|_| SequencerError::PayloadError)?;
            block.header.difficulty = DEFAULT_BLOCK_DIFFICULTY;
            if block.hash_slow() != expected_hash {
                return Err(SequencerError::PayloadError)
            }
            let result = engine.update_fcs_checked(Some(block_info), None, None).await?;
            if !result.is_valid() {
                // Any non-VALID verdict (INVALID, SYNCING, or the spec-illegal
                // ACCEPTED) leaves the mirror uncommitted and the head
                // unchanged. Proceeding would sign and gossip a block the EL
                // has not adopted and mark its L1 messages consumed. Log the
                // verdict and the EL's latest valid hash so INVALID (genuine
                // divergence, with the last ancestor the EL still accepts) is
                // distinguishable from SYNCING (a transient, e.g. after an EL
                // restart) instead of a contentless error type.
                tracing::error!(
                    target: "rollup_node::sequencer",
                    ?block_info,
                    status = ?result.payload_status.status,
                    latest_valid_hash = ?result.payload_status.latest_valid_hash,
                    "Engine did not confirm the freshly built block's forkchoice update"
                );
                return Err(SequencerError::FcuNotValid);
            }
            Ok(Some(block))
        }
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
mod tests {
    use super::*;
    use alloy_primitives::{Address, Bloom, Bytes, B256, U256};
    use alloy_rpc_types_engine::{
        ExecutionPayloadV1, ForkchoiceUpdated, PayloadStatus, PayloadStatusEnum,
    };
    use dogeos_chainspec::{DogeosChainSpec, DOGEOS_DEV};
    use rollup_node_providers::test_utils::MockL1Provider;
    use scroll_db::{test_utils::setup_test_db, Database};
    use scroll_engine::{
        test_utils::{ScriptedEngineClient, ScriptedResponse},
        ForkchoiceState,
    };

    type TestSequencer = Sequencer<MockL1Provider<Arc<Database>>, DogeosChainSpec>;

    async fn test_sequencer() -> TestSequencer {
        let db = Arc::new(setup_test_db().await);
        let provider = Arc::new(MockL1Provider { db, blobs: Default::default() });
        Sequencer::new(
            provider,
            SequencerConfig {
                chain_spec: DOGEOS_DEV.clone(),
                fee_recipient: Address::ZERO,
                auto_start: false,
                payload_building_config: PayloadBuildingConfig {
                    block_gas_limit: 30_000_000,
                    max_l1_messages_per_block: 4,
                    l1_message_inclusion_mode: L1MessageInclusionMode::default(),
                },
                block_time: 1_000,
                payload_building_duration: 0,
                allow_empty_blocks: true,
            },
        )
    }

    fn fcu(status: PayloadStatusEnum) -> ForkchoiceUpdated {
        ForkchoiceUpdated {
            payload_status: PayloadStatus { status, latest_valid_hash: None },
            payload_id: None,
        }
    }

    /// A payload whose `block_hash` matches the block the sequencer derives
    /// from it (with the difficulty pinned to `DEFAULT_BLOCK_DIFFICULTY`),
    /// computed through the same conversion `finalize_payload_building`
    /// performs so the hash check passes for the right reason.
    fn consistent_payload(number: u64) -> ExecutionPayloadV1 {
        let mut payload = ExecutionPayloadV1 {
            parent_hash: B256::repeat_byte(0x11),
            fee_recipient: Address::ZERO,
            state_root: B256::ZERO,
            receipts_root: B256::ZERO,
            logs_bloom: Bloom::default(),
            prev_randao: B256::ZERO,
            block_number: number,
            gas_limit: 30_000_000,
            gas_used: 0,
            timestamp: 1,
            extra_data: Bytes::new(),
            base_fee_per_gas: U256::from(7),
            block_hash: B256::ZERO,
            transactions: vec![],
        };
        let ExecutionData { payload: generic, sidecar } =
            ExecutionData { payload: payload.clone().into(), sidecar: Default::default() };
        let mut block: DogeosBlock = generic
            .try_into_block_with_sidecar::<ScrollTransactionSigned>(&sidecar)
            .expect("payload converts into a block");
        block.header.difficulty = DEFAULT_BLOCK_DIFFICULTY;
        payload.block_hash = block.hash_slow();
        payload
    }

    /// The engine must not adopt a head it did not confirm: a SYNCING, INVALID
    /// or (spec-illegal) ACCEPTED verdict for the freshly built payload leaves
    /// the FCS mirror untouched and surfaces as `FcuNotValid`, instead of
    /// signing and gossiping a block the EL never applied.
    #[tokio::test]
    async fn finalize_payload_building_requires_a_valid_forkchoice_update() {
        for status in [
            PayloadStatusEnum::Syncing,
            PayloadStatusEnum::Invalid { validation_error: "scripted".to_string() },
            PayloadStatusEnum::Accepted,
        ] {
            let mut sequencer = test_sequencer().await;
            let client = Arc::new(ScriptedEngineClient::new());
            let genesis = BlockInfo { number: 0, hash: B256::repeat_byte(0x11) };
            let mut engine =
                Engine::new(client.clone(), ForkchoiceState::new(genesis, genesis, genesis));

            client.push_get_payload(ScriptedResponse::Ok(consistent_payload(1)));
            client.push_fork_choice_updated(ScriptedResponse::Ok(fcu(status.clone())));

            let result =
                sequencer.finalize_payload_building(PayloadId::new([7; 8]), &mut engine).await;
            assert!(
                matches!(result, Err(SequencerError::FcuNotValid)),
                "{status:?}: expected FcuNotValid, got {result:?}"
            );
            assert_eq!(client.fork_choice_updated_calls(), 1, "{status:?}");
            assert_eq!(
                *engine.fcs().head_block_info(),
                genesis,
                "{status:?}: the mirror must not advance to an unadopted head"
            );
        }
    }

    /// The happy path is unchanged: VALID commits the head and returns the
    /// converted block.
    #[tokio::test]
    async fn finalize_payload_building_commits_head_on_valid() {
        let mut sequencer = test_sequencer().await;
        let client = Arc::new(ScriptedEngineClient::new());
        let genesis = BlockInfo { number: 0, hash: B256::repeat_byte(0x11) };
        let mut engine =
            Engine::new(client.clone(), ForkchoiceState::new(genesis, genesis, genesis));

        let payload = consistent_payload(1);
        client.push_get_payload(ScriptedResponse::Ok(payload.clone()));
        client.push_fork_choice_updated(ScriptedResponse::Ok(fcu(PayloadStatusEnum::Valid)));

        let block = sequencer
            .finalize_payload_building(PayloadId::new([7; 8]), &mut engine)
            .await
            .expect("finalization succeeds")
            .expect("a non-empty payload yields a block");
        assert_eq!(block.header.number, 1);
        assert_eq!(block.header.difficulty, DEFAULT_BLOCK_DIFFICULTY);
        assert_eq!(
            *engine.fcs().head_block_info(),
            BlockInfo { number: 1, hash: payload.block_hash },
            "VALID commits the built block as the new head"
        );
        assert_eq!(*engine.fcs().safe_block_info(), genesis);
        assert_eq!(*engine.fcs().finalized_block_info(), genesis);
    }

    /// Conversion and the hash check run before any forkchoice update: a
    /// payload whose `block_hash` does not match its contents is rejected with
    /// `PayloadError` without the engine ever being asked to adopt it. No
    /// forkchoice response is scripted, so a regression that commits the head
    /// first panics in the scripted client before the assertions run.
    #[tokio::test]
    async fn finalize_payload_building_rejects_a_payload_whose_hash_does_not_match() {
        let mut sequencer = test_sequencer().await;
        let client = Arc::new(ScriptedEngineClient::new());
        let genesis = BlockInfo { number: 0, hash: B256::repeat_byte(0x11) };
        let mut engine =
            Engine::new(client.clone(), ForkchoiceState::new(genesis, genesis, genesis));

        let mut payload = consistent_payload(1);
        payload.block_hash = B256::repeat_byte(0xbb);
        client.push_get_payload(ScriptedResponse::Ok(payload));

        let result = sequencer.finalize_payload_building(PayloadId::new([7; 8]), &mut engine).await;
        assert!(
            matches!(result, Err(SequencerError::PayloadError)),
            "expected PayloadError, got {result:?}"
        );
        assert_eq!(
            client.fork_choice_updated_calls(),
            0,
            "the hash check must run before the forkchoice update"
        );
        assert_eq!(*engine.fcs().head_block_info(), genesis);
    }

    /// A forkchoice update that fails in transport is the ambiguous case (the
    /// EL may or may not have applied it): the error propagates as
    /// `EngineError`, no block is returned and the mirror stays on the parent,
    /// so the next build re-points the engine from there.
    #[tokio::test]
    async fn finalize_payload_building_keeps_the_head_on_forkchoice_transport_failure() {
        let mut sequencer = test_sequencer().await;
        let client = Arc::new(ScriptedEngineClient::new());
        let genesis = BlockInfo { number: 0, hash: B256::repeat_byte(0x11) };
        let mut engine =
            Engine::new(client.clone(), ForkchoiceState::new(genesis, genesis, genesis));

        client.push_get_payload(ScriptedResponse::Ok(consistent_payload(1)));
        client.push_fork_choice_updated(ScriptedResponse::TransportFailure);

        let result = sequencer.finalize_payload_building(PayloadId::new([7; 8]), &mut engine).await;
        assert!(
            matches!(result, Err(SequencerError::EngineError(_))),
            "expected EngineError, got {result:?}"
        );
        assert_eq!(client.fork_choice_updated_calls(), 1);
        assert_eq!(*engine.fcs().head_block_info(), genesis);
    }
}
