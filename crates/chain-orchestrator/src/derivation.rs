//! Ordered recovery of derived batches.
//!
//! The chain orchestrator holds exactly one derived batch at a time in a [`DerivationDriver`] until
//! it has reconciled successfully and committed exactly once, or a terminal condition fail-stops
//! the node. While a batch is held, no later derivation result is polled and no new blocks are
//! sequenced, so a batch can never be overtaken or silently skipped.
//!
//! Each attempt recomputes reconciliation from *fresh* canonical L2 state (a timed-out request may
//! have taken effect after its receiver was dropped), executes the resulting actions with strict
//! Engine-status classification, and stages the block/batch outcomes in memory. Only after the
//! single atomic consolidation transaction commits are the staged events surfaced. Recovery
//! correctness comes from that fresh reconciliation plus idempotent atomic persistence — not from
//! any assumption that remote Engine effects did or did not happen.

use crate::{
    config::DerivedBatchRetryConfig,
    consolidation::{reconcile_batch, BlockConsolidationAction},
    error::ChainOrchestratorError,
    metrics::{record_engine_request_latency, DerivedBatchMetrics},
    status::{DerivationPipelineStatus, ReconcilingBatch},
};
use alloy_provider::Provider;
use alloy_rpc_types_engine::PayloadId;
use dogeos_rpc_types::Scroll;
use rollup_node_primitives::{
    BatchConsolidationOutcome, BatchInfo, BlockConsolidationOutcome, BlockInfo,
    L2BlockInfoWithL1Messages,
};
use scroll_db::{CanRetry, Database, DatabaseWriteOperations};
use scroll_derivation_pipeline::BatchDerivationResult;
use scroll_engine::{
    classify_fcu_with_attributes, classify_new_payload, Engine, EngineError, FcuAttributesOutcome,
    PayloadOutcome, ScrollEngineApi, StrictFcuStatus,
};
use std::{pin::Pin, time::Instant};
use tokio::time::{Instant as TokioInstant, Sleep};

/// The successful outcome of a single reconciliation attempt: the persisted batch outcome plus the
/// block outcomes staged in memory during the attempt. Both are surfaced as events by the caller,
/// but only after the consolidation transaction has committed.
#[derive(Debug)]
pub(crate) struct ConsolidatedBatch {
    /// The batch consolidation outcome, for the single `BatchConsolidated` event.
    pub batch_outcome: BatchConsolidationOutcome,
    /// The per-block consolidation outcomes, for the staged `BlockConsolidated` events.
    pub block_outcomes: Vec<BlockConsolidationOutcome>,
}

/// The result of a single reconciliation attempt as classified by the driver.
#[derive(Debug)]
pub(crate) enum AttemptStep {
    /// The batch reconciled and committed successfully; the pending slot has been cleared.
    Completed(ConsolidatedBatch),
    /// The attempt failed transiently and a retry has been scheduled.
    Retrying,
    /// The attempt failed terminally (a terminal Engine outcome or retry exhaustion). The node
    /// must fail-stop with this error.
    Fatal(ChainOrchestratorError),
}

/// The derived batch currently held for ordered reconciliation, plus its retry bookkeeping.
#[derive(Debug)]
struct PendingBatch {
    /// The derived batch, borrowed (never cloned) for each fresh reconciliation attempt.
    batch: BatchDerivationResult,
    /// The number of attempts started for this batch so far.
    attempts_completed: u32,
    /// The backoff scheduled before the next attempt, in milliseconds, when backing off.
    retry_backoff_ms: Option<u64>,
    /// The last classified reconciliation error, if any attempt has failed.
    last_error: Option<String>,
}

/// Holds and drives the single in-flight derived batch through ordered reconciliation with bounded
/// retry. This owns only the pending/retry state; the Engine, canonical-L2 client, and database are
/// passed in per attempt so the driver can be exercised in isolation.
#[derive(Debug)]
pub(crate) struct DerivationDriver {
    retry: DerivedBatchRetryConfig,
    pending: Option<PendingBatch>,
    /// The timer for the next attempt. It remains pinned and is reused across `select!`
    /// cancellations caused by unrelated events.
    attempt_sleep: Option<Pin<Box<Sleep>>>,
    metrics: DerivedBatchMetrics,
}

impl DerivationDriver {
    /// Creates a new driver with the given retry policy.
    pub(crate) fn new(retry: DerivedBatchRetryConfig) -> Self {
        Self { retry, pending: None, attempt_sleep: None, metrics: DerivedBatchMetrics::default() }
    }

    /// Returns true if no batch is currently held, i.e. the pipeline may be polled for the next
    /// derivation result.
    pub(crate) const fn can_accept_batch(&self) -> bool {
        self.pending.is_none()
    }

    /// Returns true if an attempt is currently scheduled (either immediately or after a backoff).
    pub(crate) const fn is_attempt_scheduled(&self) -> bool {
        self.attempt_sleep.is_some()
    }

    /// Returns true while commands may be polled without delaying a due reconciliation attempt.
    /// Commands remain available during retry backoff, but once the deadline is ready the attempt
    /// is given priority even under continuous traffic on the unbounded command channel.
    pub(crate) fn can_poll_commands(&self) -> bool {
        self.attempt_sleep.as_ref().is_none_or(|sleep| sleep.deadline() > TokioInstant::now())
    }

    /// Moves a freshly yielded derivation result into the pending slot and schedules its first
    /// attempt immediately. Must only be called when [`Self::can_accept_batch`] is true.
    pub(crate) fn hold_batch(&mut self, batch: BatchDerivationResult) {
        debug_assert!(self.pending.is_none(), "a batch is already held");
        let batch_info = batch.batch_info;
        tracing::info!(
            target: "scroll::chain_orchestrator",
            ?batch_info,
            num_blocks = batch.attributes.len(),
            "Holding derived batch for ordered reconciliation"
        );
        self.pending = Some(PendingBatch {
            batch,
            attempts_completed: 0,
            retry_backoff_ms: None,
            last_error: None,
        });
        self.attempt_sleep = Some(Box::pin(tokio::time::sleep_until(TokioInstant::now())));
    }

    /// Cancels the held batch and any scheduled retry after a control-plane unwind invalidates the
    /// L1 epoch from which it was derived.
    pub(crate) fn invalidate(&mut self) -> Option<BatchInfo> {
        self.attempt_sleep = None;
        self.pending.take().map(|pending| pending.batch.batch_info)
    }

    /// Waits until it is time to run the next attempt. Resolves immediately once the deadline has
    /// passed; never resolves when no attempt is scheduled (guarded by
    /// [`Self::is_attempt_scheduled`] in the run loop `select!`).
    pub(crate) async fn wait_for_attempt(&mut self) {
        match self.attempt_sleep.as_mut() {
            Some(sleep) => sleep.as_mut().await,
            None => std::future::pending().await,
        }
    }

    /// Runs the next reconciliation attempt for the held batch and classifies the outcome, updating
    /// retry state and metrics. The caller is responsible for emitting the returned events (only on
    /// [`AttemptStep::Completed`]) and for fail-stopping on [`AttemptStep::Fatal`].
    ///
    /// This may be cancelled by a shutdown signal racing it in the run loop `select!`; that is safe
    /// because nothing is emitted here and persistence is a single idempotent atomic transaction.
    pub(crate) async fn run_attempt<L2P, EC>(
        &mut self,
        l2_client: &L2P,
        engine: &mut Engine<EC>,
        database: &Database,
    ) -> AttemptStep
    where
        L2P: Provider<Scroll>,
        EC: ScrollEngineApi + Sync + Send + 'static,
    {
        // Consume the deadline that triggered this attempt; a retry reschedules a new one below.
        self.attempt_sleep = None;
        let attempts = {
            let pending = self.pending.as_mut().expect("run_attempt requires a held batch");
            pending.attempts_completed += 1;
            pending.retry_backoff_ms = None;
            pending.attempts_completed
        };
        self.metrics.derived_batch_attempts.increment(1);
        let batch_info = self.pending.as_ref().expect("held batch").batch.batch_info;
        tracing::info!(
            target: "scroll::chain_orchestrator",
            ?batch_info,
            attempt = attempts,
            max_attempts = self.retry.max_attempts,
            "Attempting derived batch reconciliation"
        );

        let result = {
            let batch = &self.pending.as_ref().expect("held batch").batch;
            reconcile_and_consolidate(l2_client, engine, database, batch).await
        };

        match result {
            Ok(consolidated) => {
                self.pending = None;
                self.attempt_sleep = None;
                self.metrics.derived_batch_successes.increment(1);
                tracing::info!(
                    target: "scroll::chain_orchestrator",
                    ?batch_info,
                    attempts,
                    "Derived batch reconciled and committed"
                );
                AttemptStep::Completed(consolidated)
            }
            Err(err) => {
                let retryable = err.can_retry();
                if retryable && attempts < self.retry.max_attempts {
                    let backoff = self.retry.backoff(attempts);
                    let backoff_ms = backoff.as_millis() as u64;
                    self.metrics.derived_batch_retries.increment(1);
                    self.metrics.derived_batch_retry_backoff_seconds.record(backoff.as_secs_f64());
                    tracing::warn!(
                        target: "scroll::chain_orchestrator",
                        ?batch_info,
                        attempts,
                        max_attempts = self.retry.max_attempts,
                        backoff_ms,
                        %err,
                        "Derived batch reconciliation attempt failed; scheduling retry"
                    );
                    let pending = self.pending.as_mut().expect("held batch");
                    pending.retry_backoff_ms = Some(backoff_ms);
                    pending.last_error = Some(err.to_string());
                    self.attempt_sleep =
                        Some(Box::pin(tokio::time::sleep_until(TokioInstant::now() + backoff)));
                    AttemptStep::Retrying
                } else {
                    self.metrics.derived_batch_terminal_failures.increment(1);
                    let err = if retryable {
                        ChainOrchestratorError::DerivedBatchRetriesExhausted {
                            batch_info,
                            attempts,
                            last_error: Box::new(err),
                        }
                    } else {
                        err
                    };
                    AttemptStep::Fatal(err)
                }
            }
        }
    }

    /// The number of attempts started for the held batch, if any (for the fatal event/log).
    pub(crate) fn attempts(&self) -> u32 {
        self.pending.as_ref().map(|p| p.attempts_completed).unwrap_or(0)
    }

    /// The [`BatchInfo`] of the held batch, if any (for the fatal event/log).
    pub(crate) fn pending_batch_info(&self) -> Option<BatchInfo> {
        self.pending.as_ref().map(|p| p.batch.batch_info)
    }

    /// Computes the derivation status, given the number of batches still queued/in-flight in the
    /// pipeline behind any held batch.
    pub(crate) fn status(&self, queued: u64) -> DerivationPipelineStatus {
        match &self.pending {
            Some(pending) => DerivationPipelineStatus::Reconciling(ReconcilingBatch {
                batch_index: pending.batch.batch_info.index,
                batch_hash: pending.batch.batch_info.hash,
                attempts_completed: pending.attempts_completed,
                max_attempts: self.retry.max_attempts,
                backing_off: pending.retry_backoff_ms.is_some(),
                retry_backoff_ms: pending.retry_backoff_ms,
                last_error: pending.last_error.clone(),
                queued_behind: queued,
            }),
            None if queued > 0 => DerivationPipelineStatus::Deriving { queued },
            None => DerivationPipelineStatus::Idle,
        }
    }
}

/// Reconciles a derived batch against fresh canonical L2 state, executes the resulting actions with
/// strict Engine-status classification, stages the resulting block/batch outcomes, and — only after
/// the single atomic consolidation transaction commits — returns them for the caller to emit.
///
/// The batch is borrowed so this can be re-run cheaply on every retry attempt. Any `SYNCING`
/// response is surfaced as a transient error; any `INVALID`, `VALID`-without-payload-id, or
/// (for a forkchoice update) `ACCEPTED` response is surfaced as a terminal error.
pub(crate) async fn reconcile_and_consolidate<L2P, EC>(
    l2_client: &L2P,
    engine: &mut Engine<EC>,
    database: &Database,
    batch: &BatchDerivationResult,
) -> Result<ConsolidatedBatch, ChainOrchestratorError>
where
    L2P: Provider<Scroll>,
    EC: ScrollEngineApi + Sync + Send + 'static,
{
    let batch_info = batch.batch_info;
    let reconciliation = reconcile_batch(l2_client, batch, engine.fcs()).await?;
    let target_status = reconciliation.target_status;
    let aggregated = reconciliation.aggregate_actions();

    let mut block_outcomes: Vec<BlockConsolidationOutcome> = Vec::new();
    let mut reorg_results: Vec<L2BlockInfoWithL1Messages> = Vec::new();
    let mut update_fcs_replaced_head = false;

    for action in aggregated.actions {
        let outcome = match action {
            BlockConsolidationAction::Skip(_) => {
                unreachable!("Skip actions have been filtered out in aggregation")
            }
            BlockConsolidationAction::UpdateFcs(block_info) => {
                let target = block_info.block_info;
                let finalized = target_status.is_finalized().then_some(target);
                // Advance the head coherently when the local head lags or is stale, so a matching
                // canonical block (e.g. one applied by a timed-out final FCU) never triggers
                // `HeadBelowSafe` or a needless reorg.
                let head = coherent_head_for_update_fcs(
                    l2_client,
                    *engine.fcs().head_block_info(),
                    target,
                )
                .await?;
                let replaced_head = head.is_some();

                let start = Instant::now();
                let status = engine.update_fcs_strict(head, Some(target), finalized).await;
                record_engine_request_latency(
                    "update_fcs",
                    strict_fcu_label(&status),
                    start.elapsed().as_secs_f64(),
                );
                let status =
                    status.map_err(|source| ChainOrchestratorError::DerivedBatchEngineRequest {
                        batch_info,
                        method: "update_fcs",
                        source,
                    })?;
                interpret_strict_fcu(status, batch_info, "update_fcs", Some(target.number))?;
                update_fcs_replaced_head |= replaced_head;

                BlockConsolidationOutcome::UpdateFcs(block_info)
            }
            BlockConsolidationAction::Reorg(attribute_index) => {
                let attributes = &batch.attributes[attribute_index];
                let safe = *engine.fcs().safe_block_info();
                if safe.number != attributes.block_number - 1 {
                    return Err(ChainOrchestratorError::InvalidBatchReorg {
                        batch_info,
                        safe_block_number: safe.number,
                        derived_block_number: attributes.block_number,
                    });
                }

                // Forkchoice update with payload attributes to begin the payload build.
                let start = Instant::now();
                let payload_attributes = attributes.attributes.clone();
                let payload_id = match engine.build_payload(Some(safe), payload_attributes).await {
                    Ok(fcu) => {
                        let outcome = classify_fcu_with_attributes(&fcu);
                        record_engine_request_latency(
                            "fcu_with_attributes",
                            fcu_attributes_label(&outcome),
                            start.elapsed().as_secs_f64(),
                        );
                        interpret_fcu_with_attributes(outcome, batch_info)?
                    }
                    Err(err) => {
                        record_engine_request_latency(
                            "fcu_with_attributes",
                            "error",
                            start.elapsed().as_secs_f64(),
                        );
                        return Err(ChainOrchestratorError::DerivedBatchEngineRequest {
                            batch_info,
                            method: "fcu_with_attributes",
                            source: err,
                        });
                    }
                };

                // getPayload.
                let start = Instant::now();
                let payload = engine.get_payload(payload_id).await;
                record_engine_request_latency(
                    "get_payload",
                    ok_err_label(&payload),
                    start.elapsed().as_secs_f64(),
                );
                let payload = payload.map_err(|source| {
                    ChainOrchestratorError::DerivedBatchEngineRequest {
                        batch_info,
                        method: "get_payload",
                        source,
                    }
                })?;

                let block_info: L2BlockInfoWithL1Messages = (&payload)
                    .try_into()
                    .map_err(ChainOrchestratorError::RollupNodePrimitiveError)?;
                let block_number = block_info.block_info.number;

                // newPayload.
                let start = Instant::now();
                match engine.new_payload(payload).await {
                    Ok(status) => {
                        let outcome = classify_new_payload(&status);
                        record_engine_request_latency(
                            "new_payload",
                            new_payload_label(&outcome),
                            start.elapsed().as_secs_f64(),
                        );
                        interpret_new_payload(outcome, batch_info, block_number)?;
                    }
                    Err(err) => {
                        record_engine_request_latency(
                            "new_payload",
                            "error",
                            start.elapsed().as_secs_f64(),
                        );
                        return Err(ChainOrchestratorError::DerivedBatchEngineRequest {
                            batch_info,
                            method: "new_payload",
                            source: err,
                        });
                    }
                }

                // Final forkchoice update (no attributes) to advance head/safe(/finalized).
                let finalized = target_status.is_finalized().then_some(block_info.block_info);
                let start = Instant::now();
                let status = engine
                    .update_fcs_strict(
                        Some(block_info.block_info),
                        Some(block_info.block_info),
                        finalized,
                    )
                    .await;
                record_engine_request_latency(
                    "fcu",
                    strict_fcu_label(&status),
                    start.elapsed().as_secs_f64(),
                );
                let status =
                    status.map_err(|source| ChainOrchestratorError::DerivedBatchEngineRequest {
                        batch_info,
                        method: "fcu",
                        source,
                    })?;
                interpret_strict_fcu(status, batch_info, "fcu", Some(block_number))?;

                reorg_results.push(block_info.clone());
                BlockConsolidationOutcome::Reorged(block_info)
            }
        };

        block_outcomes.push(outcome);
    }

    // Build and persist the complete outcome first; the staged events are surfaced by the caller
    // only after this atomic transaction commits.
    let batch_outcome = reconciliation
        .into_batch_consolidation_outcome(reorg_results, update_fcs_replaced_head)
        .await?;
    let mut persisted = batch_outcome.clone();
    persisted.with_skipped_l1_messages(batch.skipped_l1_messages.clone());
    database.insert_batch_consolidation_outcome(persisted).await?;

    Ok(ConsolidatedBatch { batch_outcome, block_outcomes })
}

/// Determines the head to send with a safe/finalized forkchoice update for a matching canonical
/// block, per the timed-out-final-FCU recovery rules:
///
/// * if the local head lags below the target, advance the head to the target;
/// * if the local head is at the same height with a different hash, replace it with the target;
/// * if the local head is a later block, keep it **only** if the canonical provider confirms the
///   local head is still canonical at its height; otherwise fall back to a coherent canonical head
///   (the target), rather than pairing an unrelated head with the new safe block.
async fn coherent_head_for_update_fcs<L2P: Provider<Scroll>>(
    l2_client: &L2P,
    current_head: BlockInfo,
    target: BlockInfo,
) -> Result<Option<BlockInfo>, ChainOrchestratorError> {
    if target.number > current_head.number {
        return Ok(Some(target));
    }
    if target.number == current_head.number {
        return Ok((target.hash != current_head.hash).then_some(target));
    }

    // The local head claims to be ahead of the target. Only keep it if it is still canonical at its
    // height; otherwise it is stale or on a fork and we must not pair it with the new safe block.
    let canonical = l2_client.get_block(current_head.number.into()).await?;
    match canonical {
        Some(block) if block.header.hash == current_head.hash => Ok(None),
        _ => Ok(Some(target)),
    }
}

// These interpret helpers return the shared (intentionally large — see the workspace
// `large_enum_variant = "allow"`) `ChainOrchestratorError` with a small `Ok`; boxing it here would
// only relocate the allocation. The typed validation detail is deliberately preserved inline.
#[allow(clippy::result_large_err)]
fn interpret_fcu_with_attributes(
    outcome: FcuAttributesOutcome,
    batch_info: BatchInfo,
) -> Result<PayloadId, ChainOrchestratorError> {
    match outcome {
        FcuAttributesOutcome::Valid(id) => Ok(id),
        FcuAttributesOutcome::ValidMissingPayloadId => {
            Err(ChainOrchestratorError::MissingPayloadId { batch_info })
        }
        FcuAttributesOutcome::Syncing => Err(ChainOrchestratorError::DerivedBatchEngineSyncing {
            batch_info,
            method: "fcu_with_attributes",
        }),
        FcuAttributesOutcome::Accepted => Err(ChainOrchestratorError::UnexpectedEngineStatus {
            batch_info,
            method: "fcu_with_attributes",
        }),
        FcuAttributesOutcome::Invalid(details) => {
            Err(ChainOrchestratorError::InvalidDerivedPayload {
                batch_info,
                method: "fcu_with_attributes",
                block_number: None,
                latest_valid_hash: details.latest_valid_hash,
                validation_error: details.validation_error,
            })
        }
    }
}

#[allow(clippy::result_large_err)]
fn interpret_new_payload(
    outcome: PayloadOutcome,
    batch_info: BatchInfo,
    block_number: u64,
) -> Result<(), ChainOrchestratorError> {
    match outcome {
        PayloadOutcome::Valid => Ok(()),
        // Both SYNCING and ACCEPTED from newPayload are transient: retry from fresh reconciliation.
        PayloadOutcome::Syncing => Err(ChainOrchestratorError::DerivedBatchEngineSyncing {
            batch_info,
            method: "new_payload",
        }),
        PayloadOutcome::Accepted => {
            Err(ChainOrchestratorError::DerivedBatchEngineAccepted { batch_info })
        }
        PayloadOutcome::Invalid(details) => Err(ChainOrchestratorError::InvalidDerivedPayload {
            batch_info,
            method: "new_payload",
            block_number: Some(block_number),
            latest_valid_hash: details.latest_valid_hash,
            validation_error: details.validation_error,
        }),
    }
}

#[allow(clippy::result_large_err)]
fn interpret_strict_fcu(
    status: StrictFcuStatus,
    batch_info: BatchInfo,
    method: &'static str,
    block_number: Option<u64>,
) -> Result<(), ChainOrchestratorError> {
    match status {
        StrictFcuStatus::Valid => Ok(()),
        StrictFcuStatus::Syncing => {
            Err(ChainOrchestratorError::DerivedBatchEngineSyncing { batch_info, method })
        }
        // ACCEPTED for a forkchoice update is a terminal protocol failure (unlike newPayload).
        StrictFcuStatus::Accepted => {
            Err(ChainOrchestratorError::UnexpectedEngineStatus { batch_info, method })
        }
        StrictFcuStatus::Invalid(details) => Err(ChainOrchestratorError::InvalidDerivedPayload {
            batch_info,
            method,
            block_number,
            latest_valid_hash: details.latest_valid_hash,
            validation_error: details.validation_error,
        }),
    }
}

const fn fcu_attributes_label(outcome: &FcuAttributesOutcome) -> &'static str {
    match outcome {
        FcuAttributesOutcome::Valid(_) => "valid",
        FcuAttributesOutcome::ValidMissingPayloadId => "valid_missing_payload_id",
        FcuAttributesOutcome::Syncing => "syncing",
        FcuAttributesOutcome::Invalid(_) => "invalid",
        FcuAttributesOutcome::Accepted => "accepted",
    }
}

const fn new_payload_label(outcome: &PayloadOutcome) -> &'static str {
    match outcome {
        PayloadOutcome::Valid => "valid",
        PayloadOutcome::Syncing => "syncing",
        PayloadOutcome::Accepted => "accepted",
        PayloadOutcome::Invalid(_) => "invalid",
    }
}

const fn strict_fcu_label(status: &Result<StrictFcuStatus, EngineError>) -> &'static str {
    match status {
        Ok(StrictFcuStatus::Valid) => "valid",
        Ok(StrictFcuStatus::Syncing) => "syncing",
        Ok(StrictFcuStatus::Accepted) => "accepted",
        Ok(StrictFcuStatus::Invalid(_)) => "invalid",
        Err(_) => "error",
    }
}

const fn ok_err_label<T>(result: &Result<T, EngineError>) -> &'static str {
    match result {
        Ok(_) => "ok",
        Err(_) => "error",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ChainOrchestratorEvent;
    use alloy_consensus::Header as ConsensusHeader;
    use alloy_primitives::{Address, Bloom, Bytes, B256, U256};
    use alloy_provider::{Network, ProviderBuilder};
    use alloy_rpc_types_engine::{
        ExecutionPayloadV1, ForkchoiceUpdated, PayloadId, PayloadStatus, PayloadStatusEnum,
    };
    use alloy_transport::mock::Asserter;
    use dogeos_reth_engine::ScrollPayloadAttributes;
    use rollup_node_primitives::BatchStatus;
    use scroll_db::{test_utils::setup_test_db, DatabaseReadOperations};
    use scroll_derivation_pipeline::DerivedAttributes;
    use scroll_engine::{
        test_utils::{ScriptedEngineClient, ScriptedResponse},
        ForkchoiceState,
    };
    use std::{borrow::Cow, collections::VecDeque, sync::Arc, time::Duration};

    const SAFE: u64 = 100;
    type ScrollRpcBlock = <Scroll as Network>::BlockResponse;

    fn info(number: u64, tag: u8) -> BlockInfo {
        BlockInfo { number, hash: B256::repeat_byte(tag) }
    }

    fn payload_status(status: PayloadStatusEnum) -> PayloadStatus {
        PayloadStatus { status, latest_valid_hash: None }
    }

    fn fcu(status: PayloadStatusEnum, payload_id: Option<PayloadId>) -> ForkchoiceUpdated {
        ForkchoiceUpdated { payload_status: payload_status(status), payload_id }
    }

    fn execution_payload(number: u64, tag: u8) -> ExecutionPayloadV1 {
        execution_payload_with_hash(number, B256::repeat_byte(tag))
    }

    fn execution_payload_with_hash(number: u64, block_hash: B256) -> ExecutionPayloadV1 {
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
            block_hash,
            transactions: vec![],
        }
    }

    /// Builds the concrete Scroll RPC block returned by `eth_getBlockByNumber` in matching-block
    /// recovery tests. Its empty transaction list and default header fields exactly match the
    /// derived attributes produced by [`matching_batch`].
    fn matching_rpc_block(number: u64) -> (ScrollRpcBlock, BlockInfo) {
        let consensus_header = ConsensusHeader { number, ..Default::default() };
        let block_info = BlockInfo::from(&consensus_header);
        let mut block = ScrollRpcBlock::default();
        block.header.hash = block_info.hash;
        block.header.inner = consensus_header;
        (block, block_info)
    }

    /// A canonical-L2 provider that returns queued mock responses. Each reconciliation attempt
    /// issues one `get_block` for the derived block; pushing a `null` makes it absent (forcing a
    /// reorg action) without needing a matching serialized block.
    fn mock_provider(asserter: Asserter) -> impl Provider<Scroll> {
        ProviderBuilder::<_, _, Scroll>::default().connect_mocked_client(asserter)
    }

    fn push_absent_block(asserter: &Asserter) {
        asserter.push_success(&Option::<()>::None);
    }

    fn push_block(asserter: &Asserter, block: &ScrollRpcBlock) {
        asserter.push_success(&Some(block));
    }

    fn engine_at_safe(client: Arc<ScriptedEngineClient>) -> Engine<ScriptedEngineClient> {
        let head = info(SAFE, 0x11);
        Engine::new(client, ForkchoiceState::new(head, head, head))
    }

    /// A single-block batch whose derived block (`derived_block`) is absent on chain, so
    /// reconciliation yields a reorg action. The reorg sanity check requires the current safe head
    /// to be `derived_block - 1`.
    fn reorg_batch(index: u64, derived_block: u64) -> BatchDerivationResult {
        BatchDerivationResult {
            attributes: vec![DerivedAttributes {
                block_number: derived_block,
                attributes: ScrollPayloadAttributes::default(),
            }],
            batch_info: BatchInfo::new(index, B256::repeat_byte(0xba)),
            skipped_l1_messages: vec![],
            target_status: BatchStatus::Consolidated,
        }
    }

    fn matching_batch(index: u64, derived_block: u64) -> BatchDerivationResult {
        let mut batch = reorg_batch(index, derived_block);
        batch.attributes[0].attributes.transactions = Some(vec![]);
        batch
    }

    /// Scripts one successful reorg attempt building `block`: FCU-with-attributes VALID (+id),
    /// getPayload, newPayload VALID, final FCU VALID.
    fn script_successful_reorg(client: &ScriptedEngineClient, block: u64) {
        client.push_fork_choice_updated(ScriptedResponse::Ok(fcu(
            PayloadStatusEnum::Valid,
            Some(PayloadId::new([7; 8])),
        )));
        client.push_get_payload(ScriptedResponse::Ok(execution_payload(block, 0x22)));
        client.push_new_payload(ScriptedResponse::Ok(payload_status(PayloadStatusEnum::Valid)));
        client.push_fork_choice_updated(ScriptedResponse::Ok(fcu(PayloadStatusEnum::Valid, None)));
    }

    async fn insert_batch_commit(db: &Database, index: u64) {
        // Insert the committed batch row so the consolidation outcome's status update has a target.
        db.tx_mut(move |tx| async move {
            tx.insert_batch(rollup_node_primitives::BatchCommitData {
                index,
                hash: B256::repeat_byte(0xba),
                block_number: 1,
                block_timestamp: 0,
                calldata: Arc::new(Bytes::new()),
                blob_versioned_hash: None,
                finalized_block_number: None,
                reverted_block_number: None,
            })
            .await
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn reorg_success_stages_events_and_commits() {
        let db = setup_test_db().await;
        insert_batch_commit(&db, 1).await;
        let client = Arc::new(ScriptedEngineClient::new());
        script_successful_reorg(&client, SAFE + 1);
        let mut engine = engine_at_safe(client.clone());
        let asserter = Asserter::new();
        push_absent_block(&asserter);
        let provider = mock_provider(asserter);
        let batch = reorg_batch(1, SAFE + 1);

        let consolidated =
            reconcile_and_consolidate(&provider, &mut engine, &db, &batch).await.unwrap();

        // One block outcome staged; the batch head advanced to the derived block.
        assert_eq!(consolidated.block_outcomes.len(), 1);
        assert_eq!(engine.fcs().head_block_info().number, SAFE + 1);
        assert_eq!(engine.fcs().safe_block_info().number, SAFE + 1);
        // The consolidation outcome was persisted: the l2 head is durable.
        let head = db.get_l2_head_block_number().await.unwrap();
        assert_eq!(head, SAFE + 1);
    }

    #[tokio::test]
    async fn reorg_fcu_syncing_is_transient_and_leaves_no_effects() {
        let db = setup_test_db().await;
        insert_batch_commit(&db, 1).await;
        let client = Arc::new(ScriptedEngineClient::new());
        // First (build) FCU returns SYNCING.
        client
            .push_fork_choice_updated(ScriptedResponse::Ok(fcu(PayloadStatusEnum::Syncing, None)));
        let mut engine = engine_at_safe(client.clone());
        let asserter = Asserter::new();
        push_absent_block(&asserter);
        let provider = mock_provider(asserter);
        let batch = reorg_batch(1, SAFE + 1);

        let err = reconcile_and_consolidate(&provider, &mut engine, &db, &batch).await.unwrap_err();

        assert!(err.can_retry(), "SYNCING must be transient");
        assert!(matches!(err, ChainOrchestratorError::DerivedBatchEngineSyncing { .. }));
        // No local fcs advance, and nothing persisted.
        assert_eq!(engine.fcs().head_block_info().number, SAFE);
        assert!(db.get_l2_head_block_number().await.unwrap() == 0);
    }

    #[tokio::test]
    async fn reorg_new_payload_invalid_is_terminal() {
        let db = setup_test_db().await;
        insert_batch_commit(&db, 1).await;
        let client = Arc::new(ScriptedEngineClient::new());
        client.push_fork_choice_updated(ScriptedResponse::Ok(fcu(
            PayloadStatusEnum::Valid,
            Some(PayloadId::new([7; 8])),
        )));
        client.push_get_payload(ScriptedResponse::Ok(execution_payload(SAFE + 1, 0x22)));
        client.push_new_payload(ScriptedResponse::Ok(payload_status(PayloadStatusEnum::Invalid {
            validation_error: "bad state root".to_string(),
        })));
        let mut engine = engine_at_safe(client.clone());
        let asserter = Asserter::new();
        push_absent_block(&asserter);
        let provider = mock_provider(asserter);

        let err = reconcile_and_consolidate(&provider, &mut engine, &db, &reorg_batch(1, SAFE + 1))
            .await
            .unwrap_err();

        assert!(!err.can_retry(), "INVALID must be terminal");
        assert!(matches!(
            err,
            ChainOrchestratorError::InvalidDerivedPayload { method: "new_payload", .. }
        ));
    }

    #[tokio::test]
    async fn reorg_new_payload_accepted_is_transient() {
        let db = setup_test_db().await;
        insert_batch_commit(&db, 1).await;
        let client = Arc::new(ScriptedEngineClient::new());
        client.push_fork_choice_updated(ScriptedResponse::Ok(fcu(
            PayloadStatusEnum::Valid,
            Some(PayloadId::new([7; 8])),
        )));
        client.push_get_payload(ScriptedResponse::Ok(execution_payload(SAFE + 1, 0x22)));
        client.push_new_payload(ScriptedResponse::Ok(payload_status(PayloadStatusEnum::Accepted)));
        let mut engine = engine_at_safe(client.clone());
        let asserter = Asserter::new();
        push_absent_block(&asserter);
        let provider = mock_provider(asserter);

        let err = reconcile_and_consolidate(&provider, &mut engine, &db, &reorg_batch(1, SAFE + 1))
            .await
            .unwrap_err();

        // ACCEPTED from newPayload is transient (unlike ACCEPTED from a forkchoice update).
        assert!(err.can_retry(), "newPayload ACCEPTED must be transient");
        assert!(matches!(err, ChainOrchestratorError::DerivedBatchEngineAccepted { .. }));
    }

    #[tokio::test]
    async fn unknown_payload_is_retryable_only_for_get_payload() {
        let db = setup_test_db().await;
        insert_batch_commit(&db, 1).await;

        // The authenticated Engine client wraps JSON-RPC responses as a custom jsonrpsee error.
        // `-38001` is recoverable for derived getPayload because a fresh attempt can rebuild it.
        let get_payload_client = Arc::new(ScriptedEngineClient::new());
        get_payload_client.push_fork_choice_updated(ScriptedResponse::Ok(fcu(
            PayloadStatusEnum::Valid,
            Some(PayloadId::new([7; 8])),
        )));
        get_payload_client.push_get_payload(ScriptedResponse::CallError(-38001));
        let mut get_payload_engine = engine_at_safe(get_payload_client);
        let get_payload_asserter = Asserter::new();
        push_absent_block(&get_payload_asserter);
        let get_payload_provider = mock_provider(get_payload_asserter);
        let err = reconcile_and_consolidate(
            &get_payload_provider,
            &mut get_payload_engine,
            &db,
            &reorg_batch(1, SAFE + 1),
        )
        .await
        .unwrap_err();
        assert!(err.can_retry());
        assert!(matches!(
            err,
            ChainOrchestratorError::DerivedBatchEngineRequest { method: "get_payload", .. }
        ));

        // The same code from a different Engine method is a terminal protocol response.
        let fcu_client = Arc::new(ScriptedEngineClient::new());
        fcu_client.push_fork_choice_updated(ScriptedResponse::CallError(-38001));
        let mut fcu_engine = engine_at_safe(fcu_client);
        let fcu_asserter = Asserter::new();
        push_absent_block(&fcu_asserter);
        let fcu_provider = mock_provider(fcu_asserter);
        let err = reconcile_and_consolidate(
            &fcu_provider,
            &mut fcu_engine,
            &db,
            &reorg_batch(1, SAFE + 1),
        )
        .await
        .unwrap_err();
        assert!(!err.can_retry());
        assert!(matches!(
            err,
            ChainOrchestratorError::DerivedBatchEngineRequest { method: "fcu_with_attributes", .. }
        ));
    }

    #[tokio::test]
    async fn canonical_l2_unknown_payload_code_is_terminal() {
        let db = setup_test_db().await;
        insert_batch_commit(&db, 1).await;
        let client = Arc::new(ScriptedEngineClient::new());
        let mut engine = engine_at_safe(client);
        let asserter = Asserter::new();
        asserter.push_failure(alloy_json_rpc::ErrorPayload {
            code: -38001,
            message: Cow::Borrowed("unknown payload"),
            data: None,
        });
        let provider = mock_provider(asserter);

        let err = reconcile_and_consolidate(&provider, &mut engine, &db, &reorg_batch(1, SAFE + 1))
            .await
            .unwrap_err();

        assert!(!err.can_retry(), "canonical-L2 JSON-RPC responses are terminal");
        assert!(matches!(err, ChainOrchestratorError::RpcError(_)));
    }

    #[tokio::test]
    async fn reorg_fcu_missing_payload_id_is_terminal() {
        let db = setup_test_db().await;
        insert_batch_commit(&db, 1).await;
        let client = Arc::new(ScriptedEngineClient::new());
        // VALID but no payload id.
        client.push_fork_choice_updated(ScriptedResponse::Ok(fcu(PayloadStatusEnum::Valid, None)));
        let mut engine = engine_at_safe(client.clone());
        let asserter = Asserter::new();
        push_absent_block(&asserter);
        let provider = mock_provider(asserter);

        let err = reconcile_and_consolidate(&provider, &mut engine, &db, &reorg_batch(1, SAFE + 1))
            .await
            .unwrap_err();

        assert!(!err.can_retry());
        assert!(matches!(err, ChainOrchestratorError::MissingPayloadId { .. }));
    }

    #[tokio::test]
    async fn reorg_fcu_accepted_is_terminal() {
        let db = setup_test_db().await;
        insert_batch_commit(&db, 1).await;
        let client = Arc::new(ScriptedEngineClient::new());
        client
            .push_fork_choice_updated(ScriptedResponse::Ok(fcu(PayloadStatusEnum::Accepted, None)));
        let mut engine = engine_at_safe(client.clone());
        let asserter = Asserter::new();
        push_absent_block(&asserter);
        let provider = mock_provider(asserter);

        let err = reconcile_and_consolidate(&provider, &mut engine, &db, &reorg_batch(1, SAFE + 1))
            .await
            .unwrap_err();

        assert!(!err.can_retry(), "ACCEPTED for a forkchoice update must be terminal");
        assert!(matches!(err, ChainOrchestratorError::UnexpectedEngineStatus { .. }));
    }

    #[tokio::test]
    async fn reorg_engine_timeout_is_transient() {
        let db = setup_test_db().await;
        insert_batch_commit(&db, 1).await;
        let client = Arc::new(ScriptedEngineClient::new());
        // The build FCU times out.
        client.push_fork_choice_updated(ScriptedResponse::Timeout);
        let mut engine = engine_at_safe(client.clone());
        let asserter = Asserter::new();
        push_absent_block(&asserter);
        let provider = mock_provider(asserter);

        let err = reconcile_and_consolidate(&provider, &mut engine, &db, &reorg_batch(1, SAFE + 1))
            .await
            .unwrap_err();

        assert!(err.can_retry(), "a transport timeout must be transient");
    }

    fn short_retry(max_attempts: u32) -> DerivedBatchRetryConfig {
        DerivedBatchRetryConfig { max_attempts, initial_backoff_ms: 1, max_backoff_ms: 2 }
    }

    #[tokio::test]
    async fn due_attempt_closes_command_polling_gate() {
        let mut driver = DerivationDriver::new(short_retry(3));
        assert!(driver.can_poll_commands());

        driver.hold_batch(reorg_batch(1, SAFE + 1));

        assert!(driver.is_attempt_scheduled());
        assert!(
            !driver.can_poll_commands(),
            "a ready first attempt must win over continuous unbounded command traffic"
        );
    }

    #[tokio::test]
    async fn cancelled_wait_retains_the_scheduled_timer() {
        let mut driver = DerivationDriver::new(short_retry(3));
        driver.attempt_sleep = Some(Box::pin(tokio::time::sleep(Duration::from_secs(30))));
        let timer = driver.attempt_sleep.as_ref().map(|sleep| &raw const **sleep).unwrap();

        tokio::select! {
            biased;
            () = std::future::ready(()) => {}
            () = driver.wait_for_attempt() => panic!("the timer should still be pending"),
        }

        let retained = driver.attempt_sleep.as_ref().map(|sleep| &raw const **sleep).unwrap();
        assert!(
            std::ptr::eq(retained, timer),
            "select cancellation must retain the pinned retry timer"
        );
    }

    #[tokio::test]
    async fn driver_gates_pipeline_and_retries_then_succeeds() {
        let db = setup_test_db().await;
        insert_batch_commit(&db, 1).await;
        let client = Arc::new(ScriptedEngineClient::new());
        // Attempt 1: build FCU SYNCING (transient). Attempt 2: a full successful reorg.
        client
            .push_fork_choice_updated(ScriptedResponse::Ok(fcu(PayloadStatusEnum::Syncing, None)));
        script_successful_reorg(&client, SAFE + 1);
        let mut engine = engine_at_safe(client.clone());
        let asserter = Asserter::new();
        push_absent_block(&asserter); // attempt 1 reconcile
        push_absent_block(&asserter); // attempt 2 reconcile
        let provider = mock_provider(asserter);

        let mut driver = DerivationDriver::new(short_retry(3));
        assert!(driver.can_accept_batch());
        driver.hold_batch(reorg_batch(1, SAFE + 1));
        // While a batch is held, the pipeline gate is closed: no later result may be polled.
        assert!(!driver.can_accept_batch());
        assert!(driver.is_attempt_scheduled());

        // Attempt 1 fails transiently and schedules a retry; no events, gate still closed.
        driver.wait_for_attempt().await;
        let step = driver.run_attempt(&provider, &mut engine, &db).await;
        assert!(matches!(step, AttemptStep::Retrying));
        assert!(!driver.can_accept_batch());

        // Status remains queryable during backoff and reports the pending/retrying details.
        let status = driver.status(4);
        match status {
            DerivationPipelineStatus::Reconciling(batch) => {
                assert!(batch.backing_off);
                assert_eq!(batch.attempts_completed, 1);
                assert_eq!(batch.max_attempts, 3);
                assert_eq!(batch.queued_behind, 4);
                assert!(batch.last_error.is_some());
            }
            other => panic!("expected Reconciling, got {other:?}"),
        }

        // Attempt 2 succeeds; the gate reopens and the staged events are returned.
        driver.wait_for_attempt().await;
        let step = driver.run_attempt(&provider, &mut engine, &db).await;
        match step {
            AttemptStep::Completed(consolidated) => {
                assert_eq!(consolidated.block_outcomes.len(), 1);
            }
            other => panic!("expected Completed, got {other:?}"),
        }
        assert!(driver.can_accept_batch());
        assert!(matches!(driver.status(0), DerivationPipelineStatus::Idle));
    }

    #[tokio::test]
    async fn driver_exhausts_retries_and_fails_fatally() {
        let db = setup_test_db().await;
        insert_batch_commit(&db, 1).await;
        let client = Arc::new(ScriptedEngineClient::new());
        // Both attempts return SYNCING; max_attempts is 2.
        client
            .push_fork_choice_updated(ScriptedResponse::Ok(fcu(PayloadStatusEnum::Syncing, None)));
        client
            .push_fork_choice_updated(ScriptedResponse::Ok(fcu(PayloadStatusEnum::Syncing, None)));
        let mut engine = engine_at_safe(client.clone());
        let asserter = Asserter::new();
        push_absent_block(&asserter);
        push_absent_block(&asserter);
        let provider = mock_provider(asserter);

        let mut driver = DerivationDriver::new(short_retry(2));
        driver.hold_batch(reorg_batch(1, SAFE + 1));

        driver.wait_for_attempt().await;
        assert!(matches!(
            driver.run_attempt(&provider, &mut engine, &db).await,
            AttemptStep::Retrying
        ));

        driver.wait_for_attempt().await;
        let step = driver.run_attempt(&provider, &mut engine, &db).await;
        let AttemptStep::Fatal(err) = step else {
            panic!("expected Fatal, got {step:?}");
        };
        assert!(!err.can_retry(), "the exhaustion wrapper must be terminal");
        match err {
            ChainOrchestratorError::DerivedBatchRetriesExhausted {
                batch_info,
                attempts,
                last_error,
            } => {
                assert_eq!(batch_info.index, 1);
                assert_eq!(attempts, 2);
                assert!(last_error.can_retry(), "the typed source remains transient");
            }
            other => panic!("expected typed exhaustion, got {other:?}"),
        }
        assert!(matches!(
            driver.status(0),
            DerivationPipelineStatus::Reconciling(ReconcilingBatch { attempts_completed: 2, .. })
        ));
    }

    #[tokio::test]
    async fn late_final_fcu_timeout_recovers_via_matching_block_without_rebuild() {
        let db = setup_test_db().await;
        insert_batch_commit(&db, 1).await;
        db.set_l2_head_block_number(SAFE).await.unwrap();
        let (canonical_block, target) = matching_rpc_block(SAFE + 1);
        let client = Arc::new(ScriptedEngineClient::new());

        // Attempt 1 builds and validates the block, then its final FCU takes effect remotely but
        // its response times out. Attempt 2 needs only one strict FCU for the matching canonical
        // block; no second payload build is scripted, so an accidental rebuild fails the test.
        client.push_fork_choice_updated(ScriptedResponse::Ok(fcu(
            PayloadStatusEnum::Valid,
            Some(PayloadId::new([7; 8])),
        )));
        client.push_get_payload(ScriptedResponse::Ok(execution_payload_with_hash(
            target.number,
            target.hash,
        )));
        client.push_new_payload(ScriptedResponse::Ok(payload_status(PayloadStatusEnum::Valid)));
        client.push_fork_choice_updated(ScriptedResponse::Timeout);
        client.push_fork_choice_updated(ScriptedResponse::Ok(fcu(PayloadStatusEnum::Valid, None)));

        let mut engine = engine_at_safe(client.clone());
        let asserter = Asserter::new();
        push_absent_block(&asserter); // attempt 1: block is not canonical yet
        push_block(&asserter, &canonical_block); // attempt 2: late final FCU applied it remotely
        let provider = mock_provider(asserter);
        let mut driver = DerivationDriver::new(short_retry(3));
        driver.hold_batch(matching_batch(1, target.number));
        let mut events = Vec::new();

        driver.wait_for_attempt().await;
        assert!(matches!(
            driver.run_attempt(&provider, &mut engine, &db).await,
            AttemptStep::Retrying
        ));
        assert!(events.is_empty(), "a failed attempt must not publish staged events");
        assert_eq!(client.fork_choice_updated_calls(), 2, "the timeout is the final FCU");
        assert_eq!(client.get_payload_calls(), 1);
        assert_eq!(client.new_payload_calls(), 1);
        assert_eq!(*engine.fcs().head_block_info(), info(SAFE, 0x11));
        assert_eq!(
            db.get_l2_head_block_number().await.unwrap(),
            SAFE,
            "the failed attempt must leave the durable head unchanged"
        );

        driver.wait_for_attempt().await;
        let consolidated = match driver.run_attempt(&provider, &mut engine, &db).await {
            AttemptStep::Completed(consolidated) => consolidated,
            other => panic!("expected matching-block recovery, got {other:?}"),
        };
        assert!(matches!(
            consolidated.block_outcomes.as_slice(),
            [BlockConsolidationOutcome::UpdateFcs(block)] if block.block_info == target
        ));
        for outcome in consolidated.block_outcomes {
            events.push(ChainOrchestratorEvent::BlockConsolidated(outcome));
        }
        events.push(ChainOrchestratorEvent::BatchConsolidated(consolidated.batch_outcome));

        assert_eq!(client.new_payload_calls(), 1, "the canonical block must not be rebuilt");
        assert_eq!(client.get_payload_calls(), 1, "the canonical block must not be rebuilt");
        assert_eq!(client.fork_choice_updated_calls(), 3);
        assert_eq!(*engine.fcs().head_block_info(), target);
        assert_eq!(*engine.fcs().safe_block_info(), target);
        assert_eq!(engine.fcs().finalized_block_info().number, SAFE);
        assert_eq!(
            db.get_l2_head_block_number().await.unwrap(),
            target.number,
            "matching-block recovery atomically advances the durable head"
        );
        assert_eq!(db.get_l2_block_info_by_number(target.number).await.unwrap(), Some(target));
        assert_eq!(
            db.get_l2_block_and_batch_info_by_hash(target.hash).await.unwrap(),
            Some((target, BatchInfo::new(1, B256::repeat_byte(0xba)))),
            "one durable block/batch outcome commits"
        );
        assert_eq!(events.len(), 2);
        assert!(matches!(
            &events[0],
            ChainOrchestratorEvent::BlockConsolidated(outcome)
                if outcome.block_info().block_info == target
        ));
        assert!(matches!(
            &events[1],
            ChainOrchestratorEvent::BatchConsolidated(outcome)
                if outcome.batch_info.index == 1 && outcome.blocks.len() == 1
        ));
        assert!(driver.can_accept_batch());
    }

    #[tokio::test]
    async fn update_fcs_retains_later_canonical_engine_and_database_head() {
        let db = setup_test_db().await;
        insert_batch_commit(&db, 1).await;
        let (target_block, target) = matching_rpc_block(SAFE + 1);
        let (later_block, later_head) = matching_rpc_block(SAFE + 2);
        db.set_l2_head_block_number(later_head.number).await.unwrap();

        let client = Arc::new(ScriptedEngineClient::new());
        client.push_fork_choice_updated(ScriptedResponse::Ok(fcu(PayloadStatusEnum::Valid, None)));
        let safe = info(SAFE, 0x11);
        let mut engine = Engine::new(client.clone(), ForkchoiceState::new(later_head, safe, safe));

        let asserter = Asserter::new();
        push_block(&asserter, &target_block); // reconcile the derived target
        push_block(&asserter, &later_block); // confirm the existing later head is canonical
        let provider = mock_provider(asserter);
        let mut driver = DerivationDriver::new(short_retry(1));
        driver.hold_batch(matching_batch(1, target.number));

        driver.wait_for_attempt().await;
        let consolidated = match driver.run_attempt(&provider, &mut engine, &db).await {
            AttemptStep::Completed(consolidated) => consolidated,
            other => panic!("expected successful UpdateFcs, got {other:?}"),
        };

        assert!(matches!(
            consolidated.block_outcomes.as_slice(),
            [BlockConsolidationOutcome::UpdateFcs(block)] if block.block_info == target
        ));
        assert_eq!(*engine.fcs().head_block_info(), later_head);
        assert_eq!(*engine.fcs().safe_block_info(), target);
        assert_eq!(
            db.get_l2_head_block_number().await.unwrap(),
            later_head.number,
            "retaining a later canonical Engine head must not regress the durable head"
        );
        assert_eq!(client.fork_choice_updated_calls(), 1);
        assert_eq!(client.get_payload_calls(), 0);
        assert_eq!(client.new_payload_calls(), 0);
    }

    #[tokio::test]
    async fn invalidation_cancels_retry_without_engine_persistence_or_fatal() {
        let db = setup_test_db().await;
        insert_batch_commit(&db, 1).await;
        let client = Arc::new(ScriptedEngineClient::new());
        client
            .push_fork_choice_updated(ScriptedResponse::Ok(fcu(PayloadStatusEnum::Syncing, None)));
        let mut engine = engine_at_safe(client.clone());
        let asserter = Asserter::new();
        push_absent_block(&asserter);
        let provider = mock_provider(asserter);
        let mut driver = DerivationDriver::new(DerivedBatchRetryConfig {
            max_attempts: 2,
            initial_backoff_ms: 10,
            max_backoff_ms: 10,
        });

        driver.hold_batch(reorg_batch(1, SAFE + 1));
        driver.wait_for_attempt().await;
        assert!(matches!(
            driver.run_attempt(&provider, &mut engine, &db).await,
            AttemptStep::Retrying
        ));
        assert_eq!(client.fork_choice_updated_calls(), 1);

        assert_eq!(driver.invalidate(), Some(BatchInfo::new(1, B256::repeat_byte(0xba))));
        tokio::time::sleep(Duration::from_millis(30)).await;

        assert!(driver.can_accept_batch());
        assert!(!driver.is_attempt_scheduled());
        assert_eq!(client.fork_choice_updated_calls(), 1, "the orphaned batch must not retry");
        assert_eq!(client.get_payload_calls(), 0);
        assert_eq!(client.new_payload_calls(), 0);
        assert_eq!(
            db.get_batch_status_by_hash(B256::repeat_byte(0xba)).await.unwrap(),
            Some(BatchStatus::Committed),
            "an invalidated attempt must not persist a consolidation outcome"
        );
    }

    #[tokio::test]
    async fn reorg_signal_preempts_in_flight_attempt_and_invalidates_it() {
        let db = setup_test_db().await;
        insert_batch_commit(&db, 1).await;
        let client = Arc::new(ScriptedEngineClient::new());
        client.push_fork_choice_updated(ScriptedResponse::DelayThen(
            Duration::from_secs(30),
            Box::new(ScriptedResponse::Ok(fcu(
                PayloadStatusEnum::Valid,
                Some(PayloadId::new([7; 8])),
            ))),
        ));
        let mut engine = engine_at_safe(client.clone());
        let asserter = Asserter::new();
        push_absent_block(&asserter);
        let provider = mock_provider(asserter);
        let mut driver = DerivationDriver::new(short_retry(2));
        driver.hold_batch(reorg_batch(1, SAFE + 1));
        driver.wait_for_attempt().await;

        let preempted = {
            let mut attempt = Box::pin(driver.run_attempt(&provider, &mut engine, &db));
            tokio::select! {
                biased;
                () = tokio::time::sleep(Duration::from_millis(20)) => true,
                step = &mut attempt => panic!("reorg must preempt the attempt, got {step:?}"),
            }
        };
        assert!(preempted);
        assert_eq!(driver.invalidate(), Some(BatchInfo::new(1, B256::repeat_byte(0xba))));

        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(!driver.is_attempt_scheduled(), "the invalidated batch must not retry");
        assert_eq!(client.fork_choice_updated_calls(), 1);
        assert_eq!(client.get_payload_calls(), 0);
        assert_eq!(client.new_payload_calls(), 0);
        assert_eq!(
            db.get_batch_status_by_hash(B256::repeat_byte(0xba)).await.unwrap(),
            Some(BatchStatus::Committed),
            "the preempted batch must not persist"
        );
    }

    #[tokio::test]
    async fn driver_terminal_outcome_is_fatal_immediately() {
        let db = setup_test_db().await;
        insert_batch_commit(&db, 1).await;
        let client = Arc::new(ScriptedEngineClient::new());
        client.push_fork_choice_updated(ScriptedResponse::Ok(fcu(
            PayloadStatusEnum::Valid,
            Some(PayloadId::new([7; 8])),
        )));
        client.push_get_payload(ScriptedResponse::Ok(execution_payload(SAFE + 1, 0x22)));
        client.push_new_payload(ScriptedResponse::Ok(payload_status(PayloadStatusEnum::Invalid {
            validation_error: "bad".to_string(),
        })));
        let mut engine = engine_at_safe(client.clone());
        let asserter = Asserter::new();
        push_absent_block(&asserter);
        let provider = mock_provider(asserter);

        let mut driver = DerivationDriver::new(short_retry(5));
        driver.hold_batch(reorg_batch(1, SAFE + 1));
        driver.wait_for_attempt().await;
        let step = driver.run_attempt(&provider, &mut engine, &db).await;
        match step {
            AttemptStep::Fatal(err) => assert!(!err.can_retry()),
            other => panic!("expected Fatal, got {other:?}"),
        }
    }

    /// A minimal harness mirroring the run loop's derivation branches: it polls the next batch only
    /// while the gate is open, drives attempts, and emits staged block events followed by the
    /// single batch event on success. Proves no later batch is polled before the held one
    /// commits.
    #[tokio::test]
    async fn run_loop_processes_batches_in_order_with_single_batch_event() {
        let db = setup_test_db().await;
        insert_batch_commit(&db, 1).await;
        insert_batch_commit(&db, 2).await;
        let client = Arc::new(ScriptedEngineClient::new());
        script_successful_reorg(&client, SAFE + 1); // batch 1
        script_successful_reorg(&client, SAFE + 2); // batch 2
        let mut engine = engine_at_safe(client.clone());
        let asserter = Asserter::new();
        push_absent_block(&asserter);
        push_absent_block(&asserter);
        let provider = mock_provider(asserter);

        let mut pipeline: VecDeque<BatchDerivationResult> =
            VecDeque::from([reorg_batch(1, SAFE + 1), reorg_batch(2, SAFE + 2)]);
        let mut driver = DerivationDriver::new(short_retry(3));
        let mut events: Vec<ChainOrchestratorEvent> = Vec::new();

        loop {
            if driver.can_accept_batch() {
                if let Some(batch) = pipeline.pop_front() {
                    // The first batch is held before the second is ever polled.
                    driver.hold_batch(batch);
                }
            }
            if !driver.is_attempt_scheduled() {
                break;
            }
            driver.wait_for_attempt().await;
            match driver.run_attempt(&provider, &mut engine, &db).await {
                AttemptStep::Completed(consolidated) => {
                    for outcome in consolidated.block_outcomes {
                        events.push(ChainOrchestratorEvent::BlockConsolidated(outcome));
                    }
                    events.push(ChainOrchestratorEvent::BatchConsolidated(
                        consolidated.batch_outcome,
                    ));
                }
                AttemptStep::Retrying => {}
                AttemptStep::Fatal(err) => panic!("unexpected fatal: {err}"),
            }
        }

        // Exactly one block event and one batch event per batch, in order, and no duplicates.
        let batch_events = events
            .iter()
            .filter(|e| matches!(e, ChainOrchestratorEvent::BatchConsolidated(_)))
            .count();
        let block_events = events
            .iter()
            .filter(|e| matches!(e, ChainOrchestratorEvent::BlockConsolidated(_)))
            .count();
        assert_eq!(batch_events, 2, "one batch event per batch");
        assert_eq!(block_events, 2, "one block event per batch");
        // Each batch's block event precedes its batch event.
        assert!(matches!(events[0], ChainOrchestratorEvent::BlockConsolidated(_)));
        assert!(matches!(events[1], ChainOrchestratorEvent::BatchConsolidated(_)));
        assert!(matches!(events[2], ChainOrchestratorEvent::BlockConsolidated(_)));
        assert!(matches!(events[3], ChainOrchestratorEvent::BatchConsolidated(_)));
    }

    #[tokio::test]
    async fn coherent_head_advances_when_local_head_lags() {
        // The provider must not be queried on this branch.
        let provider = mock_provider(Asserter::new());
        let head = info(100, 0xaa);
        let target = info(105, 0xbb);
        assert_eq!(
            coherent_head_for_update_fcs(&provider, head, target).await.unwrap(),
            Some(target),
            "a lagging local head is advanced to the matching canonical target"
        );
    }

    #[tokio::test]
    async fn coherent_head_replaces_stale_equal_height_head() {
        let provider = mock_provider(Asserter::new());
        let head = info(100, 0xaa);
        let target = info(100, 0xbb); // same height, different hash
        assert_eq!(
            coherent_head_for_update_fcs(&provider, head, target).await.unwrap(),
            Some(target),
            "a stale equal-height head is replaced, not paired with the new safe block"
        );
    }

    #[tokio::test]
    async fn coherent_head_keeps_matching_equal_height_head() {
        let provider = mock_provider(Asserter::new());
        let head = info(100, 0xaa);
        let target = info(100, 0xaa); // same height, same hash
        assert_eq!(
            coherent_head_for_update_fcs(&provider, head, target).await.unwrap(),
            None,
            "an already-correct head is preserved; only safe/finalized advance"
        );
    }

    #[tokio::test]
    async fn coherent_head_keeps_canonical_later_head() {
        let target = info(100, 0xaa);
        let (mut canonical_block, _) = matching_rpc_block(105);
        let head = info(105, 0xcc);
        canonical_block.header.hash = head.hash;
        let asserter = Asserter::new();
        push_block(&asserter, &canonical_block);
        let provider = mock_provider(asserter);

        assert_eq!(
            coherent_head_for_update_fcs(&provider, head, target).await.unwrap(),
            None,
            "a later head confirmed canonical remains the head"
        );
    }

    #[tokio::test]
    async fn coherent_head_replaces_absent_or_mismatched_later_head() {
        let target = info(100, 0xaa);
        let head = info(105, 0xcc);

        let absent_asserter = Asserter::new();
        push_absent_block(&absent_asserter);
        let absent_provider = mock_provider(absent_asserter);
        assert_eq!(
            coherent_head_for_update_fcs(&absent_provider, head, target).await.unwrap(),
            Some(target),
            "an absent later head falls back to the coherent target"
        );

        let (mismatched_block, _) = matching_rpc_block(head.number);
        assert_ne!(mismatched_block.header.hash, head.hash);
        let mismatched_asserter = Asserter::new();
        push_block(&mismatched_asserter, &mismatched_block);
        let mismatched_provider = mock_provider(mismatched_asserter);
        assert_eq!(
            coherent_head_for_update_fcs(&mismatched_provider, head, target).await.unwrap(),
            Some(target),
            "a noncanonical later head falls back to the coherent target"
        );
    }

    #[tokio::test]
    async fn shutdown_cancels_in_flight_attempt() {
        let db = setup_test_db().await;
        insert_batch_commit(&db, 1).await;
        let client = Arc::new(ScriptedEngineClient::new());
        // The build FCU hangs far longer than the test tolerance.
        client.push_fork_choice_updated(ScriptedResponse::DelayThen(
            Duration::from_secs(30),
            Box::new(ScriptedResponse::Ok(fcu(
                PayloadStatusEnum::Valid,
                Some(PayloadId::new([7; 8])),
            ))),
        ));
        let mut engine = engine_at_safe(client.clone());
        let asserter = Asserter::new();
        push_absent_block(&asserter);
        let provider = mock_provider(asserter);

        let mut driver = DerivationDriver::new(short_retry(3));
        driver.hold_batch(reorg_batch(1, SAFE + 1));
        driver.wait_for_attempt().await;

        // Mirror the run loop's nested select: a shutdown signal must cancel the in-flight attempt.
        let start = std::time::Instant::now();
        let cancelled = tokio::select! {
            biased;
            () = tokio::time::sleep(Duration::from_millis(20)) => true,
            _ = driver.run_attempt(&provider, &mut engine, &db) => false,
        };
        assert!(cancelled, "the in-flight attempt should be cancelled by shutdown");
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "shutdown must not wait for the hung engine request"
        );
    }
}
