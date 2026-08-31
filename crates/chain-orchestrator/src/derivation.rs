//! Ordered reconciliation for the single derived batch owned by the orchestrator.

use crate::{
    consolidation::{reconcile_batch, BlockConsolidationAction},
    frontier::{apply_pending_frontier_transition, ensure_database_frontier, stored_forkchoice},
    metrics::DerivedBatchMetrics,
    status::{DerivationStatus, HeldBatchStatus},
    ChainOrchestratorError,
};
use alloy_provider::Provider;
use alloy_rpc_types_engine::{ForkchoiceUpdated, PayloadId, PayloadStatus, PayloadStatusEnum};
use dogeos_rpc_types::Scroll;
use rollup_node_primitives::{
    BatchConsolidationOutcome, BatchInfo, BatchStatus, BlockConsolidationOutcome,
    L2BlockInfoWithL1Messages,
};
use scroll_db::{
    Database, DatabaseReadOperations, DatabaseWriteOperations, FrontierTransitionKind,
    PendingFrontierTransition, StoredForkchoiceState, UnwindResult,
};
use scroll_derivation_pipeline::BatchDerivationResult;
use scroll_engine::{payload_matches_attributes, Engine, EngineError, ScrollEngineApi};
use std::{pin::Pin, time::Duration};
use tokio::time::{Instant, Sleep};

const INITIAL_HOLD_BACKOFF_MS: u64 = 2_000;
const MAX_HOLD_BACKOFF_MS: u64 = 30_000;

const BUILD_FCU_METHOD: &str = "forkchoiceUpdated(build)";
const GET_PAYLOAD_METHOD: &str = "getPayload";
const NEW_PAYLOAD_METHOD: &str = "newPayload";
const FCU_METHOD: &str = "forkchoiceUpdated";

/// The successful result of a reconciliation attempt. Events are emitted by the run loop only
/// after the database commit performed by the attempt has succeeded.
#[derive(Debug)]
pub(crate) struct ConsolidatedBatch {
    pub(crate) batch_outcome: BatchConsolidationOutcome,
    pub(crate) block_outcomes: Vec<BlockConsolidationOutcome>,
}

/// A terminal attempt with structured classification for the fail-stop boundary.
#[derive(Debug)]
pub(crate) struct FatalAttempt {
    pub(crate) method: &'static str,
    pub(crate) outcome: &'static str,
    pub(crate) error: Box<ChainOrchestratorError>,
}

/// The result returned to the orchestrator after one scheduled attempt.
#[derive(Debug)]
pub(crate) enum AttemptStep {
    Completed(ConsolidatedBatch),
    Held,
    Fatal(FatalAttempt),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HoldStatus {
    Syncing,
    Accepted,
}

impl HoldStatus {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Syncing => "SYNCING",
            Self::Accepted => "ACCEPTED",
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct HoldReason {
    method: &'static str,
    status: HoldStatus,
}

#[derive(Debug)]
enum AttemptFailure {
    Hold(HoldReason),
    Fatal(FatalAttempt),
}

impl AttemptFailure {
    fn fatal(method: &'static str, outcome: &'static str, error: ChainOrchestratorError) -> Self {
        Self::Fatal(FatalAttempt { method, outcome, error: Box::new(error) })
    }
}

#[derive(Debug)]
struct PendingBatch {
    batch: BatchDerivationResult,
    held_since: std::time::Instant,
    attempts_started: u64,
    last_hold: Option<HoldReason>,
    current_backoff_ms: Option<u64>,
}

/// Copyable metadata used to revalidate a held batch after an L1 unwind.
#[derive(Debug, Clone, Copy)]
pub(crate) struct HeldBatchIdentity {
    pub(crate) batch_info: BatchInfo,
    pub(crate) target_status: BatchStatus,
}

/// The effect of an L1 unwind on the single held slot.
#[derive(Debug, Clone, Copy)]
pub(crate) enum HeldReorgOutcome {
    NoHeldBatch,
    Invalidated { batch_info: BatchInfo, reason: &'static str },
    Survived { batch_info: BatchInfo },
}

/// Owns exactly one derived result and its pinned hold timer.
#[derive(Debug, Default)]
pub(crate) struct DerivationDriver {
    pending: Option<PendingBatch>,
    attempt_sleep: Option<Pin<Box<Sleep>>>,
    metrics: DerivedBatchMetrics,
}

impl DerivationDriver {
    /// Returns whether the pipeline may yield another result into the owned slot.
    pub(crate) const fn can_accept_batch(&self) -> bool {
        self.pending.is_none()
    }

    /// Returns whether an immediate attempt or held retry is scheduled.
    pub(crate) const fn is_attempt_scheduled(&self) -> bool {
        self.attempt_sleep.is_some()
    }

    /// Moves a newly yielded result into the slot before any Engine action is attempted.
    pub(crate) fn hold_batch(&mut self, batch: BatchDerivationResult) {
        assert!(self.pending.is_none(), "a derived batch is already held");
        let batch_info = batch.batch_info;
        tracing::info!(
            target: "scroll::chain_orchestrator",
            batch_index = batch_info.index,
            batch_hash = ?batch_info.hash,
            blocks = batch.attributes.len(),
            "Holding derived batch for reconciliation"
        );
        self.pending = Some(PendingBatch {
            batch,
            held_since: std::time::Instant::now(),
            attempts_started: 0,
            last_hold: None,
            current_backoff_ms: None,
        });
        self.metrics.held.set(1.0);
        self.schedule_immediate();
    }

    /// Waits on the pinned deadline. The same timer survives unrelated outer `select!` polls.
    pub(crate) async fn wait_for_attempt(&mut self) {
        match self.attempt_sleep.as_mut() {
            Some(sleep) => sleep.as_mut().await,
            None => std::future::pending().await,
        }
    }

    /// Runs one attempt. Only received `SYNCING`, plus `ACCEPTED` from `newPayload`, schedule a
    /// further in-process reconciliation.
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
        self.attempt_sleep = None;
        let pending = self.pending.as_mut().expect("attempt requires a held batch");
        pending.attempts_started = pending.attempts_started.saturating_add(1);
        pending.current_backoff_ms = None;
        let attempt = pending.attempts_started;
        let batch_info = pending.batch.batch_info;
        self.metrics.attempts.increment(1);

        tracing::info!(
            target: "scroll::chain_orchestrator",
            batch_index = batch_info.index,
            batch_hash = ?batch_info.hash,
            attempt,
            held_ms = saturating_millis(pending.held_since.elapsed()),
            "Attempting derived batch reconciliation"
        );

        match reconcile_and_consolidate(l2_client, engine, database, &pending.batch).await {
            Ok(consolidated) => {
                self.pending = None;
                self.metrics.held.set(0.0);
                AttemptStep::Completed(consolidated)
            }
            Err(AttemptFailure::Hold(reason)) => {
                let backoff = hold_backoff(attempt);
                let backoff_ms = saturating_millis(backoff);
                let pending = self.pending.as_mut().expect("held batch remains owned");
                pending.last_hold = Some(reason);
                pending.current_backoff_ms = Some(backoff_ms);
                self.attempt_sleep =
                    Some(Box::pin(tokio::time::sleep_until(Instant::now() + backoff)));
                tracing::warn!(
                    target: "scroll::chain_orchestrator",
                    batch_index = batch_info.index,
                    batch_hash = ?batch_info.hash,
                    attempt,
                    held_ms = saturating_millis(pending.held_since.elapsed()),
                    backoff_ms,
                    method = reason.method,
                    status = reason.status.as_str(),
                    "Engine is not ready for derived batch; holding in place"
                );
                AttemptStep::Held
            }
            Err(AttemptFailure::Fatal(fatal)) => {
                self.metrics.fatal.increment(1);
                AttemptStep::Fatal(fatal)
            }
        }
    }

    /// Returns the identity needed to check whether an unwind invalidated the held work.
    pub(crate) fn held_identity(&self) -> Option<HeldBatchIdentity> {
        self.pending.as_ref().map(|pending| HeldBatchIdentity {
            batch_info: pending.batch.batch_info,
            target_status: pending.batch.target_status,
        })
    }

    /// Atomically unwinds the database and decides whether the held row survives `ancestor`.
    pub(crate) async fn unwind_and_revalidate(
        &mut self,
        database: &Database,
        ancestor: u64,
        transition_expected: Option<StoredForkchoiceState>,
    ) -> Result<(UnwindResult, HeldReorgOutcome), ChainOrchestratorError> {
        let held = self.held_identity();
        let (unwind_result, outcome) = database
            .tx_mut(move |tx| async move {
                let unwind_result = tx.unwind(ancestor).await?;
                if let (Some(expected), Some(mut safe)) =
                    (transition_expected, unwind_result.l2_safe_block_info)
                {
                    // Finalized history cannot be rewound by an ordinary L1 unwind. Retaining the
                    // finalized block as safe mirrors the previous administrative behavior while
                    // making the intent durable in the same database transaction as the unwind.
                    if safe.number < expected.finalized.number {
                        safe = expected.finalized;
                    }
                    let head =
                        if expected.head.number < safe.number { safe } else { expected.head };
                    let target =
                        StoredForkchoiceState { head, safe, finalized: expected.finalized };
                    if target != expected {
                        tx.set_pending_frontier_transition(PendingFrontierTransition {
                            kind: FrontierTransitionKind::UnwindL1,
                            expected,
                            target,
                            batch_hash: None,
                        })
                        .await?;
                    }
                }
                let Some(held) = held else {
                    return Ok::<_, ChainOrchestratorError>((
                        unwind_result,
                        HeldReorgOutcome::NoHeldBatch,
                    ))
                };

                let stored = tx.get_batch_by_index(held.batch_info.index).await?;
                let surviving_row = stored.as_ref().filter(|batch| {
                    batch.hash == held.batch_info.hash && batch.block_number <= ancestor
                });
                let finalization_invalid = held.target_status.is_finalized() &&
                    surviving_row.is_some_and(|batch| {
                        batch.finalized_block_number.is_none_or(|number| number > ancestor)
                    });
                let invalid_reason = match stored.as_ref() {
                    None => Some("batch row removed or reverted"),
                    Some(batch) if batch.hash != held.batch_info.hash => Some("batch hash changed"),
                    Some(batch) if batch.block_number > ancestor => Some("batch commit reverted"),
                    Some(_) if finalization_invalid => Some("batch finalization reverted"),
                    Some(_) => None,
                };

                let outcome = if let Some(reason) = invalid_reason {
                    if finalization_invalid {
                        // Do not overwrite a concurrent or unwind-produced status such as
                        // Reverted/Consolidated. Only the row still owned as Processing may be
                        // returned to Committed for catch-up rediscovery.
                        tx.transition_batch_status(
                            held.batch_info.hash,
                            BatchStatus::Processing,
                            BatchStatus::Committed,
                        )
                        .await?;
                    }
                    HeldReorgOutcome::Invalidated { batch_info: held.batch_info, reason }
                } else {
                    HeldReorgOutcome::Survived { batch_info: held.batch_info }
                };

                Ok((unwind_result, outcome))
            })
            .await?;

        if matches!(outcome, HeldReorgOutcome::Invalidated { .. }) {
            self.invalidate();
        }

        Ok((unwind_result, outcome))
    }

    /// Clears an invalid held slot and its timer.
    pub(crate) fn invalidate(&mut self) {
        self.attempt_sleep = None;
        if self.pending.take().is_some() {
            self.metrics.held.set(0.0);
        }
    }

    /// Cancels only the scheduled/in-flight attempt state while retaining ownership of the batch.
    pub(crate) fn cancel_attempt(&mut self) {
        self.attempt_sleep = None;
    }

    /// Schedules a fresh reconciliation after a surviving held batch is administratively unwound.
    pub(crate) fn schedule_fresh_reconciliation(&mut self) {
        if self.pending.is_some() {
            self.schedule_immediate();
        }
    }

    /// Returns a status snapshot including the pipeline work queued behind the held slot.
    pub(crate) fn status(&self, queued: u64) -> DerivationStatus {
        match &self.pending {
            Some(pending) => DerivationStatus::Held(HeldBatchStatus {
                batch_index: pending.batch.batch_info.index,
                batch_hash: pending.batch.batch_info.hash,
                attempts_started: pending.attempts_started,
                held_duration_ms: saturating_millis(pending.held_since.elapsed()),
                last_engine_method: pending.last_hold.map(|reason| reason.method.to_string()),
                last_engine_status: pending
                    .last_hold
                    .map(|reason| reason.status.as_str().to_string()),
                current_backoff_ms: pending.current_backoff_ms,
                queued_behind: queued,
            }),
            None if queued > 0 => DerivationStatus::Deriving { queued },
            None => DerivationStatus::Idle,
        }
    }

    /// Returns structured fields used by the single fatal boundary record.
    pub(crate) fn fatal_context(&self) -> Option<(BatchInfo, u64, u64)> {
        self.pending.as_ref().map(|pending| {
            (
                pending.batch.batch_info,
                pending.attempts_started,
                saturating_millis(pending.held_since.elapsed()),
            )
        })
    }

    /// Records a fail-stop discovered outside the Engine attempt itself.
    pub(crate) fn record_fatal(&self) {
        self.metrics.fatal.increment(1);
    }

    fn schedule_immediate(&mut self) {
        if let Some(pending) = self.pending.as_mut() {
            pending.current_backoff_ms = None;
        }
        self.attempt_sleep = Some(Box::pin(tokio::time::sleep_until(Instant::now())));
    }
}

fn hold_backoff(attempt: u64) -> Duration {
    let exponent = attempt.saturating_sub(1).min(63) as u32;
    let multiplier = 1u64.checked_shl(exponent).unwrap_or(u64::MAX);
    Duration::from_millis(
        INITIAL_HOLD_BACKOFF_MS.saturating_mul(multiplier).min(MAX_HOLD_BACKOFF_MS),
    )
}

fn saturating_millis(duration: Duration) -> u64 {
    duration.as_millis().try_into().unwrap_or(u64::MAX)
}

async fn reconcile_and_consolidate<L2P, EC>(
    l2_client: &L2P,
    engine: &mut Engine<EC>,
    database: &Database,
    batch: &BatchDerivationResult,
) -> Result<ConsolidatedBatch, AttemptFailure>
where
    L2P: Provider<Scroll>,
    EC: ScrollEngineApi + Sync + Send + 'static,
{
    let batch_info = batch.batch_info;
    ensure_database_frontier(l2_client, engine, database)
        .await
        .map_err(|error| AttemptFailure::fatal("reconcileFrontier", "FRONTIER_ERROR", error))?;
    let (frontier, _) = database.get_latest_safe_l2_info().await.map_err(|error| {
        AttemptFailure::fatal(
            "getLatestSafeL2Info",
            "ERROR",
            ChainOrchestratorError::DatabaseError(error),
        )
    })?;
    let reconciliation =
        reconcile_batch(l2_client, batch, engine.fcs(), frontier).await.map_err(|error| {
            let outcome =
                if matches!(error, ChainOrchestratorError::InvalidDerivedBlockSequence { .. }) {
                    "INVALID_BLOCK_SEQUENCE"
                } else {
                    "ERROR"
                };
            AttemptFailure::fatal("reconcileBatch", outcome, error)
        })?;
    let target_status = reconciliation.target_status;
    let aggregated = reconciliation.aggregate_actions();

    let mut block_outcomes = Vec::new();
    let mut reorg_results = Vec::new();
    let mut l2_head_updated = false;

    for action in aggregated.actions {
        match action {
            BlockConsolidationAction::Skip(_) => {
                unreachable!("Skip actions have been filtered out in aggregation")
            }
            BlockConsolidationAction::UpdateFcs(block_info) => {
                let target = block_info.block_info;
                let current_head = *engine.fcs().head_block_info();
                // Confirm only the candidate head here. The durable transition below is the sole
                // operation allowed to promote a block to safe or finalized.
                let head = (current_head.number <= target.number && current_head != target)
                    .then_some(target);
                if let Some(head) = head {
                    let response = engine
                        .update_fcs_checked(Some(head), None, None)
                        .await
                        .map_err(engine_request_failure(batch_info, FCU_METHOD))?;
                    classify_checked_fcu(response, batch_info, Some(target.number))?;
                }
                l2_head_updated |= head.is_some();
                block_outcomes.push(BlockConsolidationOutcome::UpdateFcs(block_info));
            }
            BlockConsolidationAction::ReorgSuffix {
                first_attribute_index,
                mut expected_parent,
            } => {
                if *engine.fcs().head_block_info() != expected_parent {
                    let response = engine
                        .update_fcs_checked(Some(expected_parent), None, None)
                        .await
                        .map_err(engine_request_failure(batch_info, FCU_METHOD))?;
                    classify_checked_fcu(response, batch_info, Some(expected_parent.number))?;
                    l2_head_updated = true;
                }

                for attributes in &batch.attributes[first_attribute_index..] {
                    if expected_parent.number.checked_add(1) != Some(attributes.block_number) {
                        return Err(AttemptFailure::fatal(
                            "reconcileBatch",
                            "INVARIANT_ERROR",
                            ChainOrchestratorError::InvalidBatchReorg {
                                batch_info,
                                safe_block_number: expected_parent.number,
                                derived_block_number: attributes.block_number,
                            },
                        ));
                    }

                    let build_response = engine
                        .build_payload(Some(expected_parent), attributes.attributes.clone())
                        .await
                        .map_err(engine_request_failure(batch_info, BUILD_FCU_METHOD))?;
                    let payload_id = classify_build_fcu(build_response, batch_info)?;

                    let payload = engine
                        .get_payload(payload_id)
                        .await
                        .map_err(engine_request_failure(batch_info, GET_PAYLOAD_METHOD))?;
                    if !payload_matches_attributes(
                        expected_parent,
                        attributes.block_number,
                        &attributes.attributes,
                        &payload,
                    ) {
                        return Err(AttemptFailure::fatal(
                            GET_PAYLOAD_METHOD,
                            "PAYLOAD_MISMATCH",
                            ChainOrchestratorError::BuiltPayloadMismatch {
                                batch_info,
                                expected_parent: Box::new(expected_parent),
                                expected_block_number: attributes.block_number,
                                actual_parent_hash: payload.parent_hash,
                                actual_block_number: payload.block_number,
                            },
                        ));
                    }
                    let block_info: L2BlockInfoWithL1Messages =
                        (&payload).try_into().map_err(|error| {
                            AttemptFailure::fatal(
                                GET_PAYLOAD_METHOD,
                                "CONVERSION_ERROR",
                                ChainOrchestratorError::RollupNodePrimitiveError(error),
                            )
                        })?;

                    let payload_status = engine
                        .new_payload(payload)
                        .await
                        .map_err(engine_request_failure(batch_info, NEW_PAYLOAD_METHOD))?;
                    classify_new_payload(payload_status, batch_info, block_info.block_info.number)?;

                    let final_fcu = engine
                        .update_fcs_checked(Some(block_info.block_info), None, None)
                        .await
                        .map_err(engine_request_failure(batch_info, FCU_METHOD))?;
                    classify_checked_fcu(
                        final_fcu,
                        batch_info,
                        Some(block_info.block_info.number),
                    )?;
                    l2_head_updated = true;

                    expected_parent = block_info.block_info;
                    reorg_results.push(block_info.clone());
                    block_outcomes.push(BlockConsolidationOutcome::Reorged(block_info));
                }
            }
        }
    }

    let mut batch_outcome = reconciliation
        .into_batch_consolidation_outcome(reorg_results, l2_head_updated)
        .await
        .map_err(|error| AttemptFailure::fatal("consolidateBatch", "ERROR", error))?;
    if let Some(tip) = batch_outcome.blocks.last() {
        let database_head = database.get_l2_head_block_number().await.map_err(|error| {
            AttemptFailure::fatal(
                "getL2HeadBlockNumber",
                "ERROR",
                ChainOrchestratorError::DatabaseError(error),
            )
        })?;
        batch_outcome.l2_head_updated |= tip.block_info.number > database_head;
    }
    let mut persisted = batch_outcome.clone();
    persisted.with_skipped_l1_messages(batch.skipped_l1_messages.clone());

    let expected = stored_forkchoice(engine.fcs());
    let transition = if let Some(block) = persisted.blocks.last() {
        let tip = block.block_info;
        let safe = advance_frontier(expected.safe, tip, batch_info)?;
        let finalized = if target_status.is_finalized() {
            advance_frontier(expected.finalized, tip, batch_info)?
        } else {
            expected.finalized
        };
        let target = StoredForkchoiceState { head: expected.head, safe, finalized };
        (target != expected).then_some(PendingFrontierTransition {
            kind: FrontierTransitionKind::ConsolidateBatch,
            expected,
            target,
            batch_hash: Some(batch_info.hash),
        })
    } else {
        None
    };

    database
        .tx_mut(move |tx| {
            let persisted = persisted.clone();
            async move {
                tx.insert_batch_consolidation_outcome(persisted).await?;
                if let Some(transition) = transition {
                    tx.set_pending_frontier_transition(transition).await?;
                }
                Ok::<_, ChainOrchestratorError>(())
            }
        })
        .await
        .map_err(|error| AttemptFailure::fatal("commitBatchConsolidation", "ERROR", error))?;

    if transition.is_some() {
        apply_pending_frontier_transition(l2_client, engine, database).await.map_err(|error| {
            AttemptFailure::fatal("applyFrontierTransition", "FRONTIER_ERROR", error)
        })?;
    }

    Ok(ConsolidatedBatch { batch_outcome, block_outcomes })
}

fn advance_frontier(
    current: rollup_node_primitives::BlockInfo,
    candidate: rollup_node_primitives::BlockInfo,
    batch_info: BatchInfo,
) -> Result<rollup_node_primitives::BlockInfo, AttemptFailure> {
    match candidate.number.cmp(&current.number) {
        std::cmp::Ordering::Less => Ok(current),
        std::cmp::Ordering::Equal if candidate == current => Ok(current),
        std::cmp::Ordering::Equal => Err(AttemptFailure::fatal(
            "consolidateBatch",
            "FRONTIER_ERROR",
            ChainOrchestratorError::ConflictingBatchFrontier { batch_info, current, candidate },
        )),
        std::cmp::Ordering::Greater => Ok(candidate),
    }
}

fn engine_request_failure(
    batch_info: BatchInfo,
    method: &'static str,
) -> impl FnOnce(EngineError) -> AttemptFailure {
    move |source| {
        AttemptFailure::fatal(
            method,
            "ERROR",
            ChainOrchestratorError::DerivedBatchEngineRequest { batch_info, method, source },
        )
    }
}

fn classify_build_fcu(
    response: ForkchoiceUpdated,
    batch_info: BatchInfo,
) -> Result<PayloadId, AttemptFailure> {
    let PayloadStatus { status, latest_valid_hash } = response.payload_status;
    match status {
        PayloadStatusEnum::Valid => response.payload_id.ok_or_else(|| {
            AttemptFailure::fatal(
                BUILD_FCU_METHOD,
                "VALID_MISSING_PAYLOAD_ID",
                ChainOrchestratorError::MissingDerivedPayloadId { batch_info },
            )
        }),
        PayloadStatusEnum::Syncing => Err(AttemptFailure::Hold(HoldReason {
            method: BUILD_FCU_METHOD,
            status: HoldStatus::Syncing,
        })),
        PayloadStatusEnum::Accepted => {
            Err(unexpected_status(batch_info, BUILD_FCU_METHOD, "ACCEPTED"))
        }
        PayloadStatusEnum::Invalid { validation_error } => Err(invalid_status(
            batch_info,
            BUILD_FCU_METHOD,
            None,
            latest_valid_hash,
            validation_error,
        )),
    }
}

fn classify_new_payload(
    response: PayloadStatus,
    batch_info: BatchInfo,
    block_number: u64,
) -> Result<(), AttemptFailure> {
    match response.status {
        PayloadStatusEnum::Valid => Ok(()),
        PayloadStatusEnum::Syncing => Err(AttemptFailure::Hold(HoldReason {
            method: NEW_PAYLOAD_METHOD,
            status: HoldStatus::Syncing,
        })),
        PayloadStatusEnum::Accepted => Err(AttemptFailure::Hold(HoldReason {
            method: NEW_PAYLOAD_METHOD,
            status: HoldStatus::Accepted,
        })),
        PayloadStatusEnum::Invalid { validation_error } => Err(invalid_status(
            batch_info,
            NEW_PAYLOAD_METHOD,
            Some(block_number),
            response.latest_valid_hash,
            validation_error,
        )),
    }
}

fn classify_checked_fcu(
    response: ForkchoiceUpdated,
    batch_info: BatchInfo,
    block_number: Option<u64>,
) -> Result<(), AttemptFailure> {
    match response.payload_status.status {
        PayloadStatusEnum::Valid => Ok(()),
        PayloadStatusEnum::Syncing => Err(AttemptFailure::Hold(HoldReason {
            method: FCU_METHOD,
            status: HoldStatus::Syncing,
        })),
        PayloadStatusEnum::Accepted => Err(unexpected_status(batch_info, FCU_METHOD, "ACCEPTED")),
        PayloadStatusEnum::Invalid { validation_error } => Err(invalid_status(
            batch_info,
            FCU_METHOD,
            block_number,
            response.payload_status.latest_valid_hash,
            validation_error,
        )),
    }
}

fn unexpected_status(
    batch_info: BatchInfo,
    method: &'static str,
    status: &'static str,
) -> AttemptFailure {
    AttemptFailure::fatal(
        method,
        status,
        ChainOrchestratorError::UnexpectedDerivedPayloadStatus { batch_info, method, status },
    )
}

fn invalid_status(
    batch_info: BatchInfo,
    method: &'static str,
    block_number: Option<u64>,
    latest_valid_hash: Option<alloy_primitives::B256>,
    validation_error: String,
) -> AttemptFailure {
    AttemptFailure::fatal(
        method,
        "INVALID",
        ChainOrchestratorError::InvalidDerivedPayload {
            batch_info,
            method,
            block_number,
            latest_valid_hash,
            validation_error: validation_error.into_boxed_str(),
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ChainOrchestratorStatus, SyncState};
    use alloy_consensus::Header as ConsensusHeader;
    use alloy_primitives::{Address, Bloom, Bytes, Sealable, B256, U256};
    use alloy_provider::ProviderBuilder;
    use alloy_rpc_types_engine::{ExecutionPayloadV1, PayloadId};
    use alloy_rpc_types_eth::{Block as RpcBlock, Header as RpcHeader};
    use alloy_transport::mock::Asserter;
    use dogeos_reth_engine::ScrollPayloadAttributes;
    use dogeos_rpc_types::ScrollRpcTransaction;
    use rollup_node_primitives::{BatchCommitData, BlockInfo};
    use scroll_db::{test_utils::setup_test_db, DatabaseReadOperations};
    use scroll_derivation_pipeline::DerivedAttributes;
    use scroll_engine::{
        test_utils::{ScriptedEngineClient, ScriptedResponse},
        ForkchoiceState,
    };
    use std::{collections::VecDeque, sync::Arc};

    const SAFE: u64 = 100;

    #[derive(Clone, Copy)]
    enum HoldBoundary {
        BuildFcuSyncing,
        NewPayloadSyncing,
        NewPayloadAccepted,
        FinalFcuSyncing,
    }

    fn info(number: u64, tag: u8) -> BlockInfo {
        BlockInfo { number, hash: B256::repeat_byte(tag) }
    }

    fn payload_status(status: PayloadStatusEnum) -> PayloadStatus {
        PayloadStatus { status, latest_valid_hash: None }
    }

    fn fcu(status: PayloadStatusEnum, payload_id: Option<PayloadId>) -> ForkchoiceUpdated {
        ForkchoiceUpdated { payload_status: payload_status(status), payload_id }
    }

    fn payload(number: u64) -> ExecutionPayloadV1 {
        ExecutionPayloadV1 {
            parent_hash: info(SAFE, 0x11).hash,
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

    fn engine_at_safe(client: Arc<ScriptedEngineClient>) -> Engine<ScriptedEngineClient> {
        let safe = info(SAFE, 0x11);
        Engine::new(client, ForkchoiceState::from_block_info(safe))
    }

    async fn setup_database() -> Database {
        let database = setup_test_db().await;
        database
            .insert_blocks(vec![info(SAFE, 0x11)], BatchInfo::new(0, B256::ZERO))
            .await
            .unwrap();
        database
    }

    fn batch(index: u64, target_status: BatchStatus) -> BatchDerivationResult {
        let attributes =
            ScrollPayloadAttributes { transactions: Some(vec![]), ..Default::default() };
        BatchDerivationResult {
            attributes: vec![DerivedAttributes { block_number: SAFE + 1, attributes }],
            batch_info: BatchInfo::new(index, B256::repeat_byte(index as u8)),
            skipped_l1_messages: vec![],
            target_status,
        }
    }

    async fn insert_batch(
        database: &Database,
        index: u64,
        block_number: u64,
        finalized_block_number: Option<u64>,
    ) {
        database
            .insert_batch(BatchCommitData {
                hash: B256::repeat_byte(index as u8),
                index,
                block_number,
                block_timestamp: 0,
                calldata: Arc::new(Bytes::new()),
                blob_versioned_hash: None,
                finalized_block_number,
                reverted_block_number: None,
            })
            .await
            .unwrap();
    }

    fn absent_block_provider(asserter: Asserter) -> impl Provider<Scroll> {
        ProviderBuilder::<_, _, Scroll>::default().connect_mocked_client(asserter)
    }

    fn push_absent_block(asserter: &Asserter) {
        asserter.push_success(&Option::<()>::None);
    }

    fn push_matching_empty_block(
        asserter: &Asserter,
        attributes: &ScrollPayloadAttributes,
    ) -> BlockInfo {
        push_matching_empty_block_on(asserter, attributes, info(SAFE, 0x11), SAFE + 1)
    }

    fn push_matching_empty_block_on(
        asserter: &Asserter,
        attributes: &ScrollPayloadAttributes,
        parent: BlockInfo,
        number: u64,
    ) -> BlockInfo {
        let header = ConsensusHeader {
            parent_hash: parent.hash,
            number,
            timestamp: attributes.payload_attributes.timestamp,
            mix_hash: attributes.payload_attributes.prev_randao,
            beneficiary: attributes.block_data_hint.coinbase.unwrap_or_default(),
            extra_data: attributes.block_data_hint.extra_data.clone().unwrap_or_default(),
            state_root: attributes.block_data_hint.state_root.unwrap_or_default(),
            nonce: attributes.block_data_hint.nonce.unwrap_or_default().into(),
            ..Default::default()
        };
        let sealed = header.seal_slow();
        let block_info = BlockInfo { number, hash: sealed.hash() };
        let rpc_header = RpcHeader::from_consensus(sealed, None, None);
        let block = RpcBlock::<ScrollRpcTransaction, _>::empty(rpc_header);
        asserter.push_success(&Some(block));
        block_info
    }

    fn rpc_block(
        number: u64,
        parent_hash: B256,
        tag: u8,
    ) -> (RpcBlock<ScrollRpcTransaction>, BlockInfo) {
        let header = ConsensusHeader {
            parent_hash,
            number,
            extra_data: Bytes::from(vec![tag]),
            ..Default::default()
        };
        let sealed = header.seal_slow();
        let block_info = BlockInfo { number, hash: sealed.hash() };
        let block = RpcBlock::empty(RpcHeader::from_consensus(sealed, None, None));
        (block, block_info)
    }

    fn make_attempt_due(driver: &mut DerivationDriver) {
        driver.attempt_sleep = Some(Box::pin(tokio::time::sleep(Duration::ZERO)));
    }

    fn script_hold_then_success(client: &ScriptedEngineClient, boundary: HoldBoundary) {
        let valid_build = || fcu(PayloadStatusEnum::Valid, Some(PayloadId::new([7; 8])));
        let valid_fcu = || fcu(PayloadStatusEnum::Valid, None);

        match boundary {
            HoldBoundary::BuildFcuSyncing => {
                client.push_fork_choice_updated(ScriptedResponse::Ok(fcu(
                    PayloadStatusEnum::Syncing,
                    None,
                )));
                client.push_fork_choice_updated(ScriptedResponse::Ok(valid_build()));
                client.push_get_payload(ScriptedResponse::Ok(payload(SAFE + 1)));
                client.push_new_payload(ScriptedResponse::Ok(payload_status(
                    PayloadStatusEnum::Valid,
                )));
                client.push_fork_choice_updated(ScriptedResponse::Ok(valid_fcu()));
                client.push_fork_choice_updated(ScriptedResponse::Ok(valid_fcu()));
            }
            HoldBoundary::NewPayloadSyncing | HoldBoundary::NewPayloadAccepted => {
                client.push_fork_choice_updated(ScriptedResponse::Ok(valid_build()));
                client.push_get_payload(ScriptedResponse::Ok(payload(SAFE + 1)));
                let status = if matches!(boundary, HoldBoundary::NewPayloadSyncing) {
                    PayloadStatusEnum::Syncing
                } else {
                    PayloadStatusEnum::Accepted
                };
                client.push_new_payload(ScriptedResponse::Ok(payload_status(status)));
                client.push_fork_choice_updated(ScriptedResponse::Ok(valid_build()));
                client.push_get_payload(ScriptedResponse::Ok(payload(SAFE + 1)));
                client.push_new_payload(ScriptedResponse::Ok(payload_status(
                    PayloadStatusEnum::Valid,
                )));
                client.push_fork_choice_updated(ScriptedResponse::Ok(valid_fcu()));
                client.push_fork_choice_updated(ScriptedResponse::Ok(valid_fcu()));
            }
            HoldBoundary::FinalFcuSyncing => {
                client.push_fork_choice_updated(ScriptedResponse::Ok(valid_build()));
                client.push_get_payload(ScriptedResponse::Ok(payload(SAFE + 1)));
                client.push_new_payload(ScriptedResponse::Ok(payload_status(
                    PayloadStatusEnum::Valid,
                )));
                client.push_fork_choice_updated(ScriptedResponse::Ok(fcu(
                    PayloadStatusEnum::Syncing,
                    None,
                )));
                client.push_fork_choice_updated(ScriptedResponse::Ok(valid_build()));
                client.push_get_payload(ScriptedResponse::Ok(payload(SAFE + 1)));
                client.push_new_payload(ScriptedResponse::Ok(payload_status(
                    PayloadStatusEnum::Valid,
                )));
                client.push_fork_choice_updated(ScriptedResponse::Ok(valid_fcu()));
                client.push_fork_choice_updated(ScriptedResponse::Ok(valid_fcu()));
            }
        }
    }

    async fn assert_hold_then_complete(boundary: HoldBoundary) {
        let database = setup_database().await;
        insert_batch(&database, 1, 1, None).await;
        let client = Arc::new(ScriptedEngineClient::new());
        script_hold_then_success(&client, boundary);
        let mut engine = engine_at_safe(client.clone());
        let asserter = Asserter::new();
        push_absent_block(&asserter);
        push_absent_block(&asserter);
        let provider = absent_block_provider(asserter);
        let mut driver = DerivationDriver::default();
        driver.hold_batch(batch(1, BatchStatus::Consolidated));

        driver.wait_for_attempt().await;
        assert!(matches!(
            driver.run_attempt(&provider, &mut engine, &database).await,
            AttemptStep::Held
        ));
        assert!(!driver.can_accept_batch(), "a later batch must remain behind the held slot");
        assert_eq!(engine.fcs().head_block_info().number, SAFE);
        assert_eq!(engine.fcs().safe_block_info().number, SAFE);
        assert_eq!(
            database.get_batch_status_by_hash(B256::repeat_byte(1)).await.unwrap(),
            Some(BatchStatus::Committed),
            "a hold must not commit a partial consolidation outcome"
        );

        make_attempt_due(&mut driver);
        driver.wait_for_attempt().await;
        let consolidated = match driver.run_attempt(&provider, &mut engine, &database).await {
            AttemptStep::Completed(consolidated) => consolidated,
            other => panic!("expected successful fresh reconciliation, got {other:?}"),
        };
        assert_eq!(consolidated.block_outcomes.len(), 1);
        assert_eq!(consolidated.batch_outcome.batch_info.index, 1);
        assert!(driver.can_accept_batch());
        assert_eq!(engine.fcs().head_block_info().number, SAFE + 1);
        assert_eq!(engine.fcs().safe_block_info().number, SAFE + 1);
        assert_eq!(
            database.get_batch_status_by_hash(B256::repeat_byte(1)).await.unwrap(),
            Some(BatchStatus::Consolidated),
            "the successful retry commits the batch exactly once"
        );
        assert_eq!(
            database.get_l2_block_and_batch_info_by_hash(B256::repeat_byte(0x22)).await.unwrap(),
            Some((info(SAFE + 1, 0x22), BatchInfo::new(1, B256::repeat_byte(1))))
        );
    }

    #[test]
    fn hold_backoff_saturates_at_thirty_seconds() {
        assert_eq!(hold_backoff(1), Duration::from_secs(2));
        assert_eq!(hold_backoff(2), Duration::from_secs(4));
        assert_eq!(hold_backoff(5), Duration::from_secs(30));
        assert_eq!(hold_backoff(u64::MAX), Duration::from_secs(30));
    }

    #[tokio::test]
    async fn build_fcu_syncing_holds_then_completes_once() {
        assert_hold_then_complete(HoldBoundary::BuildFcuSyncing).await;
    }

    #[tokio::test]
    async fn build_fcu_syncing_then_canonical_match_completes_once() {
        let database = setup_database().await;
        insert_batch(&database, 1, 1, None).await;
        let client = Arc::new(ScriptedEngineClient::new());
        client
            .push_fork_choice_updated(ScriptedResponse::Ok(fcu(PayloadStatusEnum::Syncing, None)));
        client.push_fork_choice_updated(ScriptedResponse::Ok(fcu(PayloadStatusEnum::Valid, None)));
        client.push_fork_choice_updated(ScriptedResponse::Ok(fcu(PayloadStatusEnum::Valid, None)));
        let mut engine = engine_at_safe(client.clone());

        let mut derived = batch(1, BatchStatus::Consolidated);
        derived.attributes[0].attributes.transactions = Some(vec![]);
        let asserter = Asserter::new();
        push_absent_block(&asserter);
        let canonical = push_matching_empty_block(&asserter, &derived.attributes[0].attributes);
        let provider = absent_block_provider(asserter);
        let mut driver = DerivationDriver::default();
        driver.hold_batch(derived);

        driver.wait_for_attempt().await;
        assert!(matches!(
            driver.run_attempt(&provider, &mut engine, &database).await,
            AttemptStep::Held
        ));
        assert_eq!(engine.fcs().head_block_info().number, SAFE);

        make_attempt_due(&mut driver);
        driver.wait_for_attempt().await;
        assert!(matches!(
            driver.run_attempt(&provider, &mut engine, &database).await,
            AttemptStep::Completed(_)
        ));
        assert_eq!(*engine.fcs().head_block_info(), canonical);
        assert_eq!(*engine.fcs().safe_block_info(), canonical);
        assert_eq!(client.fork_choice_updated_calls(), 3);
        assert_eq!(client.get_payload_calls(), 0);
        assert_eq!(client.new_payload_calls(), 0);
        assert_eq!(
            database.get_batch_status_by_hash(B256::repeat_byte(1)).await.unwrap(),
            Some(BatchStatus::Consolidated)
        );
    }

    #[tokio::test]
    async fn already_safe_batch_replay_is_idempotent() {
        let database = setup_database().await;
        insert_batch(&database, 1, 1, None).await;
        let derived = batch(1, BatchStatus::Consolidated);
        let batch_hash = derived.batch_info.hash;
        let asserter = Asserter::new();
        let canonical = push_matching_empty_block(&asserter, &derived.attributes[0].attributes);
        database.insert_blocks(vec![canonical], derived.batch_info).await.unwrap();
        let provider = absent_block_provider(asserter);
        let client = Arc::new(ScriptedEngineClient::new());
        let mut engine = Engine::new(
            client.clone(),
            ForkchoiceState::new(canonical, canonical, info(SAFE, 0x11)),
        );
        let mut driver = DerivationDriver::default();
        driver.hold_batch(derived);
        driver.wait_for_attempt().await;

        let AttemptStep::Completed(consolidated) =
            driver.run_attempt(&provider, &mut engine, &database).await
        else {
            panic!("an already-safe duplicate batch must complete idempotently")
        };
        assert_eq!(consolidated.batch_outcome.blocks.len(), 1);
        assert_eq!(client.fork_choice_updated_calls(), 0);
        assert_eq!(client.get_payload_calls(), 0);
        assert_eq!(client.new_payload_calls(), 0);
        assert_eq!(
            database.get_batch_status_by_hash(batch_hash).await.unwrap(),
            Some(BatchStatus::Consolidated)
        );
        assert_eq!(database.get_pending_frontier_transition().await.unwrap(), None);
    }

    #[tokio::test]
    async fn same_height_frontier_mismatch_never_builds_on_engine_hash() {
        let database = setup_database().await;
        insert_batch(&database, 1, 1, None).await;
        let database_safe = info(SAFE, 0x11);
        let (finalized_block, finalized) = rpc_block(0, B256::ZERO, 0xf0);
        let (engine_safe_block, engine_safe) = rpc_block(SAFE, finalized.hash, 0xb1);

        let asserter = Asserter::new();
        asserter.push_success(&Some(engine_safe_block.clone()));
        asserter.push_success(&Some(engine_safe_block));
        asserter.push_success(&Some(finalized_block));
        push_absent_block(&asserter);
        let provider = absent_block_provider(asserter);

        let client = Arc::new(ScriptedEngineClient::new());
        client.push_fork_choice_updated(ScriptedResponse::Ok(fcu(PayloadStatusEnum::Valid, None)));
        client.push_fork_choice_updated(ScriptedResponse::Ok(fcu(
            PayloadStatusEnum::Valid,
            Some(PayloadId::new([7; 8])),
        )));
        client.push_get_payload(ScriptedResponse::Ok(payload(SAFE + 1)));
        client.push_new_payload(ScriptedResponse::Ok(payload_status(PayloadStatusEnum::Valid)));
        client.push_fork_choice_updated(ScriptedResponse::Ok(fcu(PayloadStatusEnum::Valid, None)));
        client.push_fork_choice_updated(ScriptedResponse::Ok(fcu(PayloadStatusEnum::Valid, None)));
        let mut engine =
            Engine::new(client.clone(), ForkchoiceState::new(engine_safe, engine_safe, finalized));
        let mut driver = DerivationDriver::default();
        driver.hold_batch(batch(1, BatchStatus::Consolidated));
        driver.wait_for_attempt().await;

        assert!(matches!(
            driver.run_attempt(&provider, &mut engine, &database).await,
            AttemptStep::Completed(_)
        ));

        let build_inputs = client
            .fork_choice_inputs()
            .into_iter()
            .filter(|(_, has_attributes)| *has_attributes)
            .collect::<Vec<_>>();
        assert_eq!(build_inputs.len(), 1);
        assert_eq!(build_inputs[0].0.head_block_hash, database_safe.hash);
        assert_ne!(build_inputs[0].0.head_block_hash, engine_safe.hash);
    }

    #[tokio::test]
    async fn builder_payload_with_wrong_parent_is_rejected_before_new_payload() {
        let database = setup_database().await;
        insert_batch(&database, 1, 1, None).await;
        let client = Arc::new(ScriptedEngineClient::new());
        client.push_fork_choice_updated(ScriptedResponse::Ok(fcu(
            PayloadStatusEnum::Valid,
            Some(PayloadId::new([7; 8])),
        )));
        let mut wrong_payload = payload(SAFE + 1);
        wrong_payload.parent_hash = B256::repeat_byte(0x99);
        client.push_get_payload(ScriptedResponse::Ok(wrong_payload));
        let mut engine = engine_at_safe(client.clone());
        let asserter = Asserter::new();
        push_absent_block(&asserter);
        let provider = absent_block_provider(asserter);
        let mut driver = DerivationDriver::default();
        driver.hold_batch(batch(1, BatchStatus::Consolidated));
        driver.wait_for_attempt().await;

        let AttemptStep::Fatal(fatal) = driver.run_attempt(&provider, &mut engine, &database).await
        else {
            panic!("wrong-parent builder payload must fail-stop")
        };
        assert_eq!(fatal.method, GET_PAYLOAD_METHOD);
        assert_eq!(fatal.outcome, "PAYLOAD_MISMATCH");
        assert!(matches!(*fatal.error, ChainOrchestratorError::BuiltPayloadMismatch { .. }));
        assert_eq!(client.new_payload_calls(), 0);
        assert_eq!(
            database.get_batch_status_by_hash(B256::repeat_byte(1)).await.unwrap(),
            Some(BatchStatus::Committed)
        );
    }

    #[tokio::test]
    async fn engine_head_ahead_after_database_failure_is_persisted_idempotently() {
        let database = setup_database().await;
        insert_batch(&database, 1, 1, None).await;
        let derived = batch(1, BatchStatus::Consolidated);
        let asserter = Asserter::new();
        let canonical = push_matching_empty_block(&asserter, &derived.attributes[0].attributes);
        let provider = absent_block_provider(asserter);
        let client = Arc::new(ScriptedEngineClient::new());
        client.push_fork_choice_updated(ScriptedResponse::Ok(fcu(PayloadStatusEnum::Valid, None)));
        let safe = info(SAFE, 0x11);
        let mut engine = Engine::new(client.clone(), ForkchoiceState::new(canonical, safe, safe));
        let mut driver = DerivationDriver::default();
        driver.hold_batch(derived);
        driver.wait_for_attempt().await;

        let AttemptStep::Completed(consolidated) =
            driver.run_attempt(&provider, &mut engine, &database).await
        else {
            panic!("verified Engine prefix must be persisted without rebuilding")
        };
        assert!(consolidated.batch_outcome.l2_head_updated);
        assert_eq!(client.fork_choice_updated_calls(), 1, "only safe promotion is required");
        assert_eq!(client.get_payload_calls(), 0);
        assert_eq!(client.new_payload_calls(), 0);
        assert_eq!(database.get_l2_head_block_number().await.unwrap(), canonical.number);
        assert_eq!(*engine.fcs().safe_block_info(), canonical);
        assert_eq!(database.get_pending_frontier_transition().await.unwrap(), None);
    }

    #[tokio::test]
    async fn reorg_suffix_builds_on_verified_prefix_tip() {
        let database = setup_database().await;
        insert_batch(&database, 1, 1, None).await;
        let first_attributes =
            ScrollPayloadAttributes { transactions: Some(vec![]), ..Default::default() };
        let second_attributes =
            ScrollPayloadAttributes { transactions: Some(vec![]), ..Default::default() };
        let asserter = Asserter::new();
        let prefix =
            push_matching_empty_block_on(&asserter, &first_attributes, info(SAFE, 0x11), SAFE + 1);
        push_absent_block(&asserter);
        let provider = absent_block_provider(asserter);

        let client = Arc::new(ScriptedEngineClient::new());
        client.push_fork_choice_updated(ScriptedResponse::Ok(fcu(PayloadStatusEnum::Valid, None)));
        client.push_fork_choice_updated(ScriptedResponse::Ok(fcu(
            PayloadStatusEnum::Valid,
            Some(PayloadId::new([7; 8])),
        )));
        let mut built_payload = payload(SAFE + 2);
        built_payload.parent_hash = prefix.hash;
        client.push_get_payload(ScriptedResponse::Ok(built_payload));
        client.push_new_payload(ScriptedResponse::Ok(payload_status(PayloadStatusEnum::Valid)));
        client.push_fork_choice_updated(ScriptedResponse::Ok(fcu(PayloadStatusEnum::Valid, None)));
        client.push_fork_choice_updated(ScriptedResponse::Ok(fcu(PayloadStatusEnum::Valid, None)));
        let mut engine = engine_at_safe(client.clone());
        let mut driver = DerivationDriver::default();
        driver.hold_batch(BatchDerivationResult {
            attributes: vec![
                DerivedAttributes { block_number: SAFE + 1, attributes: first_attributes },
                DerivedAttributes { block_number: SAFE + 2, attributes: second_attributes },
            ],
            batch_info: BatchInfo::new(1, B256::repeat_byte(1)),
            skipped_l1_messages: vec![],
            target_status: BatchStatus::Consolidated,
        });
        driver.wait_for_attempt().await;

        let AttemptStep::Completed(consolidated) =
            driver.run_attempt(&provider, &mut engine, &database).await
        else {
            panic!("verified-prefix suffix rebuild must complete")
        };
        assert_eq!(consolidated.batch_outcome.blocks.len(), 2);
        assert_eq!(consolidated.block_outcomes.len(), 2);
        let build = client
            .fork_choice_inputs()
            .into_iter()
            .find(|(_, has_attributes)| *has_attributes)
            .expect("one payload build call");
        assert_eq!(build.0.head_block_hash, prefix.hash);
        assert_eq!(engine.fcs().safe_block_info().number, SAFE + 2);
    }

    #[tokio::test]
    async fn new_payload_syncing_holds_then_completes_once() {
        assert_hold_then_complete(HoldBoundary::NewPayloadSyncing).await;
    }

    #[tokio::test]
    async fn new_payload_accepted_holds_then_completes_once() {
        assert_hold_then_complete(HoldBoundary::NewPayloadAccepted).await;
    }

    #[tokio::test]
    async fn checked_final_fcu_syncing_does_not_advance_local_fcs() {
        assert_hold_then_complete(HoldBoundary::FinalFcuSyncing).await;
    }

    #[tokio::test]
    async fn final_fcu_syncing_then_canonical_match_advances_database_head() {
        let database = setup_database().await;
        insert_batch(&database, 1, 1, None).await;
        let mut derived = batch(1, BatchStatus::Consolidated);
        derived.attributes[0].attributes.transactions = Some(vec![]);

        let asserter = Asserter::new();
        push_absent_block(&asserter);
        let canonical = push_matching_empty_block(&asserter, &derived.attributes[0].attributes);
        let provider = absent_block_provider(asserter);

        let client = Arc::new(ScriptedEngineClient::new());
        client.push_fork_choice_updated(ScriptedResponse::Ok(fcu(
            PayloadStatusEnum::Valid,
            Some(PayloadId::new([7; 8])),
        )));
        let mut built_payload = payload(canonical.number);
        built_payload.block_hash = canonical.hash;
        client.push_get_payload(ScriptedResponse::Ok(built_payload));
        client.push_new_payload(ScriptedResponse::Ok(payload_status(PayloadStatusEnum::Valid)));
        client
            .push_fork_choice_updated(ScriptedResponse::Ok(fcu(PayloadStatusEnum::Syncing, None)));
        client.push_fork_choice_updated(ScriptedResponse::Ok(fcu(PayloadStatusEnum::Valid, None)));
        client.push_fork_choice_updated(ScriptedResponse::Ok(fcu(PayloadStatusEnum::Valid, None)));
        let mut engine = engine_at_safe(client.clone());
        let mut driver = DerivationDriver::default();
        driver.hold_batch(derived);

        driver.wait_for_attempt().await;
        assert!(matches!(
            driver.run_attempt(&provider, &mut engine, &database).await,
            AttemptStep::Held
        ));
        assert_eq!(engine.fcs().head_block_info().number, SAFE);
        assert_eq!(database.get_l2_head_block_number().await.unwrap(), 0);

        make_attempt_due(&mut driver);
        driver.wait_for_attempt().await;
        let AttemptStep::Completed(consolidated) =
            driver.run_attempt(&provider, &mut engine, &database).await
        else {
            panic!("canonical retry must complete")
        };
        assert!(consolidated.batch_outcome.l2_head_updated);
        assert_eq!(*engine.fcs().head_block_info(), canonical);
        assert_eq!(*engine.fcs().safe_block_info(), canonical);
        assert_eq!(database.get_l2_head_block_number().await.unwrap(), canonical.number);
        assert_eq!(
            database.get_batch_status_by_hash(B256::repeat_byte(1)).await.unwrap(),
            Some(BatchStatus::Consolidated)
        );
        assert_eq!(client.fork_choice_updated_calls(), 4);
        assert_eq!(client.get_payload_calls(), 1);
        assert_eq!(client.new_payload_calls(), 1);
    }

    #[derive(Debug, Clone, Copy)]
    enum ExpectedClassification {
        Success,
        Hold { method: &'static str, status: HoldStatus },
        Fatal { method: &'static str, outcome: &'static str },
    }

    fn assert_classification<T>(
        result: Result<T, AttemptFailure>,
        expected: ExpectedClassification,
    ) {
        match (result, expected) {
            (Ok(_), ExpectedClassification::Success) => {}
            (
                Err(AttemptFailure::Hold(actual)),
                ExpectedClassification::Hold { method, status },
            ) => {
                assert_eq!(actual.method, method);
                assert_eq!(actual.status, status);
            }
            (
                Err(AttemptFailure::Fatal(actual)),
                ExpectedClassification::Fatal { method, outcome },
            ) => {
                assert_eq!(actual.method, method);
                assert_eq!(actual.outcome, outcome);
                match outcome {
                    "INVALID" => assert!(matches!(
                        *actual.error,
                        ChainOrchestratorError::InvalidDerivedPayload { .. }
                    )),
                    "ACCEPTED" => assert!(matches!(
                        *actual.error,
                        ChainOrchestratorError::UnexpectedDerivedPayloadStatus { .. }
                    )),
                    "VALID_MISSING_PAYLOAD_ID" => assert!(matches!(
                        *actual.error,
                        ChainOrchestratorError::MissingDerivedPayloadId { .. }
                    )),
                    other => panic!("unhandled fatal classification {other}"),
                }
            }
            _ => panic!("classification did not match expected result"),
        }
    }

    #[test]
    fn engine_status_classification_matrix() {
        let batch_info = BatchInfo::new(1, B256::repeat_byte(1));
        for (response, expected) in [
            (
                fcu(PayloadStatusEnum::Valid, Some(PayloadId::new([7; 8]))),
                ExpectedClassification::Success,
            ),
            (
                fcu(PayloadStatusEnum::Valid, None),
                ExpectedClassification::Fatal {
                    method: BUILD_FCU_METHOD,
                    outcome: "VALID_MISSING_PAYLOAD_ID",
                },
            ),
            (
                fcu(PayloadStatusEnum::Syncing, None),
                ExpectedClassification::Hold {
                    method: BUILD_FCU_METHOD,
                    status: HoldStatus::Syncing,
                },
            ),
            (
                fcu(PayloadStatusEnum::Accepted, None),
                ExpectedClassification::Fatal { method: BUILD_FCU_METHOD, outcome: "ACCEPTED" },
            ),
            (
                fcu(PayloadStatusEnum::Invalid { validation_error: "invalid build".into() }, None),
                ExpectedClassification::Fatal { method: BUILD_FCU_METHOD, outcome: "INVALID" },
            ),
        ] {
            assert_classification(classify_build_fcu(response, batch_info), expected);
        }

        for (response, expected) in [
            (payload_status(PayloadStatusEnum::Valid), ExpectedClassification::Success),
            (
                payload_status(PayloadStatusEnum::Syncing),
                ExpectedClassification::Hold {
                    method: NEW_PAYLOAD_METHOD,
                    status: HoldStatus::Syncing,
                },
            ),
            (
                payload_status(PayloadStatusEnum::Accepted),
                ExpectedClassification::Hold {
                    method: NEW_PAYLOAD_METHOD,
                    status: HoldStatus::Accepted,
                },
            ),
            (
                payload_status(PayloadStatusEnum::Invalid {
                    validation_error: "invalid payload".into(),
                }),
                ExpectedClassification::Fatal { method: NEW_PAYLOAD_METHOD, outcome: "INVALID" },
            ),
        ] {
            assert_classification(classify_new_payload(response, batch_info, SAFE + 1), expected);
        }

        for (response, expected) in [
            (fcu(PayloadStatusEnum::Valid, None), ExpectedClassification::Success),
            (
                fcu(PayloadStatusEnum::Syncing, None),
                ExpectedClassification::Hold { method: FCU_METHOD, status: HoldStatus::Syncing },
            ),
            (
                fcu(PayloadStatusEnum::Accepted, None),
                ExpectedClassification::Fatal { method: FCU_METHOD, outcome: "ACCEPTED" },
            ),
            (
                fcu(PayloadStatusEnum::Invalid { validation_error: "invalid fcu".into() }, None),
                ExpectedClassification::Fatal { method: FCU_METHOD, outcome: "INVALID" },
            ),
        ] {
            assert_classification(
                classify_checked_fcu(response, batch_info, Some(SAFE + 1)),
                expected,
            );
        }
    }

    #[tokio::test]
    async fn terminal_build_fcu_outcomes_fail_stop() {
        let cases = [
            fcu(
                PayloadStatusEnum::Invalid {
                    validation_error: "invalid derived payload".to_string(),
                },
                None,
            ),
            fcu(PayloadStatusEnum::Valid, None),
            fcu(PayloadStatusEnum::Accepted, None),
        ];

        for (offset, response) in cases.into_iter().enumerate() {
            let index = offset as u64 + 1;
            let database = setup_database().await;
            insert_batch(&database, index, 1, None).await;
            let client = Arc::new(ScriptedEngineClient::new());
            client.push_fork_choice_updated(ScriptedResponse::Ok(response));
            let mut engine = engine_at_safe(client.clone());
            let asserter = Asserter::new();
            push_absent_block(&asserter);
            let provider = absent_block_provider(asserter);
            let mut driver = DerivationDriver::default();
            driver.hold_batch(batch(index, BatchStatus::Consolidated));
            driver.wait_for_attempt().await;

            let AttemptStep::Fatal(fatal) =
                driver.run_attempt(&provider, &mut engine, &database).await
            else {
                panic!("terminal build FCU response must fail-stop")
            };
            assert_eq!(fatal.method, BUILD_FCU_METHOD);
            assert_ne!(fatal.outcome, "ERROR");
            assert!(!driver.can_accept_batch());
            assert_eq!(client.get_payload_calls(), 0);
            assert_eq!(client.new_payload_calls(), 0);
            assert_eq!(
                database.get_batch_status_by_hash(B256::repeat_byte(index as u8)).await.unwrap(),
                Some(BatchStatus::Committed)
            );
        }
    }

    #[tokio::test]
    async fn gapped_batch_fails_before_l2_or_engine_requests() {
        let database = setup_database().await;
        insert_batch(&database, 1, 1, None).await;
        let client = Arc::new(ScriptedEngineClient::new());
        let mut engine = engine_at_safe(client.clone());
        let asserter = Asserter::new();
        let provider = absent_block_provider(asserter);
        let mut derived = batch(1, BatchStatus::Consolidated);
        derived.attributes[0].block_number = SAFE + 51;
        let mut driver = DerivationDriver::default();
        driver.hold_batch(derived);
        driver.wait_for_attempt().await;

        let AttemptStep::Fatal(fatal) = driver.run_attempt(&provider, &mut engine, &database).await
        else {
            panic!("a gap after the persisted consolidation frontier must fail-stop")
        };
        assert_eq!(fatal.method, "reconcileBatch");
        assert_eq!(fatal.outcome, "INVALID_BLOCK_SEQUENCE");
        assert!(matches!(
            *fatal.error,
            ChainOrchestratorError::InvalidDerivedBlockSequence {
                attribute_index: 0,
                previous_block_number: SAFE,
                actual_block_number,
                ..
            } if actual_block_number == SAFE + 51
        ));
        assert_eq!(client.fork_choice_updated_calls(), 0);
        assert_eq!(client.get_payload_calls(), 0);
        assert_eq!(client.new_payload_calls(), 0);
        assert_eq!(*engine.fcs().safe_block_info(), info(SAFE, 0x11));
        assert!(!driver.can_accept_batch(), "later batches must remain blocked");
        assert_eq!(
            database.get_batch_status_by_hash(B256::repeat_byte(1)).await.unwrap(),
            Some(BatchStatus::Committed)
        );
    }

    #[tokio::test]
    async fn transport_error_fail_stops_without_polling_next_batch() {
        let database = setup_database().await;
        insert_batch(&database, 1, 1, None).await;
        let client = Arc::new(ScriptedEngineClient::new());
        client.push_fork_choice_updated(ScriptedResponse::TransportFailure);
        let mut engine = engine_at_safe(client);
        let asserter = Asserter::new();
        push_absent_block(&asserter);
        let provider = absent_block_provider(asserter);
        let mut pipeline = VecDeque::from([
            batch(1, BatchStatus::Consolidated),
            batch(2, BatchStatus::Consolidated),
        ]);
        let mut driver = DerivationDriver::default();
        driver.hold_batch(pipeline.pop_front().unwrap());
        driver.wait_for_attempt().await;

        let AttemptStep::Fatal(fatal) = driver.run_attempt(&provider, &mut engine, &database).await
        else {
            panic!("transport failures must fail-stop")
        };
        assert_eq!(fatal.method, BUILD_FCU_METHOD);
        assert_eq!(fatal.outcome, "ERROR");
        assert!(!driver.can_accept_batch());
        assert_eq!(pipeline.len(), 1, "the later result must not be polled after fatal failure");
    }

    #[tokio::test]
    async fn held_batch_blocks_next_pipeline_result() {
        let mut driver = DerivationDriver::default();
        let mut pipeline = VecDeque::from([batch(2, BatchStatus::Consolidated)]);
        driver.hold_batch(batch(1, BatchStatus::Consolidated));

        if driver.can_accept_batch() {
            pipeline.pop_front();
        }
        assert_eq!(pipeline.len(), 1);
        assert!(!driver.can_accept_batch());

        driver.invalidate();
        if driver.can_accept_batch() {
            pipeline.pop_front();
        }
        assert!(pipeline.is_empty());
    }

    #[tokio::test]
    async fn held_status_is_unsynced_and_reports_progress() {
        let database = setup_database().await;
        insert_batch(&database, 1, 1, None).await;
        let client = Arc::new(ScriptedEngineClient::new());
        client
            .push_fork_choice_updated(ScriptedResponse::Ok(fcu(PayloadStatusEnum::Syncing, None)));
        let mut engine = engine_at_safe(client);
        let asserter = Asserter::new();
        push_absent_block(&asserter);
        let provider = absent_block_provider(asserter);
        let mut driver = DerivationDriver::default();
        driver.hold_batch(batch(1, BatchStatus::Consolidated));
        driver.wait_for_attempt().await;
        assert!(matches!(
            driver.run_attempt(&provider, &mut engine, &database).await,
            AttemptStep::Held
        ));

        let mut sync = SyncState::default();
        sync.l1_mut().set_synced();
        let status =
            ChainOrchestratorStatus::new(&sync, 1, 1, 1, engine.fcs().clone(), driver.status(3));
        assert!(!status.is_synced());
        let DerivationStatus::Held(held) = status.derivation else {
            panic!("expected held status")
        };
        assert_eq!(held.batch_index, 1);
        assert_eq!(held.attempts_started, 1);
        assert_eq!(held.last_engine_method.as_deref(), Some(BUILD_FCU_METHOD));
        assert_eq!(held.last_engine_status.as_deref(), Some("SYNCING"));
        assert_eq!(held.current_backoff_ms, Some(INITIAL_HOLD_BACKOFF_MS));
        assert_eq!(held.queued_behind, 3);
    }

    #[tokio::test]
    async fn shutdown_interrupts_held_backoff() {
        let database = setup_database().await;
        insert_batch(&database, 1, 1, None).await;
        let client = Arc::new(ScriptedEngineClient::new());
        client
            .push_fork_choice_updated(ScriptedResponse::Ok(fcu(PayloadStatusEnum::Syncing, None)));
        let mut engine = engine_at_safe(client);
        let asserter = Asserter::new();
        push_absent_block(&asserter);
        let provider = absent_block_provider(asserter);
        let mut driver = DerivationDriver::default();
        driver.hold_batch(batch(1, BatchStatus::Consolidated));
        driver.wait_for_attempt().await;
        assert!(matches!(
            driver.run_attempt(&provider, &mut engine, &database).await,
            AttemptStep::Held
        ));

        tokio::select! {
            biased;
            () = tokio::time::sleep(Duration::from_millis(10)) => {}
            () = driver.wait_for_attempt() => panic!("the two-second backoff fired before shutdown"),
        }
        assert!(!driver.can_accept_batch(), "clean shutdown leaves the held row owned");
        assert!(driver.is_attempt_scheduled());
        assert_eq!(
            database.get_batch_status_by_hash(B256::repeat_byte(1)).await.unwrap(),
            Some(BatchStatus::Committed)
        );
    }

    #[tokio::test]
    async fn l1_reorg_invalidates_origin_dependent_held_batch() {
        let database = setup_database().await;
        insert_batch(&database, 1, 10, None).await;
        let client = Arc::new(ScriptedEngineClient::new());
        client
            .push_fork_choice_updated(ScriptedResponse::Ok(fcu(PayloadStatusEnum::Syncing, None)));
        let mut engine = engine_at_safe(client.clone());
        let asserter = Asserter::new();
        push_absent_block(&asserter);
        let provider = absent_block_provider(asserter);
        let mut driver = DerivationDriver::default();
        driver.hold_batch(batch(1, BatchStatus::Consolidated));
        driver.wait_for_attempt().await;
        assert!(matches!(
            driver.run_attempt(&provider, &mut engine, &database).await,
            AttemptStep::Held
        ));

        let (_, outcome) = driver.unwind_and_revalidate(&database, 5, None).await.unwrap();
        assert!(matches!(
            outcome,
            HeldReorgOutcome::Invalidated { batch_info: BatchInfo { index: 1, .. }, .. }
        ));
        assert!(driver.can_accept_batch());
        assert!(!driver.is_attempt_scheduled());
        assert!(database.get_batch_by_index(1).await.unwrap().is_none());
        assert_eq!(database.get_l2_head_block_number().await.unwrap(), 0);
        assert_eq!(engine.fcs().head_block_info().number, SAFE);
        assert_eq!(client.fork_choice_updated_calls(), 1);
    }

    #[tokio::test]
    async fn surviving_unwind_waits_for_post_commit_repair_before_retry() {
        let database = setup_database().await;
        insert_batch(&database, 1, 1, None).await;
        let mut driver = DerivationDriver::default();
        driver.hold_batch(batch(1, BatchStatus::Consolidated));
        driver.cancel_attempt();

        let (_, outcome) = driver.unwind_and_revalidate(&database, 5, None).await.unwrap();
        assert!(matches!(outcome, HeldReorgOutcome::Survived { .. }));
        assert!(!driver.can_accept_batch());
        assert!(
            !driver.is_attempt_scheduled(),
            "post-unwind provider/FCS repair must succeed before retry is scheduled"
        );

        driver.schedule_fresh_reconciliation();
        assert!(driver.is_attempt_scheduled());
    }

    #[tokio::test]
    async fn database_unwind_engine_failure_replays_durable_frontier_transition() {
        let database = setup_database().await;
        insert_batch(&database, 1, 10, None).await;
        let database_safe = info(SAFE, 0x11);
        let (finalized_block, finalized) = rpc_block(0, B256::ZERO, 0xf0);
        let (engine_head_block, engine_head) = rpc_block(SAFE + 1, database_safe.hash, 0x22);
        database
            .insert_blocks(vec![engine_head], BatchInfo::new(1, B256::repeat_byte(1)))
            .await
            .unwrap();
        let expected = StoredForkchoiceState { head: engine_head, safe: engine_head, finalized };
        let mut driver = DerivationDriver::default();

        let (_, outcome) =
            driver.unwind_and_revalidate(&database, 5, Some(expected)).await.unwrap();
        assert!(matches!(outcome, HeldReorgOutcome::NoHeldBatch));
        let pending = database
            .get_pending_frontier_transition()
            .await
            .unwrap()
            .expect("unwind and intent must commit together");
        assert_eq!(pending.kind, FrontierTransitionKind::UnwindL1);
        assert_eq!(pending.expected, expected);
        assert_eq!(pending.target.safe, database_safe);
        assert!(database.get_batch_by_index(1).await.unwrap().is_none());

        let asserter = Asserter::new();
        asserter.push_success(&Some(engine_head_block.clone()));
        asserter.push_success(&Some(engine_head_block));
        asserter.push_success(&Some(finalized_block));
        let provider = absent_block_provider(asserter);
        let client = Arc::new(ScriptedEngineClient::new());
        client.push_fork_choice_updated(ScriptedResponse::TransportFailure);
        client.push_fork_choice_updated(ScriptedResponse::Ok(fcu(PayloadStatusEnum::Valid, None)));
        let mut engine = Engine::new(
            client,
            ForkchoiceState::new(expected.head, expected.safe, expected.finalized),
        );

        let error =
            apply_pending_frontier_transition(&provider, &mut engine, &database).await.unwrap_err();
        assert!(matches!(
            error,
            ChainOrchestratorError::FrontierTransitionEngineRequest {
                kind: FrontierTransitionKind::UnwindL1,
                ..
            }
        ));
        assert!(database.get_pending_frontier_transition().await.unwrap().is_some());

        assert!(apply_pending_frontier_transition(&provider, &mut engine, &database)
            .await
            .unwrap());
        assert_eq!(*engine.fcs().safe_block_info(), database_safe);
        assert_eq!(database.get_pending_frontier_transition().await.unwrap(), None);
    }

    #[tokio::test]
    async fn reverted_finalization_resets_only_its_surviving_processing_row() {
        let database = setup_database().await;
        insert_batch(&database, 1, 1, Some(10)).await;
        insert_batch(&database, 2, 1, None).await;
        database.update_batch_status(B256::repeat_byte(1), BatchStatus::Processing).await.unwrap();
        database.update_batch_status(B256::repeat_byte(2), BatchStatus::Processing).await.unwrap();
        let mut driver = DerivationDriver::default();
        driver.hold_batch(batch(1, BatchStatus::Finalized));

        let (_, outcome) = driver.unwind_and_revalidate(&database, 5, None).await.unwrap();
        assert!(matches!(outcome, HeldReorgOutcome::Invalidated { .. }));
        assert_eq!(
            database.get_batch_status_by_hash(B256::repeat_byte(1)).await.unwrap(),
            Some(BatchStatus::Committed)
        );
        assert_eq!(
            database.get_batch_status_by_hash(B256::repeat_byte(2)).await.unwrap(),
            Some(BatchStatus::Processing),
            "unrelated Processing rows must remain untouched"
        );
    }

    #[tokio::test]
    async fn reverted_held_row_is_not_reset_to_committed() {
        let database = setup_database().await;
        insert_batch(&database, 1, 1, Some(10)).await;
        database.update_batch_status(B256::repeat_byte(1), BatchStatus::Processing).await.unwrap();
        database
            .set_batch_revert_block_number_for_batch_range(
                1,
                1,
                BlockInfo { number: 4, hash: B256::repeat_byte(4) },
            )
            .await
            .unwrap();
        let mut driver = DerivationDriver::default();
        driver.hold_batch(batch(1, BatchStatus::Finalized));
        driver.cancel_attempt();

        let (_, outcome) = driver.unwind_and_revalidate(&database, 5, None).await.unwrap();
        assert!(matches!(outcome, HeldReorgOutcome::Invalidated { .. }));
        assert_eq!(
            database.get_batch_status_by_hash(B256::repeat_byte(1)).await.unwrap(),
            Some(BatchStatus::Reverted),
            "the targeted Processing -> Committed reset must not overwrite Reverted"
        );
    }
}
