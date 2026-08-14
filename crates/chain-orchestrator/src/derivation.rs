//! Ordered reconciliation for the single derived batch owned by the orchestrator.

use crate::{
    consolidation::{reconcile_batch, BlockConsolidationAction},
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
use scroll_db::{Database, DatabaseReadOperations, DatabaseWriteOperations};
use scroll_derivation_pipeline::BatchDerivationResult;
use scroll_engine::{Engine, ScrollEngineApi};
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
        debug_assert!(self.pending.is_none(), "a derived batch is already held");
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

    /// Revalidates the held row after the database has unwound to `ancestor`.
    pub(crate) async fn revalidate_after_unwind(
        &mut self,
        database: &Database,
        ancestor: u64,
    ) -> Result<HeldReorgOutcome, ChainOrchestratorError> {
        let Some(held) = self.held_identity() else { return Ok(HeldReorgOutcome::NoHeldBatch) };

        let stored = database.get_batch_by_index(held.batch_info.index).await?;
        let surviving_row = stored
            .as_ref()
            .filter(|batch| batch.hash == held.batch_info.hash && batch.block_number <= ancestor);
        let finalization_invalid = held.target_status.is_finalized() &&
            surviving_row.is_some_and(|batch| {
                batch.finalized_block_number.is_none_or(|number| number > ancestor)
            });
        let invalid_reason = match stored.as_ref() {
            None => Some("batch row removed"),
            Some(batch) if batch.hash != held.batch_info.hash => Some("batch hash changed"),
            Some(batch) if batch.block_number > ancestor => Some("batch commit reverted"),
            Some(_) if finalization_invalid => Some("batch finalization reverted"),
            Some(_) => None,
        };

        if let Some(reason) = invalid_reason {
            if finalization_invalid {
                // A yielded derivation result owns a Processing row. Reset only this surviving row
                // so L1 catch-up can rediscover it with a non-finalized target.
                database.update_batch_status(held.batch_info.hash, BatchStatus::Committed).await?;
            }
            self.invalidate();
            Ok(HeldReorgOutcome::Invalidated { batch_info: held.batch_info, reason })
        } else {
            self.schedule_fresh_reconciliation();
            Ok(HeldReorgOutcome::Survived { batch_info: held.batch_info })
        }
    }

    /// Clears an invalid held slot and its timer.
    pub(crate) fn invalidate(&mut self) -> Option<BatchInfo> {
        self.attempt_sleep = None;
        let batch_info = self.pending.take().map(|pending| pending.batch.batch_info);
        if batch_info.is_some() {
            self.metrics.held.set(0.0);
        }
        batch_info
    }

    /// Schedules a fresh reconciliation after an in-flight attempt was cancelled by an L1 reorg.
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
    let reconciliation = reconcile_batch(l2_client, batch, engine.fcs())
        .await
        .map_err(|error| AttemptFailure::fatal("reconcileBatch", "ERROR", error))?;
    let target_status = reconciliation.target_status;
    let aggregated = reconciliation.aggregate_actions();

    let mut block_outcomes = Vec::new();
    let mut reorg_results = Vec::new();

    for action in aggregated.actions {
        let outcome = match action {
            BlockConsolidationAction::Skip(_) => {
                unreachable!("Skip actions have been filtered out in aggregation")
            }
            BlockConsolidationAction::UpdateFcs(block_info) => {
                let target = block_info.block_info;
                let finalized = target_status.is_finalized().then_some(target);
                let response = engine
                    .update_fcs_checked(None, Some(target), finalized)
                    .await
                    .map_err(|source| {
                        AttemptFailure::fatal(
                            FCU_METHOD,
                            "ERROR",
                            ChainOrchestratorError::DerivedBatchEngineRequest {
                                batch_info,
                                method: FCU_METHOD,
                                source,
                            },
                        )
                    })?;
                classify_checked_fcu(response, batch_info, Some(target.number))?;
                BlockConsolidationOutcome::UpdateFcs(block_info)
            }
            BlockConsolidationAction::Reorg(attribute_index) => {
                let attributes = &batch.attributes[attribute_index];
                let safe = *engine.fcs().safe_block_info();
                if safe.number != attributes.block_number.saturating_sub(1) {
                    return Err(AttemptFailure::fatal(
                        "reconcileBatch",
                        "INVARIANT_ERROR",
                        ChainOrchestratorError::InvalidBatchReorg {
                            batch_info,
                            safe_block_number: safe.number,
                            derived_block_number: attributes.block_number,
                        },
                    ));
                }

                let build_response = engine
                    .build_payload(Some(safe), attributes.attributes.clone())
                    .await
                    .map_err(|source| {
                        AttemptFailure::fatal(
                            BUILD_FCU_METHOD,
                            "ERROR",
                            ChainOrchestratorError::DerivedBatchEngineRequest {
                                batch_info,
                                method: BUILD_FCU_METHOD,
                                source,
                            },
                        )
                    })?;
                let payload_id = classify_build_fcu(build_response, batch_info)?;

                let payload = engine.get_payload(payload_id).await.map_err(|source| {
                    AttemptFailure::fatal(
                        GET_PAYLOAD_METHOD,
                        "ERROR",
                        ChainOrchestratorError::DerivedBatchEngineRequest {
                            batch_info,
                            method: GET_PAYLOAD_METHOD,
                            source,
                        },
                    )
                })?;
                let block_info: L2BlockInfoWithL1Messages =
                    (&payload).try_into().map_err(|error| {
                        AttemptFailure::fatal(
                            GET_PAYLOAD_METHOD,
                            "CONVERSION_ERROR",
                            ChainOrchestratorError::RollupNodePrimitiveError(error),
                        )
                    })?;

                let payload_status = engine.new_payload(payload).await.map_err(|source| {
                    AttemptFailure::fatal(
                        NEW_PAYLOAD_METHOD,
                        "ERROR",
                        ChainOrchestratorError::DerivedBatchEngineRequest {
                            batch_info,
                            method: NEW_PAYLOAD_METHOD,
                            source,
                        },
                    )
                })?;
                classify_new_payload(payload_status, batch_info, block_info.block_info.number)?;

                let finalized = target_status.is_finalized().then_some(block_info.block_info);
                let final_fcu = engine
                    .update_fcs_checked(
                        Some(block_info.block_info),
                        Some(block_info.block_info),
                        finalized,
                    )
                    .await
                    .map_err(|source| {
                        AttemptFailure::fatal(
                            FCU_METHOD,
                            "ERROR",
                            ChainOrchestratorError::DerivedBatchEngineRequest {
                                batch_info,
                                method: FCU_METHOD,
                                source,
                            },
                        )
                    })?;
                classify_checked_fcu(final_fcu, batch_info, Some(block_info.block_info.number))?;

                reorg_results.push(block_info.clone());
                BlockConsolidationOutcome::Reorged(block_info)
            }
        };
        block_outcomes.push(outcome);
    }

    let batch_outcome = reconciliation
        .into_batch_consolidation_outcome(reorg_results)
        .await
        .map_err(|error| AttemptFailure::fatal("consolidateBatch", "ERROR", error))?;
    let mut persisted = batch_outcome.clone();
    persisted.with_skipped_l1_messages(batch.skipped_l1_messages.clone());
    database.insert_batch_consolidation_outcome(persisted).await.map_err(|error| {
        AttemptFailure::fatal(
            "insertBatchConsolidationOutcome",
            "ERROR",
            ChainOrchestratorError::DatabaseError(error),
        )
    })?;

    Ok(ConsolidatedBatch { batch_outcome, block_outcomes })
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
            validation_error,
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ChainOrchestratorStatus, SyncState};
    use alloy_primitives::{Address, Bloom, Bytes, B256, U256};
    use alloy_provider::ProviderBuilder;
    use alloy_rpc_types_engine::{ExecutionPayloadV1, PayloadId};
    use alloy_transport::mock::Asserter;
    use dogeos_reth_engine::ScrollPayloadAttributes;
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

    fn engine_at_safe(client: Arc<ScriptedEngineClient>) -> Engine<ScriptedEngineClient> {
        let safe = info(SAFE, 0x11);
        Engine::new(client, ForkchoiceState::from_block_info(safe))
    }

    fn batch(index: u64, target_status: BatchStatus) -> BatchDerivationResult {
        BatchDerivationResult {
            attributes: vec![DerivedAttributes {
                block_number: SAFE + 1,
                attributes: ScrollPayloadAttributes::default(),
            }],
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
            }
        }
    }

    async fn assert_hold_then_complete(boundary: HoldBoundary) {
        let database = setup_test_db().await;
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
            let database = setup_test_db().await;
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
    async fn transport_error_fail_stops_without_polling_next_batch() {
        let database = setup_test_db().await;
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
        let database = setup_test_db().await;
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
        let database = setup_test_db().await;
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
        let database = setup_test_db().await;
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

        database.unwind(5).await.unwrap();
        let outcome = driver.revalidate_after_unwind(&database, 5).await.unwrap();
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
    async fn reverted_finalization_resets_only_its_surviving_processing_row() {
        let database = setup_test_db().await;
        insert_batch(&database, 1, 1, Some(10)).await;
        insert_batch(&database, 2, 1, None).await;
        database.update_batch_status(B256::repeat_byte(1), BatchStatus::Processing).await.unwrap();
        database.update_batch_status(B256::repeat_byte(2), BatchStatus::Processing).await.unwrap();
        let mut driver = DerivationDriver::default();
        driver.hold_batch(batch(1, BatchStatus::Finalized));

        database.unwind(5).await.unwrap();
        assert!(matches!(
            driver.revalidate_after_unwind(&database, 5).await.unwrap(),
            HeldReorgOutcome::Invalidated { .. }
        ));
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
}
