use crate::sync::{SyncMode, SyncState};
use alloy_primitives::B256;
use scroll_engine::ForkchoiceState;

/// The current status of the chain orchestrator.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ChainOrchestratorStatus {
    /// The chain status for L1.
    pub l1: L1ChainStatus,
    /// The chain status for L2.
    pub l2: L2ChainStatus,
    /// The progress of the derivation pipeline and ordered derived-batch reconciliation.
    pub derivation: DerivationPipelineStatus,
}

impl ChainOrchestratorStatus {
    /// Returns true if the chain orchestrator is fully synced.
    ///
    /// This is false whenever a derived batch is queued, being derived, held, retrying, or
    /// otherwise not idle: the L2 safe chain has not caught up to the available L1 data.
    pub const fn is_synced(&self) -> bool {
        self.l1.status.is_synced() && self.l2.status.is_synced() && self.derivation.is_idle()
    }
}

impl ChainOrchestratorStatus {
    /// Creates a new [`ChainOrchestratorStatus`] from the given sync state, latest L1 block number,
    pub fn new(
        sync_state: &SyncState,
        l1_latest: u64,
        l1_finalized: u64,
        l1_processed: u64,
        l2_fcs: ForkchoiceState,
        derivation: DerivationPipelineStatus,
    ) -> Self {
        Self {
            l1: L1ChainStatus {
                status: sync_state.l1().clone(),
                latest: l1_latest,
                finalized: l1_finalized,
                processed: l1_processed,
            },
            l2: L2ChainStatus { status: sync_state.l2().clone(), fcs: l2_fcs },
            derivation,
        }
    }
}

/// The overall progress of the derivation pipeline and ordered derived-batch reconciliation. This
/// is distinct from the L1/L2 sync modes and does not overload them to represent retry backoff.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(tag = "state", rename_all = "snake_case"))]
pub enum DerivationPipelineStatus {
    /// No batches are queued, in-flight, or held for reconciliation.
    Idle,
    /// Batches are queued or being derived, but none is currently held for reconciliation.
    Deriving {
        /// The number of batches queued or in-flight in the derivation pipeline.
        queued: u64,
    },
    /// A derived batch is held and is being reconciled (or retried) against the L2 chain.
    Reconciling(ReconcilingBatch),
}

impl DerivationPipelineStatus {
    /// Returns true if the derivation pipeline is idle.
    pub const fn is_idle(&self) -> bool {
        matches!(self, Self::Idle)
    }
}

/// Details of the derived batch currently held for ordered reconciliation.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ReconcilingBatch {
    /// The index of the batch being reconciled.
    pub batch_index: u64,
    /// The hash of the batch being reconciled.
    pub batch_hash: B256,
    /// The number of reconciliation attempts started for this batch. While an attempt is in flight
    /// this equals the attempt number in progress; while backing off it equals the number of
    /// failed attempts so far.
    pub attempts_completed: u32,
    /// The maximum number of attempts before the node fail-stops.
    pub max_attempts: u32,
    /// Whether the batch is currently waiting out a retry backoff (`true`) rather than having an
    /// attempt in flight (`false`).
    pub backing_off: bool,
    /// The backoff being waited out before the next attempt, in milliseconds, when `backing_off`.
    pub retry_backoff_ms: Option<u64>,
    /// The last classified reconciliation error, if any attempt has failed.
    pub last_error: Option<String>,
    /// The number of further batches queued behind this one in the derivation pipeline.
    pub queued_behind: u64,
}

/// The status of the L1 chain.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct L1ChainStatus {
    /// The sync mode of the chain.
    pub status: SyncMode,
    /// The latest block number of the chain.
    pub latest: u64,
    /// The finalized block number of the chain.
    pub finalized: u64,
    /// The highest block number that has been processed.
    pub processed: u64,
}

/// The status of the L2 chain.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct L2ChainStatus {
    /// The sync mode of the chain.
    pub status: SyncMode,
    /// The current fork choice state of the chain.
    #[cfg_attr(feature = "serde", serde(flatten))]
    pub fcs: ForkchoiceState,
}
