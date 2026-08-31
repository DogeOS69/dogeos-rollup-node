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
    /// The progress of derivation and ordered batch reconciliation.
    pub derivation: DerivationStatus,
}

impl ChainOrchestratorStatus {
    /// Returns true if the chain orchestrator is fully synced.
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
        derivation: DerivationStatus,
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

/// The current state of the derivation pipeline and its single held reconciliation slot.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(tag = "state", rename_all = "snake_case"))]
pub enum DerivationStatus {
    /// No batch is queued, deriving, or held.
    Idle,
    /// Batches are queued or being derived, but none has yielded into the held slot.
    Deriving {
        /// The number of active pipeline batches.
        queued: u64,
    },
    /// One batch is owned until it commits, is invalidated by an L1 unwind, or fail-stops.
    Held(HeldBatchStatus),
}

impl DerivationStatus {
    /// Returns whether no derivation or reconciliation work remains.
    pub const fn is_idle(&self) -> bool {
        matches!(self, Self::Idle)
    }
}

/// Observable progress for the derived batch currently held in place.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct HeldBatchStatus {
    /// The held batch index.
    pub batch_index: u64,
    /// The held batch hash.
    pub batch_hash: B256,
    /// The number of attempts that have started, including an attempt currently in flight.
    pub attempts_started: u64,
    /// The time elapsed since the pipeline yielded this batch, in milliseconds.
    pub held_duration_ms: u64,
    /// The Engine method that most recently caused a hold, if an attempt has reached one.
    pub last_engine_method: Option<String>,
    /// The most recently received hold status (`SYNCING`, `ACCEPTED`, or `INVALID`).
    pub last_engine_status: Option<String>,
    /// Validation details supplied by the Engine for an `INVALID` response.
    pub last_engine_error: Option<String>,
    /// The full delay scheduled before the next attempt, when backing off.
    pub current_backoff_ms: Option<u64>,
    /// The number of pipeline batches queued behind the held batch.
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

#[cfg(all(test, feature = "serde"))]
mod tests {
    use super::*;

    #[test]
    fn held_status_serde_round_trip() {
        let mut sync = SyncState::default();
        sync.l1_mut().set_synced();
        let status = ChainOrchestratorStatus::new(
            &sync,
            12,
            11,
            10,
            ForkchoiceState::from_genesis(B256::repeat_byte(0x11)),
            DerivationStatus::Held(HeldBatchStatus {
                batch_index: 7,
                batch_hash: B256::repeat_byte(0x22),
                attempts_started: 3,
                held_duration_ms: 9_000,
                last_engine_method: Some("newPayload".to_string()),
                last_engine_status: Some("ACCEPTED".to_string()),
                last_engine_error: None,
                current_backoff_ms: Some(8_000),
                queued_behind: 2,
            }),
        );

        let json = serde_json::to_string(&status).unwrap();
        let decoded: ChainOrchestratorStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, status);
        assert!(!decoded.is_synced());
    }
}
