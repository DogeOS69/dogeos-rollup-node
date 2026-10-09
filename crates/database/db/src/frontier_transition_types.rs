use crate::DatabaseError;
use alloy_primitives::B256;
use rollup_node_primitives::BlockInfo;

/// The reason a durable forkchoice transition is pending.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrontierTransitionKind {
    /// Apply a newly consolidated batch to Engine forkchoice.
    ConsolidateBatch,
    /// Advance Engine finalized after an L1 finalization.
    FinalizeBatch,
    /// Rewind Engine safe after an L1 batch revert.
    RevertBatch,
    /// Rewind Engine head and/or safe after an L1 unwind.
    UnwindL1,
    /// Repair a database/Engine mismatch discovered during startup.
    StartupRepair,
}

impl FrontierTransitionKind {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::ConsolidateBatch => "consolidate_batch",
            Self::FinalizeBatch => "finalize_batch",
            Self::RevertBatch => "revert_batch",
            Self::UnwindL1 => "unwind_l1",
            Self::StartupRepair => "startup_repair",
        }
    }
}

impl TryFrom<&str> for FrontierTransitionKind {
    type Error = DatabaseError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "consolidate_batch" => Ok(Self::ConsolidateBatch),
            "finalize_batch" => Ok(Self::FinalizeBatch),
            "revert_batch" => Ok(Self::RevertBatch),
            "unwind_l1" => Ok(Self::UnwindL1),
            "startup_repair" => Ok(Self::StartupRepair),
            _ => Err(DatabaseError::InvalidFrontierTransitionKind(value.to_owned())),
        }
    }
}

/// A database representation of an Engine forkchoice state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoredForkchoiceState {
    /// The forkchoice head.
    pub head: BlockInfo,
    /// The forkchoice safe block.
    pub safe: BlockInfo,
    /// The forkchoice finalized block.
    pub finalized: BlockInfo,
}

/// A durable intent to move Engine forkchoice from an observed state to a database-backed target.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PendingFrontierTransition {
    /// Why the transition was created.
    pub kind: FrontierTransitionKind,
    /// The Engine state observed before the database mutation was committed.
    pub expected: StoredForkchoiceState,
    /// The forkchoice state that the Engine must reach before normal processing resumes.
    pub target: StoredForkchoiceState,
    /// The batch associated with this transition, when any.
    pub batch_hash: Option<B256>,
}
