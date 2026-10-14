use rollup_node_primitives::BlockInfo;

pub(crate) const FRONTIER_TRANSITION_KEY: &str = "pending_frontier_transition";
pub(crate) const CONFIRMED_FRONTIER_KEY: &str = "confirmed_frontier_transition";

/// A database representation of an Engine forkchoice state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct StoredForkchoiceState {
    /// The forkchoice head.
    pub head: BlockInfo,
    /// The forkchoice safe block.
    pub safe: BlockInfo,
    /// The forkchoice finalized block.
    pub finalized: BlockInfo,
}

/// A durable intent to move Engine forkchoice from an observed state to a database-backed target.
/// Stored as one JSON metadata value in the same transaction as the L1-backed database change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PendingFrontierTransition {
    /// The Engine state observed before the database mutation was committed.
    pub expected: StoredForkchoiceState,
    /// The forkchoice state that the Engine must reach before normal processing resumes.
    pub target: StoredForkchoiceState,
}

impl StoredForkchoiceState {
    /// Checks ordering and identity at equal heights before persisting or replaying a frontier.
    pub fn validate(&self) -> Result<(), crate::DatabaseError> {
        for (higher, lower) in [(self.head, self.safe), (self.safe, self.finalized)] {
            if higher.number < lower.number || (higher.number == lower.number && higher != lower) {
                return Err(crate::DatabaseError::InvalidFrontierTransition(
                    "expected head >= safe >= finalized with matching hashes at equal heights",
                ))
            }
        }
        Ok(())
    }
}

impl PendingFrontierTransition {
    /// Checks both states and the finalized boundary before storing or decoding a transition.
    pub fn validate(&self) -> Result<(), crate::DatabaseError> {
        self.expected.validate()?;
        self.target.validate()?;
        if self.target.finalized.number < self.expected.finalized.number ||
            (self.target.finalized.number == self.expected.finalized.number &&
                self.target.finalized != self.expected.finalized)
        {
            return Err(crate::DatabaseError::InvalidFrontierTransition("finalized cannot rewind"))
        }
        Ok(())
    }
}
