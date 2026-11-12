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

/// Acknowledged safe-tag evidence. Finalized canonical starts can be pruned; old forks remain
/// witnesses because Engine acknowledgement (including reading a tag back) is not a persistence
/// barrier. Reading a legacy record cannot reconstruct starts an older release already erased.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ConfirmedFrontierTransition {
    /// The most recently acknowledged forkchoice transition.
    /// Flattening keeps records written before safe history was introduced readable.
    #[serde(flatten)]
    pub transition: PendingFrontierTransition,
    /// Earlier safe starts connected to this target by acknowledged transitions.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) previous_safe: Vec<BlockInfo>,
}

impl ConfirmedFrontierTransition {
    pub(crate) fn acknowledge(
        transition: PendingFrontierTransition,
        previous: Option<Self>,
    ) -> Result<Self, crate::DatabaseError> {
        transition.validate()?;
        let mut previous_safe = Vec::new();
        if let Some(previous) = previous {
            // Acknowledgements cannot move finalized backwards across records either.
            PendingFrontierTransition {
                expected: previous.transition.target,
                target: transition.target,
            }
            .validate()?;
            if previous.transition.target.safe == transition.expected.safe ||
                previous.transition.target.safe == transition.target.safe
            {
                // Extend a connected move, or preserve evidence when recovery reapplies the
                // same target. Head/finalized-only moves must preserve it as well.
                previous_safe = previous.previous_safe;
                let start = previous.transition.expected.safe;
                if !previous_safe.contains(&start) {
                    previous_safe.push(start);
                }
            }
        }
        previous_safe.retain(|block| *block != transition.expected.safe);
        Ok(Self { transition, previous_safe })
    }

    /// Safe starts that can explain a lost tag write, always compared by hash as well as height.
    pub fn safe_starts(&self) -> impl Iterator<Item = BlockInfo> + '_ {
        std::iter::once(self.transition.expected.safe).chain(self.previous_safe.iter().copied())
    }

    pub(crate) fn validate(&self) -> Result<(), crate::DatabaseError> {
        self.transition.validate()?;
        let mut seen = std::collections::HashSet::new();
        if self.safe_starts().any(|block| !seen.insert((block.number, block.hash))) {
            return Err(crate::DatabaseError::InvalidFrontierTransition(
                "safe history contains duplicate starts",
            ))
        }
        Ok(())
    }
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
