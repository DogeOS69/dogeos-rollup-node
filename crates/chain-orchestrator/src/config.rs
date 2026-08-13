use std::sync::Arc;

/// Configuration for the chain orchestrator.
#[derive(Debug)]
pub struct ChainOrchestratorConfig<ChainSpec> {
    /// The chain specification.
    chain_spec: Arc<ChainSpec>,
    /// The threshold for optimistic sync. If the received block is more than this many blocks
    /// ahead of the current chain, we optimistically sync the chain.
    optimistic_sync_threshold: u64,
    /// The L1 message queue index at which the V2 L1 message queue was enabled.
    l1_v2_message_queue_start_index: u64,
    /// The retry policy applied to the ordered reconciliation of a derived batch.
    derived_batch_retry: DerivedBatchRetryConfig,
}

impl<ChainSpec> ChainOrchestratorConfig<ChainSpec> {
    /// Creates a new chain configuration after validating its derived-batch retry policy.
    pub fn new(
        chain_spec: Arc<ChainSpec>,
        optimistic_sync_threshold: u64,
        l1_v2_message_queue_start_index: u64,
        derived_batch_retry: DerivedBatchRetryConfig,
    ) -> Result<Self, DerivedBatchRetryConfigError> {
        derived_batch_retry.validate()?;
        Ok(Self {
            chain_spec,
            optimistic_sync_threshold,
            l1_v2_message_queue_start_index,
            derived_batch_retry,
        })
    }

    /// Returns a reference to the chain specification.
    pub const fn chain_spec(&self) -> &Arc<ChainSpec> {
        &self.chain_spec
    }

    /// Returns the optimistic sync threshold.
    pub const fn optimistic_sync_threshold(&self) -> u64 {
        self.optimistic_sync_threshold
    }

    /// Returns the L1 message queue index at which the V2 L1 message queue was enabled.
    pub const fn l1_v2_message_queue_start_index(&self) -> u64 {
        self.l1_v2_message_queue_start_index
    }

    /// Returns the retry policy for ordered derived-batch reconciliation.
    pub const fn derived_batch_retry(&self) -> &DerivedBatchRetryConfig {
        &self.derived_batch_retry
    }
}

/// The bounded exponential backoff policy for ordered reconciliation of a single derived batch.
///
/// A batch that fails reconciliation with a transient condition is retried up to `max_attempts`
/// times (counting the first attempt) with exponential backoff between attempts, clamped to
/// `max_backoff_ms`. When the policy is exhausted the node fail-stops rather than continuing with a
/// later batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DerivedBatchRetryConfig {
    /// The maximum number of attempts, counting the first attempt. Must be at least 1.
    pub max_attempts: u32,
    /// The backoff before the second attempt, in milliseconds. Must not exceed `max_backoff_ms`.
    pub initial_backoff_ms: u64,
    /// The maximum backoff between attempts, in milliseconds.
    pub max_backoff_ms: u64,
}

/// An invalid bounded retry policy for derived-batch reconciliation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum DerivedBatchRetryConfigError {
    /// The policy would never attempt reconciliation.
    #[error("derived batch retry max_attempts must be at least 1")]
    ZeroAttempts,
    /// The initial backoff is greater than the configured maximum.
    #[error(
        "derived batch retry initial_backoff_ms ({initial_backoff_ms}) must not exceed max_backoff_ms ({max_backoff_ms})"
    )]
    InitialBackoffExceedsMaximum {
        /// The configured initial backoff in milliseconds.
        initial_backoff_ms: u64,
        /// The configured maximum backoff in milliseconds.
        max_backoff_ms: u64,
    },
}

/// The default number of reconciliation attempts (including the first) for a derived batch.
pub const DEFAULT_DERIVED_BATCH_MAX_ATTEMPTS: u32 = 10;
/// The default backoff before the second reconciliation attempt, in milliseconds.
pub const DEFAULT_DERIVED_BATCH_INITIAL_BACKOFF_MS: u64 = 1_000;
/// The default maximum backoff between reconciliation attempts, in milliseconds.
pub const DEFAULT_DERIVED_BATCH_MAX_BACKOFF_MS: u64 = 30_000;

impl Default for DerivedBatchRetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: DEFAULT_DERIVED_BATCH_MAX_ATTEMPTS,
            initial_backoff_ms: DEFAULT_DERIVED_BATCH_INITIAL_BACKOFF_MS,
            max_backoff_ms: DEFAULT_DERIVED_BATCH_MAX_BACKOFF_MS,
        }
    }
}

impl DerivedBatchRetryConfig {
    /// Validates the policy, rejecting zero attempts and an initial backoff above the maximum.
    pub const fn validate(&self) -> Result<(), DerivedBatchRetryConfigError> {
        if self.max_attempts == 0 {
            return Err(DerivedBatchRetryConfigError::ZeroAttempts);
        }
        if self.initial_backoff_ms > self.max_backoff_ms {
            return Err(DerivedBatchRetryConfigError::InitialBackoffExceedsMaximum {
                initial_backoff_ms: self.initial_backoff_ms,
                max_backoff_ms: self.max_backoff_ms,
            });
        }
        Ok(())
    }

    /// Returns the backoff to wait after `attempts_completed` failed attempts, before the next
    /// attempt. The first retry (after one failed attempt) waits `initial_backoff_ms`; each
    /// subsequent retry doubles the backoff, clamped to `max_backoff_ms`.
    pub fn backoff(&self, attempts_completed: u32) -> std::time::Duration {
        let exponent = attempts_completed.saturating_sub(1);
        let scaled = self
            .initial_backoff_ms
            .checked_mul(2u64.saturating_pow(exponent))
            .unwrap_or(self.max_backoff_ms)
            .min(self.max_backoff_ms);
        std::time::Duration::from_millis(scaled)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_rejects_zero_attempts() {
        let config = DerivedBatchRetryConfig { max_attempts: 0, ..Default::default() };
        assert!(config.validate().is_err());
    }

    #[test]
    fn validate_rejects_initial_above_max() {
        let config = DerivedBatchRetryConfig {
            max_attempts: 3,
            initial_backoff_ms: 5_000,
            max_backoff_ms: 1_000,
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn validate_accepts_defaults() {
        assert!(DerivedBatchRetryConfig::default().validate().is_ok());
    }

    #[test]
    fn chain_orchestrator_config_constructor_rejects_invalid_retry_policy() {
        let retry = DerivedBatchRetryConfig { max_attempts: 0, ..Default::default() };
        let result = ChainOrchestratorConfig::new(Arc::new(()), 1, 0, retry);

        assert!(matches!(result, Err(DerivedBatchRetryConfigError::ZeroAttempts)));
    }

    #[test]
    fn backoff_is_exponential_and_clamped() {
        let config = DerivedBatchRetryConfig {
            max_attempts: 10,
            initial_backoff_ms: 1_000,
            max_backoff_ms: 30_000,
        };
        // After 1 failed attempt, the first retry waits the initial backoff.
        assert_eq!(config.backoff(1).as_millis(), 1_000);
        assert_eq!(config.backoff(2).as_millis(), 2_000);
        assert_eq!(config.backoff(3).as_millis(), 4_000);
        assert_eq!(config.backoff(4).as_millis(), 8_000);
        assert_eq!(config.backoff(5).as_millis(), 16_000);
        // Clamped to the maximum thereafter, including for very large exponents (no overflow).
        assert_eq!(config.backoff(6).as_millis(), 30_000);
        assert_eq!(config.backoff(100).as_millis(), 30_000);
    }
}
