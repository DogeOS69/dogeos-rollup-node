//! Follower signer-rotation watchdog.
//!
//! The watchdog polls the authorized-signer slot and resolves when a rotation away from its
//! baseline is confirmed. Its baseline is the first successful watchdog read, so a rotation in
//! the window between the node's startup read and that first read will not be detected until the
//! next rotation. The caller is responsible for shutting down the process; restarting under a
//! supervisor is the signer-refresh mechanism.

use alloy_primitives::Address;
use alloy_provider::ProviderBuilder;
use rollup_node_providers::SystemContractProvider;
use std::time::Duration;

/// Process exit code used after a confirmed authorized-signer rotation.
pub const EXIT_CODE_SIGNER_ROTATION: i32 = 70;
/// Interval between authorized-signer reads.
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(12);
/// Number of matching non-baseline reads required to confirm a rotation.
const DEFAULT_CONFIRMATIONS: u32 = 3;

/// Emit a periodic warning after this many consecutive failed signer reads.
const FAILURE_WARNING_INTERVAL: u32 = 25;
const POLL_READ_TIMEOUT: Duration = Duration::from_secs(10);

/// A confirmed signer rotation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rotation {
    /// Authorized signer observed when the watchdog started.
    pub baseline: Address,
    /// Newly observed authorized signer.
    pub observed: Address,
}

/// Pure confirmation state machine for authorized-signer observations.
#[derive(Debug)]
struct RotationDetector {
    confirmations: u32,
    baseline: Option<Address>,
    candidate: Option<Address>,
    streak: u32,
}

impl RotationDetector {
    /// Creates a detector requiring `confirmations` matching non-baseline observations.
    fn new(confirmations: u32) -> Self {
        assert!(confirmations > 0, "rotation confirmations must be greater than zero");
        Self { confirmations, baseline: None, candidate: None, streak: 0 }
    }

    /// Records one poll result and returns a rotation once it is confirmed.
    ///
    /// `None` represents a failed read. Failures leave the detector state unchanged so RPC
    /// uncertainty can never cause an exit.
    fn observe(&mut self, value: Option<Address>) -> Option<Rotation> {
        let value = value?;
        let Some(baseline) = self.baseline else {
            self.baseline = Some(value);
            return None;
        };

        if value == baseline {
            self.candidate = None;
            self.streak = 0;
            return None;
        }

        if self.candidate == Some(value) {
            self.streak = self.streak.saturating_add(1);
        } else {
            self.candidate = Some(value);
            self.streak = 1;
        }

        (self.streak >= self.confirmations).then_some(Rotation { baseline, observed: value })
    }
}

/// Polls L1 until an authorized-signer rotation is confirmed.
#[derive(Debug)]
pub struct SignerRotationWatchdog {
    l1_url: reqwest::Url,
    system_contract: Address,
}

impl SignerRotationWatchdog {
    /// Creates a watchdog using the production polling interval and confirmation count.
    pub const fn new(l1_url: reqwest::Url, system_contract: Address) -> Self {
        Self { l1_url, system_contract }
    }

    /// Resolves only after a confirmed rotation; otherwise remains pending indefinitely.
    pub async fn wait_for_rotation(self) -> Rotation {
        let Self { l1_url, system_contract } = self;
        let provider = ProviderBuilder::new().connect_http(l1_url);
        let mut detector = RotationDetector::new(DEFAULT_CONFIRMATIONS);
        let mut consecutive_failures = 0_u32;

        loop {
            let observation = match tokio::time::timeout(
                POLL_READ_TIMEOUT,
                provider.authorized_signer(system_contract),
            )
            .await
            {
                Ok(Ok(signer)) => Some(signer),
                Ok(Err(err)) => {
                    tracing::debug!(
                        target: "rollup_node::signer_rotation",
                        ?err,
                        "signer poll failed; will retry"
                    );
                    None
                }
                Err(err) => {
                    tracing::debug!(
                        target: "rollup_node::signer_rotation",
                        ?err,
                        timeout = ?POLL_READ_TIMEOUT,
                        "signer poll timed out; will retry"
                    );
                    None
                }
            };

            if observation.is_some() {
                consecutive_failures = 0;
            } else {
                consecutive_failures = consecutive_failures.saturating_add(1);
                if consecutive_failures == FAILURE_WARNING_INTERVAL {
                    tracing::warn!(
                        target: "rollup_node::signer_rotation",
                        failures = consecutive_failures,
                        "authorized-signer polling is repeatedly failing; watchdog remains fail-open"
                    );
                    consecutive_failures = 0;
                }
            }

            if detector.baseline.is_none() {
                if let Some(baseline) = observation {
                    tracing::info!(
                        target: "rollup_node::signer_rotation",
                        %baseline,
                        %system_contract,
                        "authorized-signer watchdog baseline established"
                    );
                    if baseline == Address::ZERO {
                        tracing::warn!(
                            target: "rollup_node::signer_rotation",
                            %system_contract,
                            "authorized-signer watchdog baseline is zero; verify the L1 URL and sync state if unexpected"
                        );
                    }
                }
            }

            if let Some(rotation) = detector.observe(observation) {
                return rotation;
            }

            tokio::time::sleep(DEFAULT_POLL_INTERVAL).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SIGNER_A: Address = Address::new([0x11; 20]);
    const SIGNER_B: Address = Address::new([0x22; 20]);
    const SIGNER_C: Address = Address::new([0x33; 20]);

    #[test]
    fn stable_value_never_confirms_rotation() {
        let mut detector = RotationDetector::new(3);

        for _ in 0..100 {
            assert_eq!(detector.observe(Some(SIGNER_A)), None);
        }
    }

    #[test]
    fn clean_rotation_fires_after_exact_confirmation_count() {
        let mut detector = RotationDetector::new(3);

        assert_eq!(detector.observe(Some(SIGNER_A)), None);
        assert_eq!(detector.observe(Some(SIGNER_B)), None);
        assert_eq!(detector.observe(Some(SIGNER_B)), None);
        assert_eq!(
            detector.observe(Some(SIGNER_B)),
            Some(Rotation { baseline: SIGNER_A, observed: SIGNER_B })
        );
    }

    #[test]
    fn returning_to_baseline_resets_candidate_streak() {
        let mut detector = RotationDetector::new(3);

        assert_eq!(detector.observe(Some(SIGNER_A)), None);
        assert_eq!(detector.observe(Some(SIGNER_B)), None);
        assert_eq!(detector.observe(Some(SIGNER_B)), None);
        assert_eq!(detector.observe(Some(SIGNER_A)), None);
        assert_eq!(detector.observe(Some(SIGNER_B)), None);
        assert_eq!(detector.observe(Some(SIGNER_B)), None);
    }

    #[test]
    fn switching_candidate_resets_confirmation_streak() {
        let mut detector = RotationDetector::new(3);

        assert_eq!(detector.observe(Some(SIGNER_A)), None);
        assert_eq!(detector.observe(Some(SIGNER_B)), None);
        assert_eq!(detector.observe(Some(SIGNER_B)), None);
        assert_eq!(detector.observe(Some(SIGNER_C)), None);
        assert_eq!(detector.observe(Some(SIGNER_C)), None);
        assert_eq!(
            detector.observe(Some(SIGNER_C)),
            Some(Rotation { baseline: SIGNER_A, observed: SIGNER_C })
        );
    }

    #[test]
    fn read_failures_do_not_change_confirmation_state() {
        let mut detector = RotationDetector::new(3);

        assert_eq!(detector.observe(None), None);
        assert_eq!(detector.observe(Some(SIGNER_A)), None);
        assert_eq!(detector.observe(Some(SIGNER_B)), None);
        assert_eq!(detector.observe(None), None);
        assert_eq!(detector.observe(Some(SIGNER_B)), None);
        assert_eq!(detector.observe(None), None);
        assert_eq!(
            detector.observe(Some(SIGNER_B)),
            Some(Rotation { baseline: SIGNER_A, observed: SIGNER_B })
        );
    }

    #[test]
    fn zero_address_is_valid_as_baseline_and_observed_signer() {
        let mut from_zero = RotationDetector::new(2);
        assert_eq!(from_zero.observe(Some(Address::ZERO)), None);
        assert_eq!(from_zero.observe(Some(SIGNER_A)), None);
        assert_eq!(
            from_zero.observe(Some(SIGNER_A)),
            Some(Rotation { baseline: Address::ZERO, observed: SIGNER_A })
        );

        let mut to_zero = RotationDetector::new(2);
        assert_eq!(to_zero.observe(Some(SIGNER_A)), None);
        assert_eq!(to_zero.observe(Some(Address::ZERO)), None);
        assert_eq!(
            to_zero.observe(Some(Address::ZERO)),
            Some(Rotation { baseline: SIGNER_A, observed: Address::ZERO })
        );
    }

    #[test]
    fn one_confirmation_fires_on_first_changed_value() {
        let mut detector = RotationDetector::new(1);

        assert_eq!(detector.observe(Some(SIGNER_A)), None);
        assert_eq!(
            detector.observe(Some(SIGNER_B)),
            Some(Rotation { baseline: SIGNER_A, observed: SIGNER_B })
        );
    }

    #[test]
    fn first_successful_read_after_failures_establishes_baseline() {
        let mut detector = RotationDetector::new(2);

        assert_eq!(detector.observe(None), None);
        assert_eq!(detector.observe(None), None);
        assert_eq!(detector.observe(Some(SIGNER_A)), None);
        assert_eq!(detector.observe(Some(SIGNER_B)), None);
        assert_eq!(
            detector.observe(Some(SIGNER_B)),
            Some(Rotation { baseline: SIGNER_A, observed: SIGNER_B })
        );
    }
}
