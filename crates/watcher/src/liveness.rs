use super::Header;
use alloy_primitives::{BlockNumber, B256};
use std::time::Instant;

/// A probe that checks L1 liveness by the time since a new L1 head was last received.
///
/// It uses local receive time, not the header timestamp: an L1 source may serve synthetic
/// timestamps. The event-plane L1 Interface sets each block's timestamp to its block number, so
/// header time says nothing about whether new blocks are arriving.
#[derive(Debug)]
pub(crate) struct LivenessProbe {
    /// The threshold in seconds after which to log an error if no new block is received.
    threshold: u64,
    /// The interval in seconds at which to perform the liveness check.
    check_interval: u64,
    /// The last time a liveness check was performed.
    last_check: Instant,
    /// The hash and number of the latest L1 head received, if any.
    last_head: Option<(B256, BlockNumber)>,
    /// When a new L1 head was last received, or when the probe was created if none has been.
    last_head_received_at: Instant,
}

impl LivenessProbe {
    /// Creates a new liveness probe.
    pub(crate) fn new(threshold: u64, check_interval: u64) -> Self {
        let now = Instant::now();
        Self {
            threshold,
            check_interval,
            last_check: now,
            last_head: None,
            last_head_received_at: now,
        }
    }

    /// Returns true if a liveness check is due based on the configured interval.
    pub(crate) fn is_due(&self) -> bool {
        self.last_check.elapsed().as_secs() >= self.check_interval
    }

    /// Records the latest L1 head. A head with a different hash from the last one restarts the
    /// clock, including a reorg to a lower or equal height; the same head again does not.
    pub(crate) fn on_head(&mut self, head: &Header) {
        self.on_head_at(head, Instant::now());
    }

    fn on_head_at(&mut self, head: &Header, now: Instant) {
        if self.last_head.is_some_and(|(hash, _)| hash == head.hash) {
            return;
        }
        self.last_head = Some((head.hash, head.number));
        self.last_head_received_at = now;
    }

    /// Checks L1 liveness based on when a new L1 head was last received.
    /// Logs an error if no new block has been received within the threshold.
    pub(crate) fn check(&mut self) {
        let now = Instant::now();
        self.last_check = now;

        if let Some(elapsed) = self.stalled_for(now) {
            tracing::error!(
                target: "scroll::watcher",
                latest_block_number = self.last_head.map(|(_, number)| number),
                elapsed_secs = elapsed,
                threshold_secs = self.threshold,
                "L1 liveness check failed: no new L1 block received"
            );
        }
    }

    /// Returns the seconds since a new head was received if that exceeds the threshold.
    fn stalled_for(&self, now: Instant) -> Option<u64> {
        let elapsed = now.saturating_duration_since(self.last_head_received_at).as_secs();
        (elapsed > self.threshold).then_some(elapsed)
    }

    /// Returns the hash of the latest L1 head received, if any.
    #[cfg(test)]
    pub(crate) fn last_head_hash(&self) -> Option<B256> {
        self.last_head.map(|(hash, _)| hash)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    const THRESHOLD: u64 = 60;

    /// A head as the event-plane L1 Interface serves it: the timestamp is the block number.
    fn synthetic_head(number: u64, hash_byte: u8) -> Header {
        Header {
            hash: B256::repeat_byte(hash_byte),
            inner: alloy_consensus::Header { number, timestamp: number, ..Default::default() },
            ..Default::default()
        }
    }

    #[test]
    fn head_with_synthetic_timestamp_received_now_is_live() {
        let mut probe = LivenessProbe::new(THRESHOLD, 12);
        let t0 = Instant::now();

        probe.on_head_at(&synthetic_head(5_000_000, 1), t0);

        assert_eq!(probe.stalled_for(t0 + Duration::from_secs(1)), None);
    }

    #[test]
    fn no_new_head_past_threshold_is_stalled() {
        let mut probe = LivenessProbe::new(THRESHOLD, 12);
        let t0 = Instant::now();

        probe.on_head_at(&synthetic_head(5_000_000, 1), t0);

        assert_eq!(probe.stalled_for(t0 + Duration::from_secs(THRESHOLD)), None);
        assert_eq!(probe.stalled_for(t0 + Duration::from_secs(THRESHOLD + 1)), Some(THRESHOLD + 1));
    }

    #[test]
    fn same_head_received_again_does_not_count_as_new() {
        let mut probe = LivenessProbe::new(THRESHOLD, 12);
        let t0 = Instant::now();
        let head = synthetic_head(5_000_000, 1);

        probe.on_head_at(&head, t0);
        probe.on_head_at(&head, t0 + Duration::from_secs(50));

        assert_eq!(probe.stalled_for(t0 + Duration::from_secs(61)), Some(61));
    }

    #[test]
    fn new_head_restarts_the_clock() {
        let mut probe = LivenessProbe::new(THRESHOLD, 12);
        let t0 = Instant::now();

        probe.on_head_at(&synthetic_head(5_000_000, 1), t0);
        probe.on_head_at(&synthetic_head(5_000_001, 2), t0 + Duration::from_secs(50));

        assert_eq!(probe.stalled_for(t0 + Duration::from_secs(61)), None);
        assert_eq!(probe.stalled_for(t0 + Duration::from_secs(111)), Some(61));
    }

    #[test]
    fn reorged_head_at_the_same_height_counts_as_new() {
        let mut probe = LivenessProbe::new(THRESHOLD, 12);
        let t0 = Instant::now();

        probe.on_head_at(&synthetic_head(5_000_000, 1), t0);
        probe.on_head_at(&synthetic_head(5_000_000, 2), t0 + Duration::from_secs(50));

        assert_eq!(probe.stalled_for(t0 + Duration::from_secs(61)), None);
    }

    #[test]
    fn no_head_since_start_is_stalled_after_threshold() {
        let probe = LivenessProbe::new(THRESHOLD, 12);
        let t0 = probe.last_head_received_at;

        assert_eq!(probe.stalled_for(t0 + Duration::from_secs(THRESHOLD)), None);
        assert_eq!(probe.stalled_for(t0 + Duration::from_secs(THRESHOLD + 1)), Some(THRESHOLD + 1));
    }
}
