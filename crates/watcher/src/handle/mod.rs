use crate::L1Notification;
use rollup_node_primitives::ConsensusUpdate;
use std::sync::Arc;
use tokio::sync::{mpsc, mpsc::UnboundedSender};

mod command;
pub use command::L1WatcherCommand;

/// Error returned when an [`L1WatcherHandle`] cannot reach the L1 watcher task because its command
/// channel is closed — i.e. the watcher has stopped. Distinguishes an unreachable watcher from a
/// successful reset so a caller does not treat a dropped reset command as success.
#[derive(Debug, thiserror::Error)]
#[error("L1 watcher command channel is closed; the watcher task is no longer running")]
pub struct WatcherUnavailable;

/// Handle to interact with the L1 Watcher.
#[derive(Debug)]
pub struct L1WatcherHandle {
    to_watcher_tx: UnboundedSender<L1WatcherCommand>,
    l1_notification_rx: mpsc::Receiver<Arc<L1Notification>>,
    /// Receiver for the dedicated authorization-control channel, held until the consumer takes
    /// ownership of it via [`L1WatcherHandle::take_consensus_control_receiver`].
    ///
    /// This channel carries only phase one of the head-qualified refresh protocol — the barrier
    /// openings ([`ConsensusUpdate::AuthorizationPending`]). It is intentionally separate from and
    /// unbounded relative to the ordinary L1 notification channel, so the consumer can always poll
    /// it above the derivation/sync-gated data path and open the authorization barrier promptly.
    /// Phase two (the resolved signer) instead travels in-order on the notification channel as
    /// [`L1Notification::AuthorizedSigner`], so it is applied in step with the reorg/new-block
    /// data for the same head. The receiver is owned directly by the consumer (rather than
    /// polled through the handle) so it can be borrowed independently of the notification
    /// receiver.
    consensus_control_rx: Option<mpsc::UnboundedReceiver<ConsensusUpdate>>,
}

impl L1WatcherHandle {
    /// Create a new handle with the given command sender and notification/control receivers.
    pub const fn new(
        to_watcher_tx: UnboundedSender<L1WatcherCommand>,
        l1_notification_rx: mpsc::Receiver<Arc<L1Notification>>,
        consensus_control_rx: mpsc::UnboundedReceiver<ConsensusUpdate>,
    ) -> Self {
        Self { to_watcher_tx, l1_notification_rx, consensus_control_rx: Some(consensus_control_rx) }
    }

    /// Get a mutable reference to the L1 notification receiver.
    pub const fn l1_notification_receiver(&mut self) -> &mut mpsc::Receiver<Arc<L1Notification>> {
        &mut self.l1_notification_rx
    }

    /// Takes ownership of the authorization-control receiver.
    ///
    /// The consumer owns the receiver directly so it can poll it independently of (and at a higher
    /// priority than) the notification receiver. Returns `None` if it has already been taken.
    pub const fn take_consensus_control_receiver(
        &mut self,
    ) -> Option<mpsc::UnboundedReceiver<ConsensusUpdate>> {
        self.consensus_control_rx.take()
    }

    /// Reset the L1 Watcher to a specific block number with fresh notification and
    /// authorization-control channels, returning the new authorization-control receiver.
    ///
    /// Both channels are replaced so that any notification or control message queued for the
    /// receivers being torn down is discarded; after the reset the watcher re-opens and re-confirms
    /// the authorization barrier for the current head on the fresh channels. The caller must
    /// install the returned receiver in place of the one it previously took.
    ///
    /// Fallible: if the watcher's command channel is closed (the watcher task has stopped) the
    /// reset cannot be delivered. In that case the handle's receivers are left **untouched**
    /// and [`WatcherUnavailable`] is returned, so the caller can treat the reset as failed
    /// rather than silently swapping to fresh channels whose senders were just dropped (which
    /// would strand it with no watcher able to drive them).
    #[must_use = "the returned authorization-control receiver must replace the previous one"]
    pub fn revert_to_l1_block(
        &mut self,
        block: u64,
    ) -> Result<mpsc::UnboundedReceiver<ConsensusUpdate>, WatcherUnavailable> {
        // Create a fresh notification channel with the same capacity as the original channel, and a
        // fresh authorization-control channel.
        let capacity = self.l1_notification_rx.max_capacity();
        let (tx, rx) = mpsc::channel(capacity);
        let (consensus_control_tx, consensus_control_rx) = mpsc::unbounded_channel();

        // Enqueue the reset BEFORE mutating any receiver. If the send fails the watcher is gone, so
        // we must not swap in the fresh receivers — leave the handle untouched and report the
        // failure.
        self.to_watcher_tx
            .send(L1WatcherCommand::ResetToBlock { block, tx, consensus_control_tx })
            .map_err(|_| WatcherUnavailable)?;

        // The reset is guaranteed enqueued: only now replace the old notification receiver. The
        // control receiver is owned by the caller and returned here for it to install.
        self.l1_notification_rx = rx;
        Ok(consensus_control_rx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn revert_to_l1_block_fails_closed_when_watcher_command_channel_is_closed() {
        let (to_watcher_tx, to_watcher_rx) = mpsc::unbounded_channel::<L1WatcherCommand>();
        let (notif_tx, notif_rx) = mpsc::channel::<Arc<L1Notification>>(16);
        let (_control_tx, control_rx) = mpsc::unbounded_channel::<ConsensusUpdate>();
        let mut handle = L1WatcherHandle::new(to_watcher_tx, notif_rx, control_rx);

        // Simulate a stopped watcher: drop its command receiver so any send fails.
        drop(to_watcher_rx);

        // The reset cannot be delivered: it reports `WatcherUnavailable` rather than reporting a
        // false success.
        assert!(handle.revert_to_l1_block(42).is_err());

        // The original notification receiver was NOT swapped out — a message on the original sender
        // is still delivered to the handle's receiver, proving no fresh (dead-sender) channel was
        // installed.
        notif_tx.try_send(Arc::new(L1Notification::Synced)).expect("original channel still open");
        assert!(handle.l1_notification_receiver().try_recv().is_ok());

        // The authorization-control receiver was likewise never taken/replaced.
        assert!(handle.take_consensus_control_receiver().is_some());
    }
}
