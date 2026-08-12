use dogeos_reth_primitives::DogeosBlock;
use reth_eth_wire_types::NewBlock as EthWireNewBlock;
use reth_network::{
    import::{BlockImport, BlockImportEvent, NewBlockEvent},
    message::{NewBlockMessage, PeerMessage},
    NetworkHandle,
};
use reth_network_api::PeerId;
use std::{
    task::{Context, Poll},
    time::{Duration, Instant},
};
use tokio::sync::mpsc::{error::TrySendError, Sender};
use tracing::{trace, warn};

use crate::DogeosNetworkPrimitives;

/// Capacity of the queue bridging Reth's `eth` block-import callback into the rollup network
/// manager. Bounding the queue prevents a remote peer from buffering unvalidated blocks faster
/// than the serial consumer drains them; announcements beyond the bound are dropped.
pub const ETH_WIRE_BLOCK_CHANNEL_SIZE: usize = 1000;

/// Minimum interval between warnings about announcements dropped on a full bridge queue.
const DROP_LOG_INTERVAL: Duration = Duration::from_secs(5);

/// A full block announcement received from the `eth` wire protocol.
#[derive(Debug, Clone)]
pub struct EthWireBlockWithPeer {
    /// Peer that announced the block.
    pub peer_id: PeerId,
    /// Announced block, without the total-difficulty wrapper.
    pub block: DogeosBlock,
}

/// Bridges Reth's block-import callback into the rollup-owned network manager.
#[derive(Debug)]
pub struct EthWireBlockImport {
    sender: Sender<EthWireBlockWithPeer>,
    /// Announcements dropped since the last full-queue warning.
    dropped_since_log: u64,
    /// Time of the last full-queue warning.
    last_drop_log: Option<Instant>,
}

impl EthWireBlockImport {
    /// Creates a new bridge backed by `sender`.
    pub const fn new(sender: Sender<EthWireBlockWithPeer>) -> Self {
        Self { sender, dropped_since_log: 0, last_drop_log: None }
    }
}

impl BlockImport<EthWireNewBlock<DogeosBlock>> for EthWireBlockImport {
    fn on_new_block(
        &mut self,
        peer_id: PeerId,
        incoming_block: NewBlockEvent<EthWireNewBlock<DogeosBlock>>,
    ) {
        if let NewBlockEvent::Block(message) = incoming_block {
            // Reserve a slot before cloning: on a full queue the announcement is dropped anyway,
            // so this avoids paying for a block clone that would be discarded immediately.
            match self.sender.try_reserve() {
                Ok(permit) => {
                    permit
                        .send(EthWireBlockWithPeer { peer_id, block: message.block.block.clone() });
                }
                Err(TrySendError::Full(())) => {
                    // The serial consumer is behind; drop the announcement instead of buffering
                    // unboundedly. The block is recovered later through re-announcements or sync.
                    self.dropped_since_log += 1;
                    if self.last_drop_log.is_none_or(|last| last.elapsed() >= DROP_LOG_INTERVAL) {
                        warn!(
                            target: "scroll::network::eth_wire",
                            // `dropped` aggregates announcements from every peer since the last
                            // warning; `latest_peer` is only the most recent sender, not the sole
                            // source of the drops.
                            dropped = self.dropped_since_log,
                            latest_peer = %peer_id,
                            "eth-wire block queue is full, dropping announcements"
                        );
                        self.dropped_since_log = 0;
                        self.last_drop_log = Some(Instant::now());
                    }
                }
                Err(TrySendError::Closed(())) => {
                    trace!(
                        target: "scroll::network::eth_wire",
                        %peer_id,
                        "eth-wire block queue is closed, dropping announcement"
                    );
                }
            }
        }
    }

    fn poll(
        &mut self,
        _cx: &mut Context<'_>,
    ) -> Poll<BlockImportEvent<EthWireNewBlock<DogeosBlock>>> {
        Poll::Pending
    }
}

/// Capability required by the rollup network manager to announce a block to one `eth` peer.
pub trait EthWirePeerSender {
    /// Sends `block` to `peer_id` without broadcasting it to scroll-wire capable peers.
    fn eth_wire_announce_block_to_peer(
        &self,
        peer_id: PeerId,
        block: EthWireNewBlock<DogeosBlock>,
        hash: alloy_primitives::B256,
    );
}

impl EthWirePeerSender for NetworkHandle<DogeosNetworkPrimitives> {
    fn eth_wire_announce_block_to_peer(
        &self,
        peer_id: PeerId,
        block: EthWireNewBlock<DogeosBlock>,
        hash: alloy_primitives::B256,
    ) {
        let message = NewBlockMessage { hash, block: std::sync::Arc::new(block) };
        self.send_eth_message(peer_id, PeerMessage::NewBlock(message));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::B256;
    use std::sync::Arc;

    fn block_event(number: u64) -> NewBlockEvent<EthWireNewBlock<DogeosBlock>> {
        let mut new_block = EthWireNewBlock::<DogeosBlock>::default();
        new_block.block.header.number = number;
        NewBlockEvent::Block(NewBlockMessage { hash: B256::ZERO, block: Arc::new(new_block) })
    }

    #[tokio::test]
    async fn forwards_announcements_with_peer_attribution() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(2);
        let mut import = EthWireBlockImport::new(tx);
        let peer_id = PeerId::repeat_byte(1);

        import.on_new_block(peer_id, block_event(7));

        let received = rx.recv().await.expect("announcement should be bridged");
        assert_eq!(received.peer_id, peer_id);
        assert_eq!(received.block.header.number, 7);
    }

    #[tokio::test]
    async fn drops_announcements_when_queue_is_full() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let mut import = EthWireBlockImport::new(tx);
        let peer_id = PeerId::repeat_byte(2);

        // The first announcement fills the queue; the two overflowing ones must be
        // dropped without blocking or panicking.
        import.on_new_block(peer_id, block_event(1));
        import.on_new_block(peer_id, block_event(2));
        import.on_new_block(peer_id, block_event(3));

        let received = rx.recv().await.expect("first announcement should be delivered");
        assert_eq!(received.block.header.number, 1);
        assert!(rx.try_recv().is_err(), "overflowing announcements must be dropped");

        // The first drop is logged immediately and resets the counter; the second drop
        // within the log interval is accounted for the next warning.
        assert!(import.last_drop_log.is_some());
        assert_eq!(import.dropped_since_log, 1);
    }

    #[tokio::test]
    async fn ignores_announcements_after_receiver_is_dropped() {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        drop(rx);
        let mut import = EthWireBlockImport::new(tx);

        // Must not panic when the consumer is gone.
        import.on_new_block(PeerId::repeat_byte(3), block_event(1));
        assert_eq!(import.dropped_since_log, 0);
    }
}
