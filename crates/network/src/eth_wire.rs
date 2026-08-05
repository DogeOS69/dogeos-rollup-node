use dogeos_reth_primitives::DogeosBlock;
use reth_eth_wire_types::NewBlock as EthWireNewBlock;
use reth_network::{
    import::{BlockImport, BlockImportEvent, NewBlockEvent},
    message::{NewBlockMessage, PeerMessage},
    NetworkHandle,
};
use reth_network_api::PeerId;
use std::task::{Context, Poll};
use tokio::sync::mpsc::UnboundedSender;

use crate::DogeosNetworkPrimitives;

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
    sender: UnboundedSender<EthWireBlockWithPeer>,
}

impl EthWireBlockImport {
    /// Creates a new bridge backed by `sender`.
    pub const fn new(sender: UnboundedSender<EthWireBlockWithPeer>) -> Self {
        Self { sender }
    }
}

impl BlockImport<EthWireNewBlock<DogeosBlock>> for EthWireBlockImport {
    fn on_new_block(
        &mut self,
        peer_id: PeerId,
        incoming_block: NewBlockEvent<EthWireNewBlock<DogeosBlock>>,
    ) {
        if let NewBlockEvent::Block(message) = incoming_block {
            let _ = self
                .sender
                .send(EthWireBlockWithPeer { peer_id, block: message.block.block.clone() });
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
