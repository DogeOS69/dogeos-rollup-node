use alloy_primitives::Signature;
use dogeos_reth_primitives::DogeosBlock;
use reth_network_api::PeerId;

/// A new block with the peer id that it was received from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewBlockWithPeer {
    pub peer_id: PeerId,
    pub block: DogeosBlock,
    pub signature: Signature,
}

/// An event that is emitted by the network manager to its subscribers.
#[derive(Debug, Clone)]
pub enum ScrollNetworkManagerEvent {
    NewBlock(NewBlockWithPeer),
}
