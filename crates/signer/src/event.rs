use alloy_signer::Signature;
use dogeos_reth_primitives::DogeosBlock;

/// An enum representing the events that can be emitted by the signer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SignerEvent {
    /// A block has been signed by the signer.
    SignedBlock {
        /// The signed block.
        block: DogeosBlock,
        /// The signature of the block.
        signature: Signature,
    },
}
