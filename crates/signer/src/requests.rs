use dogeos_reth_primitives::DogeosBlock;

/// An enum representing the requests that can be sent to the signer.
#[derive(Debug)]
pub enum SignerRequest {
    /// Request to sign a block.
    SignBlock(DogeosBlock),
}
