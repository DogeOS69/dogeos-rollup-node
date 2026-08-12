use dogeos_reth_primitives::DogeosBlock;

/// An enum representing the requests that can be sent to the signer.
#[derive(Debug)]
pub enum SignerRequest {
    /// Request to sign a block.
    SignBlock {
        /// The block to sign.
        block: DogeosBlock,
        /// The caller's generation tag at request time, echoed back on the resulting
        /// [`SignerEvent::SignedBlock`](super::SignerEvent::SignedBlock) so the caller can discard
        /// a result whose generation is stale (e.g. produced before a committed unwind).
        generation: u64,
    },
}
