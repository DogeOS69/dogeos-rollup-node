use super::SignerEvent;
use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

use dogeos_reth_primitives::DogeosBlock;
use rollup_node_primitives::sig_encode_hash;

/// How long one signing attempt may take before it is abandoned and retried.
pub(crate) const ATTEMPT_TIMEOUT: Duration = Duration::from_secs(10);
/// The wait before the first retry. It doubles after each failed attempt.
const FIRST_RETRY_DELAY: Duration = Duration::from_secs(1);
/// The longest wait between retries.
const MAX_RETRY_DELAY: Duration = Duration::from_secs(30);

/// A type alias for a future that resolves to a `SignerEvent`.
pub type SignerFuture = Pin<Box<dyn Future<Output = SignerEvent> + Send>>;

/// A future that signs a block using the provided signer.
///
/// Signatures are handed out in block order, so a remote signer (such as AWS KMS) call that
/// never answers would hold back every later block. Each attempt is therefore cut off after 10
/// seconds, and a failed or timed-out attempt is retried after a delay that starts at 1 second
/// and doubles up to 30 seconds, until the block is signed.
pub fn sign_block(
    block: DogeosBlock,
    signer: Arc<dyn alloy_signer::Signer + Send + Sync>,
) -> SignerFuture {
    Box::pin(async move {
        // TODO: Are we happy to sign the hash directly or do we want to use EIP-191
        // (`signer.sign_message`)?
        let hash = sig_encode_hash(&block);
        let number = block.header.number;
        let mut delay = FIRST_RETRY_DELAY;
        loop {
            match tokio::time::timeout(ATTEMPT_TIMEOUT, signer.sign_hash(&hash)).await {
                Ok(Ok(signature)) => return SignerEvent::SignedBlock { block, signature },
                Ok(Err(err)) => {
                    tracing::error!(target: "scroll::signer", number, ?err, ?delay, "Failed to sign block, retrying");
                }
                Err(_) => {
                    tracing::error!(target: "scroll::signer", number, ?delay, "Signing the block timed out, retrying");
                }
            }
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(MAX_RETRY_DELAY);
        }
    })
}
