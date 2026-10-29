//! A library responsible for signing artifacts for the rollup node.
//!
//! The signer is generic and can use any implementation of the `Signer` trait from the
//! `alloy_signer` crate, including local and remote signers such as AWS KMS.
//!
//! Currently it only supports signing L2 blocks, however it can be extended to
//! support signing other artifacts in the future such as pre-commitments.

use std::time::Instant;

use alloy_primitives::Signature;
use futures::stream::{FuturesOrdered, StreamExt};
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedSender;
use tokio_stream::wrappers::UnboundedReceiverStream;

mod error;
pub use error::SignerError;

mod event;
pub use event::SignerEvent;

mod future;
pub use future::{sign_block, SignerFuture};

mod handle;
pub use handle::SignerHandle;

mod metrics;
pub use metrics::SignerMetrics;

mod requests;
pub use requests::SignerRequest;

mod signature;
pub use signature::SignatureAsBytes;

/// The signer instance is responsible for signing artifacts for the rollup node.
pub struct Signer {
    /// The signer instance.
    signer: Arc<dyn alloy_signer::Signer<Signature> + Send + Sync>,
    /// A stream of pending signing requests.
    requests: UnboundedReceiverStream<SignerRequest>,
    /// In progress signing requests.
    in_progress: FuturesOrdered<SignerFuture>,
    /// A channel to send events to the engine driver.
    sender: UnboundedSender<SignerEvent>,
    /// The signer metrics.
    metrics: SignerMetrics,
}

impl Signer {
    /// Creates a new [`Signer`] instance and [`SignerHandle`] with the provided signer.
    fn new(
        signer: impl alloy_signer::Signer<Signature> + Send + Sync + 'static,
    ) -> (Self, SignerHandle) {
        let (req_tx, req_rx) = tokio::sync::mpsc::unbounded_channel();
        let (event_tx, event_rx) = tokio::sync::mpsc::unbounded_channel();
        let address = signer.address();
        let signer = Self {
            signer: Arc::new(signer),
            requests: req_rx.into(),
            in_progress: FuturesOrdered::new(),
            sender: event_tx,
            metrics: SignerMetrics::default(),
        };
        (signer, SignerHandle::new(req_tx, event_rx.into(), address))
    }

    /// Spawns a new `Signer` instance onto the tokio runtime.
    pub fn spawn(
        signer: impl alloy_signer::Signer<Signature> + Send + Sync + 'static,
    ) -> SignerHandle {
        let (signer, handle) = Self::new(signer);
        tokio::spawn(signer.run());
        handle
    }

    /// Execution loop for the signer.
    async fn run(mut self) {
        loop {
            tokio::select! {
                Some(request) = self.requests.next() => {
                    match request {
                        SignerRequest::SignBlock(block) => {
                            let signer = self.signer.clone();
                            let metric = self.metrics.clone();
                            let future = Box::pin(async move {
                                let now = Instant::now();
                                let res = sign_block(block, signer).await;
                                metric.signing_duration.record(now.elapsed().as_secs_f64());
                                res
                            });
                            self.in_progress.push_back(future);
                        }

                    }
                }
                Some(event) = self.in_progress.next(), if !self.in_progress.is_empty() => {
                    if self.sender.send(event).is_err() {
                        tracing::info!(target: "scroll::signer", "The event channel has been closed - shutting down.");
                        break;
                    }
                }
                // Signing retries until it succeeds, so stop as soon as nobody is listening
                // instead of waiting for pending blocks to be signed.
                _ = self.sender.closed() => {
                    tracing::info!(target: "scroll::signer", "The event channel has been closed - shutting down.");
                    break;
                }
                else => {
                    // The request channel is closed, exit the loop.
                    tracing::info!(target: "scroll::signer", "Signer request channel has been closed - shutting down.");
                    break;
                }
            }
        }
    }
}

impl std::fmt::Debug for Signer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Signer")
            .field("signer", &"alloy_signer::Signer")
            .field("requests", &self.requests)
            .field("in_progress", &self.in_progress)
            .field("sender", &self.sender)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Address, ChainId, B256};
    use alloy_signer::SignerSync;
    use alloy_signer_local::PrivateKeySigner;
    use dogeos_reth_primitives::DogeosBlock;
    use rollup_node_primitives::sig_encode_hash;
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };

    #[tokio::test]
    async fn test_signer_local() {
        reth_tracing::init_test_tracing();
        let signer = PrivateKeySigner::random();
        let mut handle = Signer::spawn(Box::new(signer.clone()));

        // Test sending a request
        let block = DogeosBlock::default();
        handle.sign_block(block.clone()).unwrap();

        // Test receiving an event
        let event = handle.next().await.unwrap();
        let (event_block, signature) = match event {
            SignerEvent::SignedBlock { block, signature } => (block, signature),
        };
        let hash = sig_encode_hash(&event_block);
        let recovered_address = signature.recover_address_from_prehash(&hash).unwrap();

        assert_eq!(event_block, block);
        assert_eq!(recovered_address, signer.address());
    }

    /// A signer whose first `failures` calls for one hash fail: the first call never returns and
    /// the later ones return an error. Every other call signs normally.
    struct FailsFirst {
        key: PrivateKeySigner,
        hash: B256,
        failures: usize,
        calls: AtomicUsize,
    }

    impl FailsFirst {
        fn new(key: PrivateKeySigner, block: &DogeosBlock, failures: usize) -> Self {
            Self { key, hash: sig_encode_hash(block), failures, calls: AtomicUsize::new(0) }
        }
    }

    #[async_trait::async_trait]
    impl alloy_signer::Signer for FailsFirst {
        async fn sign_hash(&self, hash: &B256) -> alloy_signer::Result<Signature> {
            if *hash == self.hash {
                let call = self.calls.fetch_add(1, Ordering::SeqCst);
                if call == 0 && self.failures > 0 {
                    std::future::pending::<()>().await;
                }
                if call < self.failures {
                    return Err(alloy_signer::Error::other("signer unavailable"));
                }
            }
            self.key.sign_hash_sync(hash)
        }

        fn address(&self) -> Address {
            self.key.address()
        }

        fn chain_id(&self) -> Option<ChainId> {
            None
        }

        fn set_chain_id(&mut self, _chain_id: Option<ChainId>) {}
    }

    #[tokio::test(start_paused = true)]
    async fn test_stuck_signing_call_is_retried_and_order_is_kept() {
        let mut first = DogeosBlock::default();
        first.header.number = 1;
        let mut second = DogeosBlock::default();
        second.header.number = 2;

        let key = PrivateKeySigner::random();
        let mut handle = Signer::spawn(FailsFirst::new(key.clone(), &first, 2));
        let start = tokio::time::Instant::now();
        handle.sign_block(first.clone()).unwrap();
        handle.sign_block(second.clone()).unwrap();

        // The second block signs at once but must not overtake the first. The first call for
        // the first block times out, the retry returns an error, and the next retry signs it.
        for expected in [first, second] {
            let event = tokio::time::timeout(Duration::from_secs(60), handle.next())
                .await
                .expect("block was never signed")
                .unwrap();
            let SignerEvent::SignedBlock { block, signature } = event;
            assert_eq!(block, expected);
            let hash = sig_encode_hash(&block);
            assert_eq!(signature.recover_address_from_prehash(&hash).unwrap(), key.address());
        }
        assert!(start.elapsed() >= future::ATTEMPT_TIMEOUT);
    }

    #[tokio::test(start_paused = true)]
    async fn test_drop_signer_handle_while_retrying() {
        let block = DogeosBlock::default();
        let key = PrivateKeySigner::random();
        let (signer, handle) = Signer::new(FailsFirst::new(key, &block, usize::MAX));
        let task = tokio::spawn(signer.run());
        handle.sign_block(block).unwrap();
        drop(handle);

        // The block can never be signed, so the task must stop because nobody is listening.
        tokio::time::timeout(Duration::from_secs(60), task)
            .await
            .expect("signer task kept retrying after its handle was dropped")
            .expect("signer task panicked");
    }

    // The following tests do not have any assertions, they are just to ensure that the
    // shutdown logic works correctly and does not panic. You can observer the logs to see if the
    // shutdown logic is executed correctly.

    #[tokio::test]
    async fn test_drop_signer_handle() {
        reth_tracing::init_test_tracing();

        // Create a local signer and the signer service
        let key = PrivateKeySigner::random();
        let (signer, handle) = Signer::new(Box::new(key.clone()));

        // Spawn the signer task and capture the JoinHandle
        let task = tokio::spawn(signer.run());

        // Drop the handle to simulate shutdown
        drop(handle);

        // Wait for the signer task to complete
        task.await.expect("Signer task panicked");
    }

    #[tokio::test]
    async fn test_drop_signer_handle_with_wait() {
        reth_tracing::init_test_tracing();

        // Create a local signer and the signer service
        let key = PrivateKeySigner::random();
        let (signer, handle) = Signer::new(Box::new(key.clone()));

        // Spawn the signer task and capture the JoinHandle
        let task = tokio::spawn(signer.run());

        // wait to observe if the else block will be executed.
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Drop the handle to simulate shutdown
        drop(handle);

        // Wait for the signer task to complete
        task.await.expect("signer task panicked");
    }

    #[tokio::test]
    async fn test_drop_signer_handle_with_request() {
        reth_tracing::init_test_tracing();

        // Create a local signer and the signer service
        let key = PrivateKeySigner::random();
        let (signer, handle) = Signer::new(Box::new(key.clone()));

        // Spawn the signer task and capture the JoinHandle
        let task = tokio::spawn(signer.run());

        // Send a signing request through the handle
        let block = DogeosBlock::default();
        handle.sign_block(block.clone()).unwrap();

        // Drop the handle to simulate shutdown
        drop(handle);

        // Wait for the signer task to complete
        task.await.expect("Signer task panicked");
    }
}
