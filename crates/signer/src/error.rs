/// An enum representing the errors that can occur in the signer.
#[derive(Debug, thiserror::Error)]
pub enum SignerError {
    /// The Signer request channel was closed.
    #[error("Request channel closed")]
    RequestChannelClosed,
    /// The Signer event channel was closed.
    #[error("Event channel closed")]
    EventChannelClosed,
}
