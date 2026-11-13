use alloy_transport::{RpcError, TransportErrorKind};

/// The error type for the fork choice state.
#[derive(Debug, thiserror::Error)]
pub enum FcsError {
    /// No update was provided for head, safe or finalized.
    #[error("No update provided for head, safe or finalized")]
    NoUpdateProvided,
    /// Finalized block number not increasing.
    #[error("Finalized block number not increasing")]
    FinalizedBlockNumberNotIncreasing,
    /// Head block number cannot be below safe block number.
    #[error("head block number can not be below the safe block number")]
    HeadBelowSafe,
    /// Safe block number cannot be below finalized block number.
    #[error("Safe block number can not be below the finalized block number")]
    SafeBelowFinalized,
}

/// The error type for the Engine.
#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    /// An error occurred in the fork choice state.
    #[error("Fork choice state error: {0}")]
    FcsError(#[from] FcsError),
    /// An error occurred in the transport layer.
    #[error("Transport error: {0}")]
    TransportError(#[from] alloy_transport::TransportError),
}

impl EngineError {
    /// Creates a new [`EngineError`] for a [`FcsError::NoUpdateProvided`].
    pub const fn fcs_no_update_provided() -> Self {
        Self::FcsError(FcsError::NoUpdateProvided)
    }

    /// Creates a new [`EngineError`] for a [`FcsError::FinalizedBlockNumberNotIncreasing`].
    pub const fn fcs_finalized_block_number_not_increasing() -> Self {
        Self::FcsError(FcsError::FinalizedBlockNumberNotIncreasing)
    }

    /// Returns true if the Engine API was unavailable: the authenticated client's request timed
    /// out, its connection failed, or the server disconnected. Nothing was learned about the
    /// request, which may or may not have taken effect, so it is safe only for a caller that
    /// retries it from fresh state. A JSON-RPC error response, a malformed reply and any other
    /// transport error return false.
    pub fn is_engine_unavailable(&self) -> bool {
        use jsonrpsee::core::ClientError;
        let Self::TransportError(RpcError::Transport(TransportErrorKind::Custom(error))) = self
        else {
            return false;
        };
        matches!(
            error.downcast_ref::<ClientError>(),
            Some(
                ClientError::RequestTimeout |
                    ClientError::Transport(_) |
                    ClientError::ServiceDisconnect
            )
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonrpsee::{core::ClientError, types::ErrorObject};

    fn engine_error(error: ClientError) -> EngineError {
        EngineError::TransportError(TransportErrorKind::custom(error))
    }

    #[test]
    fn timeouts_and_lost_connections_are_unavailability() {
        assert!(engine_error(ClientError::RequestTimeout).is_engine_unavailable());
        assert!(engine_error(ClientError::ServiceDisconnect).is_engine_unavailable());
        let lost = ClientError::Transport(std::io::Error::other("connection refused").into());
        assert!(engine_error(lost).is_engine_unavailable());
    }

    #[test]
    fn answers_and_other_errors_are_not_unavailability() {
        let response = ClientError::Call(ErrorObject::owned(-32000, "boom", None::<()>));
        assert!(!engine_error(response).is_engine_unavailable());
        assert!(!engine_error(ClientError::Custom("malformed".into())).is_engine_unavailable());
        let other = EngineError::TransportError(TransportErrorKind::custom(std::io::Error::other(
            "not a client error",
        )));
        assert!(!other.is_engine_unavailable());
        assert!(!EngineError::TransportError(TransportErrorKind::backend_gone())
            .is_engine_unavailable());
        assert!(!EngineError::fcs_no_update_provided().is_engine_unavailable());
    }
}
