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
    /// out, the HTTP connection could not be made, or it broke or closed before a response was
    /// read. Nothing was learned about the request, which may or may not have taken effect, so it
    /// is safe only for a caller that retries it from fresh state.
    ///
    /// Any answer returns false: a JSON-RPC error response, an HTTP rejection (such as 401 for a
    /// wrong JWT secret), an unparsable or oversized reply. So do a request too large to send and
    /// any error from another transport, since they would fail the same way on every retry.
    pub fn is_engine_unavailable(&self) -> bool {
        use jsonrpsee::{
            core::{http_helpers::HttpError, ClientError},
            http_client::transport::Error as HttpTransportError,
        };
        let Self::TransportError(RpcError::Transport(TransportErrorKind::Custom(error))) = self
        else {
            return false;
        };
        match error.downcast_ref::<ClientError>() {
            Some(ClientError::RequestTimeout | ClientError::ServiceDisconnect) => true,
            Some(ClientError::Transport(transport)) => match transport.downcast_ref() {
                Some(HttpTransportError::Http(HttpError::Stream(stream))) => {
                    is_lost_connection(stream.as_ref())
                }
                _ => false,
            },
            _ => false,
        }
    }
}

/// Returns true if an HTTP stream error from jsonrpsee's HTTP client is a failed or lost
/// connection: a connect error, an I/O error, or a connection that was closed, cancelled, timed
/// out or ended before the message was complete. A parse or protocol error and a body length
/// limit return false.
fn is_lost_connection(error: &(dyn std::error::Error + 'static)) -> bool {
    if error.is::<std::io::Error>() {
        return true;
    }
    if let Some(error) = error.downcast_ref::<hyper_util::client::legacy::Error>() {
        return error.is_connect() ||
            std::error::Error::source(error).is_some_and(is_lost_connection);
    }
    if let Some(error) = error.downcast_ref::<hyper::Error>() {
        return error.is_incomplete_message() ||
            error.is_canceled() ||
            error.is_closed() ||
            error.is_timeout() ||
            std::error::Error::source(error).is_some_and(|source| source.is::<std::io::Error>());
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonrpsee::{
        core::{client::ClientT, ClientError},
        http_client::HttpClientBuilder,
        rpc_params,
        types::ErrorObject,
    };
    use std::{
        io::{Read, Write},
        net::TcpListener,
        time::Duration,
    };

    fn engine_error(error: ClientError) -> EngineError {
        EngineError::TransportError(TransportErrorKind::custom(error))
    }

    /// Sends one request with a real jsonrpsee HTTP client (the authenticated Engine client's
    /// transport) to `url` and classifies the error.
    async fn classify(url: String, max_request_size: u32) -> (String, bool) {
        let client = HttpClientBuilder::default()
            .request_timeout(Duration::from_millis(500))
            .max_request_size(max_request_size)
            .max_response_size(1024)
            .build(url)
            .unwrap();
        let error = client
            .request::<String, _>("engine_getPayloadV1", rpc_params!["0x0000000000000001"])
            .await
            .unwrap_err();
        let error = engine_error(error);
        (error.to_string(), error.is_engine_unavailable())
    }

    /// A one-shot HTTP server: reads the request, then writes `reply` (if any) and closes, or
    /// with `hang` keeps the connection open without answering.
    fn serve_once(reply: Option<String>, hang: bool) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut buffer = [0u8; 4096];
            let _ = stream.read(&mut buffer);
            if let Some(reply) = reply {
                let _ = stream.write_all(reply.as_bytes());
            }
            if hang {
                std::thread::sleep(Duration::from_secs(2));
            }
        });
        url
    }

    #[tokio::test]
    async fn real_http_client_timeouts_and_lost_connections_are_unavailability() {
        let refused = {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            format!("http://{}", listener.local_addr().unwrap())
        };
        let closed_without_reply = serve_once(None, false);
        let no_answer = serve_once(None, true);
        for (case, url) in [
            ("connection refused", refused),
            ("closed without a reply", closed_without_reply),
            ("no answer within the timeout", no_answer),
        ] {
            let (error, unavailable) = classify(url, 10 * 1024 * 1024).await;
            assert!(unavailable, "{case}: {error}");
        }
    }

    #[tokio::test]
    async fn real_http_client_answers_and_local_errors_are_not_unavailability() {
        let unauthorized = serve_once(
            Some(String::from("HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\n\r\n")),
            false,
        );
        let not_json = serve_once(
            Some(String::from(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 8\r\n\r\nnot-json",
            )),
            false,
        );
        let error_response = serve_once(
            Some(String::from(concat!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 69\r\n\r\n",
                r#"{"jsonrpc":"2.0","id":0,"error":{"code":-38001,"message":"Unknown"}}"#,
                " "
            ))),
            false,
        );
        let malformed_headers =
            serve_once(Some(String::from("HTTP/1.1 200 OK\r\nnot a header line\r\n\r\n")), false);
        let oversized_chunked = serve_once(
            // One 2048-byte (0x800) chunk, over the client's 1024-byte response limit.
            Some(format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\
                 Transfer-Encoding: chunked\r\n\r\n800\r\n{}\r\n0\r\n\r\n",
                "x".repeat(2048)
            )),
            false,
        );
        for (case, url, max_request_size) in [
            ("HTTP 401", unauthorized, 10 * 1024 * 1024),
            ("malformed body", not_json, 10 * 1024 * 1024),
            ("JSON-RPC error response", error_response, 10 * 1024 * 1024),
            ("request too large", "http://127.0.0.1:1".to_owned(), 1),
            ("malformed HTTP headers", malformed_headers, 10 * 1024 * 1024),
            ("oversized chunked response", oversized_chunked, 10 * 1024 * 1024),
        ] {
            let (error, unavailable) = classify(url, max_request_size).await;
            assert!(!unavailable, "{case}: {error}");
        }
    }

    #[test]
    fn non_client_errors_are_not_unavailability() {
        let response = ClientError::Call(ErrorObject::owned(-32000, "boom", None::<()>));
        assert!(!engine_error(response).is_engine_unavailable());
        assert!(!engine_error(ClientError::Custom("malformed".into())).is_engine_unavailable());
        let other_transport = ClientError::Transport(std::io::Error::other("ws error").into());
        assert!(!engine_error(other_transport).is_engine_unavailable());
        let other = EngineError::TransportError(TransportErrorKind::custom(std::io::Error::other(
            "not a client error",
        )));
        assert!(!other.is_engine_unavailable());
        assert!(!EngineError::TransportError(TransportErrorKind::backend_gone())
            .is_engine_unavailable());
        assert!(!EngineError::fcs_no_update_provided().is_engine_unavailable());
    }
}
