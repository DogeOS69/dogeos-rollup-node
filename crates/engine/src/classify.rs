//! Strict classification of Engine API responses and transport errors for the derived-batch
//! reconciliation path.
//!
//! The chain orchestrator's derived-batch recovery treats only a fully `VALID` engine response as
//! success. Every other response, and every transport-layer failure, must be classified explicitly
//! as either a transient interruption (retry the whole pending batch from fresh reconciliation) or
//! a terminal condition (fail-stop). This module centralises that classification so it can be unit
//! tested in isolation and reused by the orchestrator without duplicating engine-status semantics.
//!
//! Errors are classified by type and error code only — never by message text.

use alloy_primitives::B256;
use alloy_rpc_types_engine::{ForkchoiceUpdated, PayloadId, PayloadStatus, PayloadStatusEnum};
use alloy_transport::{RpcError, TransportError, TransportErrorKind};

/// The Engine API error code returned by `engine_getPayloadV1` for an unknown or expired payload id
/// (`UnknownPayload`). This is the one method-specific JSON-RPC error the derived path treats as
/// transient: the execution client dropped a payload we just built, and a fresh reconciliation
/// attempt rebuilds it.
pub const ENGINE_UNKNOWN_PAYLOAD_CODE: i64 = -38001;

/// Typed detail extracted from an `INVALID` Engine response, preserved for structured logging and
/// the terminal error surface.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvalidPayloadDetails {
    /// The hash of the most recent valid block, if the engine provided one.
    pub latest_valid_hash: Option<B256>,
    /// The validation error message provided by the engine.
    pub validation_error: String,
}

/// The classified outcome of a forkchoice-update-with-attributes call (payload build request).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FcuAttributesOutcome {
    /// `VALID` with a payload id: proceed to `getPayload`.
    Valid(PayloadId),
    /// `VALID` without a payload id: a terminal protocol/invariant failure.
    ValidMissingPayloadId,
    /// `SYNCING`: transient, retry the whole pending batch.
    Syncing,
    /// `INVALID`: terminal invalid derived payload.
    Invalid(InvalidPayloadDetails),
    /// `ACCEPTED`: terminal protocol failure for a forkchoice update.
    Accepted,
}

/// The classified outcome of a `newPayload` call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PayloadOutcome {
    /// `VALID`: proceed to the final forkchoice update.
    Valid,
    /// `SYNCING`: transient, retry from fresh reconciliation.
    Syncing,
    /// `ACCEPTED`: transient for `newPayload` (payload accepted onto a side chain while syncing),
    /// retry from fresh reconciliation.
    Accepted,
    /// `INVALID`: terminal invalid derived payload.
    Invalid(InvalidPayloadDetails),
}

/// Classifies a forkchoice-update-with-attributes response for the derived-batch path.
pub fn classify_fcu_with_attributes(fcu: &ForkchoiceUpdated) -> FcuAttributesOutcome {
    match &fcu.payload_status.status {
        PayloadStatusEnum::Valid => match fcu.payload_id {
            Some(id) => FcuAttributesOutcome::Valid(id),
            None => FcuAttributesOutcome::ValidMissingPayloadId,
        },
        PayloadStatusEnum::Syncing => FcuAttributesOutcome::Syncing,
        PayloadStatusEnum::Accepted => FcuAttributesOutcome::Accepted,
        PayloadStatusEnum::Invalid { validation_error } => {
            FcuAttributesOutcome::Invalid(InvalidPayloadDetails {
                latest_valid_hash: fcu.payload_status.latest_valid_hash,
                validation_error: validation_error.clone(),
            })
        }
    }
}

/// The classified outcome of a forkchoice update *without* payload attributes, used to advance
/// head/safe/finalized during derived-batch reconciliation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StrictFcuStatus {
    /// `VALID`: the update succeeded and the local fork choice state may be advanced.
    Valid,
    /// `SYNCING`: transient; do not mark the action or batch complete.
    Syncing,
    /// `INVALID`: terminal failure.
    Invalid(InvalidPayloadDetails),
    /// `ACCEPTED`: terminal failure for a forkchoice update.
    Accepted,
}

/// Classifies a forkchoice-update-without-attributes response for the derived-batch path.
pub fn classify_fcu_no_attributes(fcu: &ForkchoiceUpdated) -> StrictFcuStatus {
    match &fcu.payload_status.status {
        PayloadStatusEnum::Valid => StrictFcuStatus::Valid,
        PayloadStatusEnum::Syncing => StrictFcuStatus::Syncing,
        PayloadStatusEnum::Accepted => StrictFcuStatus::Accepted,
        PayloadStatusEnum::Invalid { validation_error } => {
            StrictFcuStatus::Invalid(InvalidPayloadDetails {
                latest_valid_hash: fcu.payload_status.latest_valid_hash,
                validation_error: validation_error.clone(),
            })
        }
    }
}

/// Classifies a `newPayload` response for the derived-batch path.
pub fn classify_new_payload(status: &PayloadStatus) -> PayloadOutcome {
    match &status.status {
        PayloadStatusEnum::Valid => PayloadOutcome::Valid,
        PayloadStatusEnum::Syncing => PayloadOutcome::Syncing,
        PayloadStatusEnum::Accepted => PayloadOutcome::Accepted,
        PayloadStatusEnum::Invalid { validation_error } => {
            PayloadOutcome::Invalid(InvalidPayloadDetails {
                latest_valid_hash: status.latest_valid_hash,
                validation_error: validation_error.clone(),
            })
        }
    }
}

/// Returns `true` if a transport-layer error from an Engine call or a canonical-L2 RPC call
/// represents a transient interruption that the derived-batch driver should retry.
///
/// `TransportError` is `alloy_transport::TransportError`, i.e. `RpcError<TransportErrorKind>` — the
/// same type wrapped by both `EngineError::TransportError` and the orchestrator's `RpcError`
/// variant, so this single function classifies both.
///
/// The authenticated Engine client folds every jsonrpsee error (timeouts and JSON-RPC error
/// responses alike) into `TransportErrorKind::Custom(Box<jsonrpsee::core::ClientError>)`, so
/// precise classification downcasts the inner client error. JSON-RPC error responses are terminal
/// here regardless of code; the one method-specific exception (`getPayload` `-38001`) is handled by
/// [`get_payload_error_is_transient`]. An un-downcastable `Custom` payload is treated as terminal —
/// we never classify by message text.
pub fn transport_error_is_transient(err: &TransportError) -> bool {
    match err {
        RpcError::Transport(kind) => transport_kind_is_transient(kind),
        // JSON-RPC error responses, serialization/deserialization failures, null responses, and
        // local-usage errors are terminal without method-specific recovery context.
        _ => false,
    }
}

/// Returns `true` if an error from derived `engine_getPayloadV1` should retry the whole pending
/// batch. This includes the ordinary transient transport failures plus Engine's method-specific
/// `UnknownPayload` (`-38001`) response: a fresh reconciliation can safely rebuild the payload.
pub fn get_payload_error_is_transient(err: &TransportError) -> bool {
    transport_error_is_transient(err) || transport_error_is_unknown_payload(err)
}

fn transport_kind_is_transient(kind: &TransportErrorKind) -> bool {
    match kind {
        // A batch response went missing: the request itself may still succeed on retry.
        TransportErrorKind::MissingBatchResponse(_) => true,
        // Only rate-limit (429) and temporarily-unavailable (503) HTTP failures are transient.
        TransportErrorKind::HttpError(http) => {
            http.is_rate_limit_err() || http.is_temporarily_unavailable()
        }
        // The authenticated Engine client wraps jsonrpsee errors here.
        TransportErrorKind::Custom(boxed) => boxed
            .downcast_ref::<jsonrpsee::core::ClientError>()
            .is_some_and(client_error_is_transient),
        // `BackendGone`, `PubsubUnavailable`, and any future (`#[non_exhaustive]`) variant:
        // terminal unless positively identified above. Retrying the same client cannot
        // repair a gone backend.
        _ => false,
    }
}

const fn client_error_is_transient(err: &jsonrpsee::core::ClientError) -> bool {
    use jsonrpsee::core::ClientError;
    match err {
        // Request timeout (including the loopback client's fixed 60s timeout), low-level transport
        // failures, and a disconnected background service are all transient interruptions.
        ClientError::RequestTimeout |
        ClientError::Transport(_) |
        ClientError::ServiceDisconnect => true,
        // JSON-RPC responses need call-site context. In particular, `-38001` is retryable only for
        // derived getPayload, which is handled by `get_payload_error_is_transient`. RestartNeeded,
        // ParseError, InvalidRequestId, Custom(text), and everything else are terminal here.
        _ => false,
    }
}

fn transport_error_is_unknown_payload(err: &TransportError) -> bool {
    match err {
        RpcError::ErrorResp(payload) => payload.code == ENGINE_UNKNOWN_PAYLOAD_CODE,
        RpcError::Transport(TransportErrorKind::Custom(boxed)) => {
            boxed.downcast_ref::<jsonrpsee::core::ClientError>().is_some_and(|err| {
                matches!(
                    err,
                    jsonrpsee::core::ClientError::Call(obj)
                        if obj.code() as i64 == ENGINE_UNKNOWN_PAYLOAD_CODE
                )
            })
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_rpc_types_engine::PayloadId;
    use alloy_transport::TransportErrorKind;

    fn fcu(status: PayloadStatusEnum, payload_id: Option<PayloadId>) -> ForkchoiceUpdated {
        ForkchoiceUpdated {
            payload_status: PayloadStatus { status, latest_valid_hash: None },
            payload_id,
        }
    }

    #[test]
    fn classifies_fcu_with_attributes() {
        let id = PayloadId::new([1; 8]);
        assert_eq!(
            classify_fcu_with_attributes(&fcu(PayloadStatusEnum::Valid, Some(id))),
            FcuAttributesOutcome::Valid(id)
        );
        assert_eq!(
            classify_fcu_with_attributes(&fcu(PayloadStatusEnum::Valid, None)),
            FcuAttributesOutcome::ValidMissingPayloadId
        );
        assert_eq!(
            classify_fcu_with_attributes(&fcu(PayloadStatusEnum::Syncing, None)),
            FcuAttributesOutcome::Syncing
        );
        assert_eq!(
            classify_fcu_with_attributes(&fcu(PayloadStatusEnum::Accepted, None)),
            FcuAttributesOutcome::Accepted
        );
        let invalid = fcu(PayloadStatusEnum::Invalid { validation_error: "bad".to_string() }, None);
        assert!(matches!(
            classify_fcu_with_attributes(&invalid),
            FcuAttributesOutcome::Invalid(details) if details.validation_error == "bad"
        ));
    }

    #[test]
    fn classifies_new_payload() {
        let status = |s| PayloadStatus { status: s, latest_valid_hash: None };
        assert_eq!(classify_new_payload(&status(PayloadStatusEnum::Valid)), PayloadOutcome::Valid);
        assert_eq!(
            classify_new_payload(&status(PayloadStatusEnum::Syncing)),
            PayloadOutcome::Syncing
        );
        // ACCEPTED from newPayload is transient (retry), unlike ACCEPTED from a forkchoice update.
        assert_eq!(
            classify_new_payload(&status(PayloadStatusEnum::Accepted)),
            PayloadOutcome::Accepted
        );
        assert!(matches!(
            classify_new_payload(&status(PayloadStatusEnum::Invalid {
                validation_error: "bad".to_string()
            })),
            PayloadOutcome::Invalid(_)
        ));
    }

    #[test]
    fn classifies_fcu_no_attributes() {
        assert_eq!(
            classify_fcu_no_attributes(&fcu(PayloadStatusEnum::Valid, None)),
            StrictFcuStatus::Valid
        );
        assert_eq!(
            classify_fcu_no_attributes(&fcu(PayloadStatusEnum::Syncing, None)),
            StrictFcuStatus::Syncing
        );
        assert_eq!(
            classify_fcu_no_attributes(&fcu(PayloadStatusEnum::Accepted, None)),
            StrictFcuStatus::Accepted
        );
        assert!(matches!(
            classify_fcu_no_attributes(&fcu(
                PayloadStatusEnum::Invalid { validation_error: "bad".to_string() },
                None
            )),
            StrictFcuStatus::Invalid(_)
        ));
    }

    #[test]
    fn timeout_is_transient() {
        let err = TransportErrorKind::custom(jsonrpsee::core::ClientError::RequestTimeout);
        assert!(transport_error_is_transient(&err));
    }

    #[test]
    fn unknown_payload_call_error_requires_get_payload_context() {
        let obj = jsonrpsee::types::ErrorObject::owned(
            ENGINE_UNKNOWN_PAYLOAD_CODE as i32,
            "unknown payload",
            None::<()>,
        );
        let err = TransportErrorKind::custom(jsonrpsee::core::ClientError::Call(obj));
        assert!(!transport_error_is_transient(&err));
        assert!(get_payload_error_is_transient(&err));
    }

    #[test]
    fn other_call_error_is_terminal() {
        let obj = jsonrpsee::types::ErrorObject::owned(-32000, "server error", None::<()>);
        let err = TransportErrorKind::custom(jsonrpsee::core::ClientError::Call(obj));
        assert!(!transport_error_is_transient(&err));
    }

    #[test]
    fn unknown_custom_payload_is_terminal() {
        // A `Custom` transport payload that is not a jsonrpsee client error must be terminal: we
        // never guess from message text.
        let err = TransportErrorKind::custom(std::io::Error::other("mystery"));
        assert!(!transport_error_is_transient(&err));
    }

    #[test]
    fn http_rate_limit_and_unavailable_are_transient() {
        assert!(transport_error_is_transient(&TransportErrorKind::http_error(429, String::new())));
        assert!(transport_error_is_transient(&TransportErrorKind::http_error(503, String::new())));
        assert!(!transport_error_is_transient(&TransportErrorKind::http_error(500, String::new())));
    }

    #[test]
    fn backend_gone_is_terminal() {
        assert!(!transport_error_is_transient(&TransportErrorKind::backend_gone()));
    }
}
