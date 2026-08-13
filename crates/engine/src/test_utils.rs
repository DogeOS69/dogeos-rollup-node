//! Test utilities for the engine crate.

use crate::{ScrollEngineApi, ScrollEngineApiResult};
use alloy_rpc_types_engine::{
    ExecutionPayloadV1, ForkchoiceState, ForkchoiceUpdated, PayloadId, PayloadStatus,
};
use alloy_transport::TransportErrorKind;
use dogeos_reth_engine::ScrollPayloadAttributes;
use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicU64, Ordering},
        Mutex,
    },
    time::Duration,
};

/// A [`ScrollEngineApi`] implementation that panics when any method is called.
#[derive(Debug)]
pub struct PanicEngineClient;

#[async_trait::async_trait]
impl ScrollEngineApi for PanicEngineClient {
    async fn new_payload_v1(
        &self,
        _payload: ExecutionPayloadV1,
    ) -> ScrollEngineApiResult<PayloadStatus> {
        panic!("PanicEngineClient does not support new_payload_v1")
    }

    async fn fork_choice_updated_v1(
        &self,
        _fork_choice_state: ForkchoiceState,
        _payload_attributes: Option<ScrollPayloadAttributes>,
    ) -> ScrollEngineApiResult<ForkchoiceUpdated> {
        panic!("PanicEngineClient does not support fork_choice_updated_v1")
    }

    async fn get_payload_v1(
        &self,
        _payload_id: PayloadId,
    ) -> ScrollEngineApiResult<ExecutionPayloadV1> {
        panic!("PanicEngineClient does not support get_payload_v1")
    }
}

/// A single scripted response for a [`ScriptedEngineClient`] method call.
#[derive(Debug, Clone)]
pub enum ScriptedResponse<T> {
    /// Return the given value successfully.
    Ok(T),
    /// Return a transport-level request timeout, exactly as the authenticated Engine client would
    /// surface jsonrpsee's request timeout (a transient interruption).
    Timeout,
    /// Return a JSON-RPC error response with the given code, wrapped as the authenticated Engine
    /// client wraps jsonrpsee errors. Classification remains method-specific: `-38001` is
    /// transient only for derived `getPayload`.
    CallError(i32),
    /// Sleep for the given duration, then resolve to the inner scripted response. Models a
    /// response that is delivered (or times out) after a delay, e.g. an in-flight attempt that
    /// must be cancellable by shutdown.
    DelayThen(Duration, Box<Self>),
}

/// Constructs the transport error produced by a jsonrpsee request timeout, matching how
/// [`crate::ScrollAuthApiEngineClient`] maps client errors.
fn scripted_timeout() -> alloy_transport::TransportError {
    TransportErrorKind::custom(jsonrpsee::core::ClientError::RequestTimeout)
}

/// Constructs the transport error produced by a JSON-RPC error response with the given code.
fn scripted_call_error(code: i32) -> alloy_transport::TransportError {
    let obj = jsonrpsee::types::ErrorObject::owned(code, "scripted call error", None::<()>);
    TransportErrorKind::custom(jsonrpsee::core::ClientError::Call(obj))
}

async fn resolve<T>(
    queue: &Mutex<VecDeque<ScriptedResponse<T>>>,
    method: &'static str,
) -> ScrollEngineApiResult<T> {
    let mut response = queue
        .lock()
        .unwrap()
        .pop_front()
        .unwrap_or_else(|| panic!("scripted engine client: {method} queue is empty"));
    loop {
        match response {
            ScriptedResponse::Ok(value) => return Ok(value),
            ScriptedResponse::Timeout => return Err(scripted_timeout()),
            ScriptedResponse::CallError(code) => return Err(scripted_call_error(code)),
            ScriptedResponse::DelayThen(delay, inner) => {
                tokio::time::sleep(delay).await;
                response = *inner;
            }
        }
    }
}

/// A [`ScrollEngineApi`] implementation backed by FIFO queues of scripted responses.
///
/// Each method pops the next scripted response for that method from its queue, allowing tests to
/// drive the derived-batch reconciliation path through timeouts, `SYNCING`/`INVALID`/`ACCEPTED`
/// statuses, method-specific JSON-RPC errors, and delayed (cancellable) responses without a real
/// execution client.
#[derive(Debug, Default)]
pub struct ScriptedEngineClient {
    new_payload: Mutex<VecDeque<ScriptedResponse<PayloadStatus>>>,
    fork_choice_updated: Mutex<VecDeque<ScriptedResponse<ForkchoiceUpdated>>>,
    get_payload: Mutex<VecDeque<ScriptedResponse<ExecutionPayloadV1>>>,
    new_payload_calls: AtomicU64,
    fork_choice_updated_calls: AtomicU64,
    get_payload_calls: AtomicU64,
}

impl ScriptedEngineClient {
    /// Creates an empty scripted engine client.
    pub fn new() -> Self {
        Self::default()
    }

    /// Appends a scripted `newPayload` response.
    pub fn push_new_payload(&self, response: ScriptedResponse<PayloadStatus>) {
        self.new_payload.lock().unwrap().push_back(response);
    }

    /// Appends a scripted `forkchoiceUpdated` response (covers both the with-attributes and
    /// without-attributes calls, in call order).
    pub fn push_fork_choice_updated(&self, response: ScriptedResponse<ForkchoiceUpdated>) {
        self.fork_choice_updated.lock().unwrap().push_back(response);
    }

    /// Appends a scripted `getPayload` response.
    pub fn push_get_payload(&self, response: ScriptedResponse<ExecutionPayloadV1>) {
        self.get_payload.lock().unwrap().push_back(response);
    }

    /// Returns the number of `newPayload` calls made so far.
    pub fn new_payload_calls(&self) -> u64 {
        self.new_payload_calls.load(Ordering::SeqCst)
    }

    /// Returns the number of `forkchoiceUpdated` calls made so far.
    pub fn fork_choice_updated_calls(&self) -> u64 {
        self.fork_choice_updated_calls.load(Ordering::SeqCst)
    }

    /// Returns the number of `getPayload` calls made so far.
    pub fn get_payload_calls(&self) -> u64 {
        self.get_payload_calls.load(Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl ScrollEngineApi for ScriptedEngineClient {
    async fn new_payload_v1(
        &self,
        _payload: ExecutionPayloadV1,
    ) -> ScrollEngineApiResult<PayloadStatus> {
        self.new_payload_calls.fetch_add(1, Ordering::SeqCst);
        resolve(&self.new_payload, "new_payload_v1").await
    }

    async fn fork_choice_updated_v1(
        &self,
        _fork_choice_state: ForkchoiceState,
        _payload_attributes: Option<ScrollPayloadAttributes>,
    ) -> ScrollEngineApiResult<ForkchoiceUpdated> {
        self.fork_choice_updated_calls.fetch_add(1, Ordering::SeqCst);
        resolve(&self.fork_choice_updated, "fork_choice_updated_v1").await
    }

    async fn get_payload_v1(
        &self,
        _payload_id: PayloadId,
    ) -> ScrollEngineApiResult<ExecutionPayloadV1> {
        self.get_payload_calls.fetch_add(1, Ordering::SeqCst);
        resolve(&self.get_payload, "get_payload_v1").await
    }
}
