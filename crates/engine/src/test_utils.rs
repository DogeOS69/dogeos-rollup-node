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

/// A response returned by one scripted Engine method call.
#[derive(Debug, Clone)]
pub enum ScriptedResponse<T> {
    /// Return a successful response.
    Ok(T),
    /// Return a generic transport failure.
    TransportFailure,
    /// Delay before resolving to the nested response.
    DelayThen(Duration, Box<Self>),
}

async fn resolve<T>(
    queue: &Mutex<VecDeque<ScriptedResponse<T>>>,
    method: &'static str,
) -> ScrollEngineApiResult<T> {
    let mut response = queue
        .lock()
        .expect("scripted response mutex poisoned")
        .pop_front()
        .unwrap_or_else(|| panic!("scripted engine client: {method} queue is empty"));
    loop {
        match response {
            ScriptedResponse::Ok(value) => return Ok(value),
            ScriptedResponse::TransportFailure => {
                return Err(TransportErrorKind::custom(std::io::Error::other(
                    "scripted transport failure",
                )))
            }
            ScriptedResponse::DelayThen(delay, inner) => {
                tokio::time::sleep(delay).await;
                response = *inner;
            }
        }
    }
}

/// A [`ScrollEngineApi`] implementation backed by per-method FIFO response queues.
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
    /// Creates an empty scripted client.
    pub fn new() -> Self {
        Self::default()
    }

    /// Queues a `newPayload` response.
    pub fn push_new_payload(&self, response: ScriptedResponse<PayloadStatus>) {
        self.new_payload.lock().expect("scripted response mutex poisoned").push_back(response);
    }

    /// Queues a `forkchoiceUpdated` response. Calls with and without attributes share call order.
    pub fn push_fork_choice_updated(&self, response: ScriptedResponse<ForkchoiceUpdated>) {
        self.fork_choice_updated
            .lock()
            .expect("scripted response mutex poisoned")
            .push_back(response);
    }

    /// Queues a `getPayload` response.
    pub fn push_get_payload(&self, response: ScriptedResponse<ExecutionPayloadV1>) {
        self.get_payload.lock().expect("scripted response mutex poisoned").push_back(response);
    }

    /// Returns the number of `newPayload` calls.
    pub fn new_payload_calls(&self) -> u64 {
        self.new_payload_calls.load(Ordering::SeqCst)
    }

    /// Returns the number of `forkchoiceUpdated` calls.
    pub fn fork_choice_updated_calls(&self) -> u64 {
        self.fork_choice_updated_calls.load(Ordering::SeqCst)
    }

    /// Returns the number of `getPayload` calls.
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
