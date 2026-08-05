//! Scroll-compatible Engine API surface retained by the rollup orchestrator.

use alloy_provider::Provider;
use alloy_rpc_types_engine::{
    ExecutionPayloadV1, ForkchoiceState, ForkchoiceUpdated, PayloadId, PayloadStatus,
};
use alloy_transport::TransportErrorKind;
use dogeos_reth_engine::ScrollPayloadAttributes;
use dogeos_rpc_types::Scroll;
use jsonrpsee::core::client::ClientT;

/// Result returned by the rollup Engine API client.
pub type ScrollEngineApiResult<T> = alloy_transport::TransportResult<T>;

/// Authenticated Engine API client backed by Reth's JWT-aware JSON-RPC client.
#[derive(Debug, Clone)]
pub struct ScrollAuthApiEngineClient<C> {
    client: C,
}

impl<C> ScrollAuthApiEngineClient<C> {
    /// Creates an authenticated Engine API adapter.
    pub const fn new(client: C) -> Self {
        Self { client }
    }
}

/// Engine methods used by the rollup-owned forkchoice and sequencing services.
#[async_trait::async_trait]
pub trait ScrollEngineApi: Send + Sync {
    /// Submits a V1 execution payload.
    async fn new_payload_v1(
        &self,
        payload: ExecutionPayloadV1,
    ) -> ScrollEngineApiResult<PayloadStatus>;

    /// Updates forkchoice and optionally starts a Scroll payload build.
    async fn fork_choice_updated_v1(
        &self,
        fork_choice_state: ForkchoiceState,
        payload_attributes: Option<ScrollPayloadAttributes>,
    ) -> ScrollEngineApiResult<ForkchoiceUpdated>;

    /// Fetches a built V1 execution payload.
    async fn get_payload_v1(
        &self,
        payload_id: PayloadId,
    ) -> ScrollEngineApiResult<ExecutionPayloadV1>;
}

#[async_trait::async_trait]
impl<P> ScrollEngineApi for P
where
    P: Provider<Scroll> + Send + Sync,
{
    async fn new_payload_v1(
        &self,
        payload: ExecutionPayloadV1,
    ) -> ScrollEngineApiResult<PayloadStatus> {
        self.client().request("engine_newPayloadV1", (payload,)).await
    }

    async fn fork_choice_updated_v1(
        &self,
        fork_choice_state: ForkchoiceState,
        payload_attributes: Option<ScrollPayloadAttributes>,
    ) -> ScrollEngineApiResult<ForkchoiceUpdated> {
        self.client()
            .request("engine_forkchoiceUpdatedV1", (fork_choice_state, payload_attributes))
            .await
    }

    async fn get_payload_v1(
        &self,
        payload_id: PayloadId,
    ) -> ScrollEngineApiResult<ExecutionPayloadV1> {
        self.client().request("engine_getPayloadV1", (payload_id,)).await
    }
}

#[async_trait::async_trait]
impl<C> ScrollEngineApi for ScrollAuthApiEngineClient<C>
where
    C: ClientT + Send + Sync,
{
    async fn new_payload_v1(
        &self,
        payload: ExecutionPayloadV1,
    ) -> ScrollEngineApiResult<PayloadStatus> {
        self.client
            .request("engine_newPayloadV1", jsonrpsee::rpc_params![payload])
            .await
            .map_err(TransportErrorKind::custom)
    }

    async fn fork_choice_updated_v1(
        &self,
        fork_choice_state: ForkchoiceState,
        payload_attributes: Option<ScrollPayloadAttributes>,
    ) -> ScrollEngineApiResult<ForkchoiceUpdated> {
        self.client
            .request(
                "engine_forkchoiceUpdatedV1",
                jsonrpsee::rpc_params![fork_choice_state, payload_attributes],
            )
            .await
            .map_err(TransportErrorKind::custom)
    }

    async fn get_payload_v1(
        &self,
        payload_id: PayloadId,
    ) -> ScrollEngineApiResult<ExecutionPayloadV1> {
        self.client
            .request("engine_getPayloadV1", jsonrpsee::rpc_params![payload_id])
            .await
            .map_err(TransportErrorKind::custom)
    }
}
