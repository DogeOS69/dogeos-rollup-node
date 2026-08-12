//! Test utilities for the engine crate.

use crate::{ScrollEngineApi, ScrollEngineApiResult};
use alloy_rpc_types_engine::{
    ExecutionPayloadV1, ForkchoiceState, ForkchoiceUpdated, PayloadId, PayloadStatus,
};
use dogeos_reth_engine::ScrollPayloadAttributes;

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
