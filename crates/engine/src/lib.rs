//! Engine Driver for the Scroll Rollup Node. The [`Engine`] exposes the main interface for
//! the Rollup Node to the Engine API.

pub use error::{EngineError, FcsError};
mod error;

mod api;
pub use api::{ScrollAuthApiEngineClient, ScrollEngineApi, ScrollEngineApiResult};

mod classify;
pub use classify::{
    classify_fcu_no_attributes, classify_fcu_with_attributes, classify_new_payload,
    get_payload_error_is_transient, transport_error_is_transient, FcuAttributesOutcome,
    InvalidPayloadDetails, PayloadOutcome, StrictFcuStatus, ENGINE_UNKNOWN_PAYLOAD_CODE,
};

pub use fcs::{genesis_hash_from_chain_spec, ForkchoiceState};
mod fcs;

pub use metrics::EngineDriverMetrics;
mod metrics;

mod payload;
pub use payload::block_matches_attributes;

mod engine;
pub use engine::Engine;

#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;
