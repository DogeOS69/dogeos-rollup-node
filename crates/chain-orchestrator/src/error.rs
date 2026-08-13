use alloy_json_rpc::RpcError;
use alloy_primitives::B256;
use alloy_transport::TransportErrorKind;
use rollup_node_primitives::{BatchInfo, BlockInfo};
use rollup_node_sequencer::SequencerError;
use rollup_node_signer::SignerError;
use scroll_db::{CanRetry, DatabaseError, L1MessageKey};
use scroll_engine::{get_payload_error_is_transient, transport_error_is_transient, EngineError};

/// A type that represents an error that occurred in the chain orchestrator.
#[derive(Debug, thiserror::Error)]
pub enum ChainOrchestratorError {
    /// An error occurred while interacting with the database.
    #[error("database error occurred: {0}")]
    DatabaseError(#[from] DatabaseError),
    /// An error occurred in the engine.
    #[error("engine error occurred: {0}")]
    EngineError(#[from] EngineError),
    /// An error occurred while trying to fetch the L2 block from the database.
    #[error("L2 block not found - block number: {0}")]
    L2BlockNotFoundInDatabase(u64),
    /// An error occurred while trying to fetch the L2 block from the L2 client.
    #[error("L2 block not found in L2 client - block number: {0}")]
    L2BlockNotFoundInL2Client(u64),
    /// A fork was received from the peer that is associated with a reorg of the safe chain.
    #[error("L2 safe block reorg detected")]
    L2SafeBlockReorgDetected,
    /// A block contains invalid L1 messages.
    #[error("Block contains invalid L1 message. Expected: {expected:?}, Actual: {actual:?}")]
    L1MessageMismatch {
        /// The expected L1 messages hash.
        expected: B256,
        /// The actual L1 messages hash.
        actual: B256,
    },
    /// An L1 message was not found in the database.
    #[error("L1 message not found at {0}")]
    L1MessageNotFound(L1MessageKey),
    /// A gap was detected in the L1 message queue: the previous message before index {0} is
    /// missing.
    #[error("L1 message queue gap detected at index {0}, previous L1 message not found")]
    L1MessageQueueGap(u64),
    /// An inconsistency was detected when trying to consolidate the chain.
    #[error("Chain inconsistency detected")]
    ChainInconsistency,
    /// The peer did not provide the requested block header.
    #[error("A peer did not provide the requested block header")]
    MissingBlockHeader {
        /// The hash of the block header that was requested.
        hash: B256,
    },
    /// The peer did not provide the correct number of blocks.
    #[error("The peer did not provide the correct number of blocks. Expected: {expected}, Actual: {actual}")]
    BlockFetchMismatch {
        /// The expected number of blocks.
        expected: usize,
        /// The actual number of blocks.
        actual: usize,
    },
    /// A gap was detected in batch commit events: the previous batch before index {0} is missing.
    #[error("Batch commit gap detected at index {0}, previous batch commit not found")]
    BatchCommitGap(u64),
    /// An error occurred while making a network request.
    #[error("Network request error: {0}")]
    NetworkRequestError(#[from] reth_network_p2p::error::RequestError),
    /// An error occurred while making a JSON-RPC request to the Execution Node (EN).
    #[error("An error occurred while making a JSON-RPC request to the EN: {0}")]
    RpcError(#[from] RpcError<TransportErrorKind>),
    /// Received an invalid block from peer.
    #[error("Received an invalid block from peer")]
    InvalidBlock,
    /// An error occurred at the sequencer level.
    #[error("An error occurred at the sequencer level: {0}")]
    SequencerError(#[from] SequencerError),
    /// An error occurred at the signing level.
    #[error("An error occurred at the signer level: {0}")]
    SignerError(#[from] SignerError),
    /// The derivation pipeline found an invalid block for the given batch.
    #[error("The derivation pipeline found an invalid block: {0} for batch: {1}")]
    InvalidBatch(BlockInfo, BatchInfo),
    /// The engine returned `SYNCING` while reconciling a derived batch. This is a transient
    /// condition: the whole pending batch is retried from a fresh reconciliation.
    #[error(
        "Engine returned SYNCING during {method} while reconciling derived batch {batch_info:?}"
    )]
    DerivedBatchEngineSyncing {
        /// The batch being reconciled.
        batch_info: BatchInfo,
        /// The engine method that returned `SYNCING`.
        method: &'static str,
    },
    /// The Engine returned `ACCEPTED` from `newPayload` while reconciling a derived batch. This is
    /// transient, but remains distinct from `SYNCING` so status, fatal exhaustion, and events
    /// retain the actual Engine response.
    #[error(
        "Engine returned ACCEPTED during {method} while reconciling derived batch {batch_info:?}"
    )]
    DerivedBatchEngineAccepted {
        /// The batch being reconciled.
        batch_info: BatchInfo,
        /// The Engine method that returned `ACCEPTED`.
        method: &'static str,
    },
    /// An Engine request failed before returning an Engine status while reconciling a derived
    /// batch. The method is retained because some JSON-RPC codes are method-specific.
    #[error(
        "Engine request {method} failed while reconciling derived batch {batch_info:?}: {source}"
    )]
    DerivedBatchEngineRequest {
        /// The batch being reconciled.
        batch_info: BatchInfo,
        /// The Engine method that failed.
        method: &'static str,
        /// The typed Engine error.
        #[source]
        source: EngineError,
    },
    /// Bounded retries for a derived batch were exhausted. This wrapper is terminal even though
    /// the final underlying attempt error was transient.
    #[error(
        "derived batch {batch_info:?} exhausted {attempts} reconciliation attempts: {last_error}"
    )]
    DerivedBatchRetriesExhausted {
        /// The batch whose reconciliation could not complete.
        batch_info: BatchInfo,
        /// Total attempts made, including the first.
        attempts: u32,
        /// The typed transient error returned by the final attempt.
        #[source]
        last_error: Box<Self>,
    },
    /// A forkchoice update with payload attributes returned `VALID` but without a payload id while
    /// reconciling a derived batch. This is a terminal protocol/invariant failure.
    #[error(
        "Engine returned VALID without a payload id while reconciling derived batch {batch_info:?}"
    )]
    MissingPayloadId {
        /// The batch being reconciled.
        batch_info: BatchInfo,
    },
    /// The engine returned an unexpected status (e.g. `ACCEPTED` for a forkchoice update) while
    /// reconciling a derived batch. This is a terminal protocol failure.
    #[error("Engine returned an unexpected status during {method} while reconciling derived batch {batch_info:?}")]
    UnexpectedEngineStatus {
        /// The batch being reconciled.
        batch_info: BatchInfo,
        /// The engine method that returned the unexpected status.
        method: &'static str,
    },
    /// The engine reported a derived payload as `INVALID`. This is a terminal condition; the typed
    /// validation detail is preserved for diagnostics.
    #[error("Engine reported an invalid derived payload during {method} for batch {batch_info:?} (block {block_number:?}, latest valid hash {latest_valid_hash:?}): {validation_error}")]
    InvalidDerivedPayload {
        /// The batch being reconciled.
        batch_info: BatchInfo,
        /// The engine method that reported the invalid payload.
        method: &'static str,
        /// The block number of the derived payload, when known. A forkchoice update with payload
        /// attributes can fail before a block exists, in which case this is `None`.
        block_number: Option<u64>,
        /// The most recent valid block hash reported by the engine, if any.
        latest_valid_hash: Option<B256>,
        /// The validation error message reported by the engine.
        validation_error: String,
    },
    /// Attempted to reorg a batch but the safe block number does not match the derived
    /// block number - 1.
    #[error("Attempted to reorg batch {batch_info:?} for derived block number {derived_block_number} but expected safe block number is {safe_block_number} - we expect `safe block number = derived block number - 1`")]
    InvalidBatchReorg {
        /// The batch info.
        batch_info: BatchInfo,
        /// The current safe block number.
        safe_block_number: u64,
        /// The derived block number.
        derived_block_number: u64,
    },
    /// An error occurred while handling rollup node primitives.
    #[error("An error occurred while handling rollup node primitives: {0}")]
    RollupNodePrimitiveError(rollup_node_primitives::RollupNodePrimitiveError),
}

impl CanRetry for ChainOrchestratorError {
    fn can_retry(&self) -> bool {
        match self {
            Self::DatabaseError(err) => err.can_retry(),
            // Engine transport failures (including the loopback client's request timeout) and
            // canonical-L2 RPC transport failures are transient. JSON-RPC error responses require
            // method context and are terminal here.
            Self::EngineError(err) => engine_error_can_retry(err),
            Self::RpcError(err) => transport_error_is_transient(err),
            Self::DerivedBatchEngineRequest { method, source, .. } => {
                derived_engine_error_can_retry(method, source)
            }
            // `SYNCING` and newPayload `ACCEPTED` are transient while the pending batch is held.
            Self::DerivedBatchEngineSyncing { .. } | Self::DerivedBatchEngineAccepted { .. } => {
                true
            }
            _ => false,
        }
    }
}

/// Classifies a derived Engine request error with the method context needed for `UnknownPayload`.
fn derived_engine_error_can_retry(method: &str, err: &EngineError) -> bool {
    match err {
        EngineError::TransportError(inner) if method == "get_payload" => {
            get_payload_error_is_transient(inner)
        }
        EngineError::TransportError(inner) => transport_error_is_transient(inner),
        EngineError::FcsError(_) => false,
    }
}

/// Classifies an [`EngineError`] as retryable. Transport failures delegate to the shared transport
/// classifier; fork choice state invariant violations are terminal.
fn engine_error_can_retry(err: &EngineError) -> bool {
    match err {
        EngineError::TransportError(inner) => transport_error_is_transient(inner),
        EngineError::FcsError(_) => false,
    }
}
