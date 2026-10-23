use alloy_json_rpc::RpcError;
use alloy_primitives::B256;
use alloy_transport::TransportErrorKind;
use rollup_node_primitives::{BatchInfo, BlockInfo};
use rollup_node_sequencer::SequencerError;
use rollup_node_signer::SignerError;
use scroll_db::{CanRetry, DatabaseError, L1MessageKey};
use scroll_engine::EngineError;

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
    /// An Engine request failed before returning a status for a derived batch.
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
    /// A build forkchoice update returned `VALID` without a payload id.
    #[error(
        "Engine returned VALID without a payload id while reconciling derived batch {batch_info:?}"
    )]
    MissingDerivedPayloadId {
        /// The batch being reconciled.
        batch_info: BatchInfo,
    },
    /// The Engine returned a terminal status while reconciling a derived batch.
    #[error(
        "Engine returned {status} during {method} while reconciling derived batch {batch_info:?}"
    )]
    UnexpectedDerivedPayloadStatus {
        /// The batch being reconciled.
        batch_info: BatchInfo,
        /// The Engine method that returned the status.
        method: &'static str,
        /// The terminal Engine status.
        status: &'static str,
    },
    /// The Engine rejected a derived payload or forkchoice update as invalid.
    #[error(
        "Engine returned INVALID during {method} for derived batch {batch_info:?} at block {block_number:?}: latest_valid_hash={latest_valid_hash:?}, validation_error={validation_error}"
    )]
    InvalidDerivedPayload {
        /// The batch being reconciled.
        batch_info: BatchInfo,
        /// The Engine method that returned `INVALID`.
        method: &'static str,
        /// The affected block number, when already known.
        block_number: Option<u64>,
        /// The latest hash the Engine considers valid.
        latest_valid_hash: Option<B256>,
        /// The Engine validation detail.
        validation_error: String,
    },
    /// A rebuilt payload differs from the data derived from its batch.
    #[error("Rebuilt payload mismatch for batch {batch_info:?} at block {block_number}: {reason}")]
    DerivedPayloadMismatch {
        /// The batch being rebuilt.
        batch_info: BatchInfo,
        /// The expected block number.
        block_number: u64,
        /// The mismatched field and diagnostic detail.
        reason: String,
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
    /// Reth's canonical chain does not contain the database's safe block, so the Engine is on a
    /// fork the database has left.
    #[error("Engine is not on the database's chain before reconciling batch {batch_info:?}: database safe block {db_safe:?}, Reth canonical block at that height {reth_canonical:?}, Engine safe block {engine_safe:?}")]
    SafeFrontierMismatch {
        /// The batch being reconciled.
        batch_info: BatchInfo,
        /// The Engine's safe block.
        engine_safe: BlockInfo,
        /// The database's latest safe block.
        db_safe: BlockInfo,
        /// Reth's canonical block at the database's safe height, if it has one.
        reth_canonical: Option<BlockInfo>,
    },
    /// The forkchoice update that moves the Engine to the database's blocks after an L1 reorg or
    /// a batch revert failed or was rejected, so the Engine and the database disagree.
    #[error("Forkchoice update after {operation} failed, Engine safe block {engine_safe:?} does not follow database safe block {db_safe:?}: {reason}")]
    UnwindForkchoiceUpdateFailed {
        /// The operation that changed the database.
        operation: &'static str,
        /// The safe block the database moved to, if it changed.
        db_safe: Option<BlockInfo>,
        /// The Engine's safe block after the failed update.
        engine_safe: BlockInfo,
        /// The Engine error or `INVALID` validation detail.
        reason: String,
    },
    /// An error occurred while handling rollup node primitives.
    #[error("An error occurred while handling rollup node primitives: {0}")]
    RollupNodePrimitiveError(rollup_node_primitives::RollupNodePrimitiveError),
}

impl ChainOrchestratorError {
    /// The node stopped because its state may be wrong; restarting alone will not help.
    pub const fn is_safety_stop(&self) -> bool {
        matches!(
            self,
            Self::InvalidDerivedPayload { .. }
                | Self::DerivedPayloadMismatch { .. }
                | Self::SafeFrontierMismatch { .. }
                | Self::UnwindForkchoiceUpdateFailed { .. }
        )
    }
}

impl CanRetry for ChainOrchestratorError {
    fn can_retry(&self) -> bool {
        match &self {
            Self::DatabaseError(err) => err.can_retry(),
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_safety_stops() {
        let cases = [
            (
                ChainOrchestratorError::InvalidDerivedPayload {
                    batch_info: Default::default(),
                    method: "newPayload",
                    block_number: Some(1),
                    latest_valid_hash: None,
                    validation_error: "invalid block".into(),
                },
                true,
            ),
            (
                ChainOrchestratorError::DerivedPayloadMismatch {
                    batch_info: Default::default(),
                    block_number: 1,
                    reason: "block hash differs".into(),
                },
                true,
            ),
            (
                ChainOrchestratorError::SafeFrontierMismatch {
                    batch_info: Default::default(),
                    engine_safe: Default::default(),
                    db_safe: Default::default(),
                    reth_canonical: None,
                },
                true,
            ),
            (
                ChainOrchestratorError::UnwindForkchoiceUpdateFailed {
                    operation: "L1 reorg",
                    db_safe: None,
                    engine_safe: Default::default(),
                    reason: "forkchoice rejected".into(),
                },
                true,
            ),
            (
                ChainOrchestratorError::DatabaseError(DatabaseError::L1MessageNotFound(
                    L1MessageKey::QueueIndex(1),
                )),
                false,
            ),
            (ChainOrchestratorError::EngineError(EngineError::fcs_no_update_provided()), false),
            (ChainOrchestratorError::L2BlockNotFoundInL2Client(1), false),
            (
                ChainOrchestratorError::InvalidBatchReorg {
                    batch_info: Default::default(),
                    safe_block_number: 1,
                    derived_block_number: 3,
                },
                false,
            ),
        ];

        for (error, expected) in cases {
            assert_eq!(error.is_safety_stop(), expected, "{error:?}");
        }
    }
}
