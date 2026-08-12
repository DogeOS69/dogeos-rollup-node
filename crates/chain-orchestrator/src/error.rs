use alloy_json_rpc::RpcError;
use alloy_primitives::B256;
use alloy_transport::TransportErrorKind;
use rollup_node_primitives::{BatchInfo, BlockInfo};
use rollup_node_sequencer::SequencerError;
use rollup_node_signer::SignerError;
use scroll_db::{CanRetry, DatabaseError, L1MessageKey};
use scroll_engine::EngineError;

/// The outcome of a failed trusted [`ImportBlock`](crate::ChainOrchestratorCommand::ImportBlock)
/// command.
///
/// A trusted import is deferred (rather than applied) whenever an L1 authorization/structural
/// transition is in progress, so callers can distinguish that transient condition from a genuine
/// import failure and retry instead of surfacing an operational error.
#[derive(Debug, thiserror::Error)]
pub enum ImportBlockError {
    /// The import was deferred because an authorization barrier is currently open (a signer
    /// rotation or reorg-driven reset is in progress). This is transient: the caller should retry
    /// on its next poll once the barrier closes, not treat it as a failure.
    #[error("block import deferred: L1 authorization pending")]
    AuthorizationPending,
    /// The import failed for another reason. The wrapped string is the display form of the
    /// underlying [`ChainOrchestratorError`].
    #[error("{0}")]
    Other(String),
}

/// The typed outcome of a failed `RevertToL1Block` command, surfaced to the admin/RPC caller so it
/// can distinguish a rejected target from an operational failure and know the correct recovery
/// action.
#[derive(Debug, thiserror::Error)]
pub enum ResetCommandError {
    /// A reset to a *different* L1 block is already in progress; the correct recovery is to retry
    /// that exact `staged` target, not `requested`.
    #[error(
        "reset to L1 block {staged} is in progress; cannot revert to a different block {requested}"
    )]
    ResetInProgress {
        /// The L1 block number of the reset already staged.
        staged: u64,
        /// The L1 block number requested by the rejected command.
        requested: u64,
    },
    /// The reset failed for another reason (database unwind, forkchoice repair, or watcher
    /// delivery). The wrapped string is the display form of the underlying
    /// [`ChainOrchestratorError`].
    #[error("{0}")]
    Failed(String),
}

impl From<ChainOrchestratorError> for ResetCommandError {
    fn from(err: ChainOrchestratorError) -> Self {
        match err {
            ChainOrchestratorError::ResetInProgress { staged, requested } => {
                Self::ResetInProgress { staged, requested }
            }
            other => Self::Failed(other.to_string()),
        }
    }
}

/// A type that represents an error that occurred in the chain orchestrator.
#[derive(Debug, thiserror::Error)]
pub enum ChainOrchestratorError {
    /// An error occurred while interacting with the database.
    #[error("database error occurred: {0}")]
    DatabaseError(#[from] DatabaseError),
    /// An error occurred in the engine.
    #[error("engine error occurred: {0}")]
    EngineError(#[from] EngineError),
    /// The L1 watcher task could not be reached (its command channel is closed), so a reset could
    /// not be delivered.
    #[error("L1 watcher unavailable: {0}")]
    L1WatcherUnavailable(#[from] rollup_node_watcher::WatcherUnavailable),
    /// A `RevertToL1Block` command targeted a different L1 block while a committed reset to
    /// `staged` is still in progress. Composing a second unwind onto the first is unsafe, so
    /// the request is rejected; complete or retry the staged reset first.
    #[error(
        "reset to L1 block {staged} is in progress; cannot revert to a different block {requested}"
    )]
    ResetInProgress {
        /// The L1 block number of the reset already staged.
        staged: u64,
        /// The L1 block number requested by the rejected command.
        requested: u64,
    },
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
        match &self {
            Self::DatabaseError(err) => err.can_retry(),
            _ => false,
        }
    }
}
