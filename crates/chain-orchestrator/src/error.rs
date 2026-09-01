use alloy_json_rpc::RpcError;
use alloy_primitives::B256;
use alloy_transport::TransportErrorKind;
use rollup_node_primitives::{BatchInfo, BlockInfo};
use rollup_node_sequencer::SequencerError;
use rollup_node_signer::SignerError;
use scroll_db::{
    CanRetry, DatabaseError, FrontierTransitionKind, L1MessageKey, StoredForkchoiceState,
};
use scroll_engine::EngineError;

/// Details for a durable frontier transition whose observed Engine state matches neither its
/// expected nor target state.
#[derive(Debug, thiserror::Error)]
#[error(
    "frontier transition {kind:?} conflicted with Engine state: expected={expected:?}, target={target:?}, observed={observed:?}"
)]
pub struct FrontierTransitionConflict {
    kind: FrontierTransitionKind,
    expected: StoredForkchoiceState,
    target: StoredForkchoiceState,
    observed: StoredForkchoiceState,
}

/// A type that represents an error that occurred in the chain orchestrator.
#[derive(Debug, thiserror::Error)]
pub enum ChainOrchestratorError {
    /// The execution provider did not expose all latest/safe/finalized tags needed for recovery.
    #[error("execution provider is missing latest, safe, or finalized forkchoice tags")]
    MissingEngineForkchoiceTags,
    /// Observing the execution provider failed while frontier recovery was required.
    #[error("failed to observe Engine forkchoice during frontier recovery: {0}")]
    FrontierObservation(Box<Self>),
    /// A durable frontier transition conflicts with the Engine state observed during recovery.
    #[error("{0}")]
    FrontierTransitionConflict(Box<FrontierTransitionConflict>),
    /// A frontier transition would replace or rewind an Engine-finalized block.
    #[error("cannot move database frontier to {target} because Engine finalized is {observed}")]
    FinalizedFrontierConflict {
        /// The database-backed target block.
        target: BlockInfo,
        /// The finalized block observed from the Engine.
        observed: BlockInfo,
    },
    /// An Engine request failed while a durable frontier transition remains pending.
    #[error("Engine request failed while applying frontier transition {kind:?}: {source}")]
    FrontierTransitionEngineRequest {
        /// The pending transition kind.
        kind: FrontierTransitionKind,
        /// The Engine request error.
        #[source]
        source: EngineError,
    },
    /// Updating the unsafe Engine head after a durable L1 unwind failed. Safe/finalized are
    /// already recoverable, but processing must stop until startup reloads the database head.
    #[error("Engine request failed while applying the post-unwind head: {source}")]
    PostUnwindHeadEngineRequest {
        /// The Engine request error.
        #[source]
        source: EngineError,
    },
    /// The Engine returned a non-valid status while a durable transition remains pending.
    #[error("Engine returned {status} while applying frontier transition {kind:?}")]
    FrontierTransitionStatus {
        /// The pending transition kind.
        kind: FrontierTransitionKind,
        /// The Engine payload status.
        status: &'static str,
    },
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
        validation_error: Box<str>,
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
    /// Derived attributes are not a contiguous extension of the database-backed frontier.
    #[error(
        "derived batch {batch_info:?} has a non-contiguous block sequence: expected block {expected_block_number}, got {actual_block_number}"
    )]
    InvalidDerivedBlockSequence {
        /// The batch being reconciled.
        batch_info: BatchInfo,
        /// The next block number required by the verified prefix.
        expected_block_number: u64,
        /// The block number supplied by derivation.
        actual_block_number: u64,
    },
    /// A replayed batch does not match history already covered by the authoritative safe frontier.
    #[error(
        "replayed batch {batch_info:?} does not match safe history at block {block_number}; database frontier is {frontier}"
    )]
    SafeBatchReplayMismatch {
        /// The batch being reconciled.
        batch_info: BatchInfo,
        /// The replayed block that was missing or incompatible.
        block_number: u64,
        /// The authoritative database-backed safe frontier.
        frontier: BlockInfo,
    },
    /// The Engine builder returned a payload that did not extend the exact verified parent or
    /// match the requested derived block number.
    #[error(
        "Engine built an unexpected payload for batch {batch_info:?}: expected block {expected_block_number} on {expected_parent}, got block {actual_block_number} on parent {actual_parent_hash}"
    )]
    BuiltPayloadMismatch {
        /// The batch being reconciled.
        batch_info: BatchInfo,
        /// The exact verified parent supplied to the builder.
        expected_parent: Box<BlockInfo>,
        /// The requested derived block number.
        expected_block_number: u64,
        /// The parent hash returned by the builder.
        actual_parent_hash: B256,
        /// The block number returned by the builder.
        actual_block_number: u64,
    },
    /// A completed batch would replace the current safe/finalized hash at the same height.
    #[error(
        "batch {batch_info:?} produced a conflicting frontier: current={current}, candidate={candidate}"
    )]
    ConflictingBatchFrontier {
        /// The batch being reconciled.
        batch_info: BatchInfo,
        /// The current Engine frontier.
        current: BlockInfo,
        /// The candidate database frontier.
        candidate: BlockInfo,
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

impl ChainOrchestratorError {
    pub(crate) fn frontier_transition_conflict(
        kind: FrontierTransitionKind,
        expected: StoredForkchoiceState,
        target: StoredForkchoiceState,
        observed: StoredForkchoiceState,
    ) -> Self {
        Self::FrontierTransitionConflict(Box::new(FrontierTransitionConflict {
            kind,
            expected,
            target,
            observed,
        }))
    }

    /// Returns true when continuing the run loop could process work against an unresolved Engine
    /// frontier.
    pub(crate) const fn is_frontier_fatal(&self) -> bool {
        matches!(
            self,
            Self::MissingEngineForkchoiceTags |
                Self::FrontierObservation(_) |
                Self::FrontierTransitionConflict(_) |
                Self::FinalizedFrontierConflict { .. } |
                Self::FrontierTransitionEngineRequest { .. } |
                Self::PostUnwindHeadEngineRequest { .. } |
                Self::FrontierTransitionStatus { .. }
        )
    }
}
