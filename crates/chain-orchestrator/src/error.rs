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
    /// State this process can no longer reconcile in-place: engine/DB
    /// divergence, a consumed notification that cannot be replayed, or a
    /// post-synced consolidation failure. Most sites have no compensation
    /// (the one that does — the
    /// `UpdateFcsHead` rollback — raises this only when the compensation
    /// itself did not commit). The run loop treats this as fatal because
    /// running on would keep serving from divergent state.
    ///
    /// A restart converges only where the startup repair can reach the
    /// divergence — the head-ahead-of-anchor sites. It does NOT for the
    /// finality-boundary sites: the repair loop is gated on
    /// `l2_head > finalized`, which is the very condition those report as
    /// violated, and the finalized-marker mismatch re-raises identically on the
    /// first finalized notification after boot, so the node crash-loops. Those
    /// sites say "irreconcilable without manual intervention" in their own
    /// messages and mean it.
    #[error("fatal state divergence: {0}")]
    FatalStateDivergence(&'static str),
    /// A consolidation block fetch failed (transport error or a block
    /// temporarily missing from the L2 client). Nothing was purged and no
    /// durable state moved; `consolidate_chain_with_retry` retries these in
    /// place, and only a persistent failure reaches the callers' fatal
    /// escalation — with this cause preserved instead of a generic
    /// `InvalidBlock`.
    #[error("chain consolidation block fetch failed: {0}")]
    ConsolidationFetchFailed(Box<Self>),
    /// The engine did not apply a forkchoice update. Which verdict raises
    /// it differs by site: at the `UpdateFcsHead` site both INVALID and
    /// SYNCING raise it (nothing has been mutated and the refusal is
    /// replied to the caller); at the `RevertToL1Block` combined head+safe
    /// site only INVALID raises it (the unwind has already run and the run
    /// loop escalates to a fail-stop — SYNCING there is a retryable refusal
    /// that replies `false` instead); at the peer-chain-import site only
    /// SYNCING raises it (INVALID is an `InvalidBlock`), the L2 sync state
    /// has been set back to syncing, and the error is only logged on the
    /// gossip path — while via the `ImportBlock` command it is stringified
    /// into the reply, where the remote block source counts it toward its
    /// bounded import-rejection budget.
    #[error("forkchoice update rejected by the engine: {0}")]
    FcuRejected(&'static str),
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
