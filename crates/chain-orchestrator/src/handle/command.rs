use crate::{ChainOrchestratorEvent, ChainOrchestratorStatus, ImportBlockError, ResetCommandError};

use reth_network_api::FullNetwork;
use reth_tokio_util::EventStream;
use rollup_node_primitives::{BlockInfo, ChainImport, L1MessageEnvelope};
use scroll_db::L1MessageKey;
use scroll_network::{DogeosNetworkPrimitives, NewBlockWithPeer, ScrollNetworkHandle};
use tokio::sync::oneshot;

/// The commands that can be sent to the rollup manager.
#[derive(Debug)]
pub enum ChainOrchestratorCommand<N: FullNetwork<Primitives = DogeosNetworkPrimitives>> {
    /// Command to build a new block.
    BuildBlock,
    /// Returns an event stream for rollup manager events.
    EventListener(oneshot::Sender<EventStream<ChainOrchestratorEvent>>),
    /// Report the current status of the manager via the oneshot channel.
    Status(oneshot::Sender<ChainOrchestratorStatus>),
    /// Returns the network handle.
    NetworkHandle(oneshot::Sender<ScrollNetworkHandle<N>>),
    /// Update the head of the fcs in the engine driver.
    UpdateFcsHead((BlockInfo, oneshot::Sender<()>)),
    /// Enable automatic sequencing.
    EnableAutomaticSequencing(oneshot::Sender<bool>),
    /// Disable automatic sequencing.
    DisableAutomaticSequencing(oneshot::Sender<bool>),
    /// Send a database query to the rollup manager.
    DatabaseQuery(DatabaseQuery),
    /// Revert the rollup node state to the specified L1 block number. The response is `Ok(true)`
    /// on a completed reset, or a typed [`ResetCommandError`] — notably
    /// [`ResetCommandError::ResetInProgress`] when a reset to a different block is already staged.
    RevertToL1Block((u64, oneshot::Sender<Result<bool, ResetCommandError>>)),
    /// Import a block from a remote source.
    ImportBlock {
        /// The block to import with peer info
        block_with_peer: NewBlockWithPeer,
        /// Response channel. `Err(ImportBlockError::AuthorizationPending)` signals the import was
        /// deferred behind an open authorization barrier and should be retried, not treated as a
        /// failure.
        response: oneshot::Sender<Result<ChainImport, ImportBlockError>>,
    },
    /// Enable gossiping of blocks to peers.
    #[cfg(feature = "test-utils")]
    SetGossip((bool, oneshot::Sender<()>)),
    /// Returns a database handle for direct database access.
    #[cfg(feature = "test-utils")]
    DatabaseHandle(oneshot::Sender<std::sync::Arc<scroll_db::Database>>),
}

/// The database queries that can be sent to the rollup manager.
#[derive(Debug)]
pub enum DatabaseQuery {
    /// Get L1 message by its index.
    GetL1MessageByKey(L1MessageKey, oneshot::Sender<Option<L1MessageEnvelope>>),
}
