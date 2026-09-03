use crate::ChainOrchestratorStatus;

use super::ChainOrchestratorEvent;
// use crate::manager::metrics::HandleMetrics;
use reth_network_api::FullNetwork;
use reth_tokio_util::EventStream;
use rollup_node_primitives::{BlockInfo, ChainImport, L1MessageEnvelope};
use scroll_db::L1MessageKey;
use scroll_network::{DogeosNetworkPrimitives, NewBlockWithPeer, ScrollNetworkHandle};
use tokio::sync::{mpsc, oneshot};
use tracing::error;

mod command;
pub use command::{ChainOrchestratorCommand, DatabaseQuery};

mod metrics;
use metrics::ChainOrchestratorHandleMetrics;

/// The command channel to the rollup manager is closed (the orchestrator is
/// gone), so the command could not be delivered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChainOrchestratorCommandSendError;

impl std::fmt::Display for ChainOrchestratorCommandSendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "chain orchestrator command channel closed")
    }
}

impl std::error::Error for ChainOrchestratorCommandSendError {}

/// The handle used to send commands to the rollup manager.
#[derive(Debug, Clone)]
pub struct ChainOrchestratorHandle<N: FullNetwork<Primitives = DogeosNetworkPrimitives>> {
    /// The channel used to send commands to the rollup manager.
    to_manager_tx: mpsc::UnboundedSender<ChainOrchestratorCommand<N>>,
    /// The metrics for the handle.
    handle_metrics: ChainOrchestratorHandleMetrics,
    /// Mock for the L1 Watcher used in tests.
    #[cfg(feature = "test-utils")]
    pub l1_watcher_mock: Option<rollup_node_watcher::test_utils::L1WatcherMock>,
}

impl<N: FullNetwork<Primitives = DogeosNetworkPrimitives>> ChainOrchestratorHandle<N> {
    /// Create a new rollup manager handle.
    pub fn new(to_manager_tx: mpsc::UnboundedSender<ChainOrchestratorCommand<N>>) -> Self {
        Self {
            to_manager_tx,
            handle_metrics: ChainOrchestratorHandleMetrics::default(),
            #[cfg(feature = "test-utils")]
            l1_watcher_mock: None,
        }
    }

    /// Sets the L1 watcher mock for the handle.
    #[cfg(feature = "test-utils")]
    pub fn with_l1_watcher_mock(
        mut self,
        l1_watcher_mock: Option<rollup_node_watcher::test_utils::L1WatcherMock>,
    ) -> Self {
        self.l1_watcher_mock = l1_watcher_mock;
        self
    }

    /// Returns whether the command channel to the rollup manager is closed
    /// (the orchestrator is gone). Lets callers distinguish a dead
    /// orchestrator from a command whose handler failed and dropped its
    /// response sender — both surface as `RecvError` on the reply.
    pub fn is_closed(&self) -> bool {
        self.to_manager_tx.is_closed()
    }

    /// Sends a command to the rollup manager.
    pub fn send_command(&self, command: ChainOrchestratorCommand<N>) {
        if let Err(err) = self.to_manager_tx.send(command) {
            self.handle_metrics.handle_send_command_failed.increment(1);
            error!(target: "rollup::manager::handle", "Failed to send command to rollup manager: {}", err);
        }
    }

    /// Sends a command to the rollup manager to build a block.
    pub fn build_block(&self) {
        self.send_command(ChainOrchestratorCommand::BuildBlock);
    }

    /// Sends a command to the rollup manager to build a block, reporting
    /// whether the command could be delivered. Callers that wait for a build
    /// outcome should use this: on a closed channel the infallible
    /// `build_block` only logs, and the wait would burn its full budget for a
    /// request that was never sent.
    pub fn try_build_block(&self) -> Result<(), ChainOrchestratorCommandSendError> {
        self.to_manager_tx.send(ChainOrchestratorCommand::BuildBlock).map_err(|_| {
            self.handle_metrics.handle_send_command_failed.increment(1);
            ChainOrchestratorCommandSendError
        })
    }

    /// Sends a command to the rollup manager to get the network handle.
    pub async fn get_network_handle(
        &self,
    ) -> Result<ScrollNetworkHandle<N>, oneshot::error::RecvError> {
        let (tx, rx) = oneshot::channel();
        self.send_command(ChainOrchestratorCommand::NetworkHandle(tx));
        rx.await
    }

    /// Sends a command to the rollup manager to fetch an event listener for the rollup node
    /// manager.
    pub async fn get_event_listener(
        &self,
    ) -> Result<EventStream<ChainOrchestratorEvent>, oneshot::error::RecvError> {
        let (tx, rx) = oneshot::channel();
        self.send_command(ChainOrchestratorCommand::EventListener(tx));
        rx.await
    }

    /// Update the FCS head. The outer `Result` is the command transport;
    /// the inner one carries the handler's verdict — `Err(reason)` when the
    /// engine refused the head or persistence failed (nothing to retry
    /// blindly: read the reason). An outer `Err` means no reply ever
    /// arrived: the orchestrator is gone (see `is_closed`), or the handler
    /// fail-stopped without replying (persistence failed AND the
    /// compensating engine rollback did not commit).
    pub async fn update_fcs_head(
        &self,
        head: BlockInfo,
    ) -> Result<Result<(), String>, oneshot::error::RecvError> {
        self.update_fcs_head_if_unmoved(head, None).await
    }

    /// [`Self::update_fcs_head`] with a compare-and-swap precondition: the
    /// handler refuses (inner `Err`) when the engine's head no longer equals
    /// `expected_head` at processing time. A caller that DECIDES a head move
    /// from an observed head (the remote source's rewind) must use this —
    /// the decision and the command are not atomic, and a concurrent revert
    /// in the gap would otherwise be undone by a now-forward head move.
    pub async fn update_fcs_head_if_unmoved(
        &self,
        head: BlockInfo,
        expected_head: Option<BlockInfo>,
    ) -> Result<Result<(), String>, oneshot::error::RecvError> {
        let (tx, rx) = oneshot::channel();
        self.send_command(ChainOrchestratorCommand::UpdateFcsHead((head, expected_head, tx)));
        rx.await
    }

    /// Sends a command to the rollup manager to enable automatic sequencing.
    pub async fn enable_automatic_sequencing(&self) -> Result<bool, oneshot::error::RecvError> {
        let (tx, rx) = oneshot::channel();
        self.send_command(ChainOrchestratorCommand::EnableAutomaticSequencing(tx));
        rx.await
    }

    /// Sends a command to the rollup manager to disable automatic sequencing.
    pub async fn disable_automatic_sequencing(&self) -> Result<bool, oneshot::error::RecvError> {
        let (tx, rx) = oneshot::channel();
        self.send_command(ChainOrchestratorCommand::DisableAutomaticSequencing(tx));
        rx.await
    }

    /// Sends a command to the rollup manager to get the current status.
    pub async fn status(&self) -> Result<ChainOrchestratorStatus, oneshot::error::RecvError> {
        let (tx, rx) = oneshot::channel();
        self.send_command(ChainOrchestratorCommand::Status(tx));
        rx.await
    }

    /// Get an L1 message by its index.
    pub async fn get_l1_message_by_key(
        &self,
        key: L1MessageKey,
    ) -> Result<Option<L1MessageEnvelope>, oneshot::error::RecvError> {
        let (tx, rx) = oneshot::channel();
        self.send_command(ChainOrchestratorCommand::DatabaseQuery(
            DatabaseQuery::GetL1MessageByKey(key, tx),
        ));
        rx.await
    }

    /// Revert the rollup node state to the specified L1 block number.
    ///
    /// `Ok(false)` means the unwind was REFUSED before any durable change —
    /// both `false` sites are PRE-latch (bad finalized-L1 read, or a target
    /// below the finalized L1 block), so nothing was touched. Every failure
    /// AFTER the latch now fail-stops the run loop, which surfaces here as an
    /// outer `Err(RecvError)` (the reply channel is dropped), never `Ok(false)`.
    pub async fn revert_to_l1_block(
        &self,
        block_number: u64,
    ) -> Result<bool, oneshot::error::RecvError> {
        let (tx, rx) = oneshot::channel();
        self.send_command(ChainOrchestratorCommand::RevertToL1Block((block_number, tx)));
        rx.await
    }

    /// Import a block from a remote source.
    pub async fn import_block(
        &self,
        block_with_peer: NewBlockWithPeer,
    ) -> Result<Result<ChainImport, String>, oneshot::error::RecvError> {
        let (tx, rx) = oneshot::channel();
        self.send_command(ChainOrchestratorCommand::ImportBlock { block_with_peer, response: tx });
        rx.await
    }

    /// Sends a command to the rollup manager to enable or disable gossiping of blocks to peers.
    #[cfg(feature = "test-utils")]
    pub async fn set_gossip(&self, enabled: bool) -> Result<(), oneshot::error::RecvError> {
        let (tx, rx) = oneshot::channel();
        self.send_command(ChainOrchestratorCommand::SetGossip((enabled, tx)));
        rx.await
    }

    /// Sends a command to the rollup manager to get a database handle for direct database access.
    #[cfg(feature = "test-utils")]
    pub async fn get_database_handle(
        &self,
    ) -> Result<std::sync::Arc<scroll_db::Database>, oneshot::error::RecvError> {
        let (tx, rx) = oneshot::channel();
        self.send_command(ChainOrchestratorCommand::DatabaseHandle(tx));
        rx.await
    }
}
