use crate::{ChainOrchestratorEvent, ChainOrchestratorStatus};

use reth_network_api::FullNetwork;
use reth_tokio_util::EventStream;
use rollup_node_primitives::{BlockInfo, ChainImport, L1MessageEnvelope};
use scroll_db::L1MessageKey;
use scroll_network::{DogeosNetworkPrimitives, NewBlockWithPeer, ScrollNetworkHandle};
use std::collections::VecDeque;
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
    /// Revert the rollup node state to the specified L1 block number.
    RevertToL1Block((u64, oneshot::Sender<bool>)),
    /// Import a block from a remote source.
    ImportBlock {
        /// The block to import with peer info
        block_with_peer: NewBlockWithPeer,
        /// Response channel
        response: oneshot::Sender<Result<ChainImport, String>>,
    },
    /// Enable gossiping of blocks to peers.
    #[cfg(feature = "test-utils")]
    SetGossip((bool, oneshot::Sender<()>)),
    /// Returns a database handle for direct database access.
    #[cfg(feature = "test-utils")]
    DatabaseHandle(oneshot::Sender<std::sync::Arc<scroll_db::Database>>),
}

impl<N: FullNetwork<Primitives = DogeosNetworkPrimitives>> ChainOrchestratorCommand<N> {
    /// Returns whether this command mutates Engine or canonical L2 state and must wait for held
    /// derived work to complete.
    pub(crate) const fn must_defer_during_derivation(&self) -> bool {
        matches!(self, Self::BuildBlock | Self::UpdateFcsHead(_) | Self::ImportBlock { .. })
    }
}

/// FIFO buffer for state-mutating commands received while derived work is pending.
#[derive(Debug)]
pub(crate) struct DeferredCommands<C> {
    queue: VecDeque<C>,
}

impl<C> Default for DeferredCommands<C> {
    fn default() -> Self {
        Self { queue: VecDeque::new() }
    }
}

impl<C> DeferredCommands<C> {
    /// Returns a command for immediate handling, or retains a conflicting mutation while derived
    /// work is pending.
    pub(crate) fn route(
        &mut self,
        command: C,
        derivation_pending: bool,
        must_defer: bool,
    ) -> Option<C> {
        if derivation_pending && must_defer {
            self.queue.push_back(command);
            None
        } else {
            Some(command)
        }
    }

    /// Removes the oldest deferred mutation.
    pub(crate) fn pop_front(&mut self) -> Option<C> {
        self.queue.pop_front()
    }

    /// Returns the number of deferred mutations.
    pub(crate) fn len(&self) -> usize {
        self.queue.len()
    }

    /// Discards mutations submitted against state invalidated by an L1 unwind.
    pub(crate) fn clear(&mut self) {
        self.queue.clear();
    }
}

/// The database queries that can be sent to the rollup manager.
#[derive(Debug)]
pub enum DatabaseQuery {
    /// Get L1 message by its index.
    GetL1MessageByKey(L1MessageKey, oneshot::Sender<Option<L1MessageEnvelope>>),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq, Eq)]
    enum TestCommand {
        Mutator(u8),
        Status,
    }

    #[test]
    fn status_bypasses_queued_mutator_and_mutators_release_fifo() {
        let mut deferred = DeferredCommands::default();
        assert!(deferred.route(TestCommand::Mutator(1), true, true).is_none());
        assert_eq!(deferred.route(TestCommand::Status, true, false), Some(TestCommand::Status));
        assert!(deferred.route(TestCommand::Mutator(2), true, true).is_none());

        assert_eq!(deferred.pop_front(), Some(TestCommand::Mutator(1)));
        assert_eq!(deferred.pop_front(), Some(TestCommand::Mutator(2)));
        assert!(deferred.pop_front().is_none());
    }

    #[test]
    fn invalidation_discards_mutators_from_obsolete_state() {
        let mut deferred = DeferredCommands::default();
        assert!(deferred.route(TestCommand::Mutator(1), true, true).is_none());

        deferred.clear();

        assert!(deferred.pop_front().is_none());
    }
}
