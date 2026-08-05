mod event;
pub use event::{NewBlockWithPeer, ScrollNetworkManagerEvent};

mod eth_wire;
pub use eth_wire::{EthWireBlockImport, EthWireBlockWithPeer, EthWirePeerSender};

mod handle;
pub use handle::{NetworkHandleMessage, ScrollNetworkHandle};

mod import;
pub use import::{
    BlockImportError, BlockImportOutcome, BlockImportResult, BlockValidation, BlockValidationError,
    ConsensusError,
};

mod manager;
pub use manager::ScrollNetworkManager;

pub use dogeos_chainspec::DOGEOS_MAINNET;
pub use reth_network::{EthNetworkPrimitives, NetworkConfigBuilder};
use reth_tokio_util::EventStream;
pub use scroll_wire::ScrollWireConfig;

/// Network primitives shared by the DogeOS Reth 2 node and rollup-owned networking services.
pub type DogeosNetworkPrimitives = reth_network::primitives::BasicNetworkPrimitives<
    dogeos_reth_primitives::DogeosPrimitives,
    dogeos_protocol_types::ScrollPooledTransaction,
>;

/// The main network struct that encapsulates the network handle and event stream.
#[derive(Debug)]
pub struct ScrollNetwork<N> {
    /// The network handle to interact with the network manager.
    handle: ScrollNetworkHandle<N>,
    /// Event stream for network manager events.
    events: EventStream<ScrollNetworkManagerEvent>,
}

impl<N> ScrollNetwork<N> {
    /// Creates a new instance of `ScrollNetwork`.
    pub fn handle(&self) -> &ScrollNetworkHandle<N> {
        &self.handle
    }

    /// Returns a mutable reference to the event stream.
    pub fn events(&mut self) -> &mut EventStream<ScrollNetworkManagerEvent> {
        &mut self.events
    }
}
