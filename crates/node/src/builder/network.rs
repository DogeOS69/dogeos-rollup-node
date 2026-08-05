use dogeos_chainspec::DogeosChainSpec;
use dogeos_reth_primitives::DogeosPrimitives;
use reth_network::{
    config::NetworkMode, primitives::BasicNetworkPrimitives, protocol::RlpxSubProtocol,
    NetworkHandle, NetworkManager, PeersInfo,
};
use reth_node_api::{NodeTypes, TxTy};
use reth_node_builder::{components::NetworkBuilder, BuilderContext, FullNodeTypes};
use reth_transaction_pool::{PoolTransaction, TransactionPool};
use scroll_network::{EthWireBlockImport, EthWireBlockWithPeer};
use std::fmt::Debug;
use tokio::sync::mpsc::UnboundedSender;
use tracing::info;

/// The network builder for Scroll.
#[derive(Debug)]
pub struct ScrollNetworkBuilder {
    /// Additional `RLPx` sub-protocols to be added to the network.
    scroll_sub_protocols: Vec<RlpxSubProtocol>,
    /// Sender used to bridge Reth's `eth` block-import callback into the rollup network manager.
    eth_wire_block_tx: UnboundedSender<EthWireBlockWithPeer>,
}

impl ScrollNetworkBuilder {
    /// Create a new [`ScrollNetworkBuilder`] with provided rollup node database.
    pub fn new(eth_wire_block_tx: UnboundedSender<EthWireBlockWithPeer>) -> Self {
        Self { scroll_sub_protocols: Vec::new(), eth_wire_block_tx }
    }

    /// Add a scroll sub-protocol to the network builder.
    pub fn with_sub_protocol(mut self, protocol: RlpxSubProtocol) -> Self {
        self.scroll_sub_protocols.push(protocol);
        self
    }
}

impl<Node, Pool> NetworkBuilder<Node, Pool> for ScrollNetworkBuilder
where
    Node:
        FullNodeTypes<Types: NodeTypes<ChainSpec = DogeosChainSpec, Primitives = DogeosPrimitives>>,
    Pool: TransactionPool<
            Transaction: PoolTransaction<
                Consensus = TxTy<Node::Types>,
                Pooled = dogeos_protocol_types::ScrollPooledTransaction,
            >,
        > + Unpin
        + 'static,
{
    type Network = NetworkHandle<DogeosNetworkPrimitives>;

    async fn build_network(
        self,
        ctx: &BuilderContext<Node>,
        pool: Pool,
    ) -> eyre::Result<Self::Network> {
        let mut config = ctx
            .network_config_builder::<DogeosNetworkPrimitives>()?
            .network_mode(NetworkMode::Work)
            .block_import(Box::new(EthWireBlockImport::new(self.eth_wire_block_tx)));
        for protocol in self.scroll_sub_protocols {
            config = config.add_rlpx_sub_protocol(protocol);
        }

        let network = NetworkManager::builder(ctx.build_network_config(config)).await?;
        let handle = ctx.start_network(network, pool);
        info!(target: "reth::cli", enode=%handle.local_node_record(), "P2P networking initialized");
        Ok(handle)
    }
}

/// Network primitive types used by Scroll networks.
type DogeosNetworkPrimitives =
    BasicNetworkPrimitives<DogeosPrimitives, dogeos_protocol_types::ScrollPooledTransaction>;
