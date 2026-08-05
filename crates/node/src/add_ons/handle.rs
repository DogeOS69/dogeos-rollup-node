use reth_network_api::FullNetwork;
use reth_node_api::FullNodeComponents;
use reth_node_builder::rpc::{RpcHandle, RpcHandleProvider};
use reth_rpc_eth_api::EthApiTypes;
use rollup_node_chain_orchestrator::ChainOrchestratorHandle;
use scroll_network::DogeosNetworkPrimitives;

/// A handle for scroll addons, which includes handles for the rollup manager and RPC server.
#[derive(Debug, Clone)]
pub struct ScrollAddOnsHandle<
    Node: FullNodeComponents<Network: FullNetwork<Primitives = DogeosNetworkPrimitives>>,
    EthApi: EthApiTypes,
> {
    /// The handle used to send commands to the rollup manager.
    pub rollup_manager_handle: ChainOrchestratorHandle<Node::Network>,
    /// The handle used to send commands to the RPC server.
    pub rpc_handle: RpcHandle<Node, EthApi>,
}

impl<Node, EthApi> RpcHandleProvider<Node, EthApi> for ScrollAddOnsHandle<Node, EthApi>
where
    Node: FullNodeComponents<Network: FullNetwork<Primitives = DogeosNetworkPrimitives>>,
    EthApi: EthApiTypes,
{
    fn rpc_handle(&self) -> &RpcHandle<Node, EthApi> {
        &self.rpc_handle
    }

    fn rpc_handle_mut(&mut self) -> &mut RpcHandle<Node, EthApi> {
        &mut self.rpc_handle
    }
}
