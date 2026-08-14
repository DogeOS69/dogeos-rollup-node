use crate::args::ScrollRollupNodeConfig;

use dogeos_chainspec::{ChainConfig, DogeosChainSpec, ScrollChainConfig};
use dogeos_hardforks::DogeosHardforks;
use reth_chainspec::NamedChain;
use reth_network::NetworkProtocols;
use reth_network_api::FullNetwork;
use reth_node_api::{FullNodeTypes, NodeTypes};
use reth_node_builder::{rpc::RpcHandle, AddOnsContext, FullNodeComponents};
use reth_rpc_eth_api::EthApiTypes;
use rollup_node_chain_orchestrator::ChainOrchestratorHandle;
use scroll_network::{DogeosNetworkPrimitives, EthWireBlockWithPeer};
use scroll_wire::ScrollWireEvent;
use tokio::sync::mpsc::{Receiver, UnboundedReceiver};

/// Implementing the trait allows the type to return whether it is configured for dev chain.
#[auto_impl::auto_impl(Arc)]
pub trait IsDevChain {
    /// Returns true if the chain is a dev chain.
    fn is_dev_chain(&self) -> bool;
}

impl IsDevChain for DogeosChainSpec {
    fn is_dev_chain(&self) -> bool {
        let named: Result<NamedChain, _> = self.chain.try_into();
        named.is_ok_and(|n| matches!(n, NamedChain::Dev))
    }
}

/// The rollup node manager addon.
#[derive(Debug)]
pub struct RollupManagerAddOn {
    config: ScrollRollupNodeConfig,
    scroll_wire_event: UnboundedReceiver<ScrollWireEvent>,
    eth_wire_event: Receiver<EthWireBlockWithPeer>,
}

impl RollupManagerAddOn {
    /// Create a new rollup node manager addon.
    pub const fn new(
        config: ScrollRollupNodeConfig,
        scroll_wire_event: UnboundedReceiver<ScrollWireEvent>,
        eth_wire_event: Receiver<EthWireBlockWithPeer>,
    ) -> Self {
        Self { config, scroll_wire_event, eth_wire_event }
    }

    /// Returns a reference to the scroll rollup node config.
    pub const fn config(&self) -> &ScrollRollupNodeConfig {
        &self.config
    }

    /// Launch the rollup node manager addon.
    pub async fn launch<N: FullNodeComponents, EthApi: EthApiTypes>(
        self,
        ctx: AddOnsContext<'_, N>,
        rpc: RpcHandle<N, EthApi>,
    ) -> eyre::Result<ChainOrchestratorHandle<N::Network>>
    where
        <<N as FullNodeTypes>::Types as NodeTypes>::ChainSpec:
            ChainConfig<Config = ScrollChainConfig> + DogeosHardforks + IsDevChain,
        N::Network: NetworkProtocols
            + FullNetwork<Primitives = DogeosNetworkPrimitives>
            + scroll_network::EthWirePeerSender,
    {
        let (chain_orchestrator, handle) = self
            .config
            .build(
                (&ctx).into(),
                self.scroll_wire_event,
                self.eth_wire_event,
                rpc.rpc_server_handles,
            )
            .await?;
        ctx.node.task_executor().spawn_critical_with_graceful_shutdown_signal(
            "rollup_node_manager",
            |shutdown| async move {
                if let Err(error) =
                    chain_orchestrator.run_until_shutdown(shutdown.ignore_guard()).await
                {
                    panic!("fatal chain orchestrator error: {error}");
                }
            },
        );
        Ok(handle)
    }
}
