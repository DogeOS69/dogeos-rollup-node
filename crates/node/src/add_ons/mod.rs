//! The [`ScrollRollupNodeAddOns`] implementation for the Scroll rollup node.

use super::args::ScrollRollupNodeConfig;
use crate::constants;
use dogeos_chainspec::DogeosChainSpec;
use dogeos_reth_engine::DogeosEngineTypes;
use dogeos_reth_evm::{ScrollEvmConfig, ScrollTransactionIntoTxEnv};
use dogeos_reth_node::{DogeosEngineValidatorBuilder, DogeosEthApiBuilder, DogeosStorage};
use dogeos_reth_primitives::DogeosPrimitives;
use reth_evm::{EvmFactory, EvmFactoryFor};
use reth_network::NetworkHandle;
use reth_node_api::{AddOnsContext, NodeAddOns};
use reth_node_builder::{
    rpc::{
        BasicEngineApiBuilder, BasicEngineValidatorBuilder, EngineValidatorAddOn, EthApiBuilder,
        Identity, RethRpcAddOns, RethRpcMiddleware, RpcAddOns, RpcHandle,
    },
    FullNodeComponents,
};
use reth_node_types::NodeTypes;
use reth_revm::context::{BlockEnv, TxEnv};
use reth_rpc_builder::RethRpcModule;
use reth_rpc_eth_types::{error::FromEvmError, EthApiError};
use scroll_wire::ScrollWireEvent;
use std::sync::{Arc, OnceLock};

mod remote_block_source;
pub use remote_block_source::RemoteBlockSourceAddOn;

mod rpc;
pub use rpc::{
    RollupNodeAdminApiClient, RollupNodeAdminApiServer, RollupNodeApiClient, RollupNodeApiServer,
    RollupNodeRpcExt,
};

mod rollup;
pub use rollup::IsDevChain;
use rollup::RollupManagerAddOn;
use scroll_network::{DogeosNetworkPrimitives, EthWireBlockWithPeer};
use tokio::sync::mpsc::{Receiver, UnboundedReceiver};

pub(crate) type RollupManagerHandle = rollup_node_chain_orchestrator::ChainOrchestratorHandle<
    reth_network::NetworkHandle<DogeosNetworkPrimitives>,
>;

/// Add-ons for the Scroll follower node.
#[derive(Debug)]
pub struct ScrollRollupNodeAddOns<N, RpcMiddleware = Identity>
where
    N: FullNodeComponents<Network = NetworkHandle<DogeosNetworkPrimitives>>,
    DogeosEthApiBuilder: EthApiBuilder<N>,
{
    /// Rpc add-ons responsible for launching the RPC servers and instantiating the RPC handlers
    /// and eth-api.
    pub rpc_add_ons: RpcAddOns<
        N,
        DogeosEthApiBuilder,
        DogeosEngineValidatorBuilder,
        BasicEngineApiBuilder<DogeosEngineValidatorBuilder>,
        BasicEngineValidatorBuilder<DogeosEngineValidatorBuilder>,
        RpcMiddleware,
    >,

    /// Rollup manager addon responsible for managing the components of the rollup node.
    pub rollup_manager_addon: RollupManagerAddOn,

    /// Shared handle populated after the rollup manager launches.
    rollup_manager_handle: Arc<OnceLock<RollupManagerHandle>>,
}

impl<N> ScrollRollupNodeAddOns<N>
where
    N: FullNodeComponents<Network = NetworkHandle<DogeosNetworkPrimitives>>,
    DogeosEthApiBuilder: EthApiBuilder<N>,
{
    /// Create a new instance of [`ScrollRollupNodeAddOns`].
    pub fn new(
        config: ScrollRollupNodeConfig,
        scroll_wire_event: UnboundedReceiver<ScrollWireEvent>,
        eth_wire_event: Receiver<EthWireBlockWithPeer>,
        rollup_manager_handle: Arc<OnceLock<RollupManagerHandle>>,
    ) -> Self {
        let rpc_add_ons = RpcAddOns::new(
            DogeosEthApiBuilder::without_scroll_wire(),
            DogeosEngineValidatorBuilder::default(),
            BasicEngineApiBuilder::default(),
            BasicEngineValidatorBuilder::default(),
            Identity::new(),
        );
        let rollup_manager_addon =
            RollupManagerAddOn::new(config, scroll_wire_event, eth_wire_event);
        Self { rpc_add_ons, rollup_manager_addon, rollup_manager_handle }
    }
}

impl<N, RpcMiddleware> ScrollRollupNodeAddOns<N, RpcMiddleware>
where
    N: FullNodeComponents<Network = NetworkHandle<DogeosNetworkPrimitives>>,
    DogeosEthApiBuilder: EthApiBuilder<N>,
{
    /// Sets the provided middleware for the rollup node addons.
    pub fn with_middleware<T>(self, middleware: T) -> ScrollRollupNodeAddOns<N, T> {
        let rpc_add_ons = self.rpc_add_ons.with_rpc_middleware(middleware);
        ScrollRollupNodeAddOns {
            rpc_add_ons,
            rollup_manager_addon: self.rollup_manager_addon,
            rollup_manager_handle: self.rollup_manager_handle,
        }
    }
}

impl<N, RpcMiddleware> NodeAddOns<N> for ScrollRollupNodeAddOns<N, RpcMiddleware>
where
    N: FullNodeComponents<
        Types: NodeTypes<
            ChainSpec = DogeosChainSpec,
            Primitives = DogeosPrimitives,
            Storage = DogeosStorage,
            Payload = DogeosEngineTypes,
        >,
        Evm = ScrollEvmConfig,
        Network = NetworkHandle<DogeosNetworkPrimitives>,
    >,
    N::Provider: dogeos_reth_rpc::MultiProofProvider,
    EthApiError: FromEvmError<N::Evm>,
    EvmFactoryFor<N::Evm>: EvmFactory<Tx = ScrollTransactionIntoTxEnv<TxEnv>, BlockEnv = BlockEnv>,
    RpcMiddleware: RethRpcMiddleware,
{
    type Handle = RpcHandle<N, <DogeosEthApiBuilder as EthApiBuilder<N>>::EthApi>;

    async fn launch_add_ons(self, ctx: AddOnsContext<'_, N>) -> eyre::Result<Self::Handle> {
        let Self {
            mut rpc_add_ons,
            rollup_manager_addon: rollup_node_manager_addon,
            rollup_manager_handle: shared_rollup_manager_handle,
        } = self;
        let (tx, rx) = tokio::sync::oneshot::channel();
        let rpc_config = rollup_node_manager_addon.config().rpc_args.clone();
        let min_suggested_priority_fee =
            rollup_node_manager_addon.config().gas_price_oracle_args.default_suggested_priority_fee;
        let sequencer_url = rollup_node_manager_addon.config().network_args.sequencer_url.clone();
        let remote_block_source_config =
            rollup_node_manager_addon.config().remote_block_source_args.clone();

        // Register rollupNode API and rollupNodeAdmin API if enabled
        let rollup_node_rpc_ext = Arc::new(RollupNodeRpcExt::<N::Network>::new(rx));

        rpc_add_ons = rpc_add_ons.extend_rpc_modules(move |ctx| {
            if rpc_config.experimental_multiproof {
                // Build once: all configured transports share the adapter's admission state.
                let multiproof_api = dogeos_reth_rpc::DogeosMultiProofApi::new(
                    ctx.registry.eth_api().clone(),
                    dogeos_reth_rpc::MultiProofLimits::default(),
                );
                register_multiproof_module(ctx.modules, multiproof_api.into_rpc()?)?;
            }

            let priority_fee_api = dogeos_reth_rpc::DogeosPriorityFeeApi::new(
                ctx.registry.eth_api().clone(),
                ctx.registry.eth_api().gas_oracle().config().max_price,
                min_suggested_priority_fee,
                constants::DEFAULT_PAYLOAD_SIZE_LIMIT,
            );
            ctx.modules.add_or_replace_if_module_configured(
                RethRpcModule::Eth,
                priority_fee_api.into_rpc()?,
            )?;

            let forwarder_url = sequencer_url
                .as_deref()
                .map(reqwest::Url::parse)
                .transpose()?
                .or_else(|| ctx.config().rpc.rpc_forwarder.clone());
            if let Some(url) = forwarder_url {
                let sequencer = dogeos_reth_rpc::SequencerClient::with_http_client(
                    url.as_str(),
                    reqwest_13::Client::new(),
                )?;
                let forwarder = dogeos_reth_rpc::DogeosRawTransactionForwarder::new(
                    ctx.registry.eth_api().clone(),
                    sequencer,
                    ctx.registry.tasks().clone(),
                    !ctx.config().txpool.no_local_transactions_propagation,
                );
                ctx.modules.add_or_replace_if_module_configured(
                    RethRpcModule::Eth,
                    forwarder.into_rpc()?,
                )?;
            }

            let witness_api = dogeos_reth_rpc::DogeosDebugWitnessApi::new(ctx.registry.debug_api());
            ctx.modules.add_or_replace_if_module_configured(
                RethRpcModule::Debug,
                witness_api.into_rpc()?,
            )?;

            // Always register rollupNode API (read-only operations)
            if rpc_config.basic_enabled {
                ctx.modules
                    .merge_configured(RollupNodeApiServer::into_rpc(rollup_node_rpc_ext.clone()))?;
            }
            // Only register rollupNodeAdmin API if enabled (administrative operations)
            if rpc_config.admin_enabled {
                ctx.modules
                    .merge_configured(RollupNodeAdminApiServer::into_rpc(rollup_node_rpc_ext))?;
            }
            Ok(())
        });

        let rpc_handle = rpc_add_ons.launch_add_ons_with(ctx.clone(), |_| Ok(())).await?;
        let rollup_manager_handle =
            rollup_node_manager_addon.launch(ctx.clone(), rpc_handle.clone()).await?;
        shared_rollup_manager_handle
            .set(rollup_manager_handle.clone())
            .map_err(|_| eyre::eyre!("rollup manager handle was already initialized"))?;

        // Only send handle if RPC is enabled
        if rpc_config.basic_enabled || rpc_config.admin_enabled {
            tx.send(rollup_manager_handle.clone())
                .map_err(|_| eyre::eyre!("failed to send rollup manager handle"))?;
        }

        // Launch remote block source if enabled
        if remote_block_source_config.enabled {
            let remote_source = RemoteBlockSourceAddOn::new(
                remote_block_source_config,
                rollup_manager_handle.clone(),
                rpc_handle.provider().clone(),
            )
            .await?;
            ctx.node
                .task_executor()
                .spawn_critical_with_graceful_shutdown_signal("remote_block_source", |shutdown| async move {
                    if let Err(e) = remote_source.run_until_shutdown(shutdown.ignore_guard()).await {
                        tracing::error!(target: "scroll::remote_source", ?e, "Remote block source failed");
                    }
                });
        }

        Ok(rpc_handle)
    }
}

impl<N, RpcMiddleware> RethRpcAddOns<N> for ScrollRollupNodeAddOns<N, RpcMiddleware>
where
    N: FullNodeComponents<
        Types: NodeTypes<
            ChainSpec = DogeosChainSpec,
            Primitives = DogeosPrimitives,
            Storage = DogeosStorage,
            Payload = DogeosEngineTypes,
        >,
        Evm = ScrollEvmConfig,
        Network = NetworkHandle<DogeosNetworkPrimitives>,
    >,
    N::Provider: dogeos_reth_rpc::MultiProofProvider,
    EthApiError: FromEvmError<N::Evm>,
    EvmFactoryFor<N::Evm>: EvmFactory<Tx = ScrollTransactionIntoTxEnv<TxEnv>, BlockEnv = BlockEnv>,
    RpcMiddleware: RethRpcMiddleware,
{
    type EthApi = <DogeosEthApiBuilder as EthApiBuilder<N>>::EthApi;

    fn hooks_mut(&mut self) -> &mut reth_node_builder::rpc::RpcHooks<N, Self::EthApi> {
        self.rpc_add_ons.hooks_mut()
    }
}

impl<N> EngineValidatorAddOn<N> for ScrollRollupNodeAddOns<N>
where
    N: FullNodeComponents<
        Types: NodeTypes<
            ChainSpec = DogeosChainSpec,
            Primitives = DogeosPrimitives,
            Payload = DogeosEngineTypes,
        >,
        Evm = ScrollEvmConfig,
        Network = NetworkHandle<DogeosNetworkPrimitives>,
    >,
    DogeosEthApiBuilder: EthApiBuilder<N>,
{
    type ValidatorBuilder = BasicEngineValidatorBuilder<DogeosEngineValidatorBuilder>;

    fn engine_validator_builder(&self) -> Self::ValidatorBuilder {
        EngineValidatorAddOn::engine_validator_builder(&self.rpc_add_ons)
    }
}

/// Keep the experimental transport boundary separate from the proof handler.
fn register_multiproof_module<Context>(
    modules: &mut reth_rpc_builder::TransportRpcModules,
    module: jsonrpsee::RpcModule<Context>,
) -> Result<(), jsonrpsee::core::RegisterMethodError> {
    modules.merge_if_module_configured(RethRpcModule::Eth, module)
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonrpsee::RpcModule;
    use reth_rpc_builder::{TransportRpcModuleConfig, TransportRpcModules};

    fn proof_module() -> RpcModule<()> {
        let mut module = RpcModule::new(());
        module.register_method("dogeos_getProofs", |_, _, _| true).unwrap();
        module
    }

    #[test]
    fn multiproof_registration_respects_each_transport_and_rejects_collision() {
        for eth_transport in 0..3 {
            let selected = |transport| {
                if eth_transport == transport {
                    RethRpcModule::Eth
                } else {
                    RethRpcModule::Net
                }
            };
            let mut modules = TransportRpcModules::default()
                .with_config(
                    TransportRpcModuleConfig::default()
                        .with_http([selected(0)])
                        .with_ws([selected(1)])
                        .with_ipc([selected(2)]),
                )
                .with_http(RpcModule::new(()))
                .with_ws(RpcModule::new(()))
                .with_ipc(RpcModule::new(()));
            register_multiproof_module(&mut modules, proof_module()).unwrap();
            for (transport, methods) in [
                modules.http_methods(|_| true),
                modules.ws_methods(|_| true),
                modules.ipc_methods(|_| true),
            ]
            .into_iter()
            .enumerate()
            {
                assert_eq!(
                    methods.unwrap().method_names().any(|name| name == "dogeos_getProofs"),
                    eth_transport == transport,
                );
            }
            assert!(register_multiproof_module(&mut modules, proof_module()).is_err());
        }
    }
}
