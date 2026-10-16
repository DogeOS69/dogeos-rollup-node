//! Node specific implementations for Scroll rollup node.

use crate::{args::ScrollRollupNodeConfig, builder::network::ScrollNetworkBuilder, constants};
use std::time::Duration;

use super::add_ons::{RollupManagerHandle, ScrollRollupNodeAddOns};
use dogeos_chainspec::DogeosChainSpec;
use dogeos_reth_node::{
    DogeosConsensusBuilder, DogeosExecutorBuilder, DogeosNodeTypes, DogeosPayloadBuilderBuilder,
    DogeosPoolBuilder, DogeosStorage,
};
use reth_network::protocol::IntoRlpxSubProtocol;
use reth_node_api::NodeTypes;
use reth_node_builder::{
    components::{BasicPayloadServiceBuilder, ComponentsBuilder},
    FullNodeTypes, Node, NodeAdapter, NodeComponentsBuilder, NodeConfig,
};
use scroll_network::{EthWireBlockWithPeer, ETH_WIRE_BLOCK_CHANNEL_SIZE};
use scroll_wire::{ScrollWireConfig, ScrollWireEvent, ScrollWireProtocolHandler};
use std::sync::{Arc, OnceLock};
use tokio::sync::{
    mpsc::{Receiver, UnboundedReceiver},
    Mutex,
};

/// The Scroll node implementation.
#[derive(Clone, Debug)]
pub struct ScrollRollupNode {
    config: ScrollRollupNodeConfig,
    scroll_wire_events: Arc<Mutex<Option<UnboundedReceiver<ScrollWireEvent>>>>,
    eth_wire_events: Arc<Mutex<Option<Receiver<EthWireBlockWithPeer>>>>,
    rollup_manager_handle: Arc<OnceLock<RollupManagerHandle>>,
}

impl ScrollRollupNode {
    /// Create a new instance of [`ScrollRollupNode`].
    pub async fn new(
        mut config: ScrollRollupNodeConfig,
        node_config: NodeConfig<DogeosChainSpec>,
    ) -> Self {
        config
            .validate()
            .map_err(|e| eyre::eyre!("Configuration validation failed: {}", e))
            .expect("Configuration validation failed");
        config
            .hydrate(node_config)
            .await
            .map_err(|e| eyre::eyre!("Configuration hydration failed: {}", e))
            .expect("Configuration hydration failed");

        Self {
            config,
            scroll_wire_events: Arc::new(Mutex::new(None)),
            eth_wire_events: Arc::new(Mutex::new(None)),
            rollup_manager_handle: Arc::new(OnceLock::new()),
        }
    }

    /// Returns the rollup manager handle after the node add-ons have launched.
    #[cfg(feature = "test-utils")]
    pub(crate) fn rollup_manager_handle(&self) -> Option<&RollupManagerHandle> {
        self.rollup_manager_handle.get()
    }
}

impl<N> Node<N> for ScrollRollupNode
where
    N: FullNodeTypes<Types = Self>,
    N::Provider: dogeos_reth_rpc::MultiProofProvider,
{
    type ComponentsBuilder = ComponentsBuilder<
        N,
        DogeosPoolBuilder,
        BasicPayloadServiceBuilder<DogeosPayloadBuilderBuilder>,
        ScrollNetworkBuilder,
        DogeosExecutorBuilder,
        DogeosConsensusBuilder,
    >;

    type AddOns = ScrollRollupNodeAddOns<
        NodeAdapter<N, <Self::ComponentsBuilder as NodeComponentsBuilder<N>>::Components>,
    >;

    fn components_builder(&self) -> Self::ComponentsBuilder {
        let (scroll_wire_handler, events) =
            ScrollWireProtocolHandler::new(ScrollWireConfig::new(true));

        *self.scroll_wire_events.try_lock().unwrap() = Some(events);
        let (eth_wire_block_tx, eth_wire_events) =
            tokio::sync::mpsc::channel(ETH_WIRE_BLOCK_CHANNEL_SIZE);
        *self.eth_wire_events.try_lock().unwrap() = Some(eth_wire_events);

        let mut network_builder = ScrollNetworkBuilder::new(eth_wire_block_tx)
            .with_signer(self.config.network_args.signer_address)
            .with_legacy_geth_header_transform(
                self.config.network_args.legacy_geth_header_transform,
            );

        // Only add scroll-wire sub-protocol if enabled
        if self.config.network_args.enable_scroll_wire {
            network_builder =
                network_builder.with_sub_protocol(scroll_wire_handler.into_rlpx_sub_protocol());
        }

        ComponentsBuilder::default()
            .node_types::<N>()
            .pool(DogeosPoolBuilder::default())
            .executor(DogeosExecutorBuilder::default())
            .payload(BasicPayloadServiceBuilder::new(DogeosPayloadBuilderBuilder {
                payload_building_time_limit: Duration::from_millis(
                    self.config.sequencer_args.payload_building_duration,
                ),
                block_da_size_limit: Some(constants::DEFAULT_PAYLOAD_SIZE_LIMIT),
            }))
            .network(network_builder)
            .consensus(DogeosConsensusBuilder)
    }

    fn add_ons(&self) -> Self::AddOns {
        ScrollRollupNodeAddOns::new(
            self.config.clone(),
            self.scroll_wire_events.try_lock().unwrap().take().unwrap(),
            self.eth_wire_events.try_lock().unwrap().take().unwrap(),
            self.rollup_manager_handle.clone(),
        )
    }
}

impl NodeTypes for ScrollRollupNode {
    type Primitives = <DogeosNodeTypes as NodeTypes>::Primitives;
    type ChainSpec = DogeosChainSpec;
    type Storage = DogeosStorage;
    type Payload = <DogeosNodeTypes as NodeTypes>::Payload;
}
