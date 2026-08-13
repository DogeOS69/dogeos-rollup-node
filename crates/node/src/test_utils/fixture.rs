//! Core test fixture for setting up and managing test nodes.

use super::{
    block_builder::BlockBuilder, l1_helpers::L1Helper, setup_engine, tx_helpers::TxHelper,
};
use crate::{
    constants, BlobProviderArgs, ChainOrchestratorArgs, ConsensusAlgorithm, ConsensusArgs,
    DogeosChainSpecParser, EngineDriverArgs, L1ProviderArgs, PprofArgs, RollupNodeDatabaseArgs,
    RollupNodeGasPriceOracleArgs, RollupNodeNetworkArgs, RpcArgs, ScrollRollupNode,
    ScrollRollupNodeConfig, SequencerArgs, SignerArgs, TestArgs,
};

use alloy_eips::BlockNumberOrTag;
use alloy_network::Ethereum;
use alloy_node_bindings::{Anvil, AnvilInstance};
use alloy_primitives::Address;
use alloy_provider::{ext::AnvilApi, layers::CacheLayer, Provider, ProviderBuilder};
use alloy_rpc_client::RpcClient;
use alloy_rpc_types_anvil::ReorgOptions;
use alloy_rpc_types_eth::Block;
use alloy_signer_local::PrivateKeySigner;
use alloy_transport::layers::RetryBackoffLayer;
use dogeos_chainspec::{DogeosChainSpec, DOGEOS_DEV};
use dogeos_protocol_types::ScrollPooledTransaction;
use dogeos_reth_primitives::DogeosPrimitives;
use dogeos_rpc_types::ScrollRpcTransaction;
use reth_cli::chainspec::ChainSpecParser;
use reth_e2e_test_utils::{wallet::Wallet, NodeHelperType, TmpDB};
use reth_eth_wire_types::BasicNetworkPrimitives;
use reth_fs_util::remove_dir_all;
use reth_network::NetworkHandle;
use reth_network_peers::TrustedPeer;
use reth_node_builder::NodeTypes;
use reth_node_core::exit::NodeExitFuture;
use reth_node_types::NodeTypesWithDBAdapter;
use reth_provider::providers::BlockchainProvider;
use reth_tasks::TaskExecutor;
use reth_tokio_util::EventStream;
use rollup_node_chain_orchestrator::{ChainOrchestratorEvent, ChainOrchestratorHandle};
use rollup_node_sequencer::L1MessageInclusionMode;
use std::{
    ffi::{OsStr, OsString},
    fmt::{Debug, Formatter},
    ops::{Deref, DerefMut},
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
};
use tokio::sync::Mutex;

/// L1 provider type for making L1 RPC calls.
pub type L1Provider = Box<dyn Provider<Ethereum> + Send + Sync>;

/// Main test fixture providing a high-level interface for testing rollup nodes.
pub struct TestFixture {
    /// The list of nodes in the test setup.
    /// Using Option to allow nodes to be shutdown without changing indices.
    /// A None value means the node at that index has been shutdown.
    pub nodes: Vec<Option<NodeHandle>>,
    /// Database references for each node, used for reboot scenarios.
    pub dbs: Vec<Arc<reth_db::test_utils::TempDatabase<reth_db::DatabaseEnv>>>,
    /// Shared wallet for generating transactions.
    pub wallet: Arc<Mutex<Wallet>>,
    /// Chain spec used by the nodes.
    pub chain_spec: Arc<<ScrollRollupNode as NodeTypes>::ChainSpec>,
    /// L1 provider for making L1 RPC calls (if connected to real L1).
    pub l1_provider: Option<L1Provider>,
    /// Optional Anvil instance for L1 simulation.
    ///
    /// Owns the external `anvil` child process; dropping the fixture terminates it.
    pub anvil: Option<AnvilInstance>,
    /// The configuration for the nodes.
    pub config: ScrollRollupNodeConfig,
    /// Whether this fixture has a remote source node (always the last node).
    pub has_remote_source_node: bool,
}

impl Debug for TestFixture {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TestFixture")
            .field("nodes", &self.nodes)
            .field("wallet", &"<Mutex<Wallet>>")
            .field("chain_spec", &self.chain_spec)
            .field("anvil", &self.anvil.is_some())
            .field("config", &self.config)
            .field("has_remote_source_node", &self.has_remote_source_node)
            .field("_tasks", &"<TaskManager>")
            .finish()
    }
}

impl Drop for TestFixture {
    fn drop(&mut self) {
        // Manually cleanup test directories.
        // TempDatabase's automatic drop only removes the database file itself,
        // but the parent directory also contains other files (static files, blob store, etc.)
        // that need to be cleaned up to avoid accumulating test artifacts.
        let parent_paths: Vec<_> =
            self.dbs.iter().filter_map(|db| db.path().parent().map(|p| p.to_path_buf())).collect();
        // Delete parent directories containing all database files
        for path in parent_paths {
            let _ = remove_dir_all(&path);
        }
    }
}

/// The network handle to the Scroll network.
pub type ScrollNetworkHandle =
    NetworkHandle<BasicNetworkPrimitives<DogeosPrimitives, ScrollPooledTransaction>>;

/// The blockchain test provider.
pub type TestBlockChainProvider =
    BlockchainProvider<NodeTypesWithDBAdapter<ScrollRollupNode, TmpDB>>;

/// The test node type for Scroll nodes.
pub type ScrollTestNode = NodeHelperType<ScrollRollupNode, TestBlockChainProvider>;

/// The node type (sequencer, follower, or remote source).
#[derive(Debug)]
pub enum NodeType {
    /// A sequencer node.
    Sequencer,
    /// A follower node.
    Follower,
    /// A remote source node that imports blocks from a remote L2 and builds on top.
    RemoteSource,
}

/// Components of a test node.
pub struct ScrollNodeTestComponents {
    /// The node helper type for the test node.
    pub node: ScrollTestNode,
    /// The task executor for the test node.
    pub task_executor: TaskExecutor,
    /// The exit future for the test node.
    pub exit_future: NodeExitFuture,
    /// Handle to the rollup manager launched alongside the Reth node.
    pub rollup_manager_handle: ChainOrchestratorHandle<ScrollNetworkHandle>,
}

impl ScrollNodeTestComponents {
    /// Create new test node components.
    pub async fn new(
        node: ScrollTestNode,
        task_executor: TaskExecutor,
        exit_future: NodeExitFuture,
        rollup_manager_handle: ChainOrchestratorHandle<ScrollNetworkHandle>,
    ) -> Self {
        Self { node, task_executor, exit_future, rollup_manager_handle }
    }
}

impl std::fmt::Debug for ScrollNodeTestComponents {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScrollNodeTestComponents").finish()
    }
}

impl DerefMut for ScrollNodeTestComponents {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.node
    }
}

impl Deref for ScrollNodeTestComponents {
    type Target = ScrollTestNode;

    fn deref(&self) -> &Self::Target {
        &self.node
    }
}

/// Handle to a single test node with its components.
pub struct NodeHandle {
    /// The underlying node context.
    pub node: ScrollNodeTestComponents,
    /// Chain orchestrator listener.
    pub chain_orchestrator_rx: EventStream<ChainOrchestratorEvent>,
    /// Chain orchestrator handle.
    pub rollup_manager_handle: ChainOrchestratorHandle<ScrollNetworkHandle>,
    /// The type of the node.
    pub typ: NodeType,
}

impl NodeHandle {
    /// Create a new node handle.
    pub async fn new(node: ScrollNodeTestComponents, typ: NodeType) -> eyre::Result<Self> {
        // Block production drives the node through the rollup-manager handle, so the
        // handle only needs to observe chain-orchestrator events.
        let rollup_manager_handle = node.rollup_manager_handle.clone();
        let chain_orchestrator_rx = rollup_manager_handle.get_event_listener().await?;

        Ok(Self { node, chain_orchestrator_rx, rollup_manager_handle, typ })
    }

    /// Returns true if this is a handle to the sequencer.
    pub const fn is_sequencer(&self) -> bool {
        matches!(self.typ, NodeType::Sequencer)
    }

    /// Returns true if this is a handle to a follower.
    pub const fn is_follower(&self) -> bool {
        matches!(self.typ, NodeType::Follower)
    }

    /// Returns true if this is a handle to a remote source node.
    pub const fn is_remote_source(&self) -> bool {
        matches!(self.typ, NodeType::RemoteSource)
    }
}

impl Debug for NodeHandle {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeHandle")
            .field("node", &"NodeHelper")
            .field("rollup_manager_handle", &self.rollup_manager_handle)
            .finish()
    }
}

impl TestFixture {
    /// Create a new test fixture builder with custom configuration.
    pub fn builder() -> TestFixtureBuilder {
        TestFixtureBuilder::new()
    }

    /// Get a node by index, returning an error if it has been shutdown.
    fn get_node(&self, node_index: usize) -> eyre::Result<&NodeHandle> {
        self.nodes
            .get(node_index)
            .ok_or_else(|| eyre::eyre!("Node index {} out of bounds", node_index))?
            .as_ref()
            .ok_or_else(|| eyre::eyre!("Node at index {} has been shutdown", node_index))
    }

    /// Get the sequencer node (assumes first node is sequencer).
    pub fn sequencer(&mut self) -> &mut NodeHandle {
        let handle = self.nodes[0].as_mut().expect("sequencer node has been shutdown");
        assert!(handle.is_sequencer(), "expected sequencer, got follower");
        handle
    }

    /// Get a follower node by index.
    pub fn follower(&mut self, index: usize) -> &mut NodeHandle {
        if index == 0 && self.nodes[0].as_ref().map(|n| n.is_sequencer()).unwrap_or(false) {
            return self.nodes[index + 1].as_mut().expect("follower node has been shutdown");
        }
        self.nodes[index].as_mut().expect("follower node has been shutdown")
    }

    /// Get the remote source node.
    pub fn remote_source(&mut self) -> &mut NodeHandle {
        self.nodes
            .iter_mut()
            .find_map(|n| n.as_mut().filter(|node| matches!(node.typ, NodeType::RemoteSource)))
            .expect("remote source node not found")
    }

    /// Get the wallet.
    pub fn wallet(&self) -> Arc<Mutex<Wallet>> {
        self.wallet.clone()
    }

    /// Start building a block using the sequencer.
    pub const fn build_block(&mut self) -> BlockBuilder<'_> {
        BlockBuilder::new(self)
    }

    /// Get L1 helper for managing L1 interactions.
    pub const fn l1(&mut self) -> L1Helper<'_> {
        L1Helper::new(self)
    }

    /// Get transaction helper for creating and injecting transactions.
    pub const fn tx(&mut self) -> TxHelper<'_> {
        TxHelper::new(self)
    }

    /// Inject a simple transfer transaction and return its hash.
    pub async fn inject_transfer(&mut self) -> eyre::Result<alloy_primitives::B256> {
        self.tx().transfer().inject().await
    }

    /// Inject a raw transaction into a specific node's pool.
    pub async fn inject_tx_on(
        &mut self,
        node_index: usize,
        tx: impl Into<alloy_primitives::Bytes>,
    ) -> eyre::Result<alloy_primitives::B256> {
        let node = self.nodes[node_index]
            .as_ref()
            .ok_or_else(|| eyre::eyre!("Node at index {} has been shutdown", node_index))?;
        Ok(node.node.rpc.inject_tx(tx.into()).await?)
    }

    /// Get the current (latest) block from a specific node.
    pub async fn get_block(&self, node_index: usize) -> eyre::Result<Block<ScrollRpcTransaction>> {
        use reth_rpc_api::EthApiServer;

        let node = self.nodes[node_index]
            .as_ref()
            .ok_or_else(|| eyre::eyre!("Node at index {} has been shutdown", node_index))?;
        node.node
            .rpc
            .inner
            .eth_api()
            .block_by_number(BlockNumberOrTag::Latest, false)
            .await?
            .ok_or_else(|| eyre::eyre!("Latest block not found"))
    }

    /// Get the current (latest) block from the sequencer node.
    pub async fn get_sequencer_block(&self) -> eyre::Result<Block<ScrollRpcTransaction>> {
        self.get_block(0).await
    }

    /// Get the status (including forkchoice state) from a specific node.
    pub async fn get_status(
        &self,
        node_index: usize,
    ) -> eyre::Result<rollup_node_chain_orchestrator::ChainOrchestratorStatus> {
        let node = self.get_node(node_index)?;
        node.rollup_manager_handle
            .status()
            .await
            .map_err(|e| eyre::eyre!("Failed to get status: {}", e))
    }

    /// Get the status (including forkchoice state) from the sequencer node.
    pub async fn get_sequencer_status(
        &self,
    ) -> eyre::Result<rollup_node_chain_orchestrator::ChainOrchestratorStatus> {
        self.get_status(0).await
    }

    /// Get the Anvil HTTP provider with retry and cache layers.
    pub fn anvil_provider(&self) -> Option<impl Provider + Clone> {
        self.anvil.as_ref().map(|anvil| {
            let retry_layer = RetryBackoffLayer::new(
                constants::L1_PROVIDER_MAX_RETRIES,
                constants::L1_PROVIDER_INITIAL_BACKOFF,
                constants::PROVIDER_COMPUTE_UNITS_PER_SECOND,
            );
            let client = RpcClient::builder()
                .layer(retry_layer)
                .http(anvil.endpoint().parse().expect("failed to parse anvil http endpoint"));
            let cache_layer = CacheLayer::new(constants::L1_PROVIDER_CACHE_MAX_ITEMS);
            ProviderBuilder::new().layer(cache_layer).connect_client(client)
        })
    }

    /// Get the current block number from Anvil.
    pub async fn anvil_get_block_number(&self) -> eyre::Result<u64> {
        let provider = self.anvil_provider().ok_or_else(|| eyre::eyre!("Anvil is not running"))?;
        let block_number = provider.get_block_number().await?;
        Ok(block_number)
    }

    /// Get the finalized block number from Anvil.
    pub async fn anvil_get_finalized_block_number(&self) -> eyre::Result<u64> {
        let provider = self.anvil_provider().ok_or_else(|| eyre::eyre!("Anvil is not running"))?;
        let finalized_block = provider.get_block(BlockNumberOrTag::Finalized.into()).await?;
        Ok(finalized_block.map(|block| block.number()).unwrap_or(0u64))
    }

    /// Generate Anvil blocks by calling `anvil_mine` RPC method.
    pub async fn anvil_mine_blocks(&self, num_blocks: u64) -> eyre::Result<()> {
        let provider = self.anvil_provider().ok_or_else(|| eyre::eyre!("Anvil is not running"))?;
        Ok(provider.anvil_mine(Some(num_blocks), None).await?)
    }

    /// Inject a raw transaction to Anvil.
    pub async fn anvil_inject_tx(
        &self,
        raw_tx: impl Into<alloy_primitives::Bytes>,
    ) -> eyre::Result<alloy_primitives::B256> {
        let provider = self.anvil_provider().ok_or_else(|| eyre::eyre!("Anvil is not running"))?;
        let raw_tx_bytes = raw_tx.into();
        let pending_tx = provider.send_raw_transaction(&raw_tx_bytes).await?;
        let tx_hash = *pending_tx.tx_hash();
        tracing::info!("Sent raw transaction to Anvil: {:?}", tx_hash);
        Ok(tx_hash)
    }

    /// Reorg Anvil by a specific depth (number of blocks to rewind).
    pub async fn anvil_reorg(&self, depth: u64) -> eyre::Result<()> {
        let provider = self.anvil_provider().ok_or_else(|| eyre::eyre!("Anvil is not running"))?;
        provider.anvil_reorg(ReorgOptions { depth, tx_block_pairs: Vec::new() }).await?;
        tracing::info!("Reorged Anvil by {} blocks", depth);
        Ok(())
    }
}

/// Configuration for Anvil L1 simulation.
#[derive(Debug, Default, Clone)]
pub struct AnvilConfig {
    /// Whether to enable Anvil.
    pub enabled: bool,
    /// Optional port for Anvil.
    pub port: u16,
    /// Optional state file to load into Anvil.
    pub state_path: Option<PathBuf>,
    /// Optional chain ID for Anvil.
    pub chain_id: Option<u64>,
    /// Optional block time for Anvil (in seconds).
    pub block_time: Option<u64>,
    /// Optional slots in an epoch for Anvil.
    pub slots_in_an_epoch: u64,
}

/// Builder for creating test fixtures with a fluent API.
pub struct TestFixtureBuilder {
    config: ScrollRollupNodeConfig,
    num_nodes: usize,
    has_remote_source_node: bool,
    chain_spec: Option<Arc<<ScrollRollupNode as NodeTypes>::ChainSpec>>,
    is_dev: bool,
    no_local_transactions_propagation: bool,
    bootnodes: Option<Vec<TrustedPeer>>,
    l1_provider: Option<L1Provider>,
    anvil_config: AnvilConfig,
}

impl std::fmt::Debug for TestFixtureBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TestFixtureBuilder")
            .field("config", &self.config)
            .field("num_nodes", &self.num_nodes)
            .field("chain_spec", &self.chain_spec)
            .field("is_dev", &self.is_dev)
            .field("no_local_transactions_propagation", &self.no_local_transactions_propagation)
            .field("bootnodes", &self.bootnodes)
            .field("l1_provider", &self.l1_provider.as_ref().map(|_| "L1Provider"))
            .finish()
    }
}

impl Default for TestFixtureBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl TestFixtureBuilder {
    /// Create a new test fixture builder.
    pub fn new() -> Self {
        Self {
            config: Self::default_config(),
            num_nodes: 0,
            has_remote_source_node: false,
            chain_spec: None,
            is_dev: false,
            no_local_transactions_propagation: false,
            bootnodes: None,
            l1_provider: None,
            anvil_config: AnvilConfig::default(),
        }
    }

    /// Returns the default rollup node config.
    fn default_config() -> ScrollRollupNodeConfig {
        ScrollRollupNodeConfig {
            test_args: TestArgs { test: true, skip_l1_synced: false },
            network_args: RollupNodeNetworkArgs::default(),
            database_args: RollupNodeDatabaseArgs::default(),
            l1_provider_args: L1ProviderArgs::default(),
            engine_driver_args: EngineDriverArgs { sync_at_startup: true },
            chain_orchestrator_args: ChainOrchestratorArgs {
                optimistic_sync_trigger: 100,
                chain_buffer_size: 100,
                ..Default::default()
            },
            sequencer_args: SequencerArgs {
                payload_building_duration: 1000,
                allow_empty_blocks: true,
                ..Default::default()
            },
            blob_provider_args: BlobProviderArgs { mock: true, ..Default::default() },
            signer_args: SignerArgs::default(),
            gas_price_oracle_args: RollupNodeGasPriceOracleArgs::default(),
            consensus_args: ConsensusArgs::noop(),
            database: None,
            rpc_args: RpcArgs { basic_enabled: true, admin_enabled: true },
            remote_block_source_args: Default::default(),
            pprof_args: PprofArgs::default(),
            require_l1_data_fee_buffer: false,
        }
    }

    /// Adds a sequencer node to the test with default settings.
    pub const fn sequencer(mut self) -> Self {
        self.config.sequencer_args.sequencer_enabled = true;
        self.config.sequencer_args.auto_start = false;
        self.config.sequencer_args.block_time = 100;
        self.config.sequencer_args.payload_building_duration = 40;
        self.config.sequencer_args.l1_message_inclusion_mode =
            L1MessageInclusionMode::BlockDepth(0);
        self.config.sequencer_args.allow_empty_blocks = true;
        self.num_nodes += 1;
        self
    }

    /// Sets the bootnodes for the test nodes.
    pub fn bootnodes(mut self, bootnodes: Vec<TrustedPeer>) -> Self {
        self.bootnodes = Some(bootnodes);
        self
    }

    /// Adds `count`s follower nodes to the test.
    pub const fn followers(mut self, count: usize) -> Self {
        self.num_nodes += count;
        self
    }

    /// Adds a remote source node that follows the sequencer via `RemoteBlockSourceAddOn`.
    /// Must be used together with `.sequencer()`.
    pub const fn remote_source_node(mut self) -> Self {
        self.has_remote_source_node = true;
        self
    }

    /// Toggle the test field.
    pub const fn with_test(mut self, test: bool) -> Self {
        self.config.test_args.test = test;
        self
    }

    /// Enable test mode to skip L1 watcher Synced notifications.
    /// This is useful for tests that don't want to wait for L1 sync completion events.
    pub const fn skip_l1_synced_notifications(mut self) -> Self {
        self.config.test_args.skip_l1_synced = true;
        self
    }

    /// Set the sequencer url for the node.
    pub fn with_sequencer_url(mut self, url: String) -> Self {
        self.config.network_args.sequencer_url = Some(url);
        self
    }

    /// Set the sequencer auto start for the node.
    pub const fn with_sequencer_auto_start(mut self, auto_start: bool) -> Self {
        self.config.sequencer_args.auto_start = auto_start;
        self
    }

    /// Set a custom chain spec.
    pub fn with_chain_spec(
        mut self,
        spec: Arc<<ScrollRollupNode as NodeTypes>::ChainSpec>,
    ) -> Self {
        self.chain_spec = Some(spec);
        self
    }

    /// Set the chain by name ("dev", "sepolia", "mainnet") or by file path.
    ///
    /// This is a convenience method that loads the appropriate chain spec.
    /// If the input is a file path (contains '/' or ends with '.json'), it will
    /// load the genesis from the file.
    pub fn with_chain(mut self, chain: &str) -> eyre::Result<Self> {
        let chain_spec: Arc<DogeosChainSpec> = DogeosChainSpecParser::parse(chain)?;
        self.chain_spec = Some(chain_spec);
        Ok(self)
    }

    /// Enable dev mode.
    pub const fn with_dev_mode(mut self, enabled: bool) -> Self {
        self.is_dev = enabled;
        self
    }

    /// Disable local transaction propagation.
    pub const fn no_local_tx_propagation(mut self) -> Self {
        self.no_local_transactions_propagation = true;
        self
    }

    /// Set the block time for the sequencer.
    pub const fn block_time(mut self, millis: u64) -> Self {
        self.config.sequencer_args.block_time = millis;
        self
    }

    /// Set whether to allow empty blocks.
    pub const fn allow_empty_blocks(mut self, allow: bool) -> Self {
        self.config.sequencer_args.allow_empty_blocks = allow;
        self
    }

    /// Set L1 message inclusion mode with block depth.
    pub const fn with_l1_message_delay(mut self, depth: u64) -> Self {
        self.config.sequencer_args.l1_message_inclusion_mode =
            L1MessageInclusionMode::BlockDepth(depth);
        self
    }

    /// Set L1 message inclusion mode to finalized with optional block depth.
    pub const fn with_finalized_l1_messages(mut self, depth: u64) -> Self {
        self.config.sequencer_args.l1_message_inclusion_mode =
            L1MessageInclusionMode::FinalizedWithBlockDepth(depth);
        self
    }

    /// Use an in-memory `SQLite` database.
    pub fn with_memory_db(mut self) -> Self {
        self.config.database_args.rn_db_path = Some(PathBuf::from("sqlite::memory:"));
        self
    }

    /// Set a custom database path.
    pub fn with_db_path(mut self, path: PathBuf) -> Self {
        self.config.database_args.rn_db_path = Some(path);
        self
    }

    /// Use noop consensus (no validation).
    pub const fn with_noop_consensus(mut self) -> Self {
        self.config.consensus_args = ConsensusArgs::noop();
        self
    }

    /// Use `SystemContract` consensus with the given authorized signer address.
    pub const fn with_consensus_system_contract(
        mut self,
        authorized_signer: Option<Address>,
    ) -> Self {
        self.config.consensus_args.algorithm = ConsensusAlgorithm::SystemContract;
        self.config.consensus_args.authorized_signer = authorized_signer;
        self
    }

    /// Set the valid signer address for the network.
    pub const fn with_network_valid_signer(mut self, address: Option<Address>) -> Self {
        self.config.network_args.signer_address = address;
        self
    }

    /// Set the private key signer for the node.
    pub fn with_signer(mut self, signer: PrivateKeySigner) -> Self {
        self.config.signer_args.private_key = Some(signer);
        self
    }

    /// Set the payload building duration in milliseconds.
    pub const fn payload_building_duration(mut self, millis: u64) -> Self {
        self.config.sequencer_args.payload_building_duration = millis;
        self
    }

    /// Set the fee recipient address.
    pub const fn fee_recipient(mut self, address: Address) -> Self {
        self.config.sequencer_args.fee_recipient = address;
        self
    }

    /// Enable auto-start for the sequencer.
    pub const fn auto_start(mut self, enabled: bool) -> Self {
        self.config.sequencer_args.auto_start = enabled;
        self
    }

    /// Set the maximum number of L1 messages per block.
    pub const fn max_l1_messages(mut self, max: u64) -> Self {
        self.config.sequencer_args.max_l1_messages = Some(max);
        self
    }

    /// Enable the Scroll wire protocol.
    pub const fn with_scroll_wire(mut self, enabled: bool) -> Self {
        self.config.network_args.enable_scroll_wire = enabled;
        self
    }

    /// Enable the ETH-Scroll wire bridge.
    pub const fn with_eth_scroll_bridge(mut self, enabled: bool) -> Self {
        self.config.network_args.enable_eth_scroll_wire_bridge = enabled;
        self
    }

    /// Set the optimistic sync trigger threshold.
    pub const fn optimistic_sync_trigger(mut self, blocks: u64) -> Self {
        self.config.chain_orchestrator_args.optimistic_sync_trigger = blocks;
        self
    }

    /// Get a mutable reference to the underlying config for advanced customization.
    pub const fn config_mut(&mut self) -> &mut ScrollRollupNodeConfig {
        &mut self.config
    }

    /// Modify the underlying config using a closure.
    pub fn config<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&mut ScrollRollupNodeConfig),
    {
        f(&mut self.config);
        self
    }

    /// Set the L1 provider for making L1 RPC calls.
    pub fn with_l1_provider(mut self, provider: L1Provider) -> Self {
        self.l1_provider = Some(provider);
        self
    }

    /// Enable Anvil L1 with optional configuration.
    ///
    /// Defaults: `state_path` = `./tests/testdata/anvil_state.json`, others use Anvil defaults.
    pub fn with_anvil(
        mut self,
        state_path: Option<PathBuf>,
        chain_id: Option<u64>,
        block_time: Option<u64>,
        slots_in_an_epoch: Option<u64>,
    ) -> Self {
        self.anvil_config.enabled = true;
        self.anvil_config.state_path =
            state_path.or_else(|| Some(PathBuf::from("./tests/testdata/anvil_state.json")));
        self.anvil_config.chain_id = chain_id;
        self.anvil_config.block_time = block_time;
        self.anvil_config.slots_in_an_epoch = slots_in_an_epoch.unwrap_or(1);
        self
    }

    /// Build the test fixture.
    pub async fn build(mut self) -> eyre::Result<TestFixture> {
        let chain_spec = self.chain_spec.unwrap_or_else(|| DOGEOS_DEV.clone());

        // Start Anvil if requested
        let anvil = if self.anvil_config.enabled {
            let handle = Self::spawn_anvil(
                self.anvil_config.state_path.as_deref(),
                self.anvil_config.chain_id,
                self.anvil_config.block_time,
                self.anvil_config.slots_in_an_epoch,
            )?;

            // Parse endpoint URL once and reuse
            let endpoint_url = handle
                .endpoint()
                .parse::<reqwest::Url>()
                .map_err(|e| eyre::eyre!("Failed to parse Anvil endpoint URL: {}", e))?;

            // Configure L1 provider and blob provider to use Anvil
            self.config.l1_provider_args.url = Some(endpoint_url.clone());
            self.config.l1_provider_args.logs_query_block_range = 500;
            self.config.blob_provider_args.anvil_url = Some(endpoint_url);
            self.config.blob_provider_args.mock = false;

            Some(handle)
        } else {
            None
        };

        let (mut nodes, mut dbs, wallet) = setup_engine(
            self.config.clone(),
            self.num_nodes,
            chain_spec.clone(),
            self.is_dev,
            self.no_local_transactions_propagation,
            self.bootnodes,
            None,
        )
        .await?;

        // Launch remote source node if requested
        if self.has_remote_source_node {
            // Get sequencer's RPC URL
            let sequencer_url: reqwest::Url =
                format!("http://localhost:{}", nodes[0].rpc_url().port().unwrap()).parse()?;

            // Configure remote source node
            let mut remote_config = self.config.clone();
            remote_config.sequencer_args.sequencer_enabled = true; // needs to build blocks
            remote_config.sequencer_args.auto_start = false;
            remote_config.remote_block_source_args.build = true;
            remote_config.remote_block_source_args.enabled = true;
            remote_config.remote_block_source_args.url = Some(sequencer_url);
            // Use a fast poll interval for tests
            remote_config.remote_block_source_args.poll_interval_ms = 100;

            let (mut remote_nodes, remote_dbs, _wallet) = setup_engine(
                remote_config,
                1,
                chain_spec.clone(),
                self.is_dev,
                self.no_local_transactions_propagation,
                None,
                None,
            )
            .await?;

            nodes.push(remote_nodes.pop().unwrap());
            dbs.extend(remote_dbs);
        }

        let nodes_len = nodes.len();
        let mut node_handles = Vec::with_capacity(nodes_len);
        for (index, node) in nodes.into_iter().enumerate() {
            let typ = if self.config.sequencer_args.sequencer_enabled && index == 0 {
                NodeType::Sequencer
            } else if self.has_remote_source_node && index == nodes_len - 1 {
                NodeType::RemoteSource
            } else {
                NodeType::Follower
            };
            node_handles.push(Some(NodeHandle::new(node, typ).await?));
        }

        Ok(TestFixture {
            nodes: node_handles,
            dbs,
            wallet: Arc::new(Mutex::new(wallet)),
            chain_spec,
            l1_provider: self.l1_provider,
            anvil,
            config: self.config,
            has_remote_source_node: self.has_remote_source_node,
        })
    }

    /// Spawn an external Anvil instance with the given configuration.
    ///
    /// The `anvil` executable is resolved from the [`ANVIL_BIN_ENV`] environment
    /// variable, falling back to `anvil` on `PATH`. A compatibility preflight
    /// (see [`check_anvil_version`]) runs before every spawn so that a missing or
    /// mismatched binary produces an actionable error rather than an opaque test
    /// failure.
    fn spawn_anvil(
        state_path: Option<&Path>,
        chain_id: Option<u64>,
        block_time: Option<u64>,
        slots_in_an_epoch: u64,
    ) -> eyre::Result<AnvilInstance> {
        let program = anvil_executable();
        preflight_anvil_version(&program)?;

        // Bind to an ephemeral port; `try_spawn` parses the real port from stdout.
        let mut anvil = Anvil::new().path(program).port(0u16);

        if let Some(id) = chain_id {
            anvil = anvil.chain_id(id);
        }

        if let Some(time) = block_time {
            anvil = anvil.block_time(time);
        }

        // Load the pre-populated L1 state (batch contracts, funded accounts) that the
        // in-process backend previously deserialized via `SerializableState::load`.
        if let Some(path) = state_path {
            if !path.exists() {
                return Err(eyre::eyre!("Anvil state file not found: {}", path.display()));
            }
            anvil = anvil.arg("--load-state").arg(path);
            tracing::info!("Loading Anvil state from: {}", path.display());
        }

        anvil = anvil.arg("--slots-in-an-epoch").arg(slots_in_an_epoch.to_string());

        anvil.try_spawn().map_err(|e| eyre::eyre!("Failed to spawn Anvil: {e}"))
    }
}

/// Environment variable that overrides the `anvil` executable used by the L1
/// integration-test fixture. When unset, `anvil` is resolved from `PATH`.
pub const ANVIL_BIN_ENV: &str = "ANVIL_BIN";

/// The commit that uniquely identifies the accepted Anvil build: the official
/// Foundry `v1.5.0` release. This is the definitive pin — the same source is
/// published both as the immutable `v1.5.0` tag (whose `--version` reports
/// `1.5.0-v1.5.0`) and via the mutable `stable` channel (`1.5.0-stable`), so the
/// version string is matched only by prefix. See `.github/assets/install_anvil.sh`.
const REQUIRED_ANVIL_COMMIT: &str = "1c57854462289b2e71ee7654cd6666217ed86ffd";

/// Version prefix the accepted release reports, ignoring the build-channel suffix.
const REQUIRED_ANVIL_VERSION_PREFIX: &str = "1.5.0";

/// Resolve the `anvil` executable, honoring the [`ANVIL_BIN_ENV`] override.
fn anvil_executable() -> OsString {
    std::env::var_os(ANVIL_BIN_ENV).unwrap_or_else(|| OsString::from("anvil"))
}

/// Run the Anvil compatibility preflight, returning an actionable error unless the
/// resolved binary reports the pinned version and commit.
fn preflight_anvil_version(program: &OsStr) -> eyre::Result<()> {
    let output = Command::new(program).arg("--version").output().map_err(|e| {
        eyre::eyre!(
            "failed to execute Anvil binary `{}`: {e}\nInstall the pinned release with \
             .github/assets/install_anvil.sh and point {ANVIL_BIN_ENV} at it (or add `anvil` to PATH).",
            program.to_string_lossy(),
        )
    })?;

    if !output.status.success() {
        return Err(eyre::eyre!(
            "Anvil binary `{}` exited with {} for `--version`; stderr: {}",
            program.to_string_lossy(),
            output.status,
            String::from_utf8_lossy(&output.stderr).trim(),
        ));
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    check_anvil_version(&stdout).map_err(|e| {
        eyre::eyre!(
            "{e}\nresolved Anvil binary: `{}`\nInstall the pinned release with \
             .github/assets/install_anvil.sh and point {ANVIL_BIN_ENV} at it.",
            program.to_string_lossy(),
        )
    })
}

/// Verify that `anvil --version` output matches the pinned release.
///
/// The commit is matched exactly; the version is matched by prefix because the same
/// build is published under different channel suffixes. The expected output begins
/// with two lines of the form:
///
/// ```text
/// anvil Version: 1.5.0-v1.5.0
/// Commit SHA: 1c57854462289b2e71ee7654cd6666217ed86ffd
/// ```
fn check_anvil_version(version_output: &str) -> eyre::Result<()> {
    let version = parse_version_field(version_output, "anvil Version:").ok_or_else(|| {
        eyre::eyre!("could not find `anvil Version:` line in `anvil --version` output")
    })?;
    let commit = parse_version_field(version_output, "Commit SHA:").ok_or_else(|| {
        eyre::eyre!("could not find `Commit SHA:` line in `anvil --version` output")
    })?;

    if !version.starts_with(REQUIRED_ANVIL_VERSION_PREFIX) || commit != REQUIRED_ANVIL_COMMIT {
        return Err(eyre::eyre!(
            "incompatible Anvil: found version `{version}` commit `{commit}`, \
             require version `{REQUIRED_ANVIL_VERSION_PREFIX}*` commit `{REQUIRED_ANVIL_COMMIT}`",
        ));
    }

    Ok(())
}

/// Extract the trimmed value following `label` from the first matching line.
fn parse_version_field<'a>(output: &'a str, label: &str) -> Option<&'a str> {
    output.lines().find_map(|line| line.trim().strip_prefix(label).map(str::trim))
}

#[cfg(test)]
mod tests {
    use super::{check_anvil_version, REQUIRED_ANVIL_COMMIT};

    fn version_output(version: &str, commit: &str) -> String {
        format!(
            "anvil Version: {version}\nCommit SHA: {commit}\nBuild Timestamp: \
             2025-11-26T09:14:24.173470686Z\nBuild Profile: maxperf\n",
        )
    }

    #[test]
    fn accepts_tagged_release_suffix() {
        // The immutable `v1.5.0` archive CI installs reports this suffix.
        let output = version_output("1.5.0-v1.5.0", REQUIRED_ANVIL_COMMIT);
        assert!(check_anvil_version(&output).is_ok());
    }

    #[test]
    fn accepts_stable_channel_suffix() {
        // A local `foundryup stable` build at the same commit reports this suffix.
        let output = version_output("1.5.0-stable", REQUIRED_ANVIL_COMMIT);
        assert!(check_anvil_version(&output).is_ok());
    }

    #[test]
    fn rejects_wrong_version() {
        let output = version_output("1.4.0-stable", REQUIRED_ANVIL_COMMIT);
        assert!(check_anvil_version(&output).is_err());
    }

    #[test]
    fn rejects_wrong_commit() {
        let output = version_output("1.5.0-v1.5.0", "0000000000000000000000000000000000000000");
        assert!(check_anvil_version(&output).is_err());
    }

    #[test]
    fn rejects_missing_fields() {
        assert!(check_anvil_version("anvil 1.5.0\n").is_err());
        assert!(check_anvil_version("").is_err());
    }
}
