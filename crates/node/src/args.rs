use crate::{
    add_ons::IsDevChain,
    constants::{self},
    context::RollupNodeContext,
    pprof::PprofConfig,
    signer_rotation::SignerRotationWatchdog,
};
use alloy_chains::NamedChain;
use alloy_consensus::BlockHeader;
use alloy_primitives::{hex, Address, U128};
use alloy_provider::{layers::CacheLayer, Provider, ProviderBuilder};
use alloy_rpc_client::RpcClient;
use alloy_signer::Signer;
use alloy_signer_aws::AwsSigner;
use alloy_signer_local::PrivateKeySigner;
use alloy_transport::layers::RetryBackoffLayer;
use aws_sdk_kms::config::BehaviorVersion;
use clap::ArgAction;
use dogeos_chainspec::{ChainConfig, DogeosChainSpec, ScrollChainConfig, SCROLL_FEE_VAULT_ADDRESS};
use dogeos_hardforks::DogeosHardforks;
use dogeos_reth_consensus::DogeosConsensus;
use dogeos_rpc_types::Scroll;
use reth_chainspec::EthChainSpec;
use reth_network::NetworkProtocols;
use reth_network_api::FullNetwork;
use reth_network_p2p::FullBlockClient;
use reth_node_builder::{rpc::RethRpcServerHandles, NodeConfig as RethNodeConfig};
use rollup_node_chain_orchestrator::{
    ChainOrchestrator, ChainOrchestratorConfig, ChainOrchestratorHandle, Consensus, NoopConsensus,
    SystemContractConsensus,
};
use rollup_node_primitives::{BlockInfo, NodeConfig};
use rollup_node_providers::{
    BlobProvidersBuilder, FullL1Provider, L1MessageProvider, SystemContractProvider,
};
use rollup_node_sequencer::{
    L1MessageInclusionMode, PayloadBuildingConfig, Sequencer, SequencerConfig,
};
use rollup_node_watcher::{L1Watcher, L1WatcherCommand};
use scroll_db::{
    Database, DatabaseConnectionProvider, DatabaseError, DatabaseMaintenance,
    DatabaseReadOperations, DatabaseWriteOperations,
};
use scroll_derivation_pipeline::DerivationPipeline;
use scroll_engine::{
    genesis_hash_from_chain_spec, Engine, ForkchoiceState, ScrollAuthApiEngineClient,
    ScrollEngineApi,
};
use scroll_migration::{
    traits::ScrollMigrator, MigrationInfo, MigratorTrait, ScrollDevMigrationInfo,
    ScrollMainnetMigrationInfo, ScrollSepoliaMigrationInfo,
};
use scroll_network::{DogeosNetworkPrimitives, EthWireBlockWithPeer, ScrollNetworkManager};
use scroll_wire::ScrollWireEvent;
use std::{fmt, fs, path::PathBuf, sync::Arc};
use tokio::sync::mpsc::{Receiver, UnboundedReceiver};

/// Test-related configuration arguments.
#[derive(Debug, Clone, Default, clap::Args)]
pub struct TestArgs {
    /// Whether the rollup node should be run in test mode.
    #[arg(long)]
    pub test: bool,
    /// Test mode: skip L1 watcher Synced notifications.
    #[arg(long, default_value = "false")]
    pub skip_l1_synced: bool,
}

/// A struct that represents the arguments for the rollup node.
#[derive(Debug, Clone, clap::Args)]
pub struct ScrollRollupNodeConfig {
    /// Test-related arguments
    #[command(flatten)]
    pub test_args: TestArgs,
    /// Consensus args
    #[command(flatten)]
    pub consensus_args: ConsensusArgs,
    /// Database args
    #[command(flatten)]
    pub database_args: RollupNodeDatabaseArgs,
    /// Chain orchestrator args.
    #[command(flatten)]
    pub chain_orchestrator_args: ChainOrchestratorArgs,
    /// Engine driver args.
    #[command(flatten)]
    pub engine_driver_args: EngineDriverArgs,
    /// The blob provider arguments.
    #[command(flatten)]
    pub blob_provider_args: BlobProviderArgs,
    /// The L1 provider arguments
    #[command(flatten)]
    pub l1_provider_args: L1ProviderArgs,
    /// The sequencer arguments
    #[command(flatten)]
    pub sequencer_args: SequencerArgs,
    /// The network arguments
    #[command(flatten)]
    pub network_args: RollupNodeNetworkArgs,
    /// The rpc arguments
    #[command(flatten)]
    pub rpc_args: RpcArgs,
    /// The signer arguments
    #[command(flatten)]
    pub signer_args: SignerArgs,
    /// The gas price oracle args
    #[command(flatten)]
    pub gas_price_oracle_args: RollupNodeGasPriceOracleArgs,
    /// The pprof server arguments
    #[command(flatten)]
    pub pprof_args: PprofArgs,
    /// The remote block source arguments
    #[command(flatten)]
    pub remote_block_source_args: RemoteBlockSourceArgs,
    /// The database connection (not parsed via CLI but hydrated after validation).
    #[arg(skip)]
    pub database: Option<Arc<Database>>,
    /// Require an additional L1 data fee buffer in the account balance checks for transactions.
    #[arg(
        long = "require-l1-data-fee-buffer",
        value_name = "REQUIRE_L1_DATA_FEE_BUFFER",
        default_value = "false"
    )]
    pub require_l1_data_fee_buffer: bool,
}

impl ScrollRollupNodeConfig {
    /// Validate that either signer key file or AWS KMS key ID is provided when sequencer is enabled
    pub fn validate(&self) -> Result<(), String> {
        if self.consensus_args.exit_on_signer_rotation && self.sequencer_args.sequencer_enabled {
            return Err(
                "--consensus.exit-on-signer-rotation must not be used on a sequencer".to_string()
            );
        }

        if self.sequencer_args.sequencer_enabled &
            !matches!(self.consensus_args.algorithm, ConsensusAlgorithm::Noop)
        {
            if self.signer_args.key_file.is_none() &&
                self.signer_args.aws_kms_key_id.is_none() &&
                self.signer_args.private_key.is_none()
            {
                return Err("Either signer key file, AWS KMS key ID or private key is required when sequencer is enabled".to_string());
            }

            if (self.signer_args.key_file.is_some() as u8 +
                self.signer_args.aws_kms_key_id.is_some() as u8 +
                self.signer_args.private_key.is_some() as u8) >
                1
            {
                return Err("Cannot specify more than one signer key source".to_string());
            }
        }

        if self.consensus_args.exit_on_signer_rotation {
            if self.l1_provider_args.url.is_none() {
                return Err("--consensus.exit-on-signer-rotation requires --l1.url".to_string());
            }
            if self.consensus_args.authorized_signer.is_some() {
                return Err("--consensus.exit-on-signer-rotation cannot be used with \
                     --consensus.authorized-signer because restart would re-pin the same signer"
                    .to_string());
            }
            if self.consensus_args.algorithm != ConsensusAlgorithm::SystemContract {
                return Err("--consensus.exit-on-signer-rotation requires \
                     --consensus.algorithm system-contract"
                    .to_string());
            }
        }

        if self.consensus_args.algorithm == ConsensusAlgorithm::SystemContract &&
            self.consensus_args.authorized_signer.is_none() &&
            self.l1_provider_args.url.is_none()
        {
            return Err("System contract consensus requires either an authorized signer or a L1 provider URL".to_string());
        }

        if self.remote_block_source_args.enabled && self.remote_block_source_args.url.is_none() {
            return Err("Remote source URL required when remote source is enabled".to_string());
        }

        if self.remote_block_source_args.enabled &&
            self.remote_block_source_args.build &&
            !self.sequencer_args.sequencer_enabled
        {
            // Without a sequencer no job can ever start; every remote-source
            // build request would fail (PayloadBuildingJobCancelled) until
            // the settlement budget is exhausted and the build abandoned.
            return Err(
                "remote-source.build requires sequencer.enabled: building on top of imported \
                 blocks needs a configured sequencer (which itself requires a signer key \
                 source under non-noop consensus)"
                    .to_string(),
            );
        }

        if !self.remote_block_source_args.enabled &&
            (self.remote_block_source_args.build || self.remote_block_source_args.url.is_some())
        {
            // Remote-source flags templated into every node role with only
            // `enabled` toggled per role are a common, harmless deployment
            // shape — warn, never break the launch.
            tracing::warn!(
                target: "scroll::node::args",
                build = self.remote_block_source_args.build,
                // Presence only: the URL may carry credentials.
                url_set = self.remote_block_source_args.url.is_some(),
                "remote-source flags are set but remote-source.enabled is off; they are ignored"
            );
        }

        if self.remote_block_source_args.enabled &&
            self.remote_block_source_args.build &&
            self.sequencer_args.payload_building_duration >= 12_000
        {
            // 12s is reth's DEFAULT --builder.deadline, which is
            // runtime-configurable — this config cannot see the actual
            // value, so a hard error here would reject valid deployments
            // that raised the deadline (and could not catch ones that
            // lowered it). Warn about the likely misconfiguration instead.
            tracing::warn!(
                target: "scroll::node::args",
                payload_building_duration = self.sequencer_args.payload_building_duration,
                "sequencer.payload-building-duration is at or above reth's default 12s \
                 builder deadline; unless --builder.deadline was raised to match, every \
                 remote-source build will lose its payload and expire into the retry path"
            );
        }

        if let (true, Some(url)) =
            (self.remote_block_source_args.enabled, self.remote_block_source_args.url.as_ref())
        {
            // reqwest::Url happily parses ws:// or file://; the HTTP
            // transport would then fail every poll forever behind a
            // healthy-looking launch.
            if url.scheme() != "http" && url.scheme() != "https" {
                return Err(format!(
                    "remote-source.url must use http or https (got '{}')",
                    url.scheme()
                ));
            }
            // Warn (do not reject) on PLAINTEXT http to a non-loopback host:
            // remote-source imports bypass consensus/signer validation
            // (import_chain never calls consensus.validate_new_block) and can
            // now drive an administrative head rewind, so a MITM on the wire
            // could steer this node's head. https, or a loopback dev remote, is
            // fine.
            if url.scheme() == "http" {
                let host = url.host_str().unwrap_or_default();
                let loopback = matches!(host, "localhost" | "127.0.0.1" | "::1" | "[::1]") ||
                    host.starts_with("127.");
                if !loopback {
                    tracing::warn!(
                        target: "scroll::node::args",
                        host,
                        "remote-source.url is plaintext http to a non-loopback host; remote-source imports bypass consensus/signer validation and can rewind the local head — prefer https"
                    );
                }
            }
        }

        if self.remote_block_source_args.enabled &&
            self.remote_block_source_args.poll_interval_ms == 0
        {
            return Err("remote-source.poll-interval-ms must be greater than 0".to_string());
        }

        if self.remote_block_source_args.enabled &&
            self.sequencer_args.sequencer_enabled &&
            self.sequencer_args.auto_start
        {
            // Two reasons, and NEITHER is gated on `remote-source.build`.
            // With `build`, the remote source attributes build outcomes to its
            // own requests (see RemoteBlockSourceAddOn::await_build_outcome),
            // which is only sound when it is the sole build requester. Without
            // it the sequencer's block timer still runs, so the node builds,
            // signs and gossips a local block every block_time while the remote
            // source imports the real sequencer's chain over RPC, and each
            // import reorgs the local block out — a fork originating from a node
            // the operator configured as a read-only mirror.
            //
            // Gated on `sequencer_enabled` because `build()` only constructs a
            // Sequencer when it is set: without one there is no timer and no
            // second producer, so `auto-start` starts nothing. Rejecting it
            // anyway would break the templated fleet layout the adjacent warn
            // arm explicitly blesses — one flag set, toggled per role.
            return Err("sequencer.auto-start conflicts with remote-source.enabled on a node with \
                 sequencer.enabled: the remote block source must be the only block producer"
                .to_string());
        }

        // Without an L1 provider the L1 watcher is never constructed, and the
        // handle is unwrapped unconditionally at startup — a release build
        // aborts naming nothing the operator typed. The test-utils fallback that
        // hides this in every in-process test is not compiled into the shipped
        // binary (no default features; the Dockerfile builds --release).
        //
        // Last, so a config violating several rules reports the specific one.
        //
        // The --test exemption is itself gated on the feature: the fallback
        // watcher only exists under cfg(feature = "test-utils"), so in the
        // shipped binary --test without an L1 URL reaches the very same
        // unwrap. Exempting it unconditionally would just move the panic.
        // Keyed ONLY on the cfg, exactly like the mock-watcher fallback it guards:
        // `scroll-debug` sets `test = false` with no URL, so an extra
        // `&& test_args.test` here panics those subcommands at startup — and
        // `debug_toolkit` has no tests, so CI would stay green.
        let l1_optional = cfg!(feature = "test-utils");
        if self.l1_provider_args.url.is_none() && !l1_optional {
            return Err("l1.url is required: without it the L1 watcher is never started".to_string());
        }

        // Supplying the URL is not enough under `--test`: the watcher is skipped
        // whenever `--test` is set without an anvil provider, so on a build
        // without the test-utils fallback the node passes validation and then
        // panics on the unwrapped handle — naming nothing the operator typed.
        // Additive, so the rule above and its tests are untouched.
        if !cfg!(feature = "test-utils") &&
            self.test_args.test &&
            self.blob_provider_args.anvil_url.is_none()
        {
            return Err(
                "--test disables the L1 watcher and this build has no test-utils fallback: drop --test, set --blob.anvil_url, or rebuild with --features test-utils"
                    .to_string(),
            );
        }
        Ok(())
    }

    /// Builds the optional follower signer-rotation watchdog from the node configuration.
    pub fn signer_rotation_watchdog(
        &self,
        chain_spec: &DogeosChainSpec,
    ) -> eyre::Result<Option<SignerRotationWatchdog>> {
        if !self.consensus_args.exit_on_signer_rotation {
            return Ok(None);
        }

        self.validate().map_err(eyre::Report::msg)?;
        let l1_url =
            self.l1_provider_args.url.clone().ok_or_else(|| {
                eyre::eyre!("--consensus.exit-on-signer-rotation requires --l1.url")
            })?;
        let node_config = NodeConfig::from_chainspec(chain_spec)?;

        Ok(Some(SignerRotationWatchdog::new(
            l1_url,
            node_config.address_book.system_contract_address,
        )))
    }

    /// Hydrate the config by initializing the database connection.
    pub async fn hydrate(
        &mut self,
        node_config: RethNodeConfig<DogeosChainSpec>,
    ) -> eyre::Result<()> {
        // Instantiate the database
        let db_path = node_config.datadir().db();

        let database_path = if let Some(database_path) = &self.database_args.rn_db_path {
            database_path.to_string_lossy().to_string()
        } else {
            // append the path using strings as using `join(...)` overwrites "sqlite://"
            // if the path is absolute.
            let path = db_path.join("scroll.db?mode=rwc");
            "sqlite://".to_string() + &*path.to_string_lossy()
        };
        let db = Database::new(&database_path).await?;
        self.database = Some(Arc::new(db));
        Ok(())
    }
}

impl ScrollRollupNodeConfig {
    /// Consumes the [`ScrollRollupNodeConfig`] and builds a [`ChainOrchestrator`].
    pub async fn build<N, CS>(
        self,
        ctx: RollupNodeContext<N, CS>,
        events: UnboundedReceiver<ScrollWireEvent>,
        eth_wire_events: Receiver<EthWireBlockWithPeer>,
        rpc_server_handles: RethRpcServerHandles,
    ) -> eyre::Result<(
        ChainOrchestrator<
            N,
            impl DogeosHardforks + EthChainSpec<Header: BlockHeader> + IsDevChain + Clone + 'static,
            impl L1MessageProvider + Clone,
            impl Provider<Scroll> + Clone,
            impl ScrollEngineApi,
        >,
        ChainOrchestratorHandle<N>,
    )>
    where
        N: FullNetwork<Primitives = DogeosNetworkPrimitives>
            + NetworkProtocols
            + scroll_network::EthWirePeerSender,
        CS: EthChainSpec<Header: BlockHeader>
            + ChainConfig<Config = ScrollChainConfig>
            + DogeosHardforks
            + IsDevChain
            + 'static,
    {
        tracing::info!(target: "rollup_node::args",
            "Building rollup node with config:\n{:#?}",
            self
        );

        // Start pprof server if enabled
        if self.pprof_args.enabled {
            let pprof_config = PprofConfig::new(self.pprof_args.addr)
                .with_default_duration(self.pprof_args.default_duration);

            match pprof_config.launch_server().await {
                Ok(handle) => {
                    tracing::info!(target: "rollup_node::pprof", "pprof server started successfully");
                    // Spawn the pprof server task
                    ctx.task_executor.spawn_critical_task("pprof_server", async move {
                        if let Err(e) = handle.await {
                            tracing::error!(target: "rollup_node::pprof", "pprof server error: {:?}", e);
                        }
                    });
                }
                Err(e) => {
                    tracing::error!(target: "rollup_node::pprof", "Failed to start pprof server: {}", e);
                    return Err(e);
                }
            }
        }

        // Get the chain spec.
        let chain_spec = ctx.chain_spec;

        // Build NodeConfig directly from the chainspec.
        let node_config = Arc::new(NodeConfig::from_chainspec(&chain_spec)?);

        // Create the engine api client.
        let engine_api = ScrollAuthApiEngineClient::new(rpc_server_handles.auth.http_client());

        // Get a provider
        let l1_provider = self.l1_provider_args.url.clone().map(|url| {
            let L1ProviderArgs {
                max_retries,
                initial_backoff,
                compute_units_per_second,
                cache_max_items,
                ..
            } = self.l1_provider_args;
            let client = RpcClient::builder()
                .layer(RetryBackoffLayer::new(
                    max_retries,
                    initial_backoff,
                    compute_units_per_second,
                ))
                .http(url);
            let cache_layer = CacheLayer::new(cache_max_items);
            ProviderBuilder::new().layer(cache_layer).connect_client(client)
        });

        // Init a retry provider to the execution layer.
        let retry_layer = RetryBackoffLayer::new(
            constants::L2_PROVIDER_MAX_RETRIES,
            constants::L2_PROVIDER_INITIAL_BACKOFF,
            constants::PROVIDER_COMPUTE_UNITS_PER_SECOND,
        );
        let client = RpcClient::builder().layer(retry_layer).http(
            rpc_server_handles
                .rpc
                .http_url()
                .expect("failed to get l2 rpc url")
                .parse()
                .expect("invalid l2 rpc url"),
        );
        let l2_provider = ProviderBuilder::<_, _, Scroll>::default()
            .layer(CacheLayer::new(constants::L2_PROVIDER_CACHE_MAX_ITEMS))
            .connect_client(client);
        let l2_provider = Arc::new(l2_provider);

        // Fetch the database from the hydrated config.
        let db = self.database.clone().expect("should hydrate config before build");
        let db_maintenance = DatabaseMaintenance::new(db.clone());
        ctx.task_executor.spawn_task(db_maintenance.run());

        // Run the database migrations
        if let Some(named) = chain_spec.chain().named() {
            named
                .migrate(db.inner().get_connection(), self.test_args.test)
                .await
                .expect("failed to perform migration");
        } else {
            // We can re use the dev migration for custom chains as data source and data hash are
            // None for both. We overwrite the default genesis hash from ScrollDevMigrationInfo to
            // match the custom chain.
            // This is a workaround due to the fact that sea orm migrations are static.
            // See https://github.com/scroll-tech/rollup-node/issues/297 for more details.
            scroll_migration::Migrator::<scroll_migration::ScrollDevMigrationInfo>::up(
                db.inner().get_connection(),
                None,
            )
            .await
            .expect("failed to perform migration (custom chain)");
        }

        // The static migrations seed a FIXED genesis row (the dev migration
        // seeds upstream Scroll's dev genesis) which may not match this
        // chain's actual genesis — and insert_genesis_block cannot overwrite
        // it, because its conflict key includes the hash. A stale row shadows
        // the real genesis nondeterministically in highest-block queries (the
        // safe head reported by get_latest_safe_l2_info among them).
        //
        // The SAME source the forkchoice state uses: hardcoded constants for
        // the named Scroll chains, and `chain_spec.genesis_hash()` otherwise —
        // which returns the SEALED hash when a spec carries one, and recomputes
        // only when it does not. NOT `genesis_header().hash_slow()`: that always
        // recomputes, and chikyu's genesis document is byte-identical to
        // mainnet's in every field the header is built from, so recomputing
        // yields MAINNET's hash for chikyu and bricks the chain at startup.
        //
        // This is deliberately NOT the genesis the migration seeds — for dev and
        // every custom chain the migration writes a different value, which is
        // why `seeded_genesis` is threaded separately below.
        let genesis_hash = genesis_hash_from_chain_spec(chain_spec.clone())
            .unwrap_or_else(|| chain_spec.genesis_hash());
        // One transaction for the whole reconciliation, so a crash mid-way
        // cannot leave l2_block empty and panic the genesis expectation on the
        // next query. `reconcile_genesis_block` carries the fresh-vs-populated
        // rules and the legacy-duplicate handling. Called on `db` rather than
        // inside a `tx_mut` closure: the Database impl already wraps it in the
        // same transaction AND records the operation metric, which a hand-rolled
        // `tx_mut` here would bypass.
        // The genesis the static migration above actually seeded, which is NOT
        // always `genesis_hash`: the dev migration hardcodes upstream Scroll's
        // dev genesis while the chain spec computes DogeOS's. A database
        // written before this reconciliation existed carries only that seed, so
        // the reconciliation has to recognise it as a row THIS node wrote
        // rather than another chain's data.
        let seeded_genesis = match chain_spec.chain().named() {
            Some(NamedChain::Scroll) => ScrollMainnetMigrationInfo::genesis_hash(),
            Some(NamedChain::ScrollSepolia) => ScrollSepoliaMigrationInfo::genesis_hash(),
            // Dev, and every custom chain (which reuses the dev migration).
            _ => ScrollDevMigrationInfo::genesis_hash(),
        };
        // `map_err` rather than `expect`: the genesis errors carry actionable
        // Display text ("is the database path pointed at another chain's
        // data?") that a Debug-formatted panic would discard.
        let removed = db
            .reconcile_genesis_block(genesis_hash, seeded_genesis)
            .await
            .map_err(|err| eyre::eyre!("failed to reconcile the genesis block: {err}"))?;
        if removed > 0 {
            tracing::warn!(
                target: "scroll::node::args",
                removed,
                ?genesis_hash,
                "Removed a stale seeded genesis row that did not match the chain genesis"
            );
        }

        let chain_spec_fcs = || {
            ForkchoiceState::head_from_chain_spec(chain_spec.clone())
                .expect("failed to derive forkchoice state from chain spec")
        };
        // `from_provider` returns None on any of three swallowed RPC reads or on
        // an internally inconsistent snapshot (finalized above latest), and
        // the genesis fallback then sets head = safe = finalized = 0. Every
        // safe/finalized guard downstream is vacuous in that state — the
        // peer-block safe-head reorg refusals, the L1-reorg finalized floor and
        // the administrative unwind's symmetric check all compare against 0 —
        // so a transient hiccup must at least be visible.
        let mut provider_fcs_missing = false;
        let mut fcs = match ForkchoiceState::from_provider(&l2_provider).await {
            Some(fcs) => fcs,
            None => {
                provider_fcs_missing = true;
                tracing::warn!(
                    target: "scroll::node::args",
                    "Could not read the forkchoice state from the L2 provider; falling back to \
                     the chain spec genesis. Safe and finalized start at 0 until the first \
                     finalized notification."
                );
                chain_spec_fcs()
            }
        };

        let (l1_block_startup_info, mut l2_head_block_number) = db
            .tx_mut(move |tx| async move {
                // On startup we replay the latest batch of blocks from the database as such we set
                // the safe block hash to the latest block hash associated with the
                // previous consolidated batch in the database.
                let l1_block_startup_info = tx.prepare_l1_watcher_start_info().await?;

                let l2_head_block_number = tx.get_l2_head_block_number().await?;

                Ok::<_, DatabaseError>((l1_block_startup_info, l2_head_block_number))
            })
            .await?;

        // A genesis mirror on a POPULATED database is not a transient read
        // failure this node can run through: head/safe/finalized would all sit
        // at 0 while the database knows a head above it, leaving every
        // safe-and-finalized guard vacuous for as long as it takes a finalized
        // notification to arrive. Refuse instead of starting in that state.
        if let Some(refusal) = startup_refusal(
            l2_head_block_number,
            provider_fcs_missing,
            fcs.is_genesis(),
            fcs.finalized_block_info().number,
        ) {
            eyre::bail!("{refusal}");
        }

        // Loop to find the latest block that we have in the EN and purge L1 message mappings to
        // account for the startup block
        //
        // This is necessary as there is an edge case in which the EN may not have persisted the
        // latest block.
        let finalized_block_number = fcs.finalized_block_info().number;
        while l2_head_block_number > finalized_block_number {
            tracing::info!(target: "scroll::node::args", ?l2_head_block_number, "Checking for L2 head block in EN");

            // Check if the block exists in the EN and update the forkchoice state and L2 head block
            // number
            if let Some(block) = l2_provider
                .get_block(l2_head_block_number.into())
                .full()
                .await?
                .map(|b| b.into_consensus().map_transactions(|tx| tx.inner.into_inner()))
            {
                tracing::info!(target: "scroll::node::args", ?l2_head_block_number, "Found L2 head block in EN");
                let block_info: BlockInfo = (&block).into();
                // A stale safe marker ABOVE the head we are resuming from is
                // exactly what a crash between an unwind's database commit and
                // its FCU leaves behind. Propagating the resulting
                // `HeadBelowSafe` would fail this startup and every one after
                // it, since nothing else lowers the engine's marker. Drag safe
                // down to the head instead — the same rule the runtime reorg
                // and admin-unwind paths already apply. `finalized` needs no
                // clamp: the loop condition guarantees it is at or below this
                // block.
                if fcs.safe_block_info().number > block_info.number {
                    tracing::warn!(
                        target: "scroll::node::args",
                        safe = ?fcs.safe_block_info(),
                        ?block_info,
                        "Engine safe marker sits above the resumed L2 head; clamping it down \
                         (an unwind's forkchoice update was likely lost to a crash)"
                    );
                    fcs = ForkchoiceState::new(block_info, block_info, *fcs.finalized_block_info());
                } else {
                    fcs.update(Some(block_info), None, None)?;
                }
                db.tx_mut(move |tx| async move {
                    tx.set_l2_head_block_number(l2_head_block_number).await?;
                    tx.purge_l1_message_to_l2_block_mappings(Some(l2_head_block_number + 1)).await
                })
                .await?;
                break;
            }

            // Decrement the L2 head block number and try again
            tracing::info!(target: "scroll::node::args", ?l2_head_block_number, "L2 head block not found in EN, decrementing");
            l2_head_block_number -= 1;
        }

        let chain_spec = Arc::new(chain_spec.clone());

        // Instantiate the network manager
        let eth_wire_listener =
            self.network_args.enable_eth_scroll_wire_bridge.then_some(eth_wire_events);

        // TODO: remove this once we deprecate l2geth.
        let authorized_signer = self.network_args.effective_signer(chain_spec.chain().named());

        let (scroll_network_manager, scroll_network_handle) = ScrollNetworkManager::from_parts(
            chain_spec.clone(),
            ctx.network.clone(),
            events,
            eth_wire_listener,
            td_constant(chain_spec.chain().named()),
            authorized_signer,
        );
        ctx.task_executor.spawn_task(scroll_network_manager.run());

        tracing::info!(target: "scroll::node::args", fcs = ?fcs, payload_building_duration = ?self.sequencer_args.payload_building_duration, "Starting engine driver");
        let engine = Engine::new(Arc::new(engine_api), fcs);

        // Create the consensus.
        let authorized_signer = if let Some(provider) = l1_provider.as_ref() {
            Some(
                provider
                    .authorized_signer(node_config.address_book.system_contract_address)
                    .await?,
            )
        } else {
            None
        };
        let consensus = self.consensus_args.consensus(authorized_signer)?;

        let is_anvil_provider = self.blob_provider_args.anvil_url.is_some();

        let (_l1_notification_tx, _l1_command_rx, l1_watcher_handle): (_, _, _) = if let Some(
            provider,
        ) =
            l1_provider.filter(|_| !self.test_args.test || is_anvil_provider)
        {
            tracing::info!(target: "scroll::node::args", ?l1_block_startup_info, "Starting L1 watcher");

            let (notification_tx, handle) = L1Watcher::spawn(
                provider,
                l1_block_startup_info,
                node_config,
                self.l1_provider_args.logs_query_block_range,
                self.l1_provider_args.liveness_threshold,
                self.l1_provider_args.liveness_check_interval,
                #[cfg(feature = "test-utils")]
                self.test_args.skip_l1_synced,
            )
            .await;
            (Some(notification_tx), None::<UnboundedReceiver<L1WatcherCommand>>, Some(handle))
        } else {
            // Create a channel for L1 notifications that we can use to inject L1 messages for
            // testing
            #[cfg(feature = "test-utils")]
            {
                let (notification_tx, notification_rx) = tokio::sync::mpsc::channel(1000);
                let (command_tx, command_rx) = tokio::sync::mpsc::unbounded_channel();
                let handle = rollup_node_watcher::L1WatcherHandle::new(command_tx, notification_rx);

                (Some(notification_tx), Some(command_rx), Some(handle))
            }

            #[cfg(not(feature = "test-utils"))]
            {
                (None, None, None)
            }
        };

        // Construct the l1 provider.
        let l1_messages_provider = db.clone();
        let blob_providers_builder = BlobProvidersBuilder {
            beacon: self.blob_provider_args.beacon_node_urls,
            s3: self.blob_provider_args.s3_url,
            anvil: self.blob_provider_args.anvil_url,
            mock: self.blob_provider_args.mock,
        };
        let blob_provider =
            blob_providers_builder.build().await.expect("failed to construct L1 blob provider");
        let l1_provider = FullL1Provider::new(blob_provider, l1_messages_provider.clone()).await;

        // Construct the Sequencer.
        let chain_config = chain_spec.chain_config();
        let sequencer = self.sequencer_args.sequencer_enabled.then(|| {
            let args = &self.sequencer_args;
            let config = SequencerConfig {
                chain_spec: chain_spec.clone(),
                fee_recipient: args.fee_recipient,
                payload_building_config: PayloadBuildingConfig {
                    block_gas_limit: ctx.block_gas_limit,
                    max_l1_messages_per_block: self
                        .sequencer_args
                        .max_l1_messages
                        .unwrap_or(chain_config.l1_config.num_l1_messages_per_block),
                    l1_message_inclusion_mode: args.l1_message_inclusion_mode,
                },
                auto_start: args.auto_start,
                block_time: args.block_time,
                allow_empty_blocks: args.allow_empty_blocks,
                payload_building_duration: args.payload_building_duration,
            };
            Sequencer::new(Arc::new(l1_messages_provider), config)
        });

        // Instantiate the signer
        let chain_id = chain_spec.chain().id();
        let signer = if let Some(configured_signer) = self.signer_args.signer(chain_id).await? {
            // Use the signer configured by SignerArgs
            Some(rollup_node_signer::Signer::spawn(configured_signer))
        } else if self.test_args.test {
            // Use a random private key signer for testing
            Some(rollup_node_signer::Signer::spawn(PrivateKeySigner::random()))
        } else {
            None
        };

        // Instantiate the chain orchestrator
        let block_client = FullBlockClient::new(
            scroll_network_handle
                .inner()
                .fetch_client()
                .await
                .expect("failed to fetch block client"),
            Arc::new(DogeosConsensus),
        );
        let l1_v2_message_queue_start_index =
            l1_v2_message_queue_start_index(chain_spec.chain().named());
        let config: ChainOrchestratorConfig<Arc<CS>> = ChainOrchestratorConfig::new(
            chain_spec,
            self.chain_orchestrator_args.optimistic_sync_trigger,
            l1_v2_message_queue_start_index,
        );

        // Instantiate the derivation pipeline
        let derivation_pipeline = DerivationPipeline::new(
            l1_provider.clone(),
            db.clone(),
            l1_v2_message_queue_start_index,
        )
        .await;

        let (chain_orchestrator, handle) = ChainOrchestrator::new(
            db,
            config,
            Arc::new(block_client),
            l2_provider,
            l1_watcher_handle.expect("L1 notification receiver should be set"),
            scroll_network_handle.into_scroll_network().await,
            consensus,
            engine,
            sequencer,
            signer,
            derivation_pipeline,
        )
        .await?;

        #[cfg(feature = "test-utils")]
        let handle = {
            let command_rx = _l1_command_rx.map(|rx| Arc::new(tokio::sync::Mutex::new(rx)));
            let l1_watcher_mock = rollup_node_watcher::test_utils::L1WatcherMock {
                command_rx,
                notification_tx: _l1_notification_tx.expect("L1 notification sender should be set"),
            };
            handle.with_l1_watcher_mock(Some(l1_watcher_mock))
        };

        Ok((chain_orchestrator, handle))
    }
}

/// The database arguments.
#[derive(Debug, Default, Clone, clap::Args)]
pub struct RollupNodeDatabaseArgs {
    /// Database path
    #[arg(
        long = "rollup-node-db.path",
        value_name = "DB_PATH",
        help = "The database path for the rollup node database"
    )]
    pub rn_db_path: Option<PathBuf>,
}

/// The consensus arguments.
#[derive(Default, Clone, clap::Args)]
pub struct ConsensusArgs {
    /// The type of consensus to use.
    #[arg(
        long = "consensus.algorithm",
        value_name = "CONSENSUS_ALGORITHM",
        default_value = "system-contract"
    )]
    pub algorithm: ConsensusAlgorithm,

    /// The optional authorized signer for system contract consensus.
    #[arg(long = "consensus.authorized-signer", value_name = "ADDRESS")]
    pub authorized_signer: Option<Address>,

    /// Exit the process (code 70) when the authorized signer in the L1 system contract rotates
    /// away from the watchdog's first successful L1 observation after startup. Intended for
    /// follower nodes run under a supervisor with a restart policy; the restart re-reads the
    /// signer. Sequencer rotation remains a manual operation.
    #[arg(long = "consensus.exit-on-signer-rotation", default_value_t = false)]
    pub exit_on_signer_rotation: bool,
}

// Keep the disabled field out of Debug output so default startup config logging remains
// byte-identical. Enabled configurations include it for observability.
impl fmt::Debug for ConsensusArgs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = f.debug_struct("ConsensusArgs");
        debug
            .field("algorithm", &self.algorithm)
            .field("authorized_signer", &self.authorized_signer);
        if self.exit_on_signer_rotation {
            debug.field("exit_on_signer_rotation", &self.exit_on_signer_rotation);
        }
        debug.finish()
    }
}

impl ConsensusArgs {
    /// Create a new [`ConsensusArgs`] with the no-op consensus algorithm.
    pub const fn noop() -> Self {
        Self {
            algorithm: ConsensusAlgorithm::Noop,
            authorized_signer: None,
            exit_on_signer_rotation: false,
        }
    }

    /// Creates a consensus instance based on the configured algorithm and authorized signer.
    ///
    /// The `authorized_signer` field of `ConsensusArgs` takes precedence over the
    /// `authorized_signer` parameter passed to this method.
    pub fn consensus(
        &self,
        authorized_signer: Option<Address>,
    ) -> eyre::Result<Box<dyn Consensus>> {
        match self.algorithm {
            ConsensusAlgorithm::Noop => Ok(Box::new(NoopConsensus::default())),
            ConsensusAlgorithm::SystemContract => {
                let authorized_signer = if let Some(address) = self.authorized_signer {
                    address
                } else if let Some(address) = authorized_signer {
                    address
                } else {
                    return Err(eyre::eyre!(
                        "System contract consensus requires either an authorized signer or a L1 provider URL"
                    ));
                };
                Ok(Box::new(SystemContractConsensus::new(authorized_signer)))
            }
        }
    }
}

/// The consensus algorithm to use.
#[derive(Debug, Default, clap::ValueEnum, Clone, PartialEq, Eq)]
pub enum ConsensusAlgorithm {
    /// System contract consensus with an optional authorized signer. If the authorized signer is
    /// not provided the system will use the L1 provider to query the authorized signer from L1.
    #[default]
    SystemContract,
    /// No-op consensus that does not validate blocks.
    Noop,
}

/// The engine driver args.
#[derive(Debug, Clone, clap::Args)]
pub struct EngineDriverArgs {
    /// Whether the engine driver should try to sync at start up.
    #[arg(long = "engine.sync-at-startup", num_args=0..=1, default_value_t = true)]
    pub sync_at_startup: bool,
}

impl Default for EngineDriverArgs {
    fn default() -> Self {
        Self { sync_at_startup: true }
    }
}

/// The chain orchestrator arguments.
#[derive(Debug, Clone, clap::Args)]
pub struct ChainOrchestratorArgs {
    /// The amount of block difference between the EN and the latest block received from P2P
    /// at which the engine driver triggers optimistic sync.
    #[arg(long = "chain.optimistic-sync-trigger", default_value_t = constants::BLOCK_GAP_TRIGGER)]
    pub optimistic_sync_trigger: u64,
    /// The size of the in-memory chain buffer used by the chain orchestrator.
    /// NOTE: currently inert. `ChainOrchestratorConfig` has no corresponding
    /// field and nothing outside the test utilities reads this, so setting it
    /// changes no behaviour. Kept because it ships in the CLI; wire it to the
    /// orchestrator's buffer or remove it deliberately, but do not document it
    /// as a memory tunable until then.
    #[arg(long = "chain.chain-buffer-size", default_value_t = constants::CHAIN_BUFFER_SIZE)]
    pub chain_buffer_size: usize,
}

impl Default for ChainOrchestratorArgs {
    fn default() -> Self {
        Self {
            optimistic_sync_trigger: constants::BLOCK_GAP_TRIGGER,
            chain_buffer_size: constants::CHAIN_BUFFER_SIZE,
        }
    }
}

/// The network arguments.
#[derive(Clone, clap::Args)]
pub struct RollupNodeNetworkArgs {
    /// A bool to represent if new blocks should be bridged from the eth wire protocol to the
    /// scroll wire protocol.
    #[arg(long = "network.bridge", default_value_t = true, action = ArgAction::Set)]
    pub enable_eth_scroll_wire_bridge: bool,
    /// A bool that represents if the scroll wire protocol should be enabled.
    #[arg(long = "network.scroll-wire", default_value_t = true, action = ArgAction::Set)]
    pub enable_scroll_wire: bool,
    /// The URL for the Sequencer RPC. (can be both HTTP and WS)
    #[arg(
        long = "network.sequencer-url",
        id = "network_sequencer_url",
        value_name = "NETWORK_SEQUENCER_URL"
    )]
    pub sequencer_url: Option<String>,
    /// The valid signer address for the network.
    #[arg(long = "network.valid_signer", value_name = "VALID_SIGNER")]
    pub signer_address: Option<Address>,
    /// Temporary: enable the legacy geth-to-Reth downloaded-header transform for the one-way
    /// Testnet crossover, where a lagging Reth node canonicalizes signed headers downloaded from
    /// the last l2geth sequencer. Defaults to `false`, is rejected on `DogeOS` Mainnet, and is
    /// scheduled for removal with the rest of the geth compatibility code.
    #[arg(
        long = "network.legacy-geth-header-transform",
        default_value_t = false,
        action = ArgAction::Set
    )]
    pub legacy_geth_header_transform: bool,
}

impl Default for RollupNodeNetworkArgs {
    fn default() -> Self {
        Self {
            enable_eth_scroll_wire_bridge: true,
            enable_scroll_wire: true,
            sequencer_url: None,
            signer_address: None,
            legacy_geth_header_transform: false,
        }
    }
}

impl RollupNodeNetworkArgs {
    /// Get the default authorized signer address for the given chain.
    pub const fn default_authorized_signer(chain: Option<NamedChain>) -> Option<Address> {
        match chain {
            Some(NamedChain::Scroll) => Some(constants::DOGEOS_MAINNET_SIGNER),
            Some(NamedChain::ScrollSepolia) => Some(constants::DOGEOS_CHIKYU_SIGNER),
            _ => None,
        }
    }

    /// Get the effective signer address, using the configured signer or falling back to default.
    pub fn effective_signer(&self, chain: Option<NamedChain>) -> Option<Address> {
        self.signer_address.or_else(|| Self::default_authorized_signer(chain))
    }
}

// Hand-written `Debug` for every argument group that can carry a secret.
//
// `build()` logs the whole config at INFO with `{:#?}` on every launch, and
// `url::Url`'s own `Debug` prints userinfo, path and query verbatim — so a
// derived impl emits the L1 provider's API key (the book's own example is an
// Alchemy URL with the key in the path), the remote source's credentials, the
// blob provider's signed URL and the KMS key id. That defeats the redaction the
// remote source applies to its error logs. Host and port only, and presence
// rather than value for everything else.

fn debug_url(url: Option<&reqwest::Url>) -> String {
    match url {
        Some(url) => format!(
            "{}://{}:{}",
            url.scheme(),
            url.host_str().unwrap_or("<none>"),
            url.port_or_known_default().unwrap_or(0)
        ),
        None => "<unset>".to_string(),
    }
}

impl std::fmt::Debug for L1ProviderArgs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("L1ProviderArgs")
            .field("url", &debug_url(self.url.as_ref()))
            .field("compute_units_per_second", &self.compute_units_per_second)
            .field("max_retries", &self.max_retries)
            .field("initial_backoff", &self.initial_backoff)
            .field("logs_query_block_range", &self.logs_query_block_range)
            .field("cache_max_items", &self.cache_max_items)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for BlobProviderArgs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlobProviderArgs")
            .field("beacon_node_urls_set", &self.beacon_node_urls.is_some())
            .field("s3_url", &debug_url(self.s3_url.as_ref()))
            .field("anvil_url", &debug_url(self.anvil_url.as_ref()))
            .field("compute_units_per_second", &self.compute_units_per_second)
            .field("max_retries", &self.max_retries)
            .field("initial_backoff", &self.initial_backoff)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for SignerArgs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SignerArgs")
            .field("key_file_set", &self.key_file.is_some())
            .field("aws_kms_key_id_set", &self.aws_kms_key_id.is_some())
            .field("private_key_set", &self.private_key.is_some())
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for RemoteBlockSourceArgs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteBlockSourceArgs")
            .field("enabled", &self.enabled)
            .field("url", &debug_url(self.url.as_ref()))
            .field("poll_interval_ms", &self.poll_interval_ms)
            .field("build", &self.build)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for RollupNodeNetworkArgs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // `sequencer_url` is a plain String and prints verbatim, so a follower
        // configured with credentials in it would emit them to stdout through
        // the config dump at startup. Same reason as the other four groups.
        f.debug_struct("RollupNodeNetworkArgs")
            .field("enable_eth_scroll_wire_bridge", &self.enable_eth_scroll_wire_bridge)
            .field("enable_scroll_wire", &self.enable_scroll_wire)
            .field(
                "sequencer_url",
                &self
                    .sequencer_url
                    .as_deref()
                    .and_then(|url| url.parse::<reqwest::Url>().ok())
                    .as_ref()
                    .map_or_else(|| "<unset>".to_string(), |url| debug_url(Some(url))),
            )
            .field("signer_address", &self.signer_address)
            .field("legacy_geth_header_transform", &self.legacy_geth_header_transform)
            .finish_non_exhaustive()
    }
}

/// Why this node must refuse to start, or `None` when it may proceed.
///
/// Extracted and table-tested because the predicate is one token wide in several places and this
/// branch has been corrected repeatedly — flipping the final `<` to `<=` refuses every node whose
/// anchor sits exactly at finality, and nothing else in the suite would notice.
///
/// - `provider_fcs_missing`: no usable forkchoice state could be read at all.
/// - `fcs_is_genesis`: the provider answered, but from genesis.
/// - all three refusals apply only when the database already knows a head above genesis; a fresh
///   database must still bootstrap.
fn startup_refusal(
    l2_head_block_number: u64,
    provider_fcs_missing: bool,
    fcs_is_genesis: bool,
    finalized: u64,
) -> Option<String> {
    if l2_head_block_number == 0 {
        return None;
    }
    // Both hypotheses in one message: a wiped or resynced execution-node datadir makes reth answer
    // `null` for the safe and finalized tags, so it lands HERE rather than in the genesis arm, and
    // naming only reachability sends the operator after the wrong thing.
    if provider_fcs_missing {
        return Some(format!(
            "could not read a usable forkchoice state from the L2 provider while the database \
             holds an L2 head at {l2_head_block_number}; refusing to start with head, safe and \
             finalized all at genesis. Either the execution node is unreachable or not yet \
             synced, or its datadir was wiped or resynced while the rollup database was kept \
             (resync both, or point at the matching execution-node datadir)"
        ));
    }
    if fcs_is_genesis {
        return Some(format!(
            "the L2 provider is at genesis while the database holds an L2 head at \
             {l2_head_block_number}; the execution node's datadir looks wiped or resynced while \
             the rollup database was kept (resync both, or point at the matching execution-node \
             datadir)"
        ));
    }
    // The database anchor BELOW the engine's finalized block is the state the run loop calls
    // irreconcilable and fail-stops on, because `unwind()` commits the rewound head durably BEFORE
    // those checks fire. Nothing used to refuse it here: the repair loop is gated
    // `while l2_head > finalized`, false in exactly this shape, so the node came up with the engine
    // head above the anchor and the mappings above it already purged — and the next build
    // re-selected consumed messages into a queue gap every peer rejects.
    //
    // Strict `<`: an anchor exactly AT finality is the ordinary steady state and must launch.
    if l2_head_block_number < finalized {
        return Some(format!(
            "the database L2 head {l2_head_block_number} is below the execution node's finalized \
             block {finalized}; an unwind committed past finality and this cannot be reconciled \
             automatically (restore the database from before the unwind, or resync this node)"
        ));
    }
    None
}

/// The arguments for the L1 provider.
#[derive(Clone, clap::Args)]
pub struct L1ProviderArgs {
    /// The URL for the L1 RPC.
    #[arg(long = "l1.url", id = "l1_url", value_name = "L1_URL")]
    pub url: Option<reqwest::Url>,
    /// The compute units per second for the provider.
    #[arg(long = "l1.cups", id = "l1_compute_units_per_second", value_name = "L1_COMPUTE_UNITS_PER_SECOND", default_value_t = constants::PROVIDER_COMPUTE_UNITS_PER_SECOND)]
    pub compute_units_per_second: u64,
    /// The max amount of retries for the provider.
    #[arg(long = "l1.max-retries", id = "l1_max_retries", value_name = "L1_MAX_RETRIES", default_value_t = constants::L1_PROVIDER_MAX_RETRIES)]
    pub max_retries: u32,
    /// The initial backoff for the provider.
    #[arg(long = "l1.initial-backoff", id = "l1_initial_backoff", value_name = "L1_INITIAL_BACKOFF", default_value_t = constants::L1_PROVIDER_INITIAL_BACKOFF)]
    pub initial_backoff: u64,
    /// The logs query block range.
    #[arg(long = "l1.query-range", id = "l1_query_range", value_name = "L1_QUERY_RANGE", default_value_t = constants::LOGS_QUERY_BLOCK_RANGE)]
    pub logs_query_block_range: u64,
    /// The maximum number of items to be stored in the cache layer.
    #[arg(long = "l1.cache-max-items", id = "l1_cache_max_items", value_name = "L1_CACHE_MAX_ITEMS", default_value_t = constants::L1_PROVIDER_CACHE_MAX_ITEMS)]
    pub cache_max_items: u32,
    /// The L1 liveness threshold in seconds. If no new L1 block is received within this duration,
    /// an error is logged.
    #[arg(long = "l1.liveness-threshold", id = "l1_liveness_threshold", value_name = "L1_LIVENESS_THRESHOLD", default_value_t = constants::L1_LIVENESS_THRESHOLD)]
    pub liveness_threshold: u64,
    /// The interval in seconds at which to check L1 liveness.
    #[arg(long = "l1.liveness-check-interval", id = "l1_liveness_check_interval", value_name = "L1_LIVENESS_CHECK_INTERVAL", default_value_t = constants::L1_LIVENESS_CHECK_INTERVAL)]
    pub liveness_check_interval: u64,
}

impl Default for L1ProviderArgs {
    fn default() -> Self {
        Self {
            url: None,
            compute_units_per_second: constants::PROVIDER_COMPUTE_UNITS_PER_SECOND,
            max_retries: constants::L1_PROVIDER_MAX_RETRIES,
            initial_backoff: constants::L1_PROVIDER_INITIAL_BACKOFF,
            logs_query_block_range: constants::LOGS_QUERY_BLOCK_RANGE,
            cache_max_items: constants::L1_PROVIDER_CACHE_MAX_ITEMS,
            liveness_threshold: constants::L1_LIVENESS_THRESHOLD,
            liveness_check_interval: constants::L1_LIVENESS_CHECK_INTERVAL,
        }
    }
}

/// The arguments for the Beacon provider.
#[derive(Default, Clone, clap::Args)]
pub struct BlobProviderArgs {
    /// The URLs for the beacon node blob provider.
    #[arg(
        long = "blob.beacon_node_urls",
        id = "blob_beacon_node_urls",
        value_name = "BLOB_BEACON_NODE_URLS"
    )]
    pub beacon_node_urls: Option<Vec<reqwest::Url>>,
    /// The URL for the s3 blob provider.
    #[arg(long = "blob.s3_url", id = "blob_s3_url", value_name = "BLOB_S3_URL")]
    pub s3_url: Option<reqwest::Url>,
    /// The URL for the anvil blob provider.
    #[arg(long = "blob.anvil_url", id = "blob_anvil_url", value_name = "BLOB_ANVIL_URL")]
    pub anvil_url: Option<reqwest::Url>,
    /// Enable the mock blob source.
    #[arg(long = "blob.mock")]
    pub mock: bool,
    /// The compute units per second for the provider.
    #[arg(long = "blob.cups", id = "blob_compute_units_per_second", value_name = "BLOB_COMPUTE_UNITS_PER_SECOND", default_value_t = constants::PROVIDER_COMPUTE_UNITS_PER_SECOND)]
    pub compute_units_per_second: u64,
    /// The max amount of retries for the provider.
    #[arg(long = "blob.max-retries", id = "blob_max_retries", value_name = "BLOB_MAX_RETRIES", default_value_t = constants::L1_PROVIDER_MAX_RETRIES)]
    pub max_retries: u32,
    /// The initial backoff for the provider.
    #[arg(long = "blob.initial-backoff", id = "blob_initial_backoff", value_name = "BLOB_INITIAL_BACKOFF", default_value_t = constants::L1_PROVIDER_INITIAL_BACKOFF)]
    pub initial_backoff: u64,
}

/// The arguments for the sequencer.
#[derive(Debug, Default, Clone, clap::Args)]
pub struct SequencerArgs {
    /// Enable the scroll block sequencer.
    #[arg(long = "sequencer.enabled", default_value_t = false)]
    pub sequencer_enabled: bool,
    /// Whether the sequencer should start sequencing automatically on startup.
    #[arg(long = "sequencer.auto-start", default_value_t = false)]
    pub auto_start: bool,
    /// The block time for the sequencer.
    #[arg(long = "sequencer.block-time", id = "sequencer_block_time", value_name = "SEQUENCER_BLOCK_TIME", default_value_t = constants::DEFAULT_BLOCK_TIME)]
    pub block_time: u64,
    /// The payload building duration for the sequencer (milliseconds)
    #[arg(long = "sequencer.payload-building-duration", id = "sequencer_payload_building_duration", value_name = "SEQUENCER_PAYLOAD_BUILDING_DURATION", default_value_t = constants::DEFAULT_PAYLOAD_BUILDING_DURATION)]
    pub payload_building_duration: u64,
    /// The fee recipient for the sequencer.
    #[arg(long = "sequencer.fee-recipient", id = "sequencer_fee_recipient", value_name = "SEQUENCER_FEE_RECIPIENT", default_value_t = SCROLL_FEE_VAULT_ADDRESS)]
    pub fee_recipient: Address,
    /// L1 message inclusion mode: "finalized" or "depth:{number}"
    /// Examples: "finalized", "depth:10", "depth:6"
    #[arg(
        long = "sequencer.l1-inclusion-mode",
        id = "sequencer_l1_inclusion_mode",
        value_name = "MODE",
        default_value = "finalized:2",
        help = "L1 message inclusion mode. Use 'finalized' for finalized messages only, or 'depth:{number}' for block depth confirmation (e.g. 'depth:10')"
    )]
    pub l1_message_inclusion_mode: L1MessageInclusionMode,
    /// Enable empty blocks.
    #[arg(
        long = "sequencer.allow-empty-blocks",
        id = "sequencer_allow_empty_blocks",
        value_name = "SEQUENCER_ALLOW_EMPTY_BLOCKS",
        default_value_t = false
    )]
    pub allow_empty_blocks: bool,
    /// The maximum number of L1 messages to include per L2 block.
    #[arg(
        long = "sequencer.max-l1-messages",
        id = "sequencer_max_l1_messages",
        value_name = "SEQUENCER_MAX_L1_MESSAGES",
        help = "The maximum number of L1 messages to include per L2 block. If not set, defaults to the value specified in the chain config."
    )]
    pub max_l1_messages: Option<u64>,
}

/// The arguments for the signer.
#[derive(Default, Clone, clap::Args)]
pub struct SignerArgs {
    /// Path to the file containing the signer's private key
    #[arg(
        long = "signer.key-file",
        value_name = "FILE_PATH",
        help = "Path to the hex-encoded private key file for the signer (optional 0x prefix). Mutually exclusive with --signer.aws-kms-key-id"
    )]
    pub key_file: Option<PathBuf>,

    /// AWS KMS Key ID for signing transactions
    #[arg(
        long = "signer.aws-kms-key-id",
        value_name = "KEY_ID",
        help = "AWS KMS Key ID for signing transactions. Mutually exclusive with --signer.key-file"
    )]
    pub aws_kms_key_id: Option<String>,

    /// The private key signer, if any.
    /// `skip`, not a positional: without this clap derives one from the field,
    /// and a raw hex signing key on argv lands in `ps`, `/proc/<pid>/cmdline`
    /// and shell history. Both siblings are explicitly namespaced flags.
    #[arg(skip)]
    pub private_key: Option<PrivateKeySigner>,
}

/// The arguments for the rpc.
#[derive(Debug, Default, Clone, clap::Args)]
pub struct RpcArgs {
    /// A boolean to represent if the rollup node basic rpc should be enabled.
    #[arg(long = "rpc.rollup-node", default_value_t = true, action = ArgAction::Set, help = "Enable the rollup node basic RPC namespace(default: true)")]
    pub basic_enabled: bool,
    /// A boolean to represent if the rollup node admin rpc should be enabled.
    #[arg(long = "rpc.rollup-node-admin", help = "Enable the rollup node admin RPC namespace")]
    pub admin_enabled: bool,
}

impl SignerArgs {
    /// Create a signer based on the configured arguments
    pub async fn signer(
        &self,
        chain_id: u64,
    ) -> eyre::Result<Option<Box<dyn Signer + Send + Sync>>> {
        if let Some(key_file_path) = &self.key_file {
            // Load the private key from the file
            let key_content = fs::read_to_string(key_file_path)
                .map_err(|e| {
                    eyre::eyre!("Failed to read signer key file {}: {}", key_file_path.display(), e)
                })?
                .trim()
                .to_string();

            let hex_str = key_content.strip_prefix("0x").unwrap_or(&key_content);
            let key_bytes = hex::decode(hex_str).map_err(|e| {
                eyre::eyre!(
                    "Failed to decode hex private key from file {}: {}",
                    key_file_path.display(),
                    e
                )
            })?;

            // Create the private key signer
            let private_key_signer = PrivateKeySigner::from_slice(&key_bytes)
                .map_err(|e| eyre::eyre!("Failed to create signer from key file: {}", e))?
                .with_chain_id(Some(chain_id));

            tracing::info!(target: "scroll::node::args",
                "Created private key signer with address: {} for chain ID: {}",
                private_key_signer.address(),
                chain_id
            );

            Ok(Some(Box::new(private_key_signer) as Box<dyn Signer + Send + Sync>))
        } else if let Some(aws_kms_key_id) = &self.aws_kms_key_id {
            // Load AWS configuration
            let config_loader = aws_config::defaults(BehaviorVersion::latest());
            let config = config_loader.load().await;
            let kms_client = aws_sdk_kms::Client::new(&config);

            // Create the AWS KMS signer
            let aws_signer = AwsSigner::new(kms_client, aws_kms_key_id.clone(), Some(chain_id))
                .await
                .map_err(|e| eyre::eyre!("Failed to initialize AWS KMS signer: {}", e))?;

            tracing::info!(
                target: "scroll::node::args",
                "Created AWS KMS signer with address: {} for chain ID: {}",
                aws_signer.address(),
                chain_id
            );

            Ok(Some(Box::new(aws_signer) as Box<dyn Signer + Send + Sync>))
        } else if let Some(private_key) = &self.private_key {
            tracing::info!(target: "scroll::node::args", "Created private key signer with address: {} for chain ID: {}", private_key.address(), chain_id);
            let signer = private_key.clone().with_chain_id(Some(chain_id));
            Ok(Some(Box::new(signer) as Box<dyn Signer + Send + Sync>))
        } else {
            Ok(None)
        }
    }
}

/// The arguments for the sequencer.
#[derive(Debug, Default, Clone, clap::Args)]
pub struct RollupNodeGasPriceOracleArgs {
    /// Minimum suggested priority fee (tip) in wei, default `100`
    #[arg(long = "gpo.default-suggest-priority-fee", id = "default_suggest_priority_fee", value_name = "DEFAULT_SUGGEST_PRIORITY_FEE", default_value_t = constants::DEFAULT_SUGGEST_PRIORITY_FEE)]
    pub default_suggested_priority_fee: u64,
}

/// The arguments for the pprof server.
#[derive(Debug, Clone, clap::Args)]
pub struct PprofArgs {
    /// Enable the pprof HTTP server for performance profiling
    #[arg(id = "pprof.enabled", long = "pprof.enabled", help = "Enable the pprof HTTP server")]
    pub enabled: bool,

    /// The address to bind the pprof HTTP server to
    #[arg(
        id = "pprof.url",
        long = "pprof.addr",
        value_name = "PPROF_URL",
        help = "Address to bind the pprof HTTP server (e.g., 0.0.0.0:6868)",
        default_value = constants::DEFAULT_PPROF_URL
    )]
    pub addr: std::net::SocketAddr,

    /// Default profiling duration in seconds
    #[arg(
        id = "pprof.default_duration",
        value_name = "PPROF_DEFAULT_DURATION",
        long = "pprof.default-duration",
        help = "Default CPU profiling duration in seconds",
        default_value_t = constants::DEFAULT_PPROF_DEFAULT_DURATION
    )]
    pub default_duration: u64,
}

impl Default for PprofArgs {
    fn default() -> Self {
        Self { enabled: false, addr: ([0, 0, 0, 0], 6868).into(), default_duration: 30 }
    }
}

/// The arguments for the remote block source.
#[derive(Default, Clone, clap::Args)]
pub struct RemoteBlockSourceArgs {
    /// Enable the remote block source feature
    #[arg(long = "remote-source.enabled", default_value_t = false)]
    pub enabled: bool,

    /// URL for the remote L2 source node RPC
    #[arg(long = "remote-source.url", id = "remote_source_url", value_name = "URL")]
    pub url: Option<reqwest::Url>,

    /// Polling interval in milliseconds (between polls; catch-up runs inside a single tick)
    #[arg(
        long = "remote-source.poll-interval-ms",
        default_value_t = 100,
        value_name = "POLL_INTERVAL_MS"
    )]
    pub poll_interval_ms: u64,

    /// Whether to build blocks using the remote source.
    #[arg(long = "remote-source.build")]
    pub build: bool,
}

/// Returns the total difficulty constant for the given chain.
const fn td_constant(chain: Option<NamedChain>) -> U128 {
    match chain {
        Some(NamedChain::Scroll) => constants::DOGEOS_MAINNET_TD_CONSTANT,
        Some(NamedChain::ScrollSepolia) => constants::DOGEOS_CHIKYU_TD_CONSTANT,
        _ => U128::ZERO, // Default to zero for other chains
    }
}

/// The L1 message queue index at which queue hashes should be computed .
const fn l1_v2_message_queue_start_index(chain: Option<NamedChain>) -> u64 {
    match chain {
        Some(NamedChain::Scroll) => constants::DOGEOS_MAINNET_V2_MESSAGE_QUEUE_START_INDEX,
        Some(NamedChain::ScrollSepolia) => constants::DOGEOS_CHIKYU_V2_MESSAGE_QUEUE_START_INDEX,
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use std::path::PathBuf;

    /// The startup genesis reconciliation compares the chain spec's genesis
    /// against the height-0 rows, so it depends on knowing which genesis the
    /// static migration seeded — and that is NOT the same value. The dev
    /// migration hardcodes upstream Scroll's dev genesis while every spec
    /// shipped here computes its own, so a database written before this
    /// reconciliation existed carries a height-0 row it must recognise as its
    /// own rather than reject as another chain's data.
    ///
    /// This pins the mapping `build()` relies on: which migration each shipped
    /// spec routes to, and that the seed really does differ from the spec's
    /// genesis. `build()` runs `named.migrate()` for `Some(named)` and the dev
    /// migration otherwise, so all three shipped specs seed through
    /// `ScrollDevMigrationInfo` today. Naming mainnet or chikyu later would
    /// route it onto a different seed, and this test is what should fail then.
    #[test]
    fn genesis_seed_pairing_holds_for_shipped_chain_specs() {
        use dogeos_chainspec::{DOGEOS_CHIKYU, DOGEOS_DEV, DOGEOS_MAINNET};
        use reth_chainspec::EthChainSpec;

        for (name, chain_spec, expected_named) in [
            ("mainnet", DOGEOS_MAINNET.clone(), None),
            ("chikyu", DOGEOS_CHIKYU.clone(), None),
            ("dev", DOGEOS_DEV.clone(), Some(NamedChain::Dev)),
        ] {
            assert_eq!(
                chain_spec.chain().named(),
                expected_named,
                "{name} changed chain identity; re-check which migration build() routes it to \
                 and which genesis that migration seeds"
            );
            // Every arm above falls through build()'s `_` case.
            let seeded_genesis = match chain_spec.chain().named() {
                Some(NamedChain::Scroll) => ScrollMainnetMigrationInfo::genesis_hash(),
                Some(NamedChain::ScrollSepolia) => ScrollSepoliaMigrationInfo::genesis_hash(),
                _ => ScrollDevMigrationInfo::genesis_hash(),
            };
            assert_eq!(
                seeded_genesis,
                ScrollDevMigrationInfo::genesis_hash(),
                "{name} is expected to be seeded by the dev migration"
            );
            // THE load-bearing assertion, and the one the first version of this
            // test missed. It compared against the migration seed only, which
            // passed for chikyu for the wrong reason: the recomputed header
            // hash was MAINNET's genesis, unequal to the seed but also unequal
            // to chikyu's own sealed genesis. `genesis_hash()` returns the
            // sealed value when a spec carries one, and that is what the EL
            // stores at block 0, so the reconciliation must agree with it.
            assert_eq!(
                genesis_hash_from_chain_spec(chain_spec.clone()),
                Some(chain_spec.genesis_hash()),
                "{name}'s forkchoice genesis must be the chain spec's own genesis; a recomputed \
                 header hash diverges from the sealed one and bricks the chain at startup"
            );
            assert_ne!(
                genesis_hash_from_chain_spec(chain_spec.clone()),
                Some(seeded_genesis),
                "{name}'s genesis unexpectedly EQUALS the migration seed; if the two sources \
                 have converged, reconcile_genesis_block no longer needs the seed threaded \
                 through and this expectation should be revisited"
            );
        }
    }

    #[derive(Debug, Parser)]
    struct ConsensusCli {
        #[command(flatten)]
        consensus: ConsensusArgs,
    }

    fn rotation_watchdog_config() -> ScrollRollupNodeConfig {
        ScrollRollupNodeConfig {
            test_args: TestArgs::default(),
            consensus_args: ConsensusArgs {
                algorithm: ConsensusAlgorithm::SystemContract,
                authorized_signer: None,
                exit_on_signer_rotation: true,
            },
            database_args: RollupNodeDatabaseArgs::default(),
            chain_orchestrator_args: ChainOrchestratorArgs::default(),
            engine_driver_args: EngineDriverArgs::default(),
            blob_provider_args: BlobProviderArgs::default(),
            l1_provider_args: L1ProviderArgs {
                url: Some("http://localhost:8545".parse().unwrap()),
                ..Default::default()
            },
            sequencer_args: SequencerArgs::default(),
            network_args: RollupNodeNetworkArgs::default(),
            rpc_args: RpcArgs::default(),
            signer_args: SignerArgs::default(),
            gas_price_oracle_args: RollupNodeGasPriceOracleArgs::default(),
            pprof_args: PprofArgs::default(),
            remote_block_source_args: RemoteBlockSourceArgs::default(),
            database: None,
            require_l1_data_fee_buffer: false,
        }
    }

    #[test]
    fn exit_on_signer_rotation_defaults_to_false_when_parsed() {
        let cli = ConsensusCli::parse_from(["rollup-node"]);

        assert!(!cli.consensus.exit_on_signer_rotation);
    }

    #[test]
    fn consensus_debug_omits_disabled_rotation_flag() {
        assert_eq!(
            format!("{:#?}", ConsensusArgs::default()),
            "ConsensusArgs {\n    algorithm: SystemContract,\n    authorized_signer: None,\n}"
        );
    }

    #[test]
    fn consensus_debug_includes_enabled_rotation_flag() {
        let args = ConsensusArgs { exit_on_signer_rotation: true, ..Default::default() };

        assert!(format!("{args:#?}").contains("exit_on_signer_rotation: true"));
    }

    #[test]
    fn exit_on_signer_rotation_requires_l1_url() {
        let mut config = rotation_watchdog_config();
        config.l1_provider_args.url = None;

        assert_eq!(
            config.validate().unwrap_err(),
            "--consensus.exit-on-signer-rotation requires --l1.url"
        );
    }

    #[test]
    fn exit_on_signer_rotation_conflicts_with_pinned_signer() {
        let mut config = rotation_watchdog_config();
        config.consensus_args.authorized_signer = Some(Address::new([0x11; 20]));

        assert_eq!(
            config.validate().unwrap_err(),
            "--consensus.exit-on-signer-rotation cannot be used with \
             --consensus.authorized-signer because restart would re-pin the same signer"
        );
    }

    #[test]
    fn exit_on_signer_rotation_requires_system_contract_consensus() {
        let mut config = rotation_watchdog_config();
        config.consensus_args.algorithm = ConsensusAlgorithm::Noop;

        assert_eq!(
            config.validate().unwrap_err(),
            "--consensus.exit-on-signer-rotation requires --consensus.algorithm system-contract"
        );
    }

    #[test]
    fn exit_on_signer_rotation_rejects_sequencer() {
        let mut config = rotation_watchdog_config();
        config.sequencer_args.sequencer_enabled = true;

        assert_eq!(
            config.validate().unwrap_err(),
            "--consensus.exit-on-signer-rotation must not be used on a sequencer"
        );
    }

    #[test]
    fn test_network_args_default_authorized_signer() {
        // Test Scroll mainnet
        let mainnet_signer =
            RollupNodeNetworkArgs::default_authorized_signer(Some(NamedChain::Scroll));
        assert_eq!(mainnet_signer, Some(constants::DOGEOS_MAINNET_SIGNER));

        // Test Scroll Sepolia
        let sepolia_signer =
            RollupNodeNetworkArgs::default_authorized_signer(Some(NamedChain::ScrollSepolia));
        assert_eq!(sepolia_signer, Some(constants::DOGEOS_CHIKYU_SIGNER));

        // Test other chains
        let other_signer =
            RollupNodeNetworkArgs::default_authorized_signer(Some(NamedChain::Mainnet));
        assert_eq!(other_signer, None);

        // Test None chain
        let none_signer = RollupNodeNetworkArgs::default_authorized_signer(None);
        assert_eq!(none_signer, None);
    }

    #[test]
    fn test_network_args_effective_signer() {
        let custom_signer = Address::new([0x11; 20]);

        // Test with configured signer
        let network_args =
            RollupNodeNetworkArgs { signer_address: Some(custom_signer), ..Default::default() };
        assert_eq!(network_args.effective_signer(Some(NamedChain::Scroll)), Some(custom_signer));

        // Test without configured signer, fallback to default
        let network_args_default = RollupNodeNetworkArgs::default();
        assert_eq!(
            network_args_default.effective_signer(Some(NamedChain::Scroll)),
            Some(constants::DOGEOS_MAINNET_SIGNER)
        );
        assert_eq!(
            network_args_default.effective_signer(Some(NamedChain::ScrollSepolia)),
            Some(constants::DOGEOS_CHIKYU_SIGNER)
        );
        assert_eq!(network_args_default.effective_signer(Some(NamedChain::Mainnet)), None);
    }

    #[test]
    fn test_validate_sequencer_enabled_without_any_signer_fails() {
        let config = ScrollRollupNodeConfig {
            test_args: TestArgs::default(),
            sequencer_args: SequencerArgs { sequencer_enabled: true, ..Default::default() },
            signer_args: SignerArgs { key_file: None, aws_kms_key_id: None, private_key: None },
            database_args: RollupNodeDatabaseArgs::default(),
            engine_driver_args: EngineDriverArgs::default(),
            chain_orchestrator_args: ChainOrchestratorArgs::default(),
            l1_provider_args: L1ProviderArgs {
                // validate() requires an L1 provider: without one the L1 watcher
                // is never built and startup aborts.
                url: Some("http://localhost:8545".parse().unwrap()),
                ..Default::default()
            },
            blob_provider_args: BlobProviderArgs::default(),
            network_args: RollupNodeNetworkArgs::default(),
            gas_price_oracle_args: RollupNodeGasPriceOracleArgs::default(),
            consensus_args: ConsensusArgs {
                algorithm: ConsensusAlgorithm::SystemContract,
                authorized_signer: None,
                exit_on_signer_rotation: false,
            },
            database: None,
            rpc_args: RpcArgs::default(),
            pprof_args: PprofArgs::default(),
            remote_block_source_args: RemoteBlockSourceArgs::default(),
            require_l1_data_fee_buffer: false,
        };

        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains(
            "Either signer key file, AWS KMS key ID or private key is required when sequencer is enabled"
        ));
    }

    #[test]
    fn test_validate_remote_source_enabled_without_url_fails() {
        let config = ScrollRollupNodeConfig {
            test_args: TestArgs::default(),
            sequencer_args: SequencerArgs::default(),
            signer_args: SignerArgs::default(),
            database_args: RollupNodeDatabaseArgs::default(),
            engine_driver_args: EngineDriverArgs::default(),
            chain_orchestrator_args: ChainOrchestratorArgs::default(),
            l1_provider_args: L1ProviderArgs {
                // validate() requires an L1 provider: without one the L1 watcher
                // is never built and startup aborts.
                url: Some("http://localhost:8545".parse().unwrap()),
                ..Default::default()
            },
            blob_provider_args: BlobProviderArgs::default(),
            network_args: RollupNodeNetworkArgs::default(),
            gas_price_oracle_args: RollupNodeGasPriceOracleArgs::default(),
            consensus_args: ConsensusArgs::noop(),
            database: None,
            rpc_args: RpcArgs::default(),
            pprof_args: PprofArgs::default(),
            remote_block_source_args: RemoteBlockSourceArgs {
                enabled: true,
                url: None,
                poll_interval_ms: 100,
                build: false,
            },
            require_l1_data_fee_buffer: false,
        };

        let result = config.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("Remote source URL required when remote source is enabled"));
    }

    #[test]
    fn test_validate_remote_source_build_without_sequencer_fails() {
        let config = ScrollRollupNodeConfig {
            test_args: TestArgs::default(),
            sequencer_args: SequencerArgs { sequencer_enabled: false, ..Default::default() },
            signer_args: SignerArgs::default(),
            database_args: RollupNodeDatabaseArgs::default(),
            engine_driver_args: EngineDriverArgs::default(),
            chain_orchestrator_args: ChainOrchestratorArgs::default(),
            l1_provider_args: L1ProviderArgs {
                // validate() requires an L1 provider: without one the L1 watcher
                // is never built and startup aborts.
                url: Some("http://localhost:8545".parse().unwrap()),
                ..Default::default()
            },
            blob_provider_args: BlobProviderArgs::default(),
            network_args: RollupNodeNetworkArgs::default(),
            gas_price_oracle_args: RollupNodeGasPriceOracleArgs::default(),
            consensus_args: ConsensusArgs::noop(),
            database: None,
            rpc_args: RpcArgs::default(),
            pprof_args: PprofArgs::default(),
            remote_block_source_args: RemoteBlockSourceArgs {
                enabled: true,
                url: Some("http://localhost:8545".parse().unwrap()),
                poll_interval_ms: 100,
                build: true,
            },
            require_l1_data_fee_buffer: false,
        };

        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("remote-source.build requires sequencer.enabled"));
    }

    /// `sequencer.auto-start` conflicts with the remote source whether or not
    /// `remote-source.build` is set: with it the remote source is no longer the
    /// sole build requester, and without it the sequencer's own block timer
    /// still produces local blocks that every remote import then reorgs out.
    #[test]
    fn test_validate_remote_source_with_auto_start_fails() {
        for build in [true, false] {
            let config = ScrollRollupNodeConfig {
                test_args: TestArgs::default(),
                sequencer_args: SequencerArgs {
                    sequencer_enabled: true,
                    auto_start: true,
                    ..Default::default()
                },
                signer_args: SignerArgs::default(),
                database_args: RollupNodeDatabaseArgs::default(),
                engine_driver_args: EngineDriverArgs::default(),
                chain_orchestrator_args: ChainOrchestratorArgs::default(),
                l1_provider_args: L1ProviderArgs {
                    // validate() requires an L1 provider: without one the L1 watcher
                    // is never built and startup aborts.
                    url: Some("http://localhost:8545".parse().unwrap()),
                    ..Default::default()
                },
                blob_provider_args: BlobProviderArgs::default(),
                network_args: RollupNodeNetworkArgs::default(),
                gas_price_oracle_args: RollupNodeGasPriceOracleArgs::default(),
                consensus_args: ConsensusArgs::noop(),
                database: None,
                rpc_args: RpcArgs::default(),
                pprof_args: PprofArgs::default(),
                remote_block_source_args: RemoteBlockSourceArgs {
                    enabled: true,
                    url: Some("http://localhost:8545".parse().unwrap()),
                    poll_interval_ms: 100,
                    build,
                },
                require_l1_data_fee_buffer: false,
            };

            let result = config.validate();
            assert!(result.is_err(), "build={build} must be rejected");
            assert!(result
                .unwrap_err()
                .contains("sequencer.auto-start conflicts with remote-source.enabled"));
        }
    }

    /// The startup refusals have been corrected repeatedly and had no coverage:
    /// flipping the final `<` to `<=` refuses every node whose anchor sits
    /// exactly at finality, and nothing else in the suite would notice.
    #[test]
    fn startup_refusal_table() {
        // (l2_head, provider_missing, fcs_is_genesis, finalized) -> refuses?
        let cases: &[(u64, bool, bool, u64, bool)] = &[
            // A fresh database must bootstrap whatever the provider says.
            (0, true, true, 0, false),
            (0, false, true, 50, false),
            // Populated, no usable forkchoice state.
            (100, true, false, 0, true),
            // Populated, provider answered from genesis.
            (100, false, true, 0, true),
            // Anchor below finality: an unwind committed past it.
            (100, false, false, 140, true),
            // BOUNDARY: an anchor exactly at finality is the steady state.
            (100, false, false, 100, false),
            // Anchor above finality is ordinary.
            (100, false, false, 40, false),
        ];
        for (head, missing, genesis, finalized, want) in cases {
            let got = startup_refusal(*head, *missing, *genesis, *finalized);
            assert_eq!(
                got.is_some(),
                *want,
                "startup_refusal({head}, {missing}, {genesis}, {finalized}) -> {got:?}"
            );
        }
    }

    /// The `l1.url` rule is otherwise untested: every other config in this
    /// module sets a URL in order to SATISFY it, so deleting the rule would
    /// leave the whole suite green. The second case pins the pass-52 gating —
    /// note the unit lane builds `--all-features`, so `test-utils` is live here
    /// and the exemption applies.
    #[test]
    fn test_validate_requires_l1_url() {
        let mut config = rotation_watchdog_config();
        config.consensus_args = ConsensusArgs::noop();
        config.sequencer_args = SequencerArgs::default();
        config.l1_provider_args = L1ProviderArgs::default();

        // The exemption is keyed ONLY on the cfg, matching the mock-watcher
        // fallback it guards — `scroll-debug` reaches that fallback with
        // `test = false`. So on a build carrying test-utils (this one: the unit
        // lane builds --all-features) the URL is optional either way, and on the
        // shipped binary it is required either way.
        for test in [false, true] {
            config.test_args = TestArgs { test, skip_l1_synced: false };
            let result = config.validate();
            assert_eq!(result.is_ok(), cfg!(feature = "test-utils"), "test={test}: {result:?}");
            if let Err(err) = result {
                assert!(err.contains("l1.url is required"), "{err}");
            }
        }
    }

    /// The mirror image: without `sequencer.enabled` no Sequencer is built, so
    /// `auto-start` starts nothing and the pair is inert. A templated fleet
    /// layout that sets it for every role must stay valid on the read-only
    /// followers — the same shape the adjacent warn arm blesses.
    #[test]
    fn test_validate_remote_source_with_inert_auto_start_is_accepted() {
        let config = ScrollRollupNodeConfig {
            test_args: TestArgs::default(),
            sequencer_args: SequencerArgs {
                sequencer_enabled: false,
                auto_start: true,
                ..Default::default()
            },
            signer_args: SignerArgs::default(),
            database_args: RollupNodeDatabaseArgs::default(),
            engine_driver_args: EngineDriverArgs::default(),
            chain_orchestrator_args: ChainOrchestratorArgs::default(),
            l1_provider_args: L1ProviderArgs {
                // validate() now requires an L1 provider: without one the L1
                // watcher is never built and startup aborts.
                url: Some("http://localhost:8545".parse().unwrap()),
                ..Default::default()
            },
            blob_provider_args: BlobProviderArgs::default(),
            network_args: RollupNodeNetworkArgs::default(),
            gas_price_oracle_args: RollupNodeGasPriceOracleArgs::default(),
            consensus_args: ConsensusArgs::noop(),
            database: None,
            rpc_args: RpcArgs::default(),
            pprof_args: PprofArgs::default(),
            remote_block_source_args: RemoteBlockSourceArgs {
                enabled: true,
                url: Some("http://localhost:8545".parse().unwrap()),
                poll_interval_ms: 100,
                build: false,
            },
            require_l1_data_fee_buffer: false,
        };

        assert!(config.validate().is_ok(), "an inert auto-start must not block a read-only mirror");
    }

    #[test]
    fn test_validate_remote_source_zero_poll_interval_fails() {
        let config = ScrollRollupNodeConfig {
            test_args: TestArgs::default(),
            sequencer_args: SequencerArgs { sequencer_enabled: true, ..Default::default() },
            signer_args: SignerArgs::default(),
            database_args: RollupNodeDatabaseArgs::default(),
            engine_driver_args: EngineDriverArgs::default(),
            chain_orchestrator_args: ChainOrchestratorArgs::default(),
            l1_provider_args: L1ProviderArgs {
                // validate() requires an L1 provider: without one the L1 watcher
                // is never built and startup aborts.
                url: Some("http://localhost:8545".parse().unwrap()),
                ..Default::default()
            },
            blob_provider_args: BlobProviderArgs::default(),
            network_args: RollupNodeNetworkArgs::default(),
            gas_price_oracle_args: RollupNodeGasPriceOracleArgs::default(),
            consensus_args: ConsensusArgs::noop(),
            database: None,
            rpc_args: RpcArgs::default(),
            pprof_args: PprofArgs::default(),
            remote_block_source_args: RemoteBlockSourceArgs {
                enabled: true,
                url: Some("http://localhost:8545".parse().unwrap()),
                poll_interval_ms: 0,
                build: true,
            },
            require_l1_data_fee_buffer: false,
        };

        let err = config.validate().unwrap_err();
        assert!(err.contains("remote-source.poll-interval-ms must be greater than 0"), "{err}");
    }

    #[test]
    fn test_validate_remote_source_non_http_scheme_fails() {
        let mut config = ScrollRollupNodeConfig {
            test_args: TestArgs::default(),
            sequencer_args: SequencerArgs { sequencer_enabled: true, ..Default::default() },
            signer_args: SignerArgs::default(),
            database_args: RollupNodeDatabaseArgs::default(),
            engine_driver_args: EngineDriverArgs::default(),
            chain_orchestrator_args: ChainOrchestratorArgs::default(),
            l1_provider_args: L1ProviderArgs {
                // validate() requires an L1 provider: without one the L1 watcher
                // is never built and startup aborts.
                url: Some("http://localhost:8545".parse().unwrap()),
                ..Default::default()
            },
            blob_provider_args: BlobProviderArgs::default(),
            network_args: RollupNodeNetworkArgs::default(),
            gas_price_oracle_args: RollupNodeGasPriceOracleArgs::default(),
            consensus_args: ConsensusArgs::noop(),
            database: None,
            rpc_args: RpcArgs::default(),
            pprof_args: PprofArgs::default(),
            remote_block_source_args: RemoteBlockSourceArgs {
                enabled: true,
                url: Some("ws://localhost:8545".parse().unwrap()),
                poll_interval_ms: 100,
                build: true,
            },
            require_l1_data_fee_buffer: false,
        };

        let err = config.validate().unwrap_err();
        assert!(err.contains("must use http or https"), "{err}");

        // The same URL is ignored (warn only) when the add-on is disabled.
        config.remote_block_source_args.enabled = false;
        config.remote_block_source_args.build = false;
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_sequencer_enabled_with_both_signers_fails() {
        let config = ScrollRollupNodeConfig {
            test_args: TestArgs::default(),
            sequencer_args: SequencerArgs { sequencer_enabled: true, ..Default::default() },
            signer_args: SignerArgs {
                key_file: Some(PathBuf::from("/path/to/key")),
                aws_kms_key_id: Some("key-id".to_string()),
                private_key: None,
            },
            database_args: RollupNodeDatabaseArgs::default(),
            engine_driver_args: EngineDriverArgs::default(),
            chain_orchestrator_args: ChainOrchestratorArgs::default(),
            l1_provider_args: L1ProviderArgs {
                // validate() requires an L1 provider: without one the L1 watcher
                // is never built and startup aborts.
                url: Some("http://localhost:8545".parse().unwrap()),
                ..Default::default()
            },
            blob_provider_args: BlobProviderArgs::default(),
            network_args: RollupNodeNetworkArgs::default(),
            gas_price_oracle_args: RollupNodeGasPriceOracleArgs::default(),
            consensus_args: ConsensusArgs {
                algorithm: ConsensusAlgorithm::SystemContract,
                authorized_signer: None,
                exit_on_signer_rotation: false,
            },
            database: None,
            rpc_args: RpcArgs::default(),
            pprof_args: PprofArgs::default(),
            remote_block_source_args: RemoteBlockSourceArgs::default(),
            require_l1_data_fee_buffer: false,
        };

        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Cannot specify more than one signer key source"));
    }

    #[test]
    fn test_validate_sequencer_enabled_with_key_file_succeeds() {
        let config = ScrollRollupNodeConfig {
            test_args: TestArgs::default(),
            sequencer_args: SequencerArgs { sequencer_enabled: true, ..Default::default() },
            signer_args: SignerArgs {
                key_file: Some(PathBuf::from("/path/to/key")),
                aws_kms_key_id: None,
                private_key: None,
            },
            database_args: RollupNodeDatabaseArgs::default(),
            chain_orchestrator_args: ChainOrchestratorArgs::default(),
            engine_driver_args: EngineDriverArgs::default(),
            l1_provider_args: L1ProviderArgs {
                // validate() requires an L1 provider: without one the L1 watcher
                // is never built and startup aborts.
                url: Some("http://localhost:8545".parse().unwrap()),
                ..Default::default()
            },
            blob_provider_args: BlobProviderArgs::default(),
            network_args: RollupNodeNetworkArgs::default(),
            gas_price_oracle_args: RollupNodeGasPriceOracleArgs::default(),
            consensus_args: ConsensusArgs::noop(),
            database: None,
            rpc_args: RpcArgs::default(),
            pprof_args: PprofArgs::default(),
            remote_block_source_args: RemoteBlockSourceArgs::default(),
            require_l1_data_fee_buffer: false,
        };

        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_sequencer_enabled_with_aws_kms_succeeds() {
        let config = ScrollRollupNodeConfig {
            test_args: TestArgs::default(),
            sequencer_args: SequencerArgs { sequencer_enabled: true, ..Default::default() },
            signer_args: SignerArgs {
                key_file: None,
                aws_kms_key_id: Some("key-id".to_string()),
                private_key: None,
            },
            database_args: RollupNodeDatabaseArgs::default(),
            engine_driver_args: EngineDriverArgs::default(),
            chain_orchestrator_args: ChainOrchestratorArgs::default(),
            l1_provider_args: L1ProviderArgs {
                // validate() now requires an L1 provider: without one the L1
                // watcher is never built and startup aborts.
                url: Some("http://localhost:8545".parse().unwrap()),
                ..Default::default()
            },
            blob_provider_args: BlobProviderArgs::default(),
            network_args: RollupNodeNetworkArgs::default(),
            gas_price_oracle_args: RollupNodeGasPriceOracleArgs::default(),
            consensus_args: ConsensusArgs::noop(),
            database: None,
            rpc_args: RpcArgs::default(),
            pprof_args: PprofArgs::default(),
            remote_block_source_args: RemoteBlockSourceArgs::default(),
            require_l1_data_fee_buffer: false,
        };

        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_sequencer_disabled_without_any_signer_succeeds() {
        let config = ScrollRollupNodeConfig {
            test_args: TestArgs::default(),
            sequencer_args: SequencerArgs { sequencer_enabled: false, ..Default::default() },
            signer_args: SignerArgs { key_file: None, aws_kms_key_id: None, private_key: None },
            database_args: RollupNodeDatabaseArgs::default(),
            engine_driver_args: EngineDriverArgs::default(),
            chain_orchestrator_args: ChainOrchestratorArgs::default(),
            l1_provider_args: L1ProviderArgs {
                // validate() now requires an L1 provider: without one the L1
                // watcher is never built and startup aborts.
                url: Some("http://localhost:8545".parse().unwrap()),
                ..Default::default()
            },
            blob_provider_args: BlobProviderArgs::default(),
            network_args: RollupNodeNetworkArgs::default(),
            gas_price_oracle_args: RollupNodeGasPriceOracleArgs::default(),
            consensus_args: ConsensusArgs::noop(),
            database: None,
            rpc_args: RpcArgs::default(),
            pprof_args: PprofArgs::default(),
            remote_block_source_args: RemoteBlockSourceArgs::default(),
            require_l1_data_fee_buffer: false,
        };

        assert!(config.validate().is_ok());
    }
}
