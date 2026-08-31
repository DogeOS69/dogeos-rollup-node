//! Remote block source add-on for importing blocks from a remote L2 node
//! and building new blocks on top.

use crate::args::RemoteBlockSourceArgs;
use alloy_primitives::Signature;
use alloy_provider::{Provider, ProviderBuilder, RootProvider};
use alloy_rpc_client::RpcClient;
use alloy_transport::layers::RetryBackoffLayer;
use dogeos_rpc_types::Scroll;
use futures::{FutureExt, StreamExt};
use reth_network_api::{FullNetwork, PeerId};
use reth_provider::BlockReader;
use reth_tokio_util::EventStream;
use rollup_node_chain_orchestrator::{ChainOrchestratorEvent, ChainOrchestratorHandle};
use scroll_network::{DogeosNetworkPrimitives, NewBlockWithPeer};
use tokio::time::{interval, Duration};

/// Remote block source add-on that imports blocks from a trusted remote L2 node
/// and triggers block building on top of each imported block.
#[derive(Debug)]
pub struct RemoteBlockSourceAddOn<N, P>
where
    N: FullNetwork<Primitives = DogeosNetworkPrimitives>,
{
    /// Configuration for the remote block source.
    config: RemoteBlockSourceArgs,
    /// Handle to the chain orchestrator for sending commands.
    orchestrator_handle: ChainOrchestratorHandle<N>,
    /// An event stream for listening to chain orchestrator events, used to wait for block build
    /// completion.
    events: EventStream<ChainOrchestratorEvent>,
    /// A provider for the remote node, used to fetch blocks and block information.
    remote: RootProvider<Scroll>,
    /// Local block reader, used to find the highest common block with the remote.
    provider: P,
    /// Tracks the last block number we imported from remote.
    /// This is different from local head because we build blocks on top of imports.
    ///
    /// `None` until the remote has been reached once and the highest common
    /// block determined — construction must not depend on the remote being up
    /// (issue #38): a connection error at startup used to abort the whole node.
    last_imported_block: Option<u64>,
    /// Number of consecutive failed poll ticks, used to rate-limit error logs
    /// while the remote is unreachable.
    consecutive_failures: u64,
    /// Whether a requested build is still owed its outcome. Set before a
    /// `BuildBlock` command and cleared once the outcome arrives; if the
    /// awaited job is cancelled (its outcome never arrives), the build is
    /// re-issued on the next poll tick instead of being lost — the import
    /// that requested it has already advanced `last_imported_block`.
    pending_build: bool,
    /// Consecutive failed re-issues of an owed build. Bounded so an owed
    /// build whose outcome can never arrive does not head-of-line-block
    /// imports forever.
    pending_build_retries: u8,
}

/// After this many consecutive failed re-issues of an owed build, give it up
/// and resume importing: a build outcome that never arrives must not stall
/// the import loop indefinitely.
const MAX_PENDING_BUILD_RETRIES: u8 = 5;

impl<N, P> RemoteBlockSourceAddOn<N, P>
where
    N: FullNetwork<Primitives = DogeosNetworkPrimitives> + Send + Sync + 'static,
    P: BlockReader,
{
    /// Creates a new remote block source add-on.
    ///
    /// Performs no remote I/O: the resume point is determined lazily on the
    /// first successful poll, where errors are logged and retried at poll
    /// cadence instead of failing node launch.
    pub async fn new(
        config: RemoteBlockSourceArgs,
        handle: ChainOrchestratorHandle<N>,
        provider: P,
    ) -> eyre::Result<Self> {
        // Build remote provider with retry layer.
        let Some(url) = config.url.clone() else {
            tracing::error!(target: "scroll::remote_source", "URL required when remote-source is enabled");
            return Err(eyre::eyre!("URL required when remote-source is enabled"));
        };
        let retry_layer = RetryBackoffLayer::new(10, 100, 330);
        let client = RpcClient::builder().layer(retry_layer).http(url);
        let remote = ProviderBuilder::<_, _, Scroll>::default().connect_client(client);

        // Get event listener for waiting on block completion
        let events = match handle.get_event_listener().await {
            Ok(stream) => stream,
            Err(e) => {
                tracing::error!(target: "scroll::remote_source", ?e, "Failed to get event listener");
                return Err(eyre::eyre!(e));
            }
        };

        Ok(Self {
            config,
            orchestrator_handle: handle,
            events,
            remote,
            provider,
            last_imported_block: None,
            consecutive_failures: 0,
            pending_build: false,
            pending_build_retries: 0,
        })
    }

    /// Determines the last imported block by finding the highest common block
    /// between the local chain and the remote node.
    ///
    /// Called on the first successful contact with the remote; a failure here
    /// (e.g. the remote is not up yet) is retried on the next poll tick.
    async fn init_last_imported_block(&self) -> eyre::Result<u64> {
        let local_head = self.orchestrator_handle.status().await?.l2.fcs.head_block_info().number;
        let remote_head = self.remote.get_block_number().await?;

        let last_imported_block;
        let mut search = local_head.min(remote_head);
        loop {
            if search == 0 {
                // Genesis is always a common block (same chain spec assumed).
                last_imported_block = 0;
                break;
            }
            let local_hash = self.provider.block_hash(search)?;
            let remote_block = self.remote.get_block_by_number(search.into()).await?;
            match (local_hash, remote_block) {
                (Some(lh), Some(rb)) if lh == rb.header.hash => {
                    last_imported_block = search;
                    break;
                }
                _ => {
                    search = search.saturating_sub(1);
                }
            }
        }
        tracing::info!(
            target: "scroll::remote_source",
            last_imported_block,
            local_head,
            remote_head,
            "Determined highest common block with remote"
        );
        Ok(last_imported_block)
    }

    /// Runs the remote block source until shutdown.
    pub async fn run_until_shutdown(
        mut self,
        mut shutdown: impl std::future::Future<Output = ()> + Unpin,
    ) -> eyre::Result<()> {
        let mut poll_interval = interval(Duration::from_millis(self.config.poll_interval_ms));

        loop {
            tokio::select! {
                biased;
                _guard = &mut shutdown => break,
                _ = poll_interval.tick() => {
                    match self.follow_and_build().await {
                        Ok(()) => self.consecutive_failures = 0,
                        Err(e) => {
                            self.consecutive_failures += 1;
                            // Log the first failure and then every 50th: at the
                            // default 100ms poll interval an unreachable remote
                            // would otherwise emit ~10 identical lines/second.
                            if self.consecutive_failures == 1 ||
                                self.consecutive_failures.is_multiple_of(50)
                            {
                                tracing::error!(
                                    target: "scroll::remote_source",
                                    ?e,
                                    consecutive_failures = self.consecutive_failures,
                                    initialized = self.last_imported_block.is_some(),
                                    url = ?self.config.url,
                                    "Sync error"
                                );
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Triggers block building on top of the current head and waits (bounded)
    /// for the outcome.
    ///
    /// Stale outcomes queued by earlier requests are drained first, and a
    /// `BlockSequenced` is only accepted at or above `expected_number` (stale
    /// outcomes are strictly lower-numbered), so an outcome from a previous
    /// build cannot be attributed to this request. `BlockBuildingSkipped`
    /// carries no number and is accepted post-drain; this relies on the
    /// remote-source node being the only build requester (it runs with
    /// `auto_start = false`, as in the shipped launch script). The wait is
    /// bounded and fails fast on `PayloadBuildingJobCancelled`; `pending_build`
    /// stays set on failure so the build is re-issued on the next poll tick.
    async fn trigger_build_and_await(&mut self, expected_number: u64) -> eyre::Result<()> {
        // Drop build outcomes left over from earlier requests (e.g. a build
        // that completed after its wait timed out).
        while let Some(event) = self.events.next().now_or_never() {
            match event {
                Some(ChainOrchestratorEvent::Shutdown) => {
                    return Err(eyre::eyre!("Chain orchestrator is shutting down"));
                }
                Some(_) => {}
                None => return Err(eyre::eyre!("Event stream ended unexpectedly")),
            }
        }

        self.pending_build = true;
        self.orchestrator_handle.build_block();

        tracing::debug!(target: "scroll::remote_source", expected_number, "Waiting for block to be built...");
        // The wait covers a payload building job (default duration 800ms), so
        // it must not shrink below that when the poll interval is tuned low.
        let wait_budget = Duration::from_millis(self.config.poll_interval_ms.saturating_mul(100))
            .max(Duration::from_secs(30));
        let events = &mut self.events;
        tokio::time::timeout(wait_budget, async {
            loop {
                match events.next().await {
                    Some(ChainOrchestratorEvent::BlockSequenced(block))
                        if block.header.number >= expected_number =>
                    {
                        tracing::info!(target: "scroll::remote_source",
                            block_number = block.header.number,
                            block_hash = ?block.hash_slow(),
                            "Block built successfully, proceeding to next");
                        break Ok(());
                    }
                    Some(ChainOrchestratorEvent::BlockBuildingSkipped) => {
                        tracing::debug!(target: "scroll::remote_source", "Block building skipped (empty block)");
                        break Ok(());
                    }
                    Some(ChainOrchestratorEvent::PayloadBuildingJobCancelled) => {
                        break Err(eyre::eyre!(
                            "The payload building job was cancelled before completing"
                        ));
                    }
                    Some(ChainOrchestratorEvent::Shutdown) => {
                        break Err(eyre::eyre!("Chain orchestrator is shutting down"));
                    }
                    Some(_) => {
                        // Ignore other events, keep waiting
                    }
                    None => {
                        break Err(eyre::eyre!("Event stream ended unexpectedly"));
                    }
                }
            }
        })
        .await
        .map_err(|_| {
            eyre::eyre!(
                "Timed out after {wait_budget:?} waiting for the build outcome of block \
                 {expected_number}"
            )
        })??;

        self.pending_build = false;
        self.pending_build_retries = 0;
        Ok(())
    }

    /// Follows the remote node and builds blocks on top of imported blocks.
    async fn follow_and_build(&mut self) -> eyre::Result<()> {
        // First successful contact with the remote determines the resume point.
        if self.last_imported_block.is_none() {
            let resume = self.init_last_imported_block().await?;
            self.last_imported_block = Some(resume);
        }

        // A build owed from a previous tick is settled before importing
        // anything else: its import already advanced `last_imported_block`,
        // so without this the head comparison below would report "synced" and
        // the requested block would be lost. The head answers whether the
        // build actually landed after its wait timed out (a completed build
        // puts the head one above the imported block); only a genuinely lost
        // build is re-issued, and only a bounded number of times so an
        // outcome that can never arrive does not stall imports forever.
        if self.pending_build {
            let last_imported = self.last_imported_block.expect("initialized above");
            let head = self.orchestrator_handle.status().await?.l2.fcs.head_block_info().number;
            if head > last_imported {
                // The build landed after its wait timed out.
                self.pending_build = false;
                self.pending_build_retries = 0;
            } else if self.pending_build_retries >= MAX_PENDING_BUILD_RETRIES {
                tracing::error!(
                    target: "scroll::remote_source",
                    retries = self.pending_build_retries,
                    last_imported,
                    head,
                    "Giving up on re-issuing an owed build; resuming imports"
                );
                self.pending_build = false;
                self.pending_build_retries = 0;
            } else {
                self.pending_build_retries += 1;
                self.trigger_build_and_await(head + 1).await?;
            }
        }

        loop {
            let last_imported = self.last_imported_block.expect("initialized above");

            // Get remote head
            let remote_block = self
                .remote
                .get_block_by_number(alloy_eips::BlockNumberOrTag::Latest)
                .full()
                .await?
                .ok_or_else(|| eyre::eyre!("Remote block not found"))?;

            let remote_head = remote_block.header.number;

            // Compare against last imported block
            if remote_head <= last_imported {
                tracing::trace!(target: "scroll::remote_source",
                    last_imported,
                    remote_head,
                    "Already synced with remote");
                return Ok(());
            }

            let blocks_behind = remote_head - last_imported;
            tracing::info!(target: "scroll::remote_source",
                last_imported,
                remote_head,
                blocks_behind,
                "Catching up");

            // Fetch and import the next block from remote
            let next_block_num = last_imported + 1;
            let block = self
                .remote
                .get_block_by_number(next_block_num.into())
                .full()
                .await?
                .ok_or_else(|| eyre::eyre!("Block {} not found", next_block_num))?
                .into_consensus()
                .map_transactions(|tx| tx.inner.into_inner());

            // Create NewBlockWithPeer with dummy peer_id and signature (trusted source)
            let block_with_peer = NewBlockWithPeer {
                peer_id: PeerId::default(),
                block,
                signature: Signature::new(Default::default(), Default::default(), false),
            };

            // Import the block (this will cause a reorg if we had a locally built block at this
            // height)
            let chain_import = match self.orchestrator_handle.import_block(block_with_peer).await {
                Ok(Ok(chain_import)) => {
                    self.last_imported_block = Some(next_block_num);
                    chain_import
                }
                Ok(Err(e)) => {
                    return Err(eyre::eyre!("Import block failed: {}", e));
                }
                Err(e) => {
                    return Err(eyre::eyre!("chain orchestrator command channel error: {}", e));
                }
            };

            if !chain_import.result.is_valid() {
                tracing::info!(target: "scroll::remote_source",
                    result = ?chain_import.result,
                    "Imported block is not valid according to forkchoice, skipping build");
                continue;
            }

            if !self.config.build {
                tracing::debug!(target: "scroll::remote_source", "Imported block is valid, but build is disabled, skipping build");
                continue;
            }

            if !self.orchestrator_handle.status().await?.is_synced() {
                tracing::debug!(target: "scroll::remote_source", "Imported block is valid, but orchestrator is not synced, skipping build");
                continue;
            }

            // Trigger block building on top of the imported block and wait
            // (bounded, identity-matched) for the outcome.
            self.trigger_build_and_await(next_block_num + 1).await?;

            // Loop continues to process next block
        }
    }
}
