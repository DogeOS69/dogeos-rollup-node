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
    /// The sequencer's payload building duration (milliseconds), used to size
    /// the build-outcome wait budget.
    payload_building_duration_ms: u64,
    /// Number of consecutive failed poll ticks, reported in the error logs.
    consecutive_failures: u64,
    /// Whether a requested build is still owed its outcome. Set before a
    /// `BuildBlock` command and cleared once the outcome arrives (or the
    /// settlement gives up). While set, `settle_owed_build` runs before any
    /// import: it re-issues the build once its cancellation has been
    /// *observed* (`pending_build_cancelled`), keeps waiting while the job
    /// may still be in flight, and gives up after
    /// [`MAX_PENDING_BUILD_RETRIES`] settlement attempts.
    pending_build: bool,
    /// Whether the owed build's cancellation has been observed
    /// (`PayloadBuildingJobCancelled` consumed). Only then is re-issuing
    /// race-free — the job is provably gone and, with a single build
    /// requester, nothing else can have started one.
    pending_build_cancelled: bool,
    /// Consecutive settlement attempts for the owed build. Bounded so an
    /// outcome that never arrives does not head-of-line-block imports
    /// forever.
    pending_build_retries: u8,
    /// Number of owed builds that were given up after
    /// [`MAX_PENDING_BUILD_RETRIES`] settlement attempts, kept for the error
    /// logs (this add-on currently exports no metrics).
    builds_abandoned: u64,
    /// When the last "Sync error" line was logged and what it said, used to
    /// rate-limit repeated identical errors by elapsed time while always
    /// logging a *changed* error immediately.
    last_error_log: Option<(std::time::Instant, String)>,
}

/// After this many consecutive failed settlement attempts for an owed build,
/// give it up and resume importing: a build outcome that never arrives must
/// not stall the import loop indefinitely.
const MAX_PENDING_BUILD_RETRIES: u8 = 5;

/// Minimum interval between repeated identical "Sync error" log lines.
const ERROR_LOG_INTERVAL: Duration = Duration::from_secs(10);

/// The outcome of waiting for a requested build.
enum BuildOutcome {
    /// The build completed (a block at or above the expected height was
    /// sequenced, or building was skipped for an empty payload).
    Landed,
    /// The payload building job was cancelled; no outcome will arrive.
    Cancelled,
}

/// Marker for errors that cannot resolve on their own: retrying the poll loop
/// after one of these is pointless (the orchestrator is gone or shutting
/// down), so `run_until_shutdown` surfaces them instead of spinning.
#[derive(Debug)]
struct TerminalSyncError;

impl std::fmt::Display for TerminalSyncError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "terminal remote block source error")
    }
}

impl std::error::Error for TerminalSyncError {}

/// Returns an error that `run_until_shutdown` treats as fatal.
fn terminal_error(msg: &'static str) -> eyre::Report {
    eyre::Report::new(TerminalSyncError).wrap_err(msg)
}

/// A closed command channel means the orchestrator is gone — terminal, not
/// retryable.
fn orchestrator_gone(e: tokio::sync::oneshot::error::RecvError) -> eyre::Report {
    eyre::Report::new(TerminalSyncError)
        .wrap_err(format!("chain orchestrator command channel closed: {e}"))
}

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
        payload_building_duration_ms: u64,
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
            payload_building_duration_ms,
            last_imported_block: None,
            consecutive_failures: 0,
            pending_build: false,
            pending_build_cancelled: false,
            pending_build_retries: 0,
            builds_abandoned: 0,
            last_error_log: None,
        })
    }

    /// Clears all owed-build bookkeeping (outcome arrived or settlement gave
    /// up).
    const fn clear_pending_build(&mut self) {
        self.pending_build = false;
        self.pending_build_cancelled = false;
        self.pending_build_retries = 0;
    }

    /// Determines the last imported block by finding the highest common block
    /// between the local chain and the remote node.
    ///
    /// Called every poll tick until it succeeds — this call *is* the first
    /// contact with the remote; a failure (e.g. the remote is not up yet) is
    /// retried on the next tick.
    async fn init_last_imported_block(&self) -> eyre::Result<u64> {
        let local_head = self
            .orchestrator_handle
            .status()
            .await
            .map_err(orchestrator_gone)?
            .l2
            .fcs
            .head_block_info()
            .number;
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
                        Ok(()) => {
                            self.consecutive_failures = 0;
                            self.last_error_log = None;
                        }
                        Err(e) => {
                            // Errors that cannot resolve on their own (the
                            // orchestrator is gone or shutting down) must not
                            // be retried at poll cadence forever.
                            if e.chain().any(|c| c.downcast_ref::<TerminalSyncError>().is_some()) {
                                tracing::error!(target: "scroll::remote_source", ?e, "Terminal sync error; stopping remote block source");
                                return Err(e);
                            }
                            self.consecutive_failures += 1;
                            // Rate-limit identical errors by elapsed time (at
                            // the default 100ms poll interval an unreachable
                            // remote would otherwise emit ~10 identical
                            // lines/second), but always log a changed error
                            // immediately.
                            let msg = format!("{e:#}");
                            let now = std::time::Instant::now();
                            let should_log = match &self.last_error_log {
                                Some((at, prev)) => {
                                    *prev != msg ||
                                        now.duration_since(*at) >= ERROR_LOG_INTERVAL
                                }
                                None => true,
                            };
                            if should_log {
                                tracing::error!(
                                    target: "scroll::remote_source",
                                    ?e,
                                    consecutive_failures = self.consecutive_failures,
                                    builds_abandoned = self.builds_abandoned,
                                    initialized = self.last_imported_block.is_some(),
                                    url = ?self.config.url,
                                    "Sync error"
                                );
                                self.last_error_log = Some((now, msg));
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Waits (bounded) for the outcome of a requested build, without issuing
    /// any command.
    ///
    /// A `BlockSequenced` is only accepted at or above `expected_number`
    /// (stale outcomes are strictly lower-numbered), so an outcome from a
    /// previous build cannot be attributed to this request.
    /// `BlockBuildingSkipped` carries no number and is accepted as landed.
    /// What makes attribution sound is the single-build-requester assumption
    /// *plus* `import_chain` cancelling any in-flight job before each import:
    /// together they mean any outcome observed here belongs to this request.
    /// Config-level violations of the assumption are rejected by `validate()`
    /// (no `sequencer.auto-start` with `remote-source.build`), but the
    /// `rollupNodeAdmin_enableAutomaticSequencing` RPC can still start the
    /// timer at runtime and break it — do not enable it on a remote-source
    /// node.
    async fn await_build_outcome(&mut self, expected_number: u64) -> eyre::Result<BuildOutcome> {
        tracing::debug!(target: "scroll::remote_source", expected_number, "Waiting for block to be built...");
        // The wait covers a payload building job, so size it from the
        // configured payload building duration (with generous margin) rather
        // than the unrelated poll interval; the clamp bounds the worst-case
        // import stall a missed outcome can cause across the settlement
        // budget.
        let wait_budget =
            Duration::from_millis(self.payload_building_duration_ms.saturating_mul(5))
                .clamp(Duration::from_secs(5), Duration::from_secs(60));
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
                        break Ok(BuildOutcome::Landed);
                    }
                    Some(ChainOrchestratorEvent::BlockBuildingSkipped) => {
                        tracing::debug!(target: "scroll::remote_source", "Block building skipped (empty block)");
                        break Ok(BuildOutcome::Landed);
                    }
                    Some(ChainOrchestratorEvent::PayloadBuildingJobCancelled) => {
                        break Ok(BuildOutcome::Cancelled);
                    }
                    Some(ChainOrchestratorEvent::Shutdown) => {
                        break Err(terminal_error("Chain orchestrator is shutting down"));
                    }
                    Some(_) => {
                        // Ignore other events, keep waiting
                    }
                    None => {
                        break Err(terminal_error("Event stream ended unexpectedly"));
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
        })?
    }

    /// Requests block building and waits (bounded) for the outcome. The
    /// command may coalesce with an already in-flight job; for the remote
    /// source that job is never stale, because `import_chain` cancels the job
    /// slot before every import.
    ///
    /// Stale outcomes queued by earlier, given-up requests are drained first
    /// so they cannot be attributed to this request. `pending_build` stays set
    /// on failure so the build is settled on the next poll tick instead of
    /// being lost.
    async fn trigger_build_and_await(&mut self, expected_number: u64) -> eyre::Result<()> {
        // Drop build outcomes left over from earlier requests (e.g. a build
        // that completed after its settlement was given up).
        while let Some(event) = self.events.next().now_or_never() {
            match event {
                Some(ChainOrchestratorEvent::Shutdown) => {
                    return Err(terminal_error("Chain orchestrator is shutting down"));
                }
                Some(_) => {}
                None => return Err(terminal_error("Event stream ended unexpectedly")),
            }
        }

        self.pending_build = true;
        self.pending_build_cancelled = false;
        self.orchestrator_handle.try_build_block().map_err(|e| {
            eyre::Report::new(TerminalSyncError).wrap_err(format!("failed to send BuildBlock: {e}"))
        })?;

        match self.await_build_outcome(expected_number).await? {
            BuildOutcome::Landed => {
                self.clear_pending_build();
                Ok(())
            }
            BuildOutcome::Cancelled => {
                // Record the observation: it is what licenses the next
                // settlement to re-issue this build race-free.
                self.pending_build_cancelled = true;
                Err(eyre::eyre!("The payload building job was cancelled before completing"))
            }
        }
    }

    /// Settles a build owed from a previous tick without ever double-building.
    ///
    /// `status()` flows through the same FIFO command channel as `BuildBlock`,
    /// so once it returns, the owed command has been processed: the job either
    /// landed (head advanced past the imported block), completed as a skipped
    /// empty build (head unchanged — only re-observing the event settles
    /// this case), was cancelled, or is still in flight. Only an *observed*
    /// `PayloadBuildingJobCancelled` (recorded in `pending_build_cancelled` by whichever wait
    /// consumed it) proves no outcome will ever arrive — re-issuing is limited to that
    /// case, which (with a single build requester) is race-free. On a plain
    /// timeout the job may still be running, so we keep waiting on later
    /// ticks — bounded by [`MAX_PENDING_BUILD_RETRIES`], after which the
    /// build is abandoned and imports resume — rather than risk building the
    /// same height twice.
    async fn settle_owed_build(&mut self) -> eyre::Result<()> {
        let last_imported = self.last_imported_block.expect("initialized above");
        let head = self
            .orchestrator_handle
            .status()
            .await
            .map_err(orchestrator_gone)?
            .l2
            .fcs
            .head_block_info()
            .number;
        if head > last_imported {
            // The build landed after its wait timed out.
            self.clear_pending_build();
            return Ok(());
        }

        // Every settlement attempt — a plain wait OR a cancellation-driven
        // re-issue — consumes the same bounded budget: repeated cancellations
        // (e.g. payload creation failing every time) must not re-issue
        // forever and stall imports.
        if self.pending_build_retries >= MAX_PENDING_BUILD_RETRIES {
            self.builds_abandoned += 1;
            tracing::error!(
                target: "scroll::remote_source",
                retries = self.pending_build_retries,
                builds_abandoned = self.builds_abandoned,
                last_imported,
                head,
                "Giving up on settling an owed build; resuming imports"
            );
            self.clear_pending_build();
            return Ok(());
        }
        self.pending_build_retries += 1;

        let expected = last_imported + 1;

        // The cancellation was already observed (by the wait that set
        // pending_build_cancelled): the job is provably gone, so re-issuing
        // now cannot double-build.
        if std::mem::take(&mut self.pending_build_cancelled) {
            return self.trigger_build_and_await(expected).await;
        }

        match self.await_build_outcome(expected).await? {
            BuildOutcome::Landed => {
                self.clear_pending_build();
                Ok(())
            }
            BuildOutcome::Cancelled => self.trigger_build_and_await(expected).await,
        }
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
        // the requested block would be lost.
        if self.pending_build {
            self.settle_owed_build().await?;
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
                    return Err(orchestrator_gone(e));
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

            if !self.orchestrator_handle.status().await.map_err(orchestrator_gone)?.is_synced() {
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
