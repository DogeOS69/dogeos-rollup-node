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
    /// Also reset to `None` whenever it can no longer be trusted (settlement's
    /// `Resync`/`Abandon` outcomes, and an import the engine did not apply),
    /// forcing a fresh common-ancestor walk on the next tick.
    last_imported_block: Option<u64>,
    /// Whether `init_last_imported_block` has ever succeeded. Terminal
    /// (node-killing) escalation of the walk's divergence verdicts is
    /// reserved for the FIRST initialization: after one success, a genesis
    /// mismatch or exhausted lookback is far more likely a misrouted or
    /// lagging remote backend than a wrong `--remote-source.url`, and the
    /// re-walk (the pointer resets at runtime now) retries at poll cadence
    /// instead of fail-stopping a healthy node.
    initialized_once: bool,
    /// The sequencer's payload building duration (milliseconds), used to size
    /// the build-outcome wait budget.
    payload_building_duration_ms: u64,
    /// Number of consecutive failed poll ticks, reported in the error logs.
    consecutive_failures: u64,
    /// Whether a requested build is still owed its outcome. Set before a
    /// `BuildBlock` command and cleared when the outcome arrives, when the
    /// head proves the debt moot (`Superseded`/`Resync` clear it with no
    /// outcome ever observed), or when the settlement gives up. While set,
    /// `settle_owed_build` runs before any import: it re-issues the build
    /// once its cancellation has been *observed* (`pending_build_cancelled`)
    /// and the fresh head check plus retry budget allow it, keeps waiting
    /// while the job may still be in flight, and gives up after
    /// [`MAX_PENDING_BUILD_RETRIES`] settlement attempts.
    pending_build: bool,
    /// Whether the owed build's cancellation has been observed
    /// (`PayloadBuildingJobCancelled` consumed). Only then is re-issuing
    /// race-free — the job is provably gone and, with a single build
    /// requester, nothing else can have started one. Necessary but not
    /// sufficient: the settlement's head checks and retry budget still gate
    /// the actual re-issue.
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
    /// rate-limit repeated errors by elapsed time (changed messages log
    /// promptly but with a floor).
    last_error_log: Option<(std::time::Instant, String)>,
    /// Errors suppressed by the rate limiter since the last emitted line,
    /// reported on the next emitted line so no fault leaves zero trace.
    suppressed_errors: u64,
}

/// After this many consecutive failed settlement attempts for an owed build,
/// give it up and resume importing: a build outcome that never arrives must
/// not stall the import loop indefinitely.
const MAX_PENDING_BUILD_RETRIES: u8 = 5;

/// Minimum interval between repeated identical "Sync error" log lines.
const ERROR_LOG_INTERVAL: Duration = Duration::from_secs(10);

/// Floor for logging a *changed* error message ahead of the full interval.
const ERROR_LOG_MIN_INTERVAL: Duration = Duration::from_secs(1);

/// How far below `min(local_head, remote_head)` the common-ancestor walk may
/// search before giving up: an unbounded walk against a wrong or flaky remote
/// never finishes and restarts from the top on every failure.
const MAX_ANCESTOR_LOOKBACK: u64 = 8192;

/// The outcome of waiting for a requested build.
enum BuildOutcome {
    /// The build completed (a block at or above the expected height was
    /// sequenced, or building was skipped for an empty payload).
    Landed,
    /// The payload building job was cancelled; no outcome will arrive.
    Cancelled,
}

/// The action `settle_owed_build` takes for an owed build, decided purely
/// from `(head, expected, cancellation_observed, retries)` so the state
/// machine is table-testable without fixtures or timing.
#[derive(Debug, PartialEq, Eq)]
enum SettleAction {
    /// The build landed at the expected height; clear the debt.
    Landed,
    /// The head moved past the expected height for another reason; the owed
    /// build is moot.
    Superseded,
    /// The local head rewound below the owed build's parent (reorg or
    /// administrative rewind): the resume pointer is stale — drop the debt
    /// and re-derive the common ancestor.
    Resync,
    /// The settlement budget is exhausted; abandon the build, re-derive the
    /// resume pointer (an outcome that never arrived leaves it unreliable),
    /// and resume imports.
    Abandon,
    /// An observed cancellation proves the job is gone; a re-issue is
    /// race-free.
    Reissue,
    /// The job may still be in flight; keep waiting for its outcome.
    Wait,
}

/// Decides how to settle an owed build. Ordering is load-bearing:
/// - the head checks come first (an outcome that already materialized must never be re-issued — see
///   the `PayloadBuildingJobCancelled` contract note about the post-finalization emission sites;
///   and a rewound head proves the resume pointer is stale, so re-issuing against it would build
///   unreachable heights), and
/// - the budget check precedes the re-issue so repeated cancellations (e.g. payload creation
///   failing every time) cannot re-issue forever.
const fn settlement_decision(
    head: u64,
    expected: u64,
    cancellation_observed: bool,
    retries: u8,
) -> SettleAction {
    if head == expected {
        return SettleAction::Landed;
    }
    if head > expected {
        return SettleAction::Superseded;
    }
    // head < expected. A build in flight sits on expected - 1; anything lower
    // means the local head rewound and the resume pointer is stale.
    if head.saturating_add(1) < expected {
        return SettleAction::Resync;
    }
    if retries >= MAX_PENDING_BUILD_RETRIES {
        return SettleAction::Abandon;
    }
    if cancellation_observed {
        return SettleAction::Reissue;
    }
    SettleAction::Wait
}

/// Marker for genuine unrecoverable remote-source faults (e.g. a remote on a
/// different chain): retrying is pointless and the node should fail-stop so
/// the fault is visible.
#[derive(Debug)]
struct TerminalSyncError;

impl std::fmt::Display for TerminalSyncError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "terminal remote block source error")
    }
}

impl std::error::Error for TerminalSyncError {}

/// Returns an error that `run_until_shutdown` surfaces as fatal (the spawn
/// wrapper panics, fail-stopping the node).
fn terminal_error(msg: &'static str) -> eyre::Report {
    eyre::Report::new(TerminalSyncError).wrap_err(msg)
}

/// Marker for the orchestrator being gone or shutting down. This is never a
/// remote-source fault — the orchestrator fail-stops on its own errors and
/// returns cleanly on shutdown — so the run loop stops *gracefully* instead of
/// panicking a node that is already going down.
#[derive(Debug)]
struct OrchestratorGoneError;

impl std::fmt::Display for OrchestratorGoneError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "chain orchestrator is gone or shutting down")
    }
}

impl std::error::Error for OrchestratorGoneError {}

/// Returns an error that `run_until_shutdown` treats as a graceful stop.
fn orchestrator_gone(msg: &'static str) -> eyre::Report {
    eyre::Report::new(OrchestratorGoneError).wrap_err(msg)
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
            initialized_once: false,
            consecutive_failures: 0,
            pending_build: false,
            pending_build_cancelled: false,
            pending_build_retries: 0,
            builds_abandoned: 0,
            last_error_log: None,
            suppressed_errors: 0,
        })
    }

    /// Clears all owed-build bookkeeping (outcome arrived or settlement gave
    /// up).
    const fn clear_pending_build(&mut self) {
        self.pending_build = false;
        self.pending_build_cancelled = false;
        self.pending_build_retries = 0;
    }

    /// Classifies a `RecvError` on a command reply. The error is ambiguous: it
    /// can mean the orchestrator is gone (channel closed — the node is going
    /// down, stop gracefully) or that the command's handler failed and dropped
    /// its response sender (e.g. a transient database error — retryable). A
    /// genuine closure that races this check is classified as transient once
    /// and as gone on the next tick.
    fn classify_recv_error(&self, e: tokio::sync::oneshot::error::RecvError) -> eyre::Report {
        if self.orchestrator_handle.is_closed() {
            eyre::Report::new(OrchestratorGoneError)
                .wrap_err(format!("chain orchestrator command channel closed: {e}"))
        } else {
            eyre::eyre!("chain orchestrator dropped the command response (transient failure): {e}")
        }
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
            .map_err(|e| self.classify_recv_error(e))?
            .l2
            .fcs
            .head_block_info()
            .number;
        let remote_head = self.remote.get_block_number().await?;

        let start = local_head.min(remote_head);
        let floor = start.saturating_sub(MAX_ANCESTOR_LOOKBACK);
        let last_imported_block;
        let mut search = start;
        loop {
            if search == 0 {
                // Verify the chains actually share a genesis before declaring
                // it common: a remote on a different chain would otherwise
                // loop forever re-importing a block that can never connect.
                // Absence of either block is a transient condition (pruning,
                // lagging backend) — only two PRESENT but different hashes
                // prove divergence.
                let local_genesis = self.provider.block_hash(0)?;
                let remote_genesis = self.remote.get_block_by_number(0u64.into()).await?;
                match (local_genesis, remote_genesis) {
                    (Some(lh), Some(rb)) if lh == rb.header.hash => {}
                    (Some(lh), Some(rb)) => {
                        tracing::error!(
                            target: "scroll::remote_source",
                            local = ?lh,
                            remote = ?rb.header.hash,
                            "Remote genesis hash does not match the local chain"
                        );
                        if self.initialized_once {
                            // A remote that served our genesis before cannot
                            // have changed chains; treat as a transient
                            // backend fault and retry at poll cadence.
                            return Err(eyre::eyre!(
                                "remote genesis hash mismatch after a previously successful \
                                 initialization; retrying"
                            ));
                        }
                        return Err(terminal_error(
                            "remote genesis hash does not match the local chain; wrong \
                             --remote-source.url?",
                        ));
                    }
                    _ => {
                        return Err(eyre::eyre!(
                            "genesis block unavailable locally or remotely; retrying"
                        ));
                    }
                }
                last_imported_block = 0;
                break;
            }
            if search < floor {
                // The block at `floor` itself has been checked by now. This
                // walk only steps past PRESENT-but-different blocks, so
                // exhausting the window proves divergence, not availability.
                if self.initialized_once {
                    // See the genesis-mismatch arm: after one successful
                    // initialization this reads as a remote-side fault, not
                    // an operator error worth killing the node over.
                    return Err(eyre::eyre!(
                        "no common ancestor within the lookback window after a previously \
                         successful initialization; retrying"
                    ));
                }
                return Err(terminal_error(
                    "no common ancestor with the remote within the lookback window",
                ));
            }
            let local_hash = self.provider.block_hash(search)?;
            let remote_block = self.remote.get_block_by_number(search.into()).await?;
            match (local_hash, remote_block) {
                (Some(lh), Some(rb)) if lh == rb.header.hash => {
                    last_imported_block = search;
                    break;
                }
                (Some(_), Some(_)) => {
                    // Both present, hashes differ: genuinely divergent at this
                    // height — walk down.
                    if search.is_multiple_of(256) {
                        tracing::info!(
                            target: "scroll::remote_source",
                            search,
                            start,
                            "Searching for the highest common block with the remote"
                        );
                    }
                    search = search.saturating_sub(1);
                }
                _ => {
                    // One side lacks the block (pruned, lagging, or a fresh
                    // load-balancer backend): transient — retry the whole walk
                    // on the next tick rather than misreading absence as
                    // divergence.
                    return Err(eyre::eyre!(
                        "block {search} unavailable locally or remotely during the common-ancestor \
                         walk; retrying"
                    ));
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
        // interval() panics on a zero period; clamp a misconfigured value.
        let mut poll_interval =
            interval(Duration::from_millis(self.config.poll_interval_ms.max(1)));
        // A tick can legitimately take far longer than the interval (bounded
        // build-outcome waits, deep catch-up); Burst would then fire every
        // missed tick back-to-back, hammering an already-slow remote.
        poll_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                biased;
                _guard = &mut shutdown => break,
                _ = poll_interval.tick() => {
                    // Let shutdown preempt an in-flight tick: follow_and_build
                    // can block on multi-second waits.
                    let result = tokio::select! {
                        biased;
                        _guard = &mut shutdown => break,
                        r = self.follow_and_build() => r,
                    };
                    match result {
                        Ok(()) => {
                            // Keep last_error_log: clearing it on success
                            // would let an alternating success/failure pattern
                            // log at full poll cadence.
                            if self.suppressed_errors > 0 {
                                // Without this, a fault that appeared and
                                // cleared entirely inside one suppression
                                // window would leave zero trace.
                                tracing::warn!(
                                    target: "scroll::remote_source",
                                    suppressed_errors = self.suppressed_errors,
                                    "Recovered; some rate-limited sync errors were never logged"
                                );
                                self.suppressed_errors = 0;
                            }
                            self.consecutive_failures = 0;
                        }
                        Err(e) => {
                            // The orchestrator being gone or shutting down is
                            // not a remote-source fault: stop gracefully — the
                            // node is already going down.
                            if e.chain().any(|c| c.downcast_ref::<OrchestratorGoneError>().is_some()) {
                                tracing::info!(target: "scroll::remote_source", %e, "Chain orchestrator is gone; stopping remote block source");
                                break;
                            }
                            // Genuine unrecoverable faults must not be retried
                            // at poll cadence forever; surface them so the
                            // node fail-stops visibly.
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
                                    // A changed message logs promptly but not
                                    // unboundedly: dynamic messages (block
                                    // numbers, budgets) would otherwise defeat
                                    // the limiter entirely.
                                    let elapsed = now.duration_since(*at);
                                    elapsed >= ERROR_LOG_INTERVAL ||
                                        (*prev != msg && elapsed >= ERROR_LOG_MIN_INTERVAL)
                                }
                                None => true,
                            };
                            if should_log {
                                tracing::error!(
                                    target: "scroll::remote_source",
                                    ?e,
                                    consecutive_failures = self.consecutive_failures,
                                    builds_abandoned = self.builds_abandoned,
                                    suppressed_errors = self.suppressed_errors,
                                    initialized = self.last_imported_block.is_some(),
                                    url = ?self.config.url,
                                    "Sync error"
                                );
                                self.last_error_log = Some((now, msg));
                                self.suppressed_errors = 0;
                            } else {
                                self.suppressed_errors += 1;
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
    /// `BlockBuildingSkipped` carries the head it sat on and is accepted only
    /// when that head is the expected parent (or beyond), so stale outcomes
    /// from abandoned builds are excluded by identity, like `BlockSequenced`.
    /// `import_chain` additionally cancels any in-flight job as part of every
    /// successful import (after its validity checks), so by the time a build
    /// is requested here the job slot is empty. Config-level violations of
    /// the single-requester assumption are rejected by `validate()` (no
    /// `sequencer.auto-start` with `remote-source.build`), but the
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
                    Some(ChainOrchestratorEvent::BlockBuildingSkipped { head_block_number })
                        if head_block_number.saturating_add(1) >= expected_number =>
                    {
                        tracing::debug!(target: "scroll::remote_source", head_block_number, "Block building skipped (empty block)");
                        break Ok(BuildOutcome::Landed);
                    }
                    Some(ChainOrchestratorEvent::PayloadBuildingJobCancelled) => {
                        break Ok(BuildOutcome::Cancelled);
                    }
                    Some(ChainOrchestratorEvent::Shutdown) => {
                        break Err(orchestrator_gone("Chain orchestrator is shutting down"));
                    }
                    Some(_) => {
                        // Ignore other events, keep waiting
                    }
                    None => {
                        break Err(orchestrator_gone("Event stream ended unexpectedly"));
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
    /// slot as part of every successful import (after its validity checks).
    ///
    /// Stale outcomes queued by earlier, given-up requests are drained first
    /// so they cannot be attributed to this request. `pending_build` stays set
    /// on failure so the build is settled on the next poll tick instead of
    /// being lost.
    async fn trigger_build_and_await(&mut self, expected_number: u64) -> eyre::Result<()> {
        // Drop build outcomes left over from earlier requests (e.g. a build
        // that completed after its settlement was given up). Stale outcomes
        // are also excluded by identity: BlockSequenced and
        // BlockBuildingSkipped both carry heights and are gated against the
        // expected height in await_build_outcome.
        while let Some(event) = self.events.next().now_or_never() {
            match event {
                Some(ChainOrchestratorEvent::Shutdown) => {
                    return Err(orchestrator_gone("Chain orchestrator is shutting down"));
                }
                Some(_) => {}
                None => return Err(orchestrator_gone("Event stream ended unexpectedly")),
            }
        }

        self.pending_build = true;
        self.pending_build_cancelled = false;
        self.orchestrator_handle.try_build_block().map_err(|e| {
            eyre::Report::new(OrchestratorGoneError)
                .wrap_err(format!("failed to send BuildBlock: {e}"))
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
    /// `PayloadBuildingJobCancelled` proves no outcome will ever arrive — but
    /// only the flag carried over from an earlier tick in
    /// `pending_build_cancelled` licenses a re-issue here, against a head
    /// snapshot taken this tick. A cancellation consumed inline by the wait
    /// below is recorded and its re-issue deferred to the next tick's fresh
    /// head check (with a single build requester either path is race-free). On a plain
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
            .map_err(|e| self.classify_recv_error(e))?
            .l2
            .fcs
            .head_block_info()
            .number;
        let expected = last_imported + 1;

        match settlement_decision(
            head,
            expected,
            self.pending_build_cancelled,
            self.pending_build_retries,
        ) {
            SettleAction::Landed => {
                // The build landed after its wait timed out.
                self.clear_pending_build();
                Ok(())
            }
            SettleAction::Superseded => {
                // The head moved past the owed height for another reason
                // (e.g. derivation or a gossip import advanced it); the owed
                // build is moot — its parent has been superseded.
                tracing::info!(
                    target: "scroll::remote_source",
                    head,
                    expected,
                    "Owed build superseded by an unrelated head advance; dropping it and \
                     re-deriving the resume point"
                );
                self.clear_pending_build();
                // The head moved without us, so the pointer now trails it.
                // Importing `last_imported + 1` against an advanced head
                // would rewind the engine (ImportBlock bypasses the gossip
                // path's parent-linkage and safe-head guards) — re-derive
                // the common ancestor instead.
                self.last_imported_block = None;
                Ok(())
            }
            SettleAction::Resync => {
                tracing::warn!(
                    target: "scroll::remote_source",
                    head,
                    expected,
                    "Local head rewound below the owed build's parent; re-deriving the resume point"
                );
                self.clear_pending_build();
                self.last_imported_block = None;
                // Err for the same reason as Abandon below: Ok would reset
                // consecutive_failures (and could log a spurious recovery) on
                // a tick that only detected a rewound head.
                Err(eyre::eyre!(
                    "local head rewound below the owed build's parent (head {head}, expected \
                     {expected}); re-deriving the resume point"
                ))
            }
            SettleAction::Abandon => {
                self.builds_abandoned += 1;
                tracing::error!(
                    target: "scroll::remote_source",
                    retries = self.pending_build_retries,
                    builds_abandoned = self.builds_abandoned,
                    last_imported,
                    head,
                    "Giving up on settling an owed build; re-deriving the resume point and \
                     resuming imports"
                );
                self.clear_pending_build();
                self.last_imported_block = None;
                // Err (not Ok) so the run loop's failure accounting sees the
                // abandon: an Ok here would reset consecutive_failures on the
                // very tick that gave up, understating a permanent
                // build-failure loop to anyone watching the logs.
                Err(eyre::eyre!(
                    "gave up settling the owed build for block {expected} after \
                     {MAX_PENDING_BUILD_RETRIES} settlement attempts; re-deriving the resume point"
                ))
            }
            SettleAction::Reissue => {
                self.pending_build_retries += 1;
                self.pending_build_cancelled = false;
                self.trigger_build_and_await(expected).await
            }
            SettleAction::Wait => {
                self.pending_build_retries += 1;
                match self.await_build_outcome(expected).await? {
                    BuildOutcome::Landed => {
                        self.clear_pending_build();
                        Ok(())
                    }
                    BuildOutcome::Cancelled => {
                        // Preserve the observation instead of re-issuing
                        // inline: the head snapshot above is stale by now, and
                        // a post-finalization cancellation means the head has
                        // ALREADY advanced — the next settlement tick redoes
                        // the head check first and settles as Landed instead
                        // of double-building.
                        self.pending_build_cancelled = true;
                        Err(eyre::eyre!("The payload building job was cancelled before completing"))
                    }
                }
            }
        }
    }

    /// Follows the remote node and builds blocks on top of imported blocks.
    async fn follow_and_build(&mut self) -> eyre::Result<()> {
        // First successful contact with the remote determines the resume point.
        if self.last_imported_block.is_none() {
            let resume = self.init_last_imported_block().await?;
            self.last_imported_block = Some(resume);
            self.initialized_once = true;
        }

        // A build owed from a previous tick is settled before importing
        // anything else: its import already advanced `last_imported_block`,
        // so without this the head comparison below would report "synced" and
        // the requested block would be lost.
        if self.pending_build {
            self.settle_owed_build().await?;
            // Resync/Abandon clear the resume pointer to force a fresh
            // common-ancestor walk; hand control back so the next tick's
            // lazy-init guard performs it before any import.
            if self.last_imported_block.is_none() {
                return Ok(());
            }
        }

        loop {
            // Never unwrap here: the pointer is cleared by settlement above
            // and by unapplied imports below (which error out). A None means
            // "re-derive on the next tick", not an invariant violation worth
            // killing the node over.
            let Some(last_imported) = self.last_imported_block else {
                return Ok(());
            };

            // Get remote head (number only — fetching the full latest
            // block here pulled one unused body per catch-up iteration).
            let remote_head = self.remote.get_block_number().await?;

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
                Ok(Ok(chain_import)) => chain_import,
                Ok(Err(e)) => {
                    return Err(eyre::eyre!("Import block failed: {}", e));
                }
                Err(e) => {
                    return Err(self.classify_recv_error(e));
                }
            };

            if !chain_import.result.is_valid() {
                // The block was NOT applied (e.g. SYNCING: the EL does not
                // know the parent — a reorg or an in-progress pipeline sync).
                // Advancing the resume pointer would skip this block forever
                // and the pointer itself is now unreliable: force a fresh
                // common-ancestor walk on the next tick. Erroring out also
                // routes the fault through the rate-limited sync-error logger.
                tracing::warn!(target: "scroll::remote_source",
                    result = ?chain_import.result,
                    next_block_num,
                    "Imported block was not applied by forkchoice; re-deriving the resume point");
                self.last_imported_block = None;
                return Err(eyre::eyre!(
                    "block {next_block_num} was not applied by forkchoice; re-deriving the \
                     common ancestor"
                ));
            }
            self.last_imported_block = Some(next_block_num);

            if !self.config.build {
                tracing::debug!(target: "scroll::remote_source", "Imported block is valid, but build is disabled, skipping build");
                continue;
            }

            if !self
                .orchestrator_handle
                .status()
                .await
                .map_err(|e| self.classify_recv_error(e))?
                .is_synced()
            {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn settlement_decision_table() {
        // (head, expected, cancellation_observed, retries) -> action
        let cases: &[(u64, u64, bool, u8, SettleAction)] = &[
            // The build landed at exactly the expected height, regardless of
            // other state.
            (6, 6, false, 0, SettleAction::Landed),
            (6, 6, true, MAX_PENDING_BUILD_RETRIES, SettleAction::Landed),
            // The head moved past the expected height: superseded, even with
            // budget exhausted or a cancellation observed.
            (7, 6, false, 0, SettleAction::Superseded),
            (9, 6, true, MAX_PENDING_BUILD_RETRIES, SettleAction::Superseded),
            // The head rewound below the owed build's parent: the resume
            // pointer is stale — resync, regardless of other state.
            (4, 6, false, 0, SettleAction::Resync),
            (0, 6, true, MAX_PENDING_BUILD_RETRIES, SettleAction::Resync),
            // Budget exhausted before anything else resolves: abandon, even
            // when a cancellation was observed (repeated cancellations must
            // not re-issue forever).
            (5, 6, true, MAX_PENDING_BUILD_RETRIES, SettleAction::Abandon),
            (5, 6, false, MAX_PENDING_BUILD_RETRIES, SettleAction::Abandon),
            // An observed cancellation licenses exactly one race-free
            // re-issue per settlement attempt.
            (5, 6, true, 0, SettleAction::Reissue),
            (5, 6, true, MAX_PENDING_BUILD_RETRIES - 1, SettleAction::Reissue),
            // Otherwise the job may still be in flight: wait.
            (5, 6, false, 0, SettleAction::Wait),
            (5, 6, false, MAX_PENDING_BUILD_RETRIES - 1, SettleAction::Wait),
        ];
        for (head, expected, cancelled, retries, want) in cases {
            let got = settlement_decision(*head, *expected, *cancelled, *retries);
            assert_eq!(
                &got, want,
                "settlement_decision({head}, {expected}, {cancelled}, {retries})"
            );
        }
    }
}
