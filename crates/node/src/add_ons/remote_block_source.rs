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
use rollup_node_primitives::BlockInfo;
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
    /// Also reset to `None` whenever it can no longer be trusted (the
    /// `Superseded`/`Resync`/`Abandon` outcomes — whether from settlement or
    /// from the import loop superseding a freshly issued build, an import
    /// the engine did not apply, repeated import rejections, the pre-issue
    /// head re-check, and the follow-loop's advanced- and rewound-head
    /// guards), forcing a fresh common-ancestor walk on the next tick.
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
    /// Consecutive rejected imports. Bounded by
    /// [`MAX_IMPORT_REJECTIONS`]: repeated rejections mean the resume pointer
    /// no longer matches the canonical chain (the remote reorged below it, or
    /// the derived block does not connect), and re-requesting the identical
    /// block at poll cadence forever would livelock the source. The bound
    /// keeps one transient rejection from triggering the (potentially long)
    /// ancestor re-walk.
    consecutive_import_rejections: u32,
    /// Consecutive builds skipped because the orchestrator reported
    /// not-synced. A latched gate means the node imports but never
    /// sequences; surfaced with a periodic warning.
    consecutive_gate_skips: u64,
    /// Number of consecutive failed poll ticks, reported in the error logs.
    consecutive_failures: u64,
    /// Whether a build is owed for the last imported block. Recorded at
    /// IMPORT time (before any await — see `pending_build_issued`), released
    /// by the deliberate not-synced gate skip, and otherwise cleared when
    /// the outcome arrives, when the head proves the debt moot (`Superseded`/`Resync` clear it
    /// with no outcome ever observed), or when the settlement gives up. While set,
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
    /// Whether the owed build's `BuildBlock` command was ATTEMPTED (the
    /// flag is set immediately before the send; a send failure is a
    /// terminal orchestrator-gone stop, so attempted and sent coincide in
    /// any run that continues).
    /// The debt itself is recorded at import time (before any await), so an
    /// aborted tick cannot lose a build in the gap between the pointer
    /// advancing and the request going out; an unissued debt is simply
    /// issued by the next settlement, consuming no retry budget.
    pending_build_issued: bool,
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

/// The remote endpoint reduced to `scheme://host:port`, safe to log.
///
/// The configured URL can carry basic-auth credentials and query-string API
/// keys, and those survive into transport error messages: alloy wraps
/// `reqwest::Error`, whose `Display` appends `for url ({url})` with the
/// userinfo and query intact. Anything derived from an error chain has to be
/// scrubbed against the full URL before it reaches a log line.
fn redact_remote(url: Option<&reqwest::Url>, message: &str) -> String {
    match url {
        Some(url) => message.replace(url.as_str(), &safe_remote_host(url)),
        None => message.to_string(),
    }
}

/// `scheme://host:port` for the configured remote, with no userinfo or query.
fn safe_remote_host(url: &reqwest::Url) -> String {
    format!(
        "{}://{}:{}",
        url.scheme(),
        url.host_str().unwrap_or("<none>"),
        url.port_or_known_default().unwrap_or(0)
    )
}

/// Upper bound on one poll tick — the outer backstop behind the client's
/// 30s per-request timeout: a long catch-up or settlement chain can outlast
/// the poll interval, and the poll timer (Delay behavior) cannot fire while
/// a tick is in flight. Aborting a tick is safe: the state machine is
/// tick-resumable by design (imports advance the pointer one by one; an
/// owed build settles on the next tick), and the run loop treats an aborted
/// tick that ADVANCED the pointer as catch-up progress, not a stall.
const TICK_STALL_BUDGET: Duration = Duration::from_secs(600);

/// The Nth consecutive import rejection re-derives the resume pointer —
/// i.e. N-1 rejections are tolerated (the counter is incremented before the
/// comparison; see `consecutive_import_rejections`).
const MAX_IMPORT_REJECTIONS: u32 = 3;

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
    /// The build landed at EXACTLY the expected height — the block was
    /// sequenced, or building was skipped for an empty payload. (Anything
    /// higher is `Superseded`, never a success for this request.)
    Landed,
    /// The payload building job was cancelled; no outcome will arrive.
    Cancelled,
    /// An outcome for a strictly higher height arrived: the head advanced
    /// through another path and the owed build is moot.
    Superseded,
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

/// The per-height verdict of the common-ancestor walk, decided purely from
/// the local hash, the remote's `(number, hash)` (if any), and the requested
/// height — so the walk's classification is table-testable without a live
/// remote. A remote that answers for a DIFFERENT height than requested (an
/// offset or `latest`-answering load-balancer backend) is `Absent`, NOT
/// `Diverged`: misreading a wrong-height answer as divergence would walk to
/// genesis and drive a full administrative rewind.
#[derive(Debug, PartialEq, Eq)]
enum AncestorProbe {
    /// Both sides have the requested height with equal hashes: common block.
    Match(alloy_primitives::B256),
    /// Both sides have the requested height with different hashes: a real fork.
    Diverged,
    /// One side lacks the block, or the remote answered a different height:
    /// transient, retry the walk (never proof of divergence).
    Absent,
}

/// Classifies one common-ancestor probe. See [`AncestorProbe`].
fn classify_ancestor_probe(
    local_hash: Option<alloy_primitives::B256>,
    remote: Option<(u64, alloy_primitives::B256)>,
    requested: u64,
) -> AncestorProbe {
    match (local_hash, remote) {
        (Some(lh), Some((rn, rh))) if rn == requested && lh == rh => AncestorProbe::Match(lh),
        (Some(_), Some((rn, _))) if rn == requested => AncestorProbe::Diverged,
        _ => AncestorProbe::Absent,
    }
}

/// What `follow_and_build` does once a common ancestor `resume` is derived,
/// decided purely from `(local_head, local_safe, resume, diverged)` so the
/// destructive-rewind guard is table-testable without fixtures.
#[derive(Debug, PartialEq, Eq)]
enum FollowAction {
    /// The common ancestor is below the local safe head: importing over it
    /// would send an FCU whose safe hash is no ancestor of the head. Wait.
    RefuseBelowSafe,
    /// The local head is 2+ past the ancestor but no divergence was observed:
    /// the remote merely trails a canonical local chain. Wait, do not rewind.
    WaitForRemote,
    /// The local head is 2+ past the ancestor AND divergence was observed: the
    /// local chain sits on a fork the remote no longer serves — rewind the
    /// head down to the ancestor (down-only, guarded by a fresh re-read).
    Rewind,
    /// The local head is at or one past the ancestor: import forward normally.
    Proceed,
}

/// Decides the follow action from the resume point. Ordering is load-bearing:
/// the below-safe refusal precedes the head-distance checks, and the
/// divergence split inside them decides rewind vs wait. Inverting the
/// `local_head > resume + 1` comparison or dropping the `diverged` split would
/// silently rewind a follower's canonical head to chase a lagging replica.
const fn decide_follow_action(
    local_head: u64,
    local_safe: u64,
    resume: u64,
    diverged: bool,
) -> FollowAction {
    if resume < local_safe {
        return FollowAction::RefuseBelowSafe;
    }
    if local_head > resume.saturating_add(1) {
        if diverged {
            return FollowAction::Rewind;
        }
        return FollowAction::WaitForRemote;
    }
    FollowAction::Proceed
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
    /// first successful poll, where transient errors are logged and retried
    /// at poll cadence instead of failing node launch. (The first
    /// initialization's divergence verdicts — genesis mismatch, exhausted
    /// lookback — stay terminal and fail-stop the node via the spawn
    /// wrapper.)
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
        let http_client = reqwest_13::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|e| eyre::eyre!("failed to build the remote HTTP client: {e}"))?;
        // A per-request timeout: RetryBackoffLayer only retries ERRORS, so a
        // black-holed connection (NAT/LB idle drop) would otherwise hang a
        // request forever, with TICK_STALL_BUDGET (10 min) as the only
        // backstop and zero log output in between.
        let transport = alloy_transport_http::Http::with_client(http_client, url.clone());
        let client = RpcClient::builder().layer(retry_layer).transport(transport, false);
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
            consecutive_gate_skips: 0,
            consecutive_import_rejections: 0,
            consecutive_failures: 0,
            pending_build: false,
            pending_build_cancelled: false,
            pending_build_issued: false,
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
        self.pending_build_issued = false;
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
    async fn init_last_imported_block(
        &self,
    ) -> eyre::Result<(u64, alloy_primitives::B256, u64, u64, bool)> {
        let status =
            self.orchestrator_handle.status().await.map_err(|e| self.classify_recv_error(e))?;
        let local_head = status.l2.fcs.head_block_info().number;
        let local_safe = status.l2.fcs.safe_block_info().number;
        let remote_head = self.remote.get_block_number().await?;

        let start = local_head.min(remote_head);
        let floor = start.saturating_sub(MAX_ANCESTOR_LOOKBACK);
        let last_imported_block;
        let resume_hash;
        // True only when the walk stepped past at least one PRESENT pair of
        // differing hashes — the one observation that proves a real fork
        // (a merely lagging remote matches on its first probe).
        let mut diverged = false;
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
                let remote = remote_genesis.map(|rb| (rb.header.number, rb.header.hash));
                match classify_ancestor_probe(local_genesis, remote, 0) {
                    AncestorProbe::Match(lh) => {
                        resume_hash = lh;
                    }
                    AncestorProbe::Diverged => {
                        tracing::error!(
                            target: "scroll::remote_source",
                            local = ?local_genesis,
                            remote = ?remote,
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
                    AncestorProbe::Absent => {
                        // Includes a remote that answered a NON-genesis block
                        // for the height-0 request (an offset backend): a
                        // transient retry, not a wrong-URL fail-stop.
                        return Err(eyre::eyre!(
                            "genesis block unavailable or mismatched locally or remotely; \
                             retrying"
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
            let remote = remote_block.map(|rb| (rb.header.number, rb.header.hash));
            match classify_ancestor_probe(local_hash, remote, search) {
                AncestorProbe::Match(lh) => {
                    last_imported_block = search;
                    resume_hash = lh;
                    break;
                }
                AncestorProbe::Diverged => {
                    // Both present at the requested height, hashes differ:
                    // genuinely divergent — walk down.
                    diverged = true;
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
                AncestorProbe::Absent => {
                    // One side lacks the block (pruned, lagging, or a fresh
                    // load-balancer backend), OR the remote answered a
                    // DIFFERENT height than requested: transient — retry the
                    // whole walk rather than misreading it as divergence
                    // (which could walk to genesis and drive a full rewind).
                    return Err(eyre::eyre!(
                        "block {search} unavailable or mismatched during the common-ancestor \
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
        Ok((last_imported_block, resume_hash, local_head, local_safe, diverged))
    }

    /// Runs the remote block source until shutdown.
    pub async fn run_until_shutdown(
        mut self,
        mut shutdown: impl std::future::Future<Output = ()> + Unpin,
    ) -> eyre::Result<()> {
        // interval() panics on a zero period. Launch validation already
        // rejects `--remote-source.poll-interval-ms 0`; this clamp guards
        // programmatic construction only.
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
                    let pointer_before = self.last_imported_block;
                    let result = tokio::select! {
                        biased;
                        _guard = &mut shutdown => break,
                        r = tokio::time::timeout(TICK_STALL_BUDGET, self.follow_and_build()) => {
                            match r {
                                Ok(r) => r,
                                // A NUMERIC advance, not `Option` ordering:
                                // `Some(_)` sorts above `None`, so comparing the
                                // options directly classified the tick that
                                // merely ESTABLISHES the pointer as deep
                                // catch-up — a full stall of the budget then
                                // logged at info, returned Ok and reset
                                // consecutive_failures.
                                Err(_)
                                    if matches!(
                                        (pointer_before, self.last_imported_block),
                                        (Some(before), Some(now)) if now.gt(&before)
                                    ) =>
                                {
                                    // The budget elapsed while IMPORTING — a
                                    // deep catch-up, not a stall. Treat as a
                                    // healthy tick so consecutive_failures
                                    // does not climb through the add-on's
                                    // most common operational scenario.
                                    tracing::info!(
                                        target: "scroll::remote_source",
                                        last_imported = ?self.last_imported_block,
                                        budget = ?TICK_STALL_BUDGET,
                                        "Deep catch-up tick exceeded the stall budget; continuing"
                                    );
                                    Ok(())
                                }
                                Err(_) => Err(eyre::eyre!(
                                    "poll tick stalled for {TICK_STALL_BUDGET:?} with no import \
                                     progress (last_imported {:?}; deep settlement chain, or the \
                                     remote connection is black-holed); retrying",
                                    self.last_imported_block
                                )),
                            }
                        }
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
                                let redacted =
                                    redact_remote(self.config.url.as_ref(), &format!("{e:#}"));
                                tracing::error!(target: "scroll::remote_source", error = %redacted, "Terminal sync error; stopping remote block source");
                                return Err(e);
                            }
                            self.consecutive_failures += 1;
                            // Rate-limit identical errors by elapsed time (at
                            // the default 100ms poll interval an unreachable
                            // remote would otherwise emit ~10 identical
                            // lines/second), but always log a changed error
                            // immediately.
                            // Scrubbed BEFORE it is stored or compared: this
                            // string is both the log payload and the
                            // rate-limiter's memory.
                            let msg =
                                redact_remote(self.config.url.as_ref(), &format!("{e:#}"));
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
                                // Host/port only: the full URL carries
                                // basic-auth credentials and query strings
                                // (API keys). `?e` is NOT logged for the same
                                // reason — the transport's Display appends the
                                // full URL — so the scrubbed `msg` stands in.
                                let remote_host =
                                    self.config.url.as_ref().map(safe_remote_host);
                                tracing::error!(
                                    target: "scroll::remote_source",
                                    error = %msg,
                                    consecutive_failures = self.consecutive_failures,
                                    builds_abandoned = self.builds_abandoned,
                                    suppressed_errors = self.suppressed_errors,
                                    initialized = self.last_imported_block.is_some(),
                                    remote_host = ?remote_host,
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
    /// any BUILD command (the event-lag re-subscribe path does send an
    /// `EventListener` command).
    ///
    /// A `BlockSequenced` is accepted as landed only at EXACTLY
    /// `expected_number` (stale outcomes are strictly lower-numbered and
    /// ignored; strictly higher ones settle as `Superseded`), so an outcome
    /// from another build cannot be attributed to this request.
    /// `BlockBuildingSkipped` carries the head it sat on and is accepted only
    /// when that head is the expected parent (or beyond), so stale outcomes
    /// from abandoned builds are excluded by identity, like `BlockSequenced`.
    /// `import_chain` additionally cancels any in-flight job as part of every
    /// successful import (after its validity checks), so by the time a build
    /// is requested here the job slot is empty. Config-level violations of
    /// the single-requester assumption are rejected by `validate()` (no
    /// `sequencer.auto-start` on a node with `sequencer.enabled`, whether or
    /// not `remote-source.build` is set), but the
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
        let result = tokio::time::timeout(wait_budget, async {
            loop {
                match events.next().await {
                    Some(ChainOrchestratorEvent::BlockSequenced(block))
                        if block.header.number == expected_number =>
                    {
                        tracing::info!(target: "scroll::remote_source",
                            block_number = block.header.number,
                            block_hash = ?block.hash_slow(),
                            "Block built successfully, proceeding to next");
                        break Ok(BuildOutcome::Landed);
                    }
                    Some(ChainOrchestratorEvent::BlockSequenced(block))
                        if block.header.number > expected_number =>
                    {
                        // A strictly-higher build is NOT this request's
                        // outcome: the head advanced through another path and
                        // a build issued against the stale parent would sit
                        // one height above it. Treat as superseded, never as
                        // landed.
                        break Ok(BuildOutcome::Superseded);
                    }
                    Some(ChainOrchestratorEvent::BlockBuildingSkipped { head_block_number })
                        if head_block_number.saturating_add(1) == expected_number =>
                    {
                        tracing::debug!(target: "scroll::remote_source", head_block_number, "Block building skipped (empty block)");
                        break Ok(BuildOutcome::Landed);
                    }
                    Some(ChainOrchestratorEvent::BlockBuildingSkipped { head_block_number })
                        if head_block_number.saturating_add(1) > expected_number =>
                    {
                        // Skip at a higher head: same supersession logic.
                        break Ok(BuildOutcome::Superseded);
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
        .await;
        match result {
            Ok(outcome) => outcome,
            Err(_) => {
                // The broadcast stream swallows Lagged with an upstream warn
                // and drops events, so the awaited outcome may already be
                // gone. Re-subscribe so the NEXT wait starts on a fresh,
                // unlagged stream before surfacing the timeout.
                match self.orchestrator_handle.get_event_listener().await {
                    Ok(fresh) => self.events = fresh,
                    Err(err) => {
                        tracing::warn!(
                            target: "scroll::remote_source",
                            ?err,
                            "Failed to re-subscribe the event listener after a wait timeout; \
                             the next wait may run on a lagged stream"
                        );
                    }
                }
                Err(eyre::eyre!(
                    "Timed out after {wait_budget:?} waiting for the build outcome of block \
                     {expected_number}"
                ))
            }
        }
    }

    /// Requests block building and waits (bounded) for the outcome. The
    /// command may coalesce with an already in-flight job; for the remote
    /// source that job is never stale, because `import_chain` cancels the job
    /// slot as part of every successful import (after its validity checks).
    ///
    /// Stale outcomes queued by earlier, given-up requests are drained first
    /// so they cannot be attributed to this request. `pending_build` stays set
    /// on cancellation and timeout failures so the build is settled on the
    /// next poll tick instead of being lost; supersession and the pre-issue
    /// head mismatch clear it (with the resume pointer) instead — the debt is
    /// moot once the head moved past it.
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

        // Fresh head check after the drain: a gossip or derivation import can
        // land between the settlement's snapshot and here (the drain above
        // consumes its events), and issuing then would build one height above
        // the imported block, only for it to be reorged out. Bail to a
        // re-derive instead.
        let head = self
            .orchestrator_handle
            .status()
            .await
            .map_err(|e| self.classify_recv_error(e))?
            .l2
            .fcs
            .head_block_info()
            .number;
        if head == expected_number {
            // The build landed between the settlement snapshot and here (the
            // drain above consumed its event). Same classification the
            // settlement table gives this pair: Landed.
            self.clear_pending_build();
            return Ok(());
        }
        if head.saturating_add(1) != expected_number {
            self.clear_pending_build();
            self.last_imported_block = None;
            self.consecutive_import_rejections = 0;
            return Err(eyre::eyre!(
                "head moved to {head} before the build for {expected_number} was issued; \
                 re-deriving the resume point"
            ));
        }

        self.pending_build = true;
        self.pending_build_cancelled = false;
        self.pending_build_issued = true;
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
            BuildOutcome::Superseded => {
                self.clear_pending_build();
                self.last_imported_block = None;
                self.consecutive_import_rejections = 0;
                Err(eyre::eyre!(
                    "build outcome superseded by an unrelated head advance; re-deriving \
                     the resume point"
                ))
            }
        }
    }

    /// Settles a build owed from a previous tick without ever double-building.
    ///
    /// (First branch: a debt recorded at import but never SENT is simply
    /// issued — nothing was ever in flight, so no outcome is owed yet and no
    /// budget is consumed.)
    ///
    /// For an ISSUED debt: `status()` flows through the same FIFO command
    /// channel as `BuildBlock`, so once it returns, the owed command has
    /// been processed: the job either
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
        // Never unwrap: a None pointer just means "re-derive next tick".
        let Some(last_imported) = self.last_imported_block else {
            return Ok(());
        };
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

        if !self.pending_build_issued && head.saturating_add(1) == expected {
            // The debt was recorded at import time but the request was never
            // sent (the tick was aborted in between): issue it now —
            // race-free by construction, no retry budget consumed. A moved
            // head falls through to the normal decision (Landed/Superseded/
            // Resync all handle an unissued debt correctly: nothing was ever
            // in flight).
            tracing::debug!(
                target: "scroll::remote_source",
                expected,
                "Issuing an owed build that was recorded but never sent"
            );
            return self.trigger_build_and_await(expected).await;
        }
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
                // the common ancestor instead. Err for the same reason as
                // Resync/Abandon below: Ok would reset consecutive_failures
                // and could log a spurious recovery.
                self.last_imported_block = None;
                self.consecutive_import_rejections = 0;
                Err(eyre::eyre!(
                    "owed build superseded by an unrelated head advance (head {head}, expected \
                     {expected}); re-deriving the resume point"
                ))
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
                self.consecutive_import_rejections = 0;
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
                self.consecutive_import_rejections = 0;
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
                    BuildOutcome::Superseded => {
                        self.clear_pending_build();
                        self.last_imported_block = None;
                        self.consecutive_import_rejections = 0;
                        Err(eyre::eyre!(
                            "owed build superseded by an unrelated head advance; \
                             re-deriving the resume point"
                        ))
                    }
                }
            }
        }
    }

    /// Follows the remote node and builds blocks on top of imported blocks.
    async fn follow_and_build(&mut self) -> eyre::Result<()> {
        // First successful contact with the remote determines the resume point.
        if self.last_imported_block.is_none() {
            let (resume, resume_hash, local_head, local_safe, diverged) =
                self.init_last_imported_block().await?;
            // The walk SUCCEEDING is what proves the remote serves our chain
            // (it returned a matching hash) — set the flag here, before the
            // guards below: a benign lagging-remote/below-safe tick must not
            // keep the node in first-init mode where a later misrouted
            // backend's genesis mismatch would fail-stop it. A wrong URL can
            // never produce a matching hash, so first-init protection is
            // not weakened.
            self.initialized_once = true;
            // Checked UNCONDITIONALLY (not only when the head is 2+ past the
            // ancestor): with head == safe == S and the remote forking at S,
            // resume is S-1 and the head is exactly resume+1 — importing over
            // it would send an FCU whose safe hash is no ancestor of the
            // head, which the EL refuses forever (a permanent import wedge).
            match decide_follow_action(local_head, local_safe, resume, diverged) {
                FollowAction::RefuseBelowSafe => {
                    return Err(eyre::eyre!(
                        "common ancestor {resume} is below the local safe head {local_safe}; \
                         waiting for the remote to catch up instead of importing over safe \
                         state"
                    ));
                }
                FollowAction::WaitForRemote => {
                    // No present-but-different hash pair was observed: the
                    // remote is merely BEHIND the local chain (lagging
                    // replica, resyncing backend), not on another fork.
                    // Rewinding our own head to follow it would throw away
                    // canonical blocks — wait for it to catch up instead.
                    return Err(eyre::eyre!(
                        "remote head trails the local chain (common ancestor {resume}, local \
                         head {local_head}) with no divergence observed; waiting for the \
                         remote to catch up"
                    ));
                }
                FollowAction::Proceed => {}
                FollowAction::Rewind => {
                    // The local chain extends past the freshly derived common
                    // ancestor by more than our own single build: it sits on a
                    // fork the remote no longer serves (remote reorg), or the
                    // remote lags far behind. Refusing forever would wedge the
                    // follower (the next walk returns the same ancestor), and
                    // importing over it would silently reorg blocks out — so
                    // rewind through the administrative path, which collects
                    // reverted transactions, uses a checked FCU, and cancels any
                    // in-flight job. The remote is authoritative in this
                    // deployment shape; rewound derivation blocks are re-imported
                    // as the remote serves them.
                    // The guards above ran against a PRE-WALK snapshot, and the
                    // ancestor walk can take a long time (thousands of
                    // sequential remote RPCs). fcs.update has no
                    // head-monotonicity check, so a rewind decided on stale
                    // values could move the head FORWARD, re-canonicalizing
                    // blocks a concurrent revert or reorg removed. Re-read the
                    // head and safe immediately before the rewind: the remote
                    // source must only ever move the head DOWN.
                    let fresh = self
                        .orchestrator_handle
                        .status()
                        .await
                        .map_err(|e| self.classify_recv_error(e))?;
                    let fresh_head_info = *fresh.l2.fcs.head_block_info();
                    let fresh_head = fresh_head_info.number;
                    let fresh_safe = fresh.l2.fcs.safe_block_info().number;
                    if resume < fresh_safe {
                        return Err(eyre::eyre!(
                            "common ancestor {resume} fell below the local safe head {fresh_safe} \
                         while the ancestor walk ran; re-deriving next tick"
                        ));
                    }
                    if fresh_head <= resume {
                        return Err(eyre::eyre!(
                            "local head moved to {fresh_head} (at or below the common ancestor \
                         {resume}) while the ancestor walk ran; a rewind would move the head \
                         forward — re-deriving next tick"
                        ));
                    }
                    tracing::warn!(
                        target: "scroll::remote_source",
                        local_head,
                        resume,
                        "Local head extends past the common ancestor; rewinding to follow the remote"
                    );
                    // The observed head rides along as a compare-and-swap
                    // precondition: the fresh read above and this command are
                    // still not atomic, and the handler refuses the rewind if
                    // the head moved in the gap.
                    self.orchestrator_handle
                        .update_fcs_head_if_unmoved(
                            BlockInfo { number: resume, hash: resume_hash },
                            Some(fresh_head_info),
                        )
                        .await
                        .map_err(|e| self.classify_recv_error(e))?
                        .map_err(|refusal| {
                            eyre::eyre!("rewind to the common ancestor was refused: {refusal}")
                        })?;
                }
            }
            self.last_imported_block = Some(resume);
        }

        // A build owed from a previous tick is settled before importing
        // anything else: its import already advanced `last_imported_block`,
        // so without this the head comparison below would report "synced" and
        // the requested block would be lost.
        if self.pending_build {
            self.settle_owed_build().await?;
            // DEFENSIVE: every current pointer-clearing settlement path
            // returns Err out of the tick first, so this should be
            // unreachable — but a cleared pointer must always mean
            // "re-derive next tick", never an unwrap. (Do not "simplify"
            // settlement arms to Ok: their Err keeps consecutive_failures
            // honest.)
            if self.last_imported_block.is_none() {
                return Ok(());
            }
        }

        loop {
            // DEFENSIVE (see the settlement guard above): all current
            // pointer-clearing paths error out of the tick first, but a None
            // always means "re-derive on the next tick", never an invariant
            // violation worth killing the node over.
            let Some(last_imported) = self.last_imported_block else {
                return Ok(());
            };

            // Guard against importing behind an advanced local head: gossip
            // or derivation may have moved it past the pointer while the
            // remote lagged, and ImportBlock bypasses the gossip path's
            // parent-linkage and safe-head guards — importing
            // last_imported+1 would silently reorg the local chain out. (The
            // settlement's Superseded arm covers this only while a build is
            // owed; this covers the steady state.)
            let local_head = self
                .orchestrator_handle
                .status()
                .await
                .map_err(|e| self.classify_recv_error(e))?
                .l2
                .fcs
                .head_block_info()
                .number;
            if local_head > last_imported.saturating_add(1) {
                self.last_imported_block = None;
                self.consecutive_import_rejections = 0;
                return Err(eyre::eyre!(
                    "local head {local_head} advanced past the resume pointer \
                     {last_imported}; re-deriving the common ancestor"
                ));
            }
            if local_head < last_imported {
                // The local head was rewound below the pointer (L1 reorg or
                // administrative unwind) with no build owed — the settlement
                // Resync arm only covers the owed-build case. Continuing
                // would either report "already synced" forever (remote still
                // at the pointer) or skip the rewound range and its purged
                // L1-message mappings.
                self.last_imported_block = None;
                self.consecutive_import_rejections = 0;
                return Err(eyre::eyre!(
                    "local head {local_head} rewound below the resume pointer \
                     {last_imported}; re-deriving the common ancestor"
                ));
            }

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
                .ok_or_else(|| eyre::eyre!("Block {} not found", next_block_num))?;
            // A remote answering for the wrong HEIGHT must be caught BEFORE the
            // import lands an FCU and the pointer advances on a block that was
            // never the one requested.
            eyre::ensure!(
                block.header.number == next_block_num,
                "remote returned block {} for request {}",
                block.header.number,
                next_block_num
            );
            // ...and a load-balanced remote whose backend sits on another FORK
            // answers the right height with a wrong parent. The ancestry walk
            // pins the resume hash once at initialization and never re-checks
            // during catch-up, so without this the fork is only discovered by
            // the engine — which answers SYNCING, not INVALID, for an unknown
            // parent. That drops a healthy follower out of synced mode; the
            // next tick then takes the optimistic branch, commits the mirror on
            // SYNCING, and plants a head the local EL never adopts, after which
            // every ancestor probe reads that absent head and returns
            // immediately, forever.
            if let Some(expected_parent) = self.provider.block_hash(last_imported)? {
                if block.header.parent_hash != expected_parent {
                    // A changed parent is the ordinary shape of a remote reorg
                    // at or below the pointer, not only a misrouted backend, so
                    // the POINTER is what went stale: clear it and let the next
                    // tick re-walk to the new common ancestor. Returning an
                    // error without clearing it would re-fetch this same block
                    // on every poll and wedge the follower on the old fork.
                    let stale = block.header.parent_hash;
                    self.last_imported_block = None;
                    self.consecutive_import_rejections = 0;
                    return Err(eyre::eyre!(
                        "remote block {next_block_num} does not build on local block \
                         {last_imported} (parent {stale} != {expected_parent}); re-deriving \
                         the common ancestor"
                    ));
                }
            }
            // The same failure class as the height check, one field over. If the
            // remote ignored `fullTransactions` — a proxy that drops the flag,
            // or one that degrades to hashes above a response-size cap —
            // `into_consensus` silently builds an EMPTY body while keeping the
            // original header, because `BlockTransactions::into_transactions_vec`
            // yields `vec![]` for anything that is not `Full`. The EL then
            // recomputes a different transactions root, answers INVALID, and the
            // source reports "invalid block", burns its rejection budget,
            // re-walks ancestry and repeats forever — pointing the operator at a
            // chain fork rather than at RPC serialization. A genuinely empty
            // block deserializes to `Full(vec![])` and still passes.
            eyre::ensure!(
                block.transactions.is_full(),
                "remote returned block {next_block_num} without full transactions \
                 (fullTransactions ignored by the endpoint or a proxy)"
            );
            let block = block.into_consensus().map_transactions(|tx| tx.inner.into_inner());

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
                    self.consecutive_import_rejections += 1;
                    if self.consecutive_import_rejections >= MAX_IMPORT_REJECTIONS {
                        self.consecutive_import_rejections = 0;
                        self.last_imported_block = None;
                        return Err(eyre::eyre!(
                            "import rejected {MAX_IMPORT_REJECTIONS} times in a row (last: \
                             {e}); re-deriving the resume point"
                        ));
                    }
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
                self.consecutive_import_rejections = 0;
                return Err(eyre::eyre!(
                    "block {next_block_num} was not applied by forkchoice; re-deriving the \
                     common ancestor"
                ));
            }
            self.last_imported_block = Some(next_block_num);
            self.consecutive_import_rejections = 0;
            if self.config.build {
                // Record the build debt BEFORE any await: if the tick is
                // aborted (stall budget) between here and the request going
                // out, the next tick's settlement finds an unissued debt and
                // issues it — the build cannot be silently lost.
                self.pending_build = true;
                self.pending_build_cancelled = false;
                self.pending_build_issued = false;
            }

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
                self.consecutive_gate_skips += 1;
                if self.consecutive_gate_skips.is_multiple_of(100) {
                    // A latched gate means this node imports but never
                    // sequences — that must not hide at debug forever.
                    tracing::warn!(
                        target: "scroll::remote_source",
                        consecutive_gate_skips = self.consecutive_gate_skips,
                        "Builds keep being skipped because the orchestrator reports \
                         not-synced"
                    );
                } else {
                    tracing::debug!(target: "scroll::remote_source", "Imported block is valid, but orchestrator is not synced, skipping build");
                }
                // The skip is deliberate: release the debt recorded at the
                // import above (the not-synced skip semantics are tracked in
                // the follow-ups ledger).
                self.clear_pending_build();
                continue;
            }

            self.consecutive_gate_skips = 0;

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
    fn classify_ancestor_probe_table() {
        use alloy_primitives::B256;
        let a = B256::repeat_byte(0xaa);
        let b = B256::repeat_byte(0xbb);
        // Both sides at the requested height, equal hashes: common block.
        assert_eq!(classify_ancestor_probe(Some(a), Some((5, a)), 5), AncestorProbe::Match(a));
        // Both sides at the requested height, different hashes: divergence.
        assert_eq!(classify_ancestor_probe(Some(a), Some((5, b)), 5), AncestorProbe::Diverged);
        // Remote answered a DIFFERENT height than requested (offset / latest
        // backend): Absent, never Diverged — this is the height guard that
        // stops a misbehaving remote from driving a rewind to genesis.
        assert_eq!(classify_ancestor_probe(Some(a), Some((4, b)), 5), AncestorProbe::Absent);
        assert_eq!(classify_ancestor_probe(Some(a), Some((4, a)), 5), AncestorProbe::Absent);
        // One side lacks the block.
        assert_eq!(classify_ancestor_probe(Some(a), None, 5), AncestorProbe::Absent);
        assert_eq!(classify_ancestor_probe(None, Some((5, a)), 5), AncestorProbe::Absent);
        assert_eq!(classify_ancestor_probe(None, None, 5), AncestorProbe::Absent);
    }

    #[test]
    fn decide_follow_action_table() {
        // (local_head, local_safe, resume, diverged) -> action
        let cases: &[(u64, u64, u64, bool, FollowAction)] = &[
            // Ancestor below the local safe head: refuse regardless of divergence.
            (100, 50, 40, false, FollowAction::RefuseBelowSafe),
            (100, 50, 40, true, FollowAction::RefuseBelowSafe),
            // Ancestor exactly AT the local safe head: the steady state, and
            // the boundary the refusal must not swallow. Widening the guard to
            // `<=` leaves every other row in this table green while turning
            // normal operation into a permanent RefuseBelowSafe.
            (50, 50, 50, false, FollowAction::Proceed),
            (100, 50, 50, true, FollowAction::Rewind),
            // Head 2+ past the ancestor, no divergence: the remote merely
            // trails — wait, never rewind.
            (100, 0, 10, false, FollowAction::WaitForRemote),
            // Head 2+ past the ancestor WITH divergence: rewind to follow.
            (100, 0, 10, true, FollowAction::Rewind),
            // Head exactly one past the ancestor (a build in flight): proceed,
            // divergence or not.
            (11, 0, 10, false, FollowAction::Proceed),
            (11, 0, 10, true, FollowAction::Proceed),
            // Head at the ancestor: proceed.
            (10, 0, 10, false, FollowAction::Proceed),
            // Below-safe takes precedence over the head-distance rewind.
            (100, 50, 10, true, FollowAction::RefuseBelowSafe),
        ];
        for (head, safe, resume, diverged, want) in cases {
            let got = decide_follow_action(*head, *safe, *resume, *diverged);
            assert_eq!(&got, want, "decide_follow_action({head}, {safe}, {resume}, {diverged})");
        }
    }

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
