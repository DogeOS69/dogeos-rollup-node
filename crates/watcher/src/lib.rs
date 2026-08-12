//! L1 watcher for the Scroll Rollup Node.

mod error;
pub use error::{EthRequestError, FilterLogError, L1WatcherError};

mod handle;
pub use handle::{L1WatcherCommand, L1WatcherHandle, WatcherUnavailable};

mod liveness;
use liveness::LivenessProbe;

mod metrics;
pub use metrics::WatcherMetrics;

#[cfg(any(test, feature = "test-utils"))]
/// Common test helpers
pub mod test_utils;

use alloy_network::Ethereum;
use alloy_primitives::{ruint::UintTryTo, Address, BlockNumber, B256};
use alloy_provider::{Network, Provider};
use alloy_rpc_types_eth::{BlockId, BlockNumberOrTag, Filter, Log, TransactionTrait};
use alloy_sol_types::SolEvent;
use dogeos_protocol_types::TxL1Message;
use error::L1WatcherResult;
use rollup_node_primitives::{
    BatchCommitData, BatchInfo, BlockInfo, BoundedVec, ConsensusUpdate, L1BlockStartupInfo,
    NodeConfig,
};
use rollup_node_providers::SystemContractProvider;
use scroll_l1::abi::logs::{
    CommitBatch, FinalizeBatch, QueueTransaction, RevertBatch_0, RevertBatch_1,
};
use std::{
    fmt::{Debug, Display, Formatter},
    num::NonZeroUsize,
    sync::Arc,
    time::Duration,
};
use tokio::sync::mpsc;

/// The maximum count of unfinalized blocks we can have in Ethereum.
pub const MAX_UNFINALIZED_BLOCK_COUNT: usize = 96;

/// The main loop interval when L1 watcher is synced to the tip of the L1.
#[cfg(any(test, feature = "test-utils"))]
pub const SLOW_SYNC_INTERVAL: Duration = Duration::from_millis(1);
/// The main loop interval when L1 watcher is synced to the tip of the L1.
#[cfg(not(any(test, feature = "test-utils")))]
pub const SLOW_SYNC_INTERVAL: Duration = Duration::from_secs(2);

/// The bounded delay applied after a failed [`L1Watcher::step`] before the next attempt.
///
/// This caps retry pressure when a step fails while the watcher is not yet synced (for example a
/// non-retryable or exhausted authorized-signer read), preventing a tight watcher-level retry loop.
/// The provider retains its own retry/backoff for classified-retryable responses; this delay is the
/// last-resort watcher-level guard on top of that.
#[cfg(any(test, feature = "test-utils"))]
pub const STEP_RETRY_BACKOFF: Duration = Duration::from_millis(1);
/// The bounded delay applied after a failed [`L1Watcher::step`] before the next attempt.
#[cfg(not(any(test, feature = "test-utils")))]
pub const STEP_RETRY_BACKOFF: Duration = Duration::from_secs(2);

/// The maximum amount of retained headers for reorg detection.
#[cfg(any(test, feature = "test-utils"))]
pub const HEADER_CAPACITY: usize = 100 * MAX_UNFINALIZED_BLOCK_COUNT;
/// The maximum amount of retained headers for reorg detection.
#[cfg(not(any(test, feature = "test-utils")))]
pub const HEADER_CAPACITY: usize = 2 * MAX_UNFINALIZED_BLOCK_COUNT;

/// The default capacity for the transaction cache.
pub const TRANSACTION_CACHE_CAPACITY: NonZeroUsize =
    NonZeroUsize::new(100).expect("non zero capacity");

/// The Ethereum L1 block response.
pub type Block = <Ethereum as Network>::BlockResponse;

/// The Ethereum L1 header response.
pub type Header = <Ethereum as Network>::HeaderResponse;

/// The state of the L1.
#[derive(Debug, Default, Clone)]
pub struct L1State {
    head: u64,
    finalized: u64,
}

/// Whether the [`L1Watcher`] refreshes the authorized signer from the L1 system contract at
/// runtime.
///
/// This is the single, named policy that determines whether the watcher participates in the
/// head-qualified authorization protocol. It is derived once, at construction, from the consensus
/// configuration (see the node's `ConsensusArgs`), so the producer boundary — not the consumer —
/// decides whether any authorization-control traffic is generated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignerRefreshPolicy {
    /// The authorized signer is read from the L1 system contract on every distinct observed head
    /// (system-contract consensus with no explicit signer). The watcher opens and closes an
    /// authorization barrier per head.
    L1Dynamic,
    /// The authorized signer is fixed by configuration (an explicit
    /// `--consensus.authorized-signer`) or consensus does not use an L1 signer at all (no-op).
    /// The watcher never reads the signer from L1 and never opens the authorization barrier.
    Static,
}

impl SignerRefreshPolicy {
    /// Returns `true` if the watcher refreshes the authorized signer from L1 at runtime.
    pub const fn is_dynamic(&self) -> bool {
        matches!(self, Self::L1Dynamic)
    }
}

/// The L1 watcher indexes L1 blocks, applying a first level of filtering via log filters.
#[derive(Debug)]
pub struct L1Watcher<EP> {
    /// The L1 execution node provider. The provider should implement some backoff strategy using
    /// [`alloy_transport::layers::RetryBackoffLayer`], some caching strategy using
    /// [`alloy_provider::layers::CacheProvider`] and some rate limiting policy with
    /// [`alloy_transport::layers::RateLimitRetryPolicy`] in the client/transport in order to avoid
    /// excessive queries on the RPC provider.
    execution_provider: EP,
    /// The buffered unfinalized chain of blocks. Used to detect reorgs of the L1.
    unfinalized_blocks: BoundedVec<Header>,
    /// The L1 state info relevant to the rollup node.
    l1_state: L1State,
    /// The latest indexed block.
    current_block_number: BlockNumber,
    /// Whether the watcher refreshes the authorized signer from L1 at runtime.
    signer_refresh: SignerRefreshPolicy,
    /// The latest L1 head observed by the watcher, by full `(number, hash)` identity.
    ///
    /// Distinct from [`L1State::head`], which is only the head number; readiness and refresh
    /// deduplication compare the complete identity so a same-height reorg is never mistaken for
    /// the replaced head.
    observed_head: Option<BlockInfo>,
    /// The head whose authorized signer was successfully confirmed (barrier closed), i.e. the head
    /// for which a phase-two [`L1Notification::AuthorizedSigner`] was delivered on the ordinary
    /// notification channel.
    ///
    /// This is a single last-observed identity, so an A -> B -> A head sequence re-reads A.
    last_checked_head: Option<BlockInfo>,
    /// The head for which a phase-one [`ConsensusUpdate::AuthorizationPending`] was queued but
    /// whose signer read has not yet succeeded (barrier open). Prevents re-emitting the
    /// pending phase on each retry while a read keeps failing.
    pending_refresh_head: Option<BlockInfo>,
    /// The last authorized signer successfully delivered. Used only for log verbosity (info on an
    /// actual change, a distinct warning on transition into the zero/halt signer, trace
    /// otherwise); it never suppresses delivery, since every opened barrier must be closed for
    /// its head.
    last_delivered_signer: Option<Address>,
    /// The command receiver for the L1 watcher.
    command_rx: mpsc::UnboundedReceiver<L1WatcherCommand>,
    /// The sender part of the channel for [`L1Notification`].
    sender: mpsc::Sender<Arc<L1Notification>>,
    /// The sender part of the dedicated authorization-control channel. It carries only phase one
    /// of the head-qualified refresh — the barrier openings
    /// ([`ConsensusUpdate::AuthorizationPending`]). Unbounded and consumed above the ordinary
    /// L1 data path so the authorization barrier can always be opened promptly; the barrier is
    /// *closed* by phase two on the ordinary notification
    /// channel ([`L1Notification::AuthorizedSigner`]).
    consensus_control_tx: mpsc::UnboundedSender<ConsensusUpdate>,
    /// The rollup node configuration.
    config: Arc<NodeConfig>,
    /// The metrics for the watcher.
    metrics: WatcherMetrics,
    /// Whether the watcher is synced to the L1 head.
    is_synced: bool,
    /// The log query block range.
    log_query_block_range: u64,
    /// The L1 liveness probe.
    liveness_probe: LivenessProbe,
    /// Test mode: skip sending `L1Notification::Synced` events.
    #[cfg(feature = "test-utils")]
    test_mode_skip_synced_notification: bool,
}

/// The L1 notification type yielded by the [`L1Watcher`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum L1Notification {
    /// A notification that the L1 watcher has processed up to a given block info.
    Processed(u64),
    /// A notification for a reorg of the L1 up to a given block number.
    Reorg(u64),
    /// A new batch has been committed on the L1 rollup contract.
    BatchCommit {
        /// The block info the batch was committed at.
        block_info: BlockInfo,
        /// The data of the committed batch.
        data: BatchCommitData,
    },
    /// A new batch has been finalized on the L1 rollup contract.
    BatchFinalization {
        /// The hash of the finalized batch.
        hash: B256,
        /// The index of the finalized batch.
        index: u64,
        /// The block info the batch was finalized at.
        block_info: BlockInfo,
    },
    /// A batch has been reverted.
    BatchRevert {
        /// The batch info of the reverted batch.
        batch_info: BatchInfo,
        /// The L1 block info at which the Batch Revert occurred.
        block_info: BlockInfo,
    },
    /// A range of batches have been reverted.
    BatchRevertRange {
        /// The start index of the reverted batches.
        start: u64,
        /// The end index of the reverted batches.
        end: u64,
        /// The L1 block info at which the Batch Revert Range occurred.
        block_info: BlockInfo,
    },
    /// A new `L1Message` has been added to the L1 message queue.
    L1Message {
        /// The L1 message.
        message: TxL1Message,
        /// The block info at which the L1 message was emitted.
        block_info: BlockInfo,
        /// The timestamp at which the L1 message was emitted.
        block_timestamp: u64,
    },
    /// A new block has been added to the L1.
    NewBlock(BlockInfo),
    /// Phase two of the head-qualified authorized-signer refresh: the signer read (pinned to
    /// `head`'s hash) that closes the authorization barrier opened for `head`.
    ///
    /// This travels on the ordinary FIFO notification channel, emitted *after* the
    /// `Reorg`/`NewBlock` for `head`, so the consumer applies the structural transition
    /// (database unwind, forkchoice repair) before the barrier is cleared. Phase one
    /// (`ConsensusUpdate::AuthorizationPending`) is delivered separately on the priority
    /// control channel so the barrier opens promptly.
    AuthorizedSigner {
        /// The L1 head the signer was read at.
        head: BlockInfo,
        /// The authorized signer at `head`.
        signer: Address,
    },
    /// A block has been finalized on the L1.
    Finalized(u64),
    /// A notification that the L1 watcher is synced to the L1 head.
    Synced,
}

impl Display for L1Notification {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Processed(n) => write!(f, "Processed({n})"),
            Self::Reorg(n) => write!(f, "Reorg({n:?})"),
            Self::BatchCommit { block_info, data } => {
                write!(
                    f,
                    "BatchCommit {{ block_info: {}, batch_index: {}, batch_hash: {} }}",
                    block_info, data.index, data.hash
                )
            }
            Self::BatchRevert { batch_info, block_info } => {
                write!(f, "BatchRevert{{ batch_info: {batch_info}, block_info: {block_info} }}",)
            }
            Self::BatchRevertRange { start, end, block_info } => {
                write!(
                    f,
                    "BatchRevertRange{{ start: {start}, end: {end}, block_info: {block_info} }}",
                )
            }
            Self::BatchFinalization { hash, index, block_info } => write!(
                f,
                "BatchFinalization{{ hash: {hash}, index: {index}, block_info: {block_info} }}",
            ),
            Self::L1Message { message, block_info, .. } => write!(
                f,
                "L1Message{{ index: {}, block_info: {} }}",
                message.queue_index, block_info
            ),
            Self::NewBlock(n) => write!(f, "NewBlock({n})"),
            Self::AuthorizedSigner { head, signer } => {
                write!(f, "AuthorizedSigner{{ head: {head}, signer: {signer} }}")
            }
            Self::Finalized(n) => write!(f, "Finalized({n})"),
            Self::Synced => write!(f, "Synced"),
        }
    }
}

impl<EP> L1Watcher<EP>
where
    EP: Provider + SystemContractProvider + 'static,
{
    /// Spawn a new [`L1Watcher`], starting at `start_block`. The watcher will iterate the L1,
    /// returning [`L1Notification`] in the returned channel.
    #[allow(clippy::too_many_arguments)]
    pub async fn spawn(
        execution_provider: EP,
        l1_block_startup_info: L1BlockStartupInfo,
        config: Arc<NodeConfig>,
        log_query_block_range: u64,
        liveness_threshold: u64,
        liveness_check_interval: u64,
        signer_refresh: SignerRefreshPolicy,
        #[cfg(feature = "test-utils")] test_mode_skip_synced_notification: bool,
    ) -> (mpsc::Sender<Arc<L1Notification>>, L1WatcherHandle) {
        tracing::trace!(target: "scroll::watcher", ?l1_block_startup_info, ?config, ?signer_refresh, "spawning L1 watcher");

        let (notification_tx, notification_rx) = mpsc::channel(log_query_block_range as usize);
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        let (consensus_control_tx, consensus_control_rx) = mpsc::unbounded_channel();
        let handle = L1WatcherHandle::new(command_tx, notification_rx, consensus_control_rx);

        let fetch_block_info = async |tag: BlockNumberOrTag| {
            let block = loop {
                match execution_provider.get_block(tag.into()).await {
                    Err(err) => {
                        tracing::error!(target: "scroll::watcher", ?err, "failed to fetch {tag} block")
                    }
                    Ok(Some(block)) => break block,
                    _ => unreachable!("should always be a {tag} block"),
                }
            };
            BlockInfo { number: block.header.number, hash: block.header.hash }
        };

        // fetch l1 state.
        let head = fetch_block_info(BlockNumberOrTag::Latest).await;
        let finalized = fetch_block_info(BlockNumberOrTag::Finalized).await;
        let l1_state = L1State { head: head.number, finalized: finalized.number };

        let (reorg, start_block) = match l1_block_startup_info {
            L1BlockStartupInfo::UnsafeBlocks(blocks) => {
                let mut reorg = true;
                let mut start_block = blocks.first().expect("at least one unsafe block").number;
                for (i, block) in blocks.into_iter().rev().enumerate() {
                    let current_block =
                        fetch_block_info(BlockNumberOrTag::Number(block.number)).await;
                    if current_block.hash == block.hash {
                        tracing::info!(target: "scroll::watcher", ?block, "found reorg block from unsafe blocks");
                        reorg = i != 0;
                        start_block = current_block.number;
                        break;
                    }
                }

                (reorg, start_block)
            }
            L1BlockStartupInfo::FinalizedBlockNumber(number) => {
                tracing::info!(target: "scroll::watcher", ?number, "starting from finalized block number");

                (false, number)
            }
            L1BlockStartupInfo::None => {
                tracing::info!(target: "scroll::watcher", "no L1 startup info, starting from config start block");
                (false, config.start_l1_block)
            }
        };

        // init the watcher.
        let watcher = Self {
            execution_provider,
            unfinalized_blocks: BoundedVec::new(HEADER_CAPACITY),
            current_block_number: start_block.saturating_sub(1),
            signer_refresh,
            observed_head: None,
            last_checked_head: None,
            pending_refresh_head: None,
            last_delivered_signer: None,
            l1_state,
            command_rx,
            sender: notification_tx.clone(),
            consensus_control_tx,
            config,
            metrics: WatcherMetrics::default(),
            is_synced: false,
            log_query_block_range,
            liveness_probe: LivenessProbe::new(liveness_threshold, liveness_check_interval),
            #[cfg(feature = "test-utils")]
            test_mode_skip_synced_notification,
        };

        // notify at spawn.
        if reorg {
            watcher
                .notify(L1Notification::Reorg(start_block))
                .await
                .expect("channel is open in this context");
        }
        watcher
            .notify(L1Notification::Finalized(finalized.number))
            .await
            .expect("channel is open in this context");
        watcher
            .notify(L1Notification::NewBlock(head))
            .await
            .expect("channel is open in this context");

        tokio::spawn(watcher.run());

        (notification_tx, handle)
    }

    /// Main execution loop for the [`L1Watcher`].
    pub async fn run(mut self) {
        loop {
            // Process any pending commands.
            self.drain_commands_for_reset();

            // Check L1 liveness if due.
            if self.liveness_probe.is_due() {
                self.liveness_probe.check(self.unfinalized_blocks.last());
            }

            // step the watcher.
            if let Err(err) = self.step().await {
                if err.is_channel_closed() {
                    // Every terminal (channel-closed) send is routed through the same reset-aware
                    // recovery, so a reset racing any send point is recovered identically.
                    if self.recover_or_stop_on_closed_channel() {
                        continue;
                    }
                    break;
                }

                // A non-terminal step failure (e.g. a non-retryable or exhausted authorized-signer
                // read). Log with context, record the failure, back off to cap retry pressure, and
                // skip the post-step readiness logic entirely so a pending refresh cannot expose a
                // false `Synced`.
                tracing::error!(
                    target: "scroll::watcher",
                    ?err,
                    observed_head = ?self.observed_head,
                    system_contract = ?self.config.address_book.system_contract_address,
                    "L1 watcher step failed; backing off before retry"
                );
                self.metrics.step_failures.increment(1);
                tokio::time::sleep(STEP_RETRY_BACKOFF).await;
                continue;
            }

            // sleep if we are synced.
            if self.is_synced {
                tokio::time::sleep(SLOW_SYNC_INTERVAL).await;
            } else if self.current_block_number == self.l1_state.head &&
                self.refresh_ready_for_head()
            {
                // In test mode, skip notification if flag is set
                #[cfg(feature = "test-utils")]
                if self.test_mode_skip_synced_notification {
                    self.is_synced = true;
                    continue;
                }

                // if we have synced to the head of the L1, notify the channel and set the
                // `is_synced`` flag. A reset can race this send exactly like the `step` sends, so
                // it goes through the same recovery: recover onto the fresh
                // channels and retry, or stop.
                if let Err(err) = self.notify(L1Notification::Synced).await {
                    if err.is_channel_closed() && self.recover_or_stop_on_closed_channel() {
                        continue;
                    }
                    break;
                }
                self.is_synced = true;
            }
        }
    }

    /// Handles a terminal (channel-closed) send by attempting reset-aware recovery.
    ///
    /// Returns `true` if a queued [`L1WatcherCommand::ResetToBlock`] was applied (fresh channels
    /// installed) and the run loop should continue, or `false` if there is no pending reset and the
    /// watcher should stop. `revert_to_l1_block` enqueues the reset before dropping the old
    /// receivers, so any send that fails because the consumer swapped its receiver has the reset
    /// already queued. The command channel is unbounded, so this cannot deadlock behind a full
    /// bounded notification channel. This single handler covers every terminal send point in the
    /// run loop (both the `step` sends and the post-step `Synced` send).
    fn recover_or_stop_on_closed_channel(&mut self) -> bool {
        if self.drain_commands_for_reset() {
            tracing::warn!(target: "scroll::watcher", "recovered from a closed-channel send via a queued reset");
            true
        } else {
            tracing::warn!(target: "scroll::watcher", "L1 watcher channel closed with no pending reset, stopping the watcher");
            false
        }
    }

    /// Returns whether the authorized signer for the current observed head has been confirmed, so
    /// that `Synced` may be emitted.
    ///
    /// In [`SignerRefreshPolicy::Static`] mode the watcher never reads the signer, so it is always
    /// ready. In dynamic mode the full head identity (number and hash) must match, so a same-height
    /// reorg does not satisfy readiness using the replaced head.
    fn refresh_ready_for_head(&self) -> bool {
        match self.signer_refresh {
            SignerRefreshPolicy::Static => true,
            // Compare the complete `(number, hash)` identity, not just the number, so a same-height
            // reorg cannot satisfy readiness using the replaced head. Also require no pending
            // barrier: an outstanding `pending_refresh_head` (for this or another head) means the
            // consumer barrier is still open, so the watcher is not ready even if
            // `last_checked_head` equals the observed head.
            SignerRefreshPolicy::L1Dynamic => {
                self.observed_head.is_some() &&
                    self.last_checked_head == self.observed_head &&
                    self.pending_refresh_head.is_none()
            }
        }
    }

    /// Drains and applies all currently-pending commands, returning `true` if at least one was
    /// applied.
    ///
    /// The only command is [`L1WatcherCommand::ResetToBlock`], which installs fresh notification
    /// and authorization-control channels, so a `true` return means a reset was applied. This
    /// is used both for ordinary per-loop command processing and to recover from a
    /// closed-channel send: after `revert_to_l1_block`, an in-flight send on the old channel
    /// fails, but the queued reset (already enqueued before the old receivers were dropped) can
    /// be applied to continue on the fresh channels rather than terminating the watcher.
    fn drain_commands_for_reset(&mut self) -> bool {
        let mut applied = false;
        while let Ok(command) = self.command_rx.try_recv() {
            applied = true;
            if let Err(err) = self.handle_command(command) {
                tracing::error!(target: "scroll::watcher", ?err, "failed to handle L1 watcher command");
            }
        }
        applied
    }

    /// Handle a command sent to the L1 watcher.
    fn handle_command(&mut self, command: L1WatcherCommand) -> L1WatcherResult<()> {
        match command {
            L1WatcherCommand::ResetToBlock { block, tx, consensus_control_tx } => {
                tracing::info!(target: "scroll::watcher", ?block, "resetting L1 watcher to block");

                // reset the state.
                self.current_block_number = block;
                self.observed_head = None;
                self.is_synced = false;

                // Clear the authorization cursors so the next step re-opens and re-confirms the
                // barrier for the current head on the fresh control channel.
                // `last_delivered_signer` is intentionally retained: it only
                // affects log verbosity, and forced re-delivery is already
                // guaranteed by clearing the head cursors.
                self.last_checked_head = None;
                self.pending_refresh_head = None;

                // Replace both senders. The handle created a fresh notification and
                // authorization-control channel pair; swapping to them here drops any message still
                // queued for the receivers being torn down, so a stale notification or
                // `ConsensusUpdate` cannot be delivered across the reset boundary.
                self.sender = tx;
                self.consensus_control_tx = consensus_control_tx;
            }
        }
        Ok(())
    }

    /// A step of work for the [`L1Watcher`].
    pub async fn step(&mut self) -> L1WatcherResult<()> {
        // fetch the finalized and latest blocks before any notification, so the authorization
        // barrier can be opened for the newly observed head ahead of structural handling.
        let finalized = self.finalized_block().await?;
        let latest = self.latest_block().await?;
        let latest_head = BlockInfo::from(&latest.header);
        self.observed_head = Some(latest_head);

        // Phase one: open the authorization barrier for the observed head *before* any structural
        // notification for it. Publishing the pending state first means old-signer sequencing and
        // block import are withheld the moment the consumer learns of the transition; if structural
        // handling then fails, the barrier simply stays open and the pending phase is not
        // re-emitted on retry.
        self.open_authorization_barrier(latest_head).await?;

        // handle the finalized block.
        self.handle_finalized_block(&finalized.header).await?;

        // handle the latest block (emits `Reorg` then `NewBlock`).
        self.handle_latest_block(&finalized.header, &latest.header).await?;

        // Phase two: confirm the signer for the observed head, after structural handling so a
        // signer-read retry does not duplicate `NewBlock`, but before log progress so `Processed`
        // means the head's signer confirmation was queued (and the barrier closed).
        self.confirm_authorized_signer(latest_head).await?;

        if latest.header.number != self.current_block_number {
            // index the next range of blocks.
            let logs = self.next_filtered_logs(latest.header.number).await?;
            let num_logs = logs.len();

            // prepare notifications.
            let mut notifications = Vec::with_capacity(logs.len());

            // Process logs grouped by signature.
            let mut i = 0;
            while i < logs.len() {
                let sig = logs[i].topics()[0];
                let start = i;

                // Find the end of the group with the same signature.
                while i < logs.len() && logs[i].topics()[0] == sig {
                    i += 1;
                }

                // Create a slice for the current group of logs.
                let group_logs = &logs[start..i];

                let group_notifications = match sig {
                    QueueTransaction::SIGNATURE_HASH => self.handle_l1_messages(group_logs).await?,
                    CommitBatch::SIGNATURE_HASH => self.handle_batch_commits(group_logs).await?,
                    FinalizeBatch::SIGNATURE_HASH => {
                        self.handle_batch_finalization(group_logs).await?
                    }
                    RevertBatch_0::SIGNATURE_HASH => self.handle_batch_reverts(group_logs).await?,
                    RevertBatch_1::SIGNATURE_HASH => {
                        self.handle_batch_revert_ranges(group_logs).await?
                    }
                    _ => unreachable!("log signature already filtered"),
                };

                notifications.extend(group_notifications);
            }

            // Check that we haven't generated more notifications than logs
            // Note: notifications.len() may be less than logs.len() because genesis batch
            // (batch_index=0) is intentionally skipped
            if notifications.len() > num_logs {
                return Err(L1WatcherError::Logs(FilterLogError::InvalidNotificationCount(
                    num_logs,
                    notifications.len(),
                )))
            }

            // send all notifications on the channel.
            self.notify_all(notifications).await?;

            // update the latest block the l1 watcher has indexed.
            self.update_current_block(&latest).await?;
        }

        Ok(())
    }

    /// Handle the finalized block:
    ///   - Update state and notify channel about finalization.
    ///   - Drain finalized blocks from state.
    #[tracing::instrument(
        target = "scroll::watcher",
        skip_all,
        fields(curr_finalized = ?self.l1_state.finalized, new_finalized = ?finalized.number)
    )]
    async fn handle_finalized_block(&mut self, finalized: &Header) -> L1WatcherResult<()> {
        // update the state and notify on channel.
        if self.l1_state.finalized < finalized.number {
            tracing::trace!(target: "scroll::watcher", number = finalized.number, hash = ?finalized.hash, "new finalized block");

            self.l1_state.finalized = finalized.number;
            self.notify(L1Notification::Finalized(finalized.number)).await?;
        }

        // shortcircuit.
        if self.unfinalized_blocks.is_empty() {
            tracing::trace!(target: "scroll::watcher", "no unfinalized blocks");
            return Ok(());
        }

        let tail_block = self.unfinalized_blocks.last().expect("tail exists");
        if tail_block.number < finalized.number {
            // clear, the finalized block is past the tail.
            tracing::trace!(target: "scroll::watcher", tail = ?tail_block.number, finalized = ?finalized.number, "draining all unfinalized blocks");
            self.unfinalized_blocks.clear();
            return Ok(());
        }

        let finalized_block_position =
            self.unfinalized_blocks.iter().position(|header| header.hash == finalized.hash);

        // drain all blocks up to and including the finalized block.
        if let Some(position) = finalized_block_position {
            tracing::trace!(target: "scroll::watcher", "draining range {:?}", 0..=position);
            self.unfinalized_blocks.drain(0..=position);
        }

        Ok(())
    }

    /// Handle the latest block:
    ///   - Skip if latest matches last unfinalized block.
    ///   - Add to unfinalized blocks if it extends the chain.
    ///   - Fetch chain of unfinalized blocks and emit potential reorg otherwise.
    ///   - Finally, update state and notify channel about latest block.
    #[tracing::instrument(target = "scroll::watcher", skip_all, fields(latest = ?latest.number))]
    async fn handle_latest_block(
        &mut self,
        finalized: &Header,
        latest: &Header,
    ) -> L1WatcherResult<()> {
        let tail = self.unfinalized_blocks.last();

        if tail.is_some_and(|h| h.hash == latest.hash) {
            return Ok(());
        } else if tail.is_some_and(|h| h.hash == latest.parent_hash) {
            // latest block extends the tip.
            tracing::trace!(target: "scroll::watcher", number = ?latest.number, hash = ?latest.hash, "block extends chain");
            self.unfinalized_blocks.push_back(latest.clone());
        } else {
            // chain reorged or need to backfill.
            tracing::trace!(target: "scroll::watcher", number = ?latest.number, hash = ?latest.hash, "gap or reorg");
            let chain = self.fetch_unfinalized_chain(finalized, latest).await?;

            let reorg_block_number = self
                .unfinalized_blocks
                .iter()
                .zip(chain.iter())
                .find(|(old, new)| old.hash != new.hash)
                .map(|(old, _)| old.number.saturating_sub(1));

            // set the unfinalized chain.
            self.unfinalized_blocks = chain;

            if let Some(number) = reorg_block_number {
                tracing::debug!(target: "scroll::watcher", ?number, "reorg");

                // update metrics.
                self.metrics.reorgs.increment(1);
                self.metrics.reorg_depths.record(self.l1_state.head.saturating_sub(number) as f64);

                // reset the current block number to the reorged block number if
                // we have indexed passed the reorg.
                if number < self.current_block_number {
                    self.current_block_number = number;
                }

                // send the reorg block number on the channel.
                self.notify(L1Notification::Reorg(number)).await?;
            }
        }

        // Update the state and notify on the channel.
        tracing::trace!(target: "scroll::watcher", number = ?latest.number, hash = ?latest.hash, "new block");
        self.l1_state.head = latest.number;
        self.notify(L1Notification::NewBlock(latest.into())).await?;

        Ok(())
    }

    /// Handles L1 message events.
    #[tracing::instrument(skip_all)]
    async fn handle_l1_messages(&self, logs: &[Log]) -> L1WatcherResult<Vec<L1Notification>> {
        let mut notifications = Vec::with_capacity(logs.len());

        for log in logs {
            let l1_message: TxL1Message = QueueTransaction::decode_log(&log.inner)
                .map_err(|error| FilterLogError::DecodeLogFailed {
                    log_type: "QueueTransaction",
                    error,
                })?
                .data
                .into();
            let block_number = log.block_number.ok_or(FilterLogError::MissingBlockNumber)?;
            let block_hash = log.block_hash.ok_or(FilterLogError::MissingBlockHash)?;
            let block_timestamp = if let Some(ts) = log.block_timestamp {
                ts
            } else {
                self.execution_provider
                    .get_block(block_number.into())
                    .await?
                    .map(|b| b.header.timestamp)
                    .ok_or(FilterLogError::MissingBlockTimestamp)?
            };

            notifications.push(L1Notification::L1Message {
                message: l1_message,
                block_info: BlockInfo { number: block_number, hash: block_hash },
                block_timestamp,
            });
        }

        Ok(notifications)
    }

    /// Handles batch commits events.
    #[tracing::instrument(skip_all)]
    async fn handle_batch_commits(&self, logs: &[Log]) -> L1WatcherResult<Vec<L1Notification>> {
        // prepare notifications
        let mut notifications = Vec::with_capacity(logs.len());

        // Process batch commits grouped by transaction hash
        for logs in logs.chunk_by(|a, b| a.transaction_hash == b.transaction_hash) {
            // Extract common data from the first log in the group
            let block_number = logs
                .first()
                .and_then(|log| log.block_number)
                .ok_or(FilterLogError::MissingBlockNumber)?;
            let block_hash = logs
                .first()
                .and_then(|log| log.block_hash)
                .ok_or(FilterLogError::MissingBlockHash)?;
            let block_timestamp = if let Some(ts) = logs.first().and_then(|log| log.block_timestamp)
            {
                ts
            } else {
                self.execution_provider
                    .get_block(block_number.into())
                    .await?
                    .map(|b| b.header.timestamp)
                    .ok_or(FilterLogError::MissingBlockTimestamp)?
            };
            let tx_hash = logs
                .first()
                .and_then(|log| log.transaction_hash)
                .ok_or(FilterLogError::MissingTransactionHash)?;
            let tx = self
                .execution_provider
                .get_transaction_by_hash(tx_hash)
                .await?
                .ok_or(EthRequestError::MissingTransactionHash(tx_hash))?;
            let tx_input = Arc::new(tx.input().clone());

            for (idx, log) in logs.iter().enumerate() {
                let commit_batch = CommitBatch::decode_log(&log.inner)
                    .map_err(|error| FilterLogError::DecodeLogFailed {
                        log_type: "CommitBatch",
                        error,
                    })?
                    .data;

                if commit_batch.batch_index.is_zero() {
                    // skip genesis batch.
                    continue;
                }

                let batch_index =
                    commit_batch.batch_index.uint_try_to().expect("u256 to u64 conversion error");
                let blob_versioned_hash =
                    tx.blob_versioned_hashes().and_then(|hashes| hashes.get(idx).copied());

                // push in vector.
                notifications.push(L1Notification::BatchCommit {
                    block_info: BlockInfo { number: block_number, hash: block_hash },
                    data: BatchCommitData {
                        hash: commit_batch.batch_hash,
                        index: batch_index,
                        block_number,
                        block_timestamp,
                        calldata: tx_input.clone(),
                        blob_versioned_hash,
                        finalized_block_number: None,
                        reverted_block_number: None,
                    },
                });
            }
        }

        Ok(notifications)
    }

    /// Handles the batch revert events.
    #[tracing::instrument(skip_all)]
    async fn handle_batch_reverts(&self, logs: &[Log]) -> L1WatcherResult<Vec<L1Notification>> {
        let mut notifications = Vec::with_capacity(logs.len());

        for log in logs {
            let revert_batch = RevertBatch_0::decode_log(&log.inner)
                .map_err(|error| FilterLogError::DecodeLogFailed {
                    log_type: "RevertBatch_0",
                    error,
                })?
                .data;
            let block_number = log.block_number.ok_or(FilterLogError::MissingBlockNumber)?;
            let block_hash = log.block_hash.ok_or(FilterLogError::MissingBlockHash)?;
            let batch_hash = revert_batch.batchHash;
            let batch_index =
                revert_batch.batchIndex.uint_try_to().expect("u256 to u64 conversion error");
            notifications.push(L1Notification::BatchRevert {
                batch_info: BatchInfo { index: batch_index, hash: batch_hash },
                block_info: BlockInfo { number: block_number, hash: block_hash },
            });
        }

        Ok(notifications)
    }

    /// Handle the batch revert range events.
    #[tracing::instrument(skip_all)]
    async fn handle_batch_revert_ranges(
        &self,
        logs: &[Log],
    ) -> L1WatcherResult<Vec<L1Notification>> {
        let mut notifications = Vec::with_capacity(logs.len());

        for log in logs {
            let revert_batch_range = RevertBatch_1::decode_log(&log.inner)
                .map_err(|error| FilterLogError::DecodeLogFailed {
                    log_type: "RevertBatch_1",
                    error,
                })?
                .data;
            let block_number = log.block_number.ok_or(FilterLogError::MissingBlockNumber)?;
            let block_hash = log.block_hash.ok_or(FilterLogError::MissingBlockHash)?;
            let start_index = revert_batch_range
                .startBatchIndex
                .uint_try_to()
                .expect("u256 to u64 conversion error");
            let end_index = revert_batch_range
                .finishBatchIndex
                .uint_try_to()
                .expect("u256 to u64 conversion error");
            notifications.push(L1Notification::BatchRevertRange {
                start: start_index,
                end: end_index,
                block_info: BlockInfo { number: block_number, hash: block_hash },
            });
        }

        Ok(notifications)
    }

    /// Handles the finalize batch events.
    #[tracing::instrument(skip_all)]
    async fn handle_batch_finalization(
        &self,
        logs: &[Log],
    ) -> L1WatcherResult<Vec<L1Notification>> {
        let mut notifications = Vec::with_capacity(logs.len());

        for log in logs {
            let finalize_batch = FinalizeBatch::decode_log(&log.inner)
                .map_err(|error| FilterLogError::DecodeLogFailed {
                    log_type: "FinalizeBatch",
                    error,
                })?
                .data;

            if finalize_batch.batch_index.is_zero() {
                // skip genesis batch.
                continue;
            }

            let block_number = log.block_number.ok_or(FilterLogError::MissingBlockNumber)?;
            let block_hash = log.block_hash.ok_or(FilterLogError::MissingBlockHash)?;
            let index =
                finalize_batch.batch_index.uint_try_to().expect("u256 to u64 conversion error");
            notifications.push(L1Notification::BatchFinalization {
                hash: finalize_batch.batch_hash,
                index,
                block_info: BlockInfo { number: block_number, hash: block_hash },
            });
        }

        Ok(notifications)
    }

    /// Returns whether the consumer's authorization barrier is already correctly reflected as
    /// closed-and-confirmed for `head`: the head was confirmed *and* no different pending barrier
    /// is currently open. `pending_refresh_head` owning a different head (for example after
    /// A -> B(read failure) -> A) means the consumer barrier is still open on that other head, so
    /// `head` must be re-opened and re-confirmed even though `last_checked_head == head`.
    fn barrier_confirmed_for(&self, head: BlockInfo) -> bool {
        self.last_checked_head == Some(head) && self.pending_refresh_head.is_none()
    }

    /// Phase one of the head-qualified authorized-signer refresh: open the authorization barrier
    /// for a newly observed head.
    ///
    /// No-op in [`SignerRefreshPolicy::Static`] mode, when the head is already confirmed with no
    /// other pending barrier, or when the barrier is already open for this exact head. Otherwise it
    /// queues [`ConsensusUpdate::AuthorizationPending`] on the priority control channel and records
    /// the pending head — which supersedes any stale pending barrier on a different head. It is
    /// emitted before any structural notification for the head so the consumer withholds old-signer
    /// work immediately.
    ///
    /// TODO(greg): replace polling with event-driven refresh when the system contract emits a
    /// suitable signer-update event.
    async fn open_authorization_barrier(&mut self, head: BlockInfo) -> L1WatcherResult<()> {
        if !self.signer_refresh.is_dynamic() ||
            self.pending_refresh_head == Some(head) ||
            self.barrier_confirmed_for(head)
        {
            return Ok(());
        }

        self.notify_consensus(ConsensusUpdate::AuthorizationPending(head))?;
        self.pending_refresh_head = Some(head);
        Ok(())
    }

    /// Phase two of the head-qualified authorized-signer refresh: read the signer pinned to the
    /// head's hash and close the barrier.
    ///
    /// No-op in [`SignerRefreshPolicy::Static`] mode or when the head is already confirmed with no
    /// other pending barrier. On a storage-read failure the barrier is left open
    /// (`pending_refresh_head` unchanged) and the error propagates so the step retries without
    /// advancing; a send failure propagates and stops the watcher.
    ///
    /// On success it delivers [`L1Notification::AuthorizedSigner`] on the **ordinary FIFO
    /// notification channel**, after the head's `Reorg`/`NewBlock`, so the consumer applies the
    /// structural transition before the barrier is cleared. The signer is always delivered so the
    /// opened barrier is always closed, even when the value is unchanged; only the log verbosity
    /// depends on the change.
    async fn confirm_authorized_signer(&mut self, head: BlockInfo) -> L1WatcherResult<()> {
        if !self.signer_refresh.is_dynamic() || self.barrier_confirmed_for(head) {
            return Ok(());
        }

        let signer = self
            .execution_provider
            .authorized_signer_at(
                self.config.address_book.system_contract_address,
                BlockId::from(head.hash),
            )
            .await?;
        self.notify(L1Notification::AuthorizedSigner { head, signer }).await?;
        self.last_checked_head = Some(head);
        self.pending_refresh_head = None;

        self.log_authorized_signer(head, signer);
        self.last_delivered_signer = Some(signer);

        Ok(())
    }

    /// Emits the authorized-signer log at a verbosity that avoids routine per-head spam: a distinct
    /// warning on transition into the zero/halt signer, info on an actual nonzero change, and trace
    /// for an unchanged confirmation.
    fn log_authorized_signer(&self, head: BlockInfo, signer: Address) {
        let changed = self.last_delivered_signer != Some(signer);
        if signer.is_zero() {
            if changed {
                tracing::warn!(
                    target: "scroll::watcher",
                    number = head.number,
                    hash = ?head.hash,
                    "authorized signer is the zero address; sequencing is halted (fail-closed) until a nonzero signer is set"
                );
            }
        } else if changed {
            tracing::info!(
                target: "scroll::watcher",
                number = head.number,
                hash = ?head.hash,
                %signer,
                "authorized signer updated for L1 head"
            );
        } else {
            tracing::trace!(
                target: "scroll::watcher",
                number = head.number,
                hash = ?head.hash,
                %signer,
                "authorized signer unchanged for L1 head"
            );
        }
    }

    /// Fetches the chain of unfinalized blocks up to and including the latest block, ensuring no
    /// gaps are present in the chain.
    #[tracing::instrument(target = "scroll::watcher", skip_all)]
    async fn fetch_unfinalized_chain(
        &self,
        finalized: &Header,
        latest: &Header,
    ) -> L1WatcherResult<BoundedVec<Header>> {
        let mut current_block = latest.clone();
        let mut chain = vec![current_block.clone()];

        // loop until we find a block contained in the chain, connected to finalized or latest is
        // finalized.
        let (split_position, mut chain) = loop {
            let pos = self.unfinalized_blocks.iter().rposition(|h| h == &current_block);
            if pos.is_some() ||
                current_block.parent_hash == finalized.hash ||
                current_block.hash == finalized.hash
            {
                break (pos, chain);
            }

            tracing::trace!(target: "scroll::watcher", number = ?(current_block.number.saturating_sub(1)), "fetching block");
            let block = self
                .execution_provider
                .get_block((current_block.number.saturating_sub(1)).into())
                .await?
                .ok_or_else(|| {
                    EthRequestError::MissingBlock(current_block.number.saturating_sub(1))
                })?;
            chain.push(block.header.clone());
            current_block = block.header;
        };

        // order new chain from lowest to highest block number.
        chain.reverse();

        // combine with the available unfinalized blocks.
        let split_position = split_position.unwrap_or(0);
        let mut prefix = BoundedVec::new(HEADER_CAPACITY);
        prefix.extend(self.unfinalized_blocks.iter().take(split_position).cloned());
        prefix.extend(chain.into_iter());

        Ok(prefix)
    }

    /// Send all notifications on the channel.
    async fn notify_all(&self, notifications: Vec<L1Notification>) -> L1WatcherResult<()> {
        for notification in notifications {
            self.metrics.process_l1_notification(&notification);
            tracing::trace!(target: "scroll::watcher", %notification, "sending l1 notification");
            self.notify(notification).await?;
        }
        Ok(())
    }

    /// Send the notification in the channel.
    async fn notify(&self, notification: L1Notification) -> L1WatcherResult<()> {
        Ok(self.sender.send(Arc::new(notification)).await.inspect_err(
            |err| tracing::error!(target: "scroll::watcher", ?err, "failed to send notification"),
        )?)
    }

    /// Send a head-qualified [`ConsensusUpdate`] on the dedicated authorization-control channel.
    ///
    /// The channel is unbounded, so this never blocks on backpressure; a closed channel propagates
    /// as [`L1WatcherError::ControlSendError`] and stops the watcher.
    fn notify_consensus(&self, update: ConsensusUpdate) -> L1WatcherResult<()> {
        Ok(self.consensus_control_tx.send(update).inspect_err(
            |err| tracing::error!(target: "scroll::watcher", ?err, "failed to send consensus control update"),
        )?)
    }

    /// Updates the current block number, saturating at the head of the chain.
    async fn update_current_block(&mut self, latest: &Block) -> L1WatcherResult<()> {
        self.current_block_number = self
            .current_block_number
            .saturating_add(self.log_query_block_range)
            .min(latest.header.number);
        self.notify(L1Notification::Processed(self.current_block_number)).await
    }

    /// Returns the latest L1 block.
    async fn latest_block(&self) -> L1WatcherResult<Block> {
        Ok(self
            .execution_provider
            .get_block(BlockNumberOrTag::Latest.into())
            .await?
            .expect("latest block should always exist"))
    }

    /// Returns the finalized L1 block.
    async fn finalized_block(&self) -> L1WatcherResult<Block> {
        #[cfg(not(feature = "test-utils"))]
        {
            Ok(self
                .execution_provider
                .get_block(BlockNumberOrTag::Finalized.into())
                .await?
                .expect("finalized block should always exist"))
        }

        #[cfg(feature = "test-utils")]
        {
            // We do not use BlockNumberOrTag::Finalized directly because there is an issue with
            // Anvil. See https://github.com/foundry-rs/foundry/issues/12645.
            let block = self
                .execution_provider
                .get_block(BlockNumberOrTag::Finalized.into())
                .await?
                .expect("finalized block should always exist");

            Ok(self
                .execution_provider
                .get_block(block.number().into())
                .await?
                .expect("finalized block should always exist"))
        }
    }

    /// Returns the next range of logs, for the block range in
    /// \[[`current_block`](field@L1Watcher::current_block_number);
    /// [`current_block`](field@L1Watcher::current_block_number) +
    /// [`field@L1Watcher::log_query_block_range`]\].
    async fn next_filtered_logs(&self, latest_block_number: u64) -> L1WatcherResult<Vec<Log>> {
        // set the block range for the query
        let address_book = &self.config.address_book;
        let mut filter = Filter::new()
            .address(vec![
                address_book.rollup_node_contract_address,
                address_book.v1_message_queue_address,
                address_book.v2_message_queue_address,
            ])
            .event_signature(vec![
                QueueTransaction::SIGNATURE_HASH,
                CommitBatch::SIGNATURE_HASH,
                FinalizeBatch::SIGNATURE_HASH,
                RevertBatch_0::SIGNATURE_HASH,
                RevertBatch_1::SIGNATURE_HASH,
            ]);
        let to_block = self
            .current_block_number
            .saturating_add(self.log_query_block_range)
            .min(latest_block_number);

        // skip a block for `from_block` since `self.current_block_number` is the last indexed
        // block.
        filter = filter.from_block(self.current_block_number.saturating_add(1)).to_block(to_block);

        tracing::trace!(target: "scroll::watcher", ?filter, "fetching logs");

        Ok(self.execution_provider.get_logs(&filter).await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{chain, chain_from, provider::MockProvider};

    use alloy_consensus::{transaction::Recovered, Signed, TxEip1559};
    use alloy_primitives::{address, Address, StorageValue, U256};
    use alloy_rpc_types_eth::Transaction;
    use alloy_sol_types::{SolCall, SolEvent};
    use alloy_transport::{TransportErrorKind, TransportResult};
    use arbitrary::Arbitrary;
    use scroll_l1::abi::calls::commitBatchCall;

    const LOG_QUERY_BLOCK_RANGE: u64 = 500;

    // Returns a L1Watcher along with the receiver end of the L1Notifications.
    fn l1_watcher(
        unfinalized_blocks: Vec<Header>,
        provider_blocks: Vec<Header>,
        transactions: Vec<Transaction>,
        finalized: Header,
        latest: Header,
    ) -> (L1Watcher<MockProvider>, L1WatcherHandle) {
        let provider_blocks =
            provider_blocks.into_iter().map(|h| Block { header: h, ..Default::default() });
        let finalized = Block { header: finalized, ..Default::default() };
        let latest = Block { header: latest, ..Default::default() };
        let provider = MockProvider::new(
            provider_blocks,
            transactions.into_iter(),
            std::iter::empty(),
            vec![finalized],
            vec![latest],
        );

        let (notification_tx, notification_rx) = mpsc::channel(LOG_QUERY_BLOCK_RANGE as usize);
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        let (consensus_control_tx, consensus_control_rx) = mpsc::unbounded_channel();
        let handle = L1WatcherHandle::new(command_tx, notification_rx, consensus_control_rx);
        (
            L1Watcher {
                execution_provider: provider,
                unfinalized_blocks: unfinalized_blocks.into(),
                l1_state: L1State { head: Default::default(), finalized: Default::default() },
                current_block_number: 0,
                // These helpers exercise structural/log handling, not signer refresh.
                signer_refresh: SignerRefreshPolicy::Static,
                observed_head: None,
                last_checked_head: None,
                pending_refresh_head: None,
                last_delivered_signer: None,
                command_rx,
                sender: notification_tx,
                consensus_control_tx,
                config: Arc::new(NodeConfig::mainnet()),
                metrics: WatcherMetrics::default(),
                is_synced: false,
                log_query_block_range: LOG_QUERY_BLOCK_RANGE,
                liveness_probe: LivenessProbe::new(60, 12),
                #[cfg(feature = "test-utils")]
                test_mode_skip_synced_notification: false,
            },
            handle,
        )
    }

    struct StepWatcherBlocks {
        unfinalized_blocks: Vec<Header>,
        provider_blocks: Vec<Header>,
        finalized_blocks: Vec<Header>,
        latest_blocks: Vec<Header>,
    }

    /// Builds a dynamic-mode watcher (it refreshes the authorized signer from L1), returning the
    /// watcher, its handle, and the dedicated authorization-control receiver.
    fn step_watcher(
        blocks: StepWatcherBlocks,
        l1_state: L1State,
        current_block_number: u64,
        log_query_block_range: u64,
        storage_responses: Vec<TransportResult<StorageValue>>,
    ) -> (L1Watcher<MockProvider>, L1WatcherHandle, mpsc::UnboundedReceiver<ConsensusUpdate>) {
        step_watcher_with_policy(
            blocks,
            l1_state,
            current_block_number,
            log_query_block_range,
            storage_responses,
            SignerRefreshPolicy::L1Dynamic,
        )
    }

    fn step_watcher_with_policy(
        blocks: StepWatcherBlocks,
        l1_state: L1State,
        current_block_number: u64,
        log_query_block_range: u64,
        storage_responses: Vec<TransportResult<StorageValue>>,
        signer_refresh: SignerRefreshPolicy,
    ) -> (L1Watcher<MockProvider>, L1WatcherHandle, mpsc::UnboundedReceiver<ConsensusUpdate>) {
        let provider_blocks =
            blocks.provider_blocks.into_iter().map(|header| Block { header, ..Default::default() });
        let finalized_blocks = blocks
            .finalized_blocks
            .into_iter()
            .map(|header| Block { header, ..Default::default() })
            .collect();
        let latest_blocks = blocks
            .latest_blocks
            .into_iter()
            .map(|header| Block { header, ..Default::default() })
            .collect();
        let provider = MockProvider::new(
            provider_blocks,
            std::iter::empty(),
            std::iter::empty(),
            finalized_blocks,
            latest_blocks,
        )
        .with_storage_responses(storage_responses);

        let (notification_tx, notification_rx) = mpsc::channel(LOG_QUERY_BLOCK_RANGE as usize);
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        let (consensus_control_tx, consensus_control_rx) = mpsc::unbounded_channel();
        let mut handle = L1WatcherHandle::new(command_tx, notification_rx, consensus_control_rx);
        let control_rx = handle
            .take_consensus_control_receiver()
            .expect("authorization-control receiver present");
        (
            L1Watcher {
                execution_provider: provider,
                unfinalized_blocks: blocks.unfinalized_blocks.into(),
                l1_state,
                current_block_number,
                signer_refresh,
                observed_head: None,
                last_checked_head: None,
                pending_refresh_head: None,
                last_delivered_signer: None,
                command_rx,
                sender: notification_tx,
                consensus_control_tx,
                config: Arc::new(NodeConfig::mainnet()),
                metrics: WatcherMetrics::default(),
                is_synced: false,
                log_query_block_range,
                liveness_probe: LivenessProbe::new(60, 12),
                #[cfg(feature = "test-utils")]
                test_mode_skip_synced_notification: false,
            },
            handle,
            control_rx,
        )
    }

    fn storage_value(address: Address) -> StorageValue {
        StorageValue::from_be_slice(address.as_slice())
    }

    fn received_notifications(handle: &mut L1WatcherHandle) -> Vec<L1Notification> {
        let receiver = handle.l1_notification_receiver();
        let mut notifications = Vec::new();
        while let Ok(notification) = receiver.try_recv() {
            notifications.push(notification.as_ref().clone());
        }
        notifications
    }

    /// Drains the head-qualified consensus updates queued on the authorization-control channel.
    fn received_consensus_updates(
        control_rx: &mut mpsc::UnboundedReceiver<ConsensusUpdate>,
    ) -> Vec<ConsensusUpdate> {
        let mut updates = Vec::new();
        while let Ok(update) = control_rx.try_recv() {
            updates.push(update);
        }
        updates
    }

    /// The phase-one control-channel contents for a distinct dynamic head: just the barrier open.
    /// Phase two (`AuthorizedSigner`) travels on the ordinary notification channel.
    fn pending(head: BlockInfo) -> Vec<ConsensusUpdate> {
        vec![ConsensusUpdate::AuthorizationPending(head)]
    }

    /// The phase-two notification (barrier close) carried on the ordinary FIFO channel.
    fn signer_notif(head: BlockInfo, signer: Address) -> L1Notification {
        L1Notification::AuthorizedSigner { head, signer }
    }

    #[tokio::test]
    async fn unchanged_head_reconciles_signer_once_at_startup() -> eyre::Result<()> {
        let (finalized, latest, chain) = chain(3);
        let signer = address!("1111111111111111111111111111111111111111");
        let (mut watcher, mut handle, mut control_rx) = step_watcher(
            StepWatcherBlocks {
                unfinalized_blocks: chain[1..].to_vec(),
                provider_blocks: vec![finalized.clone()],
                finalized_blocks: vec![finalized.clone(), finalized.clone()],
                latest_blocks: vec![latest.clone(), latest.clone()],
            },
            L1State { head: latest.number, finalized: finalized.number },
            latest.number,
            LOG_QUERY_BLOCK_RANGE,
            vec![Ok(storage_value(signer))],
        );
        let head = BlockInfo::from(&latest);

        watcher.step().await?;
        assert_eq!(watcher.execution_provider.storage_read_count(), 1);
        // The read is pinned to the observed head hash, not the unqualified `latest`.
        assert_eq!(watcher.execution_provider.storage_block_ids(), vec![BlockId::from(head.hash)]);
        // Phase one on the control channel; phase two on the ordinary channel (no `NewBlock` since
        // the tail already matches, no `Processed` since already caught up).
        assert_eq!(received_consensus_updates(&mut control_rx), pending(head));
        assert_eq!(received_notifications(&mut handle), vec![signer_notif(head, signer)]);

        watcher.step().await?;
        assert_eq!(watcher.execution_provider.storage_read_count(), 1);
        assert!(received_consensus_updates(&mut control_rx).is_empty());
        assert!(received_notifications(&mut handle).is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn pending_barrier_opens_before_structural_new_block() -> eyre::Result<()> {
        let (finalized, latest, chain) = chain(3);
        let previous_head = chain[1].clone();
        let signer = address!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let (mut watcher, mut handle, mut control_rx) = step_watcher(
            StepWatcherBlocks {
                unfinalized_blocks: vec![previous_head.clone()],
                provider_blocks: vec![finalized.clone()],
                finalized_blocks: vec![finalized.clone()],
                latest_blocks: vec![latest.clone()],
            },
            L1State { head: previous_head.number, finalized: finalized.number },
            previous_head.number,
            LOG_QUERY_BLOCK_RANGE,
            vec![Ok(storage_value(signer))],
        );
        let head = BlockInfo::from(&latest);

        // Opening the barrier publishes the pending phase with no storage read and no structural
        // notification.
        watcher.open_authorization_barrier(head).await?;
        assert_eq!(
            received_consensus_updates(&mut control_rx),
            vec![ConsensusUpdate::AuthorizationPending(head)]
        );
        assert!(received_notifications(&mut handle).is_empty());
        assert_eq!(watcher.execution_provider.storage_read_count(), 0);

        // The structural `NewBlock` is emitted only by later structural handling, i.e. after the
        // barrier is already open.
        watcher.handle_latest_block(&finalized, &latest).await?;
        assert_eq!(
            received_notifications(&mut handle),
            vec![L1Notification::NewBlock((&latest).into())]
        );
        assert!(received_consensus_updates(&mut control_rx).is_empty());

        // Confirming reads the signer (hash-pinned) and closes the barrier — the close travels on
        // the ordinary channel *after* `NewBlock`, never on the priority control channel, so the
        // consumer cannot clear the barrier before it applies the structural transition.
        watcher.confirm_authorized_signer(head).await?;
        assert_eq!(watcher.execution_provider.storage_read_count(), 1);
        assert_eq!(watcher.execution_provider.storage_block_ids(), vec![BlockId::from(head.hash)]);
        assert!(received_consensus_updates(&mut control_rx).is_empty());
        assert_eq!(received_notifications(&mut handle), vec![signer_notif(head, signer)]);

        Ok(())
    }

    #[tokio::test]
    async fn new_head_with_no_logs_refreshes_signer_and_succeeds() -> eyre::Result<()> {
        let (finalized, latest, chain) = chain(3);
        let previous_head = chain[1].clone();
        let signer = address!("2222222222222222222222222222222222222222");
        let (mut watcher, mut handle, mut control_rx) = step_watcher(
            StepWatcherBlocks {
                unfinalized_blocks: vec![previous_head.clone()],
                provider_blocks: vec![finalized.clone()],
                finalized_blocks: vec![finalized.clone()],
                latest_blocks: vec![latest.clone()],
            },
            L1State { head: previous_head.number, finalized: finalized.number },
            previous_head.number,
            LOG_QUERY_BLOCK_RANGE,
            vec![Ok(storage_value(signer))],
        );
        let head = BlockInfo::from(&latest);

        watcher.step().await?;

        assert_eq!(watcher.execution_provider.storage_read_count(), 1);
        assert!(watcher.refresh_ready_for_head());
        assert_eq!(received_consensus_updates(&mut control_rx), pending(head));
        // Phase two (`AuthorizedSigner`) is ordered after `NewBlock` and before `Processed`.
        assert_eq!(
            received_notifications(&mut handle),
            vec![
                L1Notification::NewBlock((&latest).into()),
                signer_notif(head, signer),
                L1Notification::Processed(latest.number),
            ]
        );

        Ok(())
    }

    #[tokio::test]
    async fn forward_head_signer_rotation_refreshes_each_head() -> eyre::Result<()> {
        let (finalized, latest, chain) = chain(4);
        let initial_head = chain[1].clone();
        let first_forward_head = chain[2].clone();
        let old_signer = address!("8888888888888888888888888888888888888888");
        let new_signer = address!("9999999999999999999999999999999999999999");
        let (mut watcher, mut handle, mut control_rx) = step_watcher(
            StepWatcherBlocks {
                unfinalized_blocks: vec![initial_head.clone()],
                provider_blocks: vec![finalized.clone()],
                finalized_blocks: vec![finalized.clone(), finalized.clone()],
                latest_blocks: vec![first_forward_head.clone(), latest.clone()],
            },
            L1State { head: initial_head.number, finalized: finalized.number },
            initial_head.number,
            LOG_QUERY_BLOCK_RANGE,
            vec![Ok(storage_value(old_signer)), Ok(storage_value(new_signer))],
        );

        watcher.step().await?;
        assert_eq!(
            received_consensus_updates(&mut control_rx),
            pending(BlockInfo::from(&first_forward_head))
        );
        assert_eq!(
            received_notifications(&mut handle),
            vec![
                L1Notification::NewBlock((&first_forward_head).into()),
                signer_notif(BlockInfo::from(&first_forward_head), old_signer),
                L1Notification::Processed(first_forward_head.number),
            ]
        );

        watcher.step().await?;
        assert_eq!(watcher.execution_provider.storage_read_count(), 2);
        assert_eq!(received_consensus_updates(&mut control_rx), pending(BlockInfo::from(&latest)));
        assert_eq!(
            received_notifications(&mut handle),
            vec![
                L1Notification::NewBlock((&latest).into()),
                signer_notif(BlockInfo::from(&latest), new_signer),
                L1Notification::Processed(latest.number),
            ]
        );

        Ok(())
    }

    #[tokio::test]
    async fn identical_head_does_not_refresh_while_log_cursor_catches_up() -> eyre::Result<()> {
        let (mut finalized, mut latest, _) = chain(2);
        finalized.number = 90;
        latest.number = 105;
        let signer = address!("3333333333333333333333333333333333333333");
        let (mut watcher, mut handle, mut control_rx) = step_watcher(
            StepWatcherBlocks {
                unfinalized_blocks: vec![latest.clone()],
                provider_blocks: vec![finalized.clone()],
                finalized_blocks: vec![finalized.clone(), finalized.clone()],
                latest_blocks: vec![latest.clone(), latest.clone()],
            },
            L1State { head: latest.number, finalized: finalized.number },
            100,
            2,
            vec![Ok(storage_value(signer))],
        );

        watcher.step().await?;
        let head = BlockInfo::from(&latest);
        assert_eq!(received_consensus_updates(&mut control_rx), pending(head));
        assert_eq!(
            received_notifications(&mut handle),
            vec![signer_notif(head, signer), L1Notification::Processed(102)]
        );

        watcher.step().await?;
        assert_eq!(watcher.execution_provider.storage_read_count(), 1);
        assert!(received_consensus_updates(&mut control_rx).is_empty());
        assert_eq!(received_notifications(&mut handle), vec![L1Notification::Processed(104)]);

        Ok(())
    }

    #[tokio::test]
    async fn same_height_reorg_refreshes_signer_after_new_block() -> eyre::Result<()> {
        let (finalized, old_latest, old_chain) = chain(3);
        let replacement_chain = chain_from(&finalized, 3);
        let replacement_latest = replacement_chain.last().unwrap().clone();
        let old_signer = address!("4444444444444444444444444444444444444444");
        let new_signer = address!("5555555555555555555555555555555555555555");
        let (mut watcher, mut handle, mut control_rx) = step_watcher(
            StepWatcherBlocks {
                unfinalized_blocks: old_chain[1..].to_vec(),
                provider_blocks: vec![finalized.clone(), replacement_chain[1].clone()],
                finalized_blocks: vec![finalized.clone(), finalized.clone()],
                latest_blocks: vec![old_latest.clone(), replacement_latest.clone()],
            },
            L1State { head: old_latest.number, finalized: finalized.number },
            old_latest.number,
            LOG_QUERY_BLOCK_RANGE,
            vec![Ok(storage_value(old_signer)), Ok(storage_value(new_signer))],
        );

        watcher.step().await?;
        assert_eq!(
            received_consensus_updates(&mut control_rx),
            pending(BlockInfo::from(&old_latest))
        );
        assert_eq!(
            received_notifications(&mut handle),
            vec![signer_notif(BlockInfo::from(&old_latest), old_signer)]
        );

        watcher.step().await?;
        assert_eq!(watcher.execution_provider.storage_read_count(), 2);
        // The reorged head has the same number but a different hash; the read is pinned to the new
        // hash and a fresh barrier is opened and closed for it.
        assert_eq!(
            watcher.execution_provider.storage_block_ids(),
            vec![
                BlockId::from(BlockInfo::from(&old_latest).hash),
                BlockId::from(BlockInfo::from(&replacement_latest).hash),
            ]
        );
        assert_eq!(
            received_consensus_updates(&mut control_rx),
            pending(BlockInfo::from(&replacement_latest))
        );
        // Phase two (`AuthorizedSigner`) is delivered on the ordinary channel *after* the reorg
        // unwind and `NewBlock`, so the barrier does not clear before the structural transition.
        assert_eq!(
            received_notifications(&mut handle),
            vec![
                L1Notification::Reorg(finalized.number),
                L1Notification::NewBlock((&replacement_latest).into()),
                signer_notif(BlockInfo::from(&replacement_latest), new_signer),
                L1Notification::Processed(replacement_latest.number),
            ]
        );

        Ok(())
    }

    #[tokio::test]
    async fn signer_read_failure_retries_same_head_without_duplicate_new_block() -> eyre::Result<()>
    {
        let (finalized, latest, chain) = chain(3);
        let previous_head = chain[1].clone();
        let signer = address!("6666666666666666666666666666666666666666");
        let (mut watcher, mut handle, mut control_rx) = step_watcher(
            StepWatcherBlocks {
                unfinalized_blocks: vec![previous_head.clone()],
                provider_blocks: vec![finalized.clone()],
                finalized_blocks: vec![finalized.clone(), finalized.clone()],
                latest_blocks: vec![latest.clone(), latest.clone()],
            },
            L1State { head: previous_head.number, finalized: finalized.number },
            previous_head.number,
            LOG_QUERY_BLOCK_RANGE,
            vec![
                Err(TransportErrorKind::custom_str("injected signer read failure")),
                Ok(storage_value(signer)),
            ],
        );
        let head = BlockInfo::from(&latest);

        // The barrier opens (pending) before the structural `NewBlock`; the read then fails, so the
        // step aborts before any `Processed` and the signer is not confirmed.
        let err = watcher.step().await.unwrap_err();
        assert!(!err.is_channel_closed());
        assert!(!watcher.refresh_ready_for_head());
        assert_eq!(
            received_consensus_updates(&mut control_rx),
            vec![ConsensusUpdate::AuthorizationPending(head)]
        );
        assert_eq!(
            received_notifications(&mut handle),
            vec![L1Notification::NewBlock((&latest).into())]
        );

        // On retry the same head is not re-announced (no duplicate `NewBlock`) and the pending
        // phase is not re-emitted; the read succeeds, closing the barrier via the ordinary channel.
        watcher.step().await?;
        assert_eq!(watcher.execution_provider.storage_read_count(), 2);
        assert!(watcher.refresh_ready_for_head());
        assert!(received_consensus_updates(&mut control_rx).is_empty());
        assert_eq!(
            received_notifications(&mut handle),
            vec![signer_notif(head, signer), L1Notification::Processed(latest.number)]
        );

        Ok(())
    }

    #[tokio::test]
    async fn persistent_read_failure_emits_no_processed_or_synced() -> eyre::Result<()> {
        let (finalized, latest, chain) = chain(3);
        let previous_head = chain[1].clone();
        let (mut watcher, mut handle, mut control_rx) = step_watcher(
            StepWatcherBlocks {
                unfinalized_blocks: vec![previous_head.clone()],
                provider_blocks: vec![finalized.clone()],
                finalized_blocks: vec![finalized.clone(), finalized.clone()],
                latest_blocks: vec![latest.clone(), latest.clone()],
            },
            L1State { head: previous_head.number, finalized: finalized.number },
            previous_head.number,
            LOG_QUERY_BLOCK_RANGE,
            // No scripted responses: strict mode returns an error for every read.
            vec![],
        );
        let head = BlockInfo::from(&latest);

        // First step: barrier opens, `NewBlock` is delivered, then the read fails; no `Processed`.
        let err = watcher.step().await.unwrap_err();
        assert!(!err.is_channel_closed());
        assert!(!watcher.refresh_ready_for_head());
        assert_eq!(
            received_consensus_updates(&mut control_rx),
            vec![ConsensusUpdate::AuthorizationPending(head)]
        );
        assert_eq!(
            received_notifications(&mut handle),
            vec![L1Notification::NewBlock((&latest).into())]
        );

        // Subsequent steps keep failing: no duplicate structural notification, no re-emitted
        // pending, and still no `Processed` (and hence the run loop would never emit `Synced`).
        for _ in 0..3 {
            let err = watcher.step().await.unwrap_err();
            assert!(!err.is_channel_closed());
            assert!(!watcher.refresh_ready_for_head());
            assert!(received_consensus_updates(&mut control_rx).is_empty());
            assert!(received_notifications(&mut handle).is_empty());
        }

        Ok(())
    }

    #[tokio::test]
    async fn static_mode_never_reads_or_opens_barrier() -> eyre::Result<()> {
        let (finalized, latest, chain) = chain(3);
        let previous_head = chain[1].clone();
        let (mut watcher, mut handle, mut control_rx) = step_watcher_with_policy(
            StepWatcherBlocks {
                unfinalized_blocks: vec![previous_head.clone()],
                provider_blocks: vec![finalized.clone()],
                finalized_blocks: vec![finalized.clone()],
                latest_blocks: vec![latest.clone()],
            },
            L1State { head: previous_head.number, finalized: finalized.number },
            previous_head.number,
            LOG_QUERY_BLOCK_RANGE,
            // No scripted responses: a read in static mode would hit strict mode and fail the
            // step.
            vec![],
            SignerRefreshPolicy::Static,
        );

        watcher.step().await?;

        // No storage read, no barrier traffic; static mode is always ready for `Synced`.
        assert_eq!(watcher.execution_provider.storage_read_count(), 0);
        assert!(watcher.refresh_ready_for_head());
        assert!(received_consensus_updates(&mut control_rx).is_empty());
        assert_eq!(
            received_notifications(&mut handle),
            vec![
                L1Notification::NewBlock((&latest).into()),
                L1Notification::Processed(latest.number),
            ]
        );

        Ok(())
    }

    #[tokio::test]
    async fn reset_clears_refresh_head_and_redelivers_on_replacement_channel() -> eyre::Result<()> {
        let (finalized, latest, chain) = chain(3);
        let signer = address!("7777777777777777777777777777777777777777");
        let (mut watcher, mut handle, mut control_rx) = step_watcher(
            StepWatcherBlocks {
                unfinalized_blocks: chain[1..].to_vec(),
                provider_blocks: vec![finalized.clone()],
                finalized_blocks: vec![finalized.clone(), finalized.clone()],
                latest_blocks: vec![latest.clone(), latest.clone()],
            },
            L1State { head: latest.number, finalized: finalized.number },
            latest.number,
            LOG_QUERY_BLOCK_RANGE,
            vec![Ok(storage_value(signer)), Ok(storage_value(signer))],
        );
        let head = BlockInfo::from(&latest);

        watcher.step().await?;
        assert_eq!(received_consensus_updates(&mut control_rx), pending(head));
        assert_eq!(received_notifications(&mut handle), vec![signer_notif(head, signer)]);

        // Reset replaces both channels; the reset returns the fresh control receiver that the
        // consumer installs, and the stale one is dropped.
        let mut new_control_rx =
            handle.revert_to_l1_block(latest.number).expect("watcher command channel is open");
        let command = watcher.command_rx.try_recv()?;
        watcher.handle_command(command)?;
        watcher.step().await?;

        // The barrier is re-opened (control) and re-confirmed (ordinary channel) for the current
        // head on the replacement channels; nothing is delivered on the stale control receiver.
        assert_eq!(watcher.execution_provider.storage_read_count(), 2);
        assert!(received_consensus_updates(&mut control_rx).is_empty());
        assert_eq!(received_consensus_updates(&mut new_control_rx), pending(head));
        assert_eq!(received_notifications(&mut handle), vec![signer_notif(head, signer)]);

        Ok(())
    }

    #[tokio::test]
    async fn control_channel_send_failure_stops_watcher_without_advancing() -> eyre::Result<()> {
        let (finalized, latest, chain) = chain(3);
        let previous_head = chain[1].clone();
        let signer = address!("cccccccccccccccccccccccccccccccccccccccc");
        let (mut watcher, _handle, control_rx) = step_watcher(
            StepWatcherBlocks {
                unfinalized_blocks: vec![previous_head.clone()],
                provider_blocks: vec![finalized.clone()],
                finalized_blocks: vec![finalized.clone()],
                latest_blocks: vec![latest.clone()],
            },
            L1State { head: previous_head.number, finalized: finalized.number },
            previous_head.number,
            LOG_QUERY_BLOCK_RANGE,
            vec![Ok(storage_value(signer))],
        );

        // Drop the authorization-control receiver so the phase-one send fails.
        drop(control_rx);

        // The pending send fails before the storage read, so the step aborts with a terminal
        // channel-closed error, no storage read occurs, and no cursor advances.
        let err = watcher.step().await.unwrap_err();
        assert!(err.is_channel_closed());
        assert_eq!(watcher.execution_provider.storage_read_count(), 0);
        assert!(watcher.last_checked_head.is_none());
        assert!(watcher.pending_refresh_head.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn reset_recovers_watcher_from_in_flight_send_failure() -> eyre::Result<()> {
        let (finalized, latest, chain) = chain(3);
        let signer = address!("00000000000000000000000000000000000000e5");
        let (mut watcher, mut handle, _control_rx) = step_watcher(
            StepWatcherBlocks {
                unfinalized_blocks: chain[1..].to_vec(),
                provider_blocks: vec![finalized.clone()],
                finalized_blocks: vec![finalized.clone()],
                latest_blocks: vec![latest.clone()],
            },
            L1State { head: latest.number, finalized: finalized.number },
            latest.number,
            LOG_QUERY_BLOCK_RANGE,
            vec![Ok(storage_value(signer))],
        );

        // Reproduce the reset/send race at the `Synced` send point specifically (the previously
        // unrecovered branch): `revert_to_l1_block` enqueues the reset (with fresh channels) and
        // drops the old notification receiver, so the `Synced` send on the old channel now fails
        // with a terminal channel-closed error.
        let _new_control_rx =
            handle.revert_to_l1_block(latest.number).expect("watcher command channel is open");
        let err = watcher.notify(L1Notification::Synced).await.unwrap_err();
        assert!(err.is_channel_closed());

        // The run loop routes this terminal send through `recover_or_stop_on_closed_channel`, the
        // same decision both the `step` and `Synced` send points use. With the reset queued it
        // recovers (returns `true`) onto the fresh channels rather than stopping; a subsequent send
        // then succeeds on the replacement channel.
        assert!(watcher.recover_or_stop_on_closed_channel());
        watcher.notify(L1Notification::Synced).await?;
        assert_eq!(received_notifications(&mut handle), vec![L1Notification::Synced]);

        // With no reset queued, the same handler reports that the watcher must stop.
        drop(handle);
        let err = watcher.notify(L1Notification::Synced).await.unwrap_err();
        assert!(err.is_channel_closed());
        assert!(!watcher.recover_or_stop_on_closed_channel());

        Ok(())
    }

    #[tokio::test]
    async fn head_a_then_failed_b_then_a_reopens_and_rereads_a() -> eyre::Result<()> {
        let (finalized, latest, chain) = chain(3);
        let a = BlockInfo::from(&chain[1]);
        let b = BlockInfo::from(&chain[2]);
        let signer = address!("dddddddddddddddddddddddddddddddddddddddd");
        let (mut watcher, mut handle, mut control_rx) = step_watcher(
            StepWatcherBlocks {
                unfinalized_blocks: vec![],
                provider_blocks: vec![],
                finalized_blocks: vec![finalized.clone()],
                latest_blocks: vec![latest.clone()],
            },
            L1State { head: a.number, finalized: finalized.number },
            a.number,
            LOG_QUERY_BLOCK_RANGE,
            vec![
                Ok(storage_value(signer)), // A confirm succeeds
                Err(TransportErrorKind::custom_str("injected B read failure")),
                Ok(storage_value(signer)), // A re-confirm succeeds
            ],
        );

        // Head A: barrier opens and closes; consumer confirmed on A.
        watcher.open_authorization_barrier(a).await?;
        watcher.confirm_authorized_signer(a).await?;
        assert_eq!(received_consensus_updates(&mut control_rx), pending(a));
        assert_eq!(received_notifications(&mut handle), vec![signer_notif(a, signer)]);
        assert_eq!(watcher.last_checked_head, Some(a));
        assert!(watcher.pending_refresh_head.is_none());

        // Head B: barrier opens for B; the read fails, so the consumer stays pending on B while
        // `last_checked_head` remains A.
        watcher.open_authorization_barrier(b).await?;
        assert!(watcher.confirm_authorized_signer(b).await.is_err());
        assert_eq!(received_consensus_updates(&mut control_rx), pending(b));
        assert_eq!(watcher.pending_refresh_head, Some(b));
        assert_eq!(watcher.last_checked_head, Some(a));
        // Not ready: the consumer barrier is open on B even though the head is back at A.
        watcher.observed_head = Some(a);
        assert!(!watcher.refresh_ready_for_head());

        // Head returns to A: despite `last_checked_head == A`, a different pending head (B) owns
        // the consumer barrier, so A must be re-opened (Pending A) and re-read
        // (AuthorizedSigner A) to move and close the barrier — otherwise it would stay
        // stuck on B.
        watcher.open_authorization_barrier(a).await?;
        watcher.confirm_authorized_signer(a).await?;
        assert_eq!(received_consensus_updates(&mut control_rx), pending(a));
        assert_eq!(received_notifications(&mut handle), vec![signer_notif(a, signer)]);
        assert_eq!(watcher.execution_provider.storage_read_count(), 3);
        assert_eq!(watcher.last_checked_head, Some(a));
        assert!(watcher.pending_refresh_head.is_none());
        assert!(watcher.refresh_ready_for_head());

        Ok(())
    }

    #[tokio::test]
    async fn test_should_fetch_unfinalized_chain_without_reorg() -> eyre::Result<()> {
        // Given
        let (finalized, latest, chain) = chain(21);
        let unfinalized_blocks = chain[1..11].to_vec();

        let (watcher, _) = l1_watcher(
            unfinalized_blocks,
            chain.clone(),
            vec![],
            finalized.clone(),
            latest.clone(),
        );

        // When
        let unfinalized_chain = watcher.fetch_unfinalized_chain(&finalized, &latest).await?;

        // Then
        assert_eq!(unfinalized_chain, chain[1..].to_vec());

        Ok(())
    }

    #[tokio::test]
    async fn test_should_fetch_unfinalized_chain_with_reorg() -> eyre::Result<()> {
        // Given
        let (finalized, _, chain) = chain(21);
        let unfinalized_blocks = chain[1..21].to_vec();
        let mut provider_blocks = chain_from(&chain[10], 10);
        let latest = provider_blocks[9].clone();

        let (watcher, _) = l1_watcher(
            unfinalized_blocks,
            provider_blocks.clone(),
            vec![],
            finalized.clone(),
            latest.clone(),
        );

        // When
        let unfinalized_chain = watcher.fetch_unfinalized_chain(&finalized, &latest).await?;

        // Then
        let mut reorged_chain = chain[1..10].to_vec();
        reorged_chain.append(&mut provider_blocks);
        assert_eq!(unfinalized_chain, reorged_chain);

        Ok(())
    }

    #[tokio::test]
    async fn test_should_handle_finalized_with_empty_state() -> eyre::Result<()> {
        // Given
        let (finalized, latest, _) = chain(2);
        let (mut watcher, _handle) = l1_watcher(vec![], vec![], vec![], finalized.clone(), latest);

        // When
        watcher.handle_finalized_block(&finalized).await?;

        // Then
        assert_eq!(watcher.unfinalized_blocks.len(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_handle_finalize_at_mid_state() -> eyre::Result<()> {
        // Given
        let (_, latest, chain) = chain(10);
        let finalized = chain[5].clone();
        let (mut watcher, _handle) = l1_watcher(chain, vec![], vec![], finalized.clone(), latest);

        // When
        watcher.handle_finalized_block(&finalized).await?;

        // Then
        assert_eq!(watcher.unfinalized_blocks.len(), 4);

        Ok(())
    }

    #[tokio::test]
    async fn test_handle_finalized_at_end_state() -> eyre::Result<()> {
        // Given
        let (_, latest, chain) = chain(10);
        let finalized = latest.clone();
        let (mut watcher, _handle) = l1_watcher(chain, vec![], vec![], finalized.clone(), latest);

        // When
        watcher.handle_finalized_block(&finalized).await?;

        // Then
        assert_eq!(watcher.unfinalized_blocks.len(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_should_match_unfinalized_tail() -> eyre::Result<()> {
        // Given
        let (finalized, latest, chain) = chain(10);
        let (mut watcher, _) = l1_watcher(chain, vec![], vec![], finalized.clone(), latest.clone());

        // When
        watcher.handle_latest_block(&finalized, &latest).await?;

        // Then
        assert_eq!(watcher.unfinalized_blocks.len(), 10);
        assert_eq!(watcher.unfinalized_blocks.pop().unwrap(), latest);

        Ok(())
    }

    #[tokio::test]
    async fn test_handle_latest_block_should_extend_unfinalized_chain() -> eyre::Result<()> {
        // Given
        let (finalized, latest, chain) = chain(10);
        let unfinalized_chain = chain[..9].to_vec();
        let (mut watcher, _handle) =
            l1_watcher(unfinalized_chain, vec![], vec![], finalized.clone(), latest.clone());

        assert_eq!(watcher.unfinalized_blocks.len(), 9);

        // When
        watcher.handle_latest_block(&finalized, &latest).await?;

        // Then
        assert_eq!(watcher.unfinalized_blocks.len(), 10);
        assert_eq!(watcher.unfinalized_blocks.pop().unwrap(), latest);

        Ok(())
    }

    #[tokio::test]
    async fn test_should_fetch_missing_unfinalized_blocks() -> eyre::Result<()> {
        // Given
        let (finalized, latest, chain) = chain(10);
        let unfinalized_chain = chain[..5].to_vec();
        let (mut watcher, mut handle) =
            l1_watcher(unfinalized_chain, chain, vec![], finalized.clone(), latest.clone());

        // When
        watcher.handle_latest_block(&finalized, &latest).await?;

        // Then
        assert_eq!(watcher.unfinalized_blocks.len(), 10);
        assert_eq!(watcher.unfinalized_blocks.pop().unwrap(), latest);
        let notification = handle.l1_notification_receiver().recv().await.unwrap();
        assert!(matches!(*notification, L1Notification::NewBlock(_)));

        Ok(())
    }

    #[tokio::test]
    async fn test_should_handle_latest_block_with_reorg() -> eyre::Result<()> {
        // Given
        let (finalized, _, chain) = chain(10);
        let reorged = chain_from(&chain[5], 10);
        let latest = reorged[9].clone();
        let (mut watcher, mut handle) =
            l1_watcher(chain.clone(), reorged, vec![], finalized.clone(), latest.clone());

        // When
        watcher.current_block_number = chain[9].number;
        watcher.handle_latest_block(&finalized, &latest).await?;

        // Then
        assert_eq!(watcher.unfinalized_blocks.pop().unwrap(), latest);
        assert_eq!(watcher.current_block_number, chain[5].number);

        let notification = handle.l1_notification_receiver().recv().await.unwrap();
        assert!(matches!(*notification, L1Notification::Reorg(_)));
        let notification = handle.l1_notification_receiver().recv().await.unwrap();
        assert!(matches!(*notification, L1Notification::NewBlock(_)));

        Ok(())
    }

    #[tokio::test]
    async fn test_should_handle_l1_messages() -> eyre::Result<()> {
        // Given
        let (finalized, latest, chain) = chain(10);
        let (watcher, _) = l1_watcher(chain, vec![], vec![], finalized.clone(), latest.clone());

        // build test logs.
        let mut logs = Vec::new();

        // Produce a random log
        let mut queue_transaction = random!(Log);
        let mut inner_log = random!(alloy_primitives::Log);
        inner_log.data = random!(QueueTransaction).encode_log_data();
        queue_transaction.inner = inner_log;
        queue_transaction.block_number = Some(random!(u64));
        queue_transaction.block_timestamp = Some(random!(u64));
        queue_transaction.block_hash = Some(random!(B256));
        queue_transaction.topics_mut()[0] = QueueTransaction::SIGNATURE_HASH;
        logs.push(queue_transaction);

        // Produce another random log
        let mut queue_transaction = random!(Log);
        let mut inner_log = random!(alloy_primitives::Log);
        inner_log.data = random!(QueueTransaction).encode_log_data();
        queue_transaction.inner = inner_log;
        queue_transaction.block_number = Some(random!(u64));
        queue_transaction.block_timestamp = Some(random!(u64));
        queue_transaction.block_hash = Some(random!(B256));
        queue_transaction.topics_mut()[0] = QueueTransaction::SIGNATURE_HASH;
        logs.push(queue_transaction);

        // When
        let notifications = watcher.handle_l1_messages(&logs).await?;

        // Then
        assert_eq!(notifications.len(), logs.len());
        for notification in notifications {
            assert!(matches!(notification, L1Notification::L1Message { .. }));
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_should_handle_batch_commits() -> eyre::Result<()> {
        // Given
        let (finalized, latest, chain) = chain(10);

        // prepare the commit batch call transaction.
        let mut inner = random!(Signed<TxEip1559>);
        inner.tx_mut().input = random!(commitBatchCall).abi_encode().into();
        let recovered = Recovered::new_unchecked(inner.into(), random!(Address));
        let tx = Transaction {
            inner: recovered,
            block_hash: None,
            block_number: None,
            transaction_index: None,
            effective_gas_price: None,
        };

        let (watcher, _) =
            l1_watcher(chain, vec![], vec![tx.clone()], finalized.clone(), latest.clone());

        // build test logs.
        let mut logs = Vec::new();
        let block_number = random!(u64);
        let block_hash = random!(B256);
        let block_timestamp = random!(u64);

        // Produce a random batch commit log.
        let mut batch_commit = random!(Log);
        let mut inner_log = random!(alloy_primitives::Log);
        inner_log.data =
            CommitBatch { batch_index: U256::from(random!(u64)), batch_hash: random!(B256) }
                .encode_log_data();
        batch_commit.inner = inner_log;
        batch_commit.transaction_hash = Some(*tx.inner.tx_hash());
        batch_commit.block_number = Some(block_number);
        batch_commit.block_hash = Some(block_hash);
        batch_commit.block_timestamp = Some(block_timestamp);
        logs.push(batch_commit);

        // Produce another random batch commit log.
        let mut batch_commit = random!(Log);
        let mut inner_log = random!(alloy_primitives::Log);
        inner_log.data =
            CommitBatch { batch_index: U256::from(random!(u64)), batch_hash: random!(B256) }
                .encode_log_data();
        batch_commit.inner = inner_log;
        batch_commit.transaction_hash = Some(*tx.inner.tx_hash());
        batch_commit.block_number = Some(block_number);
        batch_commit.block_hash = Some(block_hash);
        batch_commit.block_timestamp = Some(block_timestamp);
        logs.push(batch_commit);

        // When
        let notifications = watcher.handle_batch_commits(&logs).await?;

        // Then
        assert_eq!(notifications.len(), logs.len());
        for notification in notifications {
            assert!(matches!(notification, L1Notification::BatchCommit { .. }));
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_should_handle_batch_reverts() -> eyre::Result<()> {
        // Given
        let (finalized, latest, chain) = chain(10);
        let (watcher, _) = l1_watcher(chain, vec![], vec![], finalized.clone(), latest.clone());

        // build test logs.
        let mut logs = Vec::new();
        let mut revert_batch = random!(Log);
        let mut inner_log = random!(alloy_primitives::Log);
        inner_log.data =
            RevertBatch_0 { batchHash: random!(B256), batchIndex: U256::from(random!(u64)) }
                .encode_log_data();
        revert_batch.inner = inner_log;
        revert_batch.block_number = Some(random!(u64));
        revert_batch.block_hash = Some(random!(B256));
        logs.push(revert_batch);

        // When
        let notification = watcher.handle_batch_reverts(&logs).await?.pop().unwrap();

        // Then
        assert!(matches!(notification, L1Notification::BatchRevert { .. }));

        Ok(())
    }

    #[tokio::test]
    async fn test_should_handle_batch_revert_range() -> eyre::Result<()> {
        // Given
        let (finalized, latest, chain) = chain(10);
        let (watcher, _) = l1_watcher(chain, vec![], vec![], finalized.clone(), latest.clone());

        // build test logs.
        let mut logs = Vec::new();
        let mut revert_batch_range = random!(Log);
        let mut inner_log = random!(alloy_primitives::Log);
        inner_log.data = RevertBatch_1 {
            startBatchIndex: U256::from(random!(u64)),
            finishBatchIndex: U256::from(random!(u64)),
        }
        .encode_log_data();
        revert_batch_range.inner = inner_log;
        revert_batch_range.block_number = Some(random!(u64));
        revert_batch_range.block_hash = Some(random!(B256));
        logs.push(revert_batch_range);

        // When
        let notification = watcher.handle_batch_revert_ranges(&logs).await?.pop().unwrap();

        // Then
        assert!(matches!(notification, L1Notification::BatchRevertRange { .. }));

        Ok(())
    }

    #[tokio::test]
    async fn test_should_handle_finalize_commits() -> eyre::Result<()> {
        // Given
        let (finalized, latest, chain) = chain(10);
        let (watcher, _) = l1_watcher(chain, vec![], vec![], finalized.clone(), latest.clone());

        // build test logs.
        let mut logs = Vec::new();

        // Produce a random finalize commit log.
        let mut finalize_commit = random!(Log);
        let mut inner_log = random!(alloy_primitives::Log);
        let mut batch = random!(FinalizeBatch);
        batch.batch_index = U256::from(random!(u64));
        inner_log.data = batch.encode_log_data();
        finalize_commit.inner = inner_log;
        finalize_commit.block_number = Some(random!(u64));
        finalize_commit.block_hash = Some(random!(B256));
        logs.push(finalize_commit);

        // When
        let notification = watcher.handle_batch_finalization(&logs).await?.pop().unwrap();

        // Then
        assert!(matches!(notification, L1Notification::BatchFinalization { .. }));

        Ok(())
    }
}
