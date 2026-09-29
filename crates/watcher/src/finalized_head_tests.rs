//! Watcher behaviour against a synthetic L1 whose `finalized` block is its head.
//!
//! dogeos-core's `l1_interface` answers `eth_getBlockByNumber` for `finalized` (and `safe`) with
//! the synthetic head (`crates/l1_interface/src/rpc/eth_api.rs`:
//! `Latest | Safe | Pending | Finalized => ctx.visible_block_head`). These tests drive
//! [`L1Watcher::step`] one poll at a time against such a provider and pin what the watcher
//! reports today. They verify the Lean reorg model's findings F1 and F13
//! (`REORG_RUST_TEST_CASES.md` cases 1(a), 1(b), 1(d), 19, 20 and 32). Every assertion documents
//! current behaviour, not intended behaviour.
//!
//! Tracker rows (Reorg Issue Tracker, Private Mainnet): RG-41 (finalized = head misleads the
//! watcher's replacement detection), RG-45 (the finalized/latest race) and RG-52 (the runtime
//! signer refresh never fires). Each test name starts with the RG key of the row it pins. Every
//! test passes today and documents behaviour that is not fixed; when a row is fixed, change the
//! matching assertion instead of deleting the test.

use super::*;
use alloy_primitives::{Address, Bytes, StorageValue, TxHash, U256};
use alloy_provider::{EthGetBlock, ProviderCall, RootProvider, RpcWithBlock};
use alloy_rpc_types_eth::{BlockId, Transaction};
use alloy_transport::TransportResult;
use std::{collections::HashMap, sync::Mutex};

/// The mutable state of the synthetic L1.
#[derive(Debug, Default)]
struct Served {
    /// The served chain, indexed by block number (index 0 is the genesis block).
    chain: Vec<Header>,
    /// Logs keyed by the hash of the block that carries them.
    logs: HashMap<B256, Vec<Log>>,
    /// A chain to serve right after the next `finalized` query: the finalized/latest race.
    switch_after_finalized: Option<Vec<Header>>,
    /// Control only: serve `finalized` this many blocks below the head (0 = `l1_interface`).
    finalized_lag: u64,
    /// Blocks below this number are not served (an L1 whose history starts above genesis).
    floor: u64,
    /// How many times block 0 is served before the provider stops answering. Bounds a walk
    /// that would otherwise never end.
    genesis_budget: Option<u64>,
    genesis_lookups: u64,
    by_number_lookups: u64,
    by_hash_lookups: u64,
    signer: Address,
    storage_reads: u64,
}

/// A synthetic L1 provider: `finalized` and `latest` both return the served head.
#[derive(Debug, Clone, Default)]
struct SyntheticL1(Arc<Mutex<Served>>);

impl SyntheticL1 {
    fn new(chain: Vec<Header>, logs: Vec<Log>) -> Self {
        let l1 = Self::default();
        l1.serve(chain, logs);
        l1
    }

    /// Replaces the served chain and adds the logs of any new blocks.
    fn serve(&self, chain: Vec<Header>, logs: Vec<Log>) {
        let mut s = self.0.lock().unwrap();
        s.chain = chain;
        for log in logs {
            s.logs
                .entry(log.block_hash.expect("test logs carry a block hash"))
                .or_default()
                .push(log);
        }
    }

    fn state(&self) -> std::sync::MutexGuard<'_, Served> {
        self.0.lock().unwrap()
    }
}

fn block(header: Header) -> Block {
    Block { header, ..Default::default() }
}

#[async_trait::async_trait]
impl Provider for SyntheticL1 {
    fn root(&self) -> &RootProvider<Ethereum> {
        unreachable!("the watcher only uses the overridden calls")
    }

    fn get_block(&self, block_id: BlockId) -> EthGetBlock<Block> {
        let val: Option<Block> = {
            let mut s = self.0.lock().unwrap();
            match block_id {
                BlockId::Number(BlockNumberOrTag::Latest) => s.chain.last().cloned().map(block),
                BlockId::Number(BlockNumberOrTag::Finalized) => {
                    // finalized == latest, as `l1_interface` serves it (unless a control test
                    // sets a lag).
                    let index = (s.chain.len() as u64).saturating_sub(1 + s.finalized_lag);
                    let head = s.chain.get(index as usize).cloned().map(block);
                    if let Some(next) = s.switch_after_finalized.take() {
                        s.chain = next;
                    }
                    head
                }
                BlockId::Number(BlockNumberOrTag::Number(n)) => {
                    s.by_number_lookups += 1;
                    let mut serve = n >= s.floor;
                    if n == 0 {
                        s.genesis_lookups += 1;
                        if s.genesis_budget.is_some_and(|budget| s.genesis_lookups > budget) {
                            serve = false;
                        }
                    }
                    if serve {
                        s.chain.get(n as usize).cloned().map(block)
                    } else {
                        None
                    }
                }
                BlockId::Hash(hash) => {
                    s.by_hash_lookups += 1;
                    s.chain.iter().find(|h| h.hash == hash.block_hash).cloned().map(block)
                }
                other => unimplemented!("unexpected block query {other:?}"),
            }
        };
        EthGetBlock::new_provider(
            block_id,
            Box::new(move |_kind| ProviderCall::Ready(Some(Ok(val.clone())))),
        )
    }

    async fn get_logs(&self, filter: &Filter) -> TransportResult<Vec<Log>> {
        let s = self.0.lock().unwrap();
        let from = filter.get_from_block().unwrap_or(0);
        let to = filter.get_to_block().unwrap_or(u64::MAX);
        // A range that starts above the served head is answered with an empty list, as
        // `l1_interface` does.
        Ok(s.chain
            .iter()
            .filter(|h| h.number >= from && h.number <= to)
            .flat_map(|h| s.logs.get(&h.hash).cloned().unwrap_or_default())
            .collect())
    }

    fn get_storage_at(
        &self,
        _address: Address,
        _key: U256,
    ) -> RpcWithBlock<(Address, U256), StorageValue> {
        let value = {
            let mut s = self.0.lock().unwrap();
            s.storage_reads += 1;
            U256::from_be_slice(s.signer.as_slice())
        };
        RpcWithBlock::new_provider(move |_| ProviderCall::Ready(Some(Ok(value))))
    }

    fn get_transaction_by_hash(
        &self,
        _hash: TxHash,
    ) -> ProviderCall<(TxHash,), Option<Transaction>> {
        ProviderCall::Ready(Some(Ok(None)))
    }
}

/// A deterministic block hash that differs per (number, branch).
fn hash(number: u64, branch: u8) -> B256 {
    let mut bytes = [0u8; 32];
    bytes[..8].copy_from_slice(&number.to_be_bytes());
    bytes[31] = branch;
    B256::from(bytes)
}

fn header(number: u64, branch: u8, parent: &Header) -> Header {
    Header {
        hash: hash(number, branch),
        inner: alloy_consensus::Header {
            number,
            parent_hash: parent.hash,
            timestamp: 1_000 + number,
            ..Default::default()
        },
        total_difficulty: None,
        size: None,
    }
}

fn genesis() -> Header {
    Header {
        hash: hash(0, 0),
        inner: alloy_consensus::Header { number: 0, timestamp: 1_000, ..Default::default() },
        total_difficulty: None,
        size: None,
    }
}

/// A `QueueTransaction` log for queue index `queue_index` carried by `block`; `variant` changes
/// the message content (the deposit amount).
fn deposit(block: &Header, queue_index: u64, variant: u64) -> Log {
    let event = QueueTransaction {
        sender: Address::repeat_byte(0x11),
        target: Address::repeat_byte(0x22),
        value: U256::from(1_000 * (variant + 1)),
        queueIndex: queue_index,
        gasLimit: U256::from(100_000),
        data: Bytes::new(),
    };
    Log {
        inner: alloy_primitives::Log {
            address: Address::repeat_byte(0x33),
            data: event.encode_log_data(),
        },
        block_hash: Some(block.hash),
        block_number: Some(block.number),
        block_timestamp: Some(block.timestamp),
        transaction_hash: Some(B256::with_last_byte(queue_index as u8)),
        ..Default::default()
    }
}

fn watcher(l1: SyntheticL1) -> (L1Watcher<SyntheticL1>, L1WatcherHandle) {
    let (notification_tx, notification_rx) = mpsc::channel(1024);
    let (command_tx, command_rx) = mpsc::unbounded_channel();
    let handle = L1WatcherHandle::new(command_tx, notification_rx);
    (
        L1Watcher {
            execution_provider: l1,
            unfinalized_blocks: BoundedVec::new(HEADER_CAPACITY),
            l1_state: L1State { head: 0, finalized: 0 },
            current_block_number: 0,
            command_rx,
            sender: notification_tx,
            config: Arc::new(NodeConfig::mainnet()),
            metrics: WatcherMetrics::default(),
            is_synced: false,
            log_query_block_range: 500,
            liveness_probe: LivenessProbe::new(60, 12),
            #[cfg(feature = "test-utils")]
            test_mode_skip_synced_notification: false,
        },
        handle,
    )
}

fn drain(handle: &mut L1WatcherHandle) -> Vec<L1Notification> {
    let mut out = Vec::new();
    while let Ok(notification) = handle.l1_notification_receiver().try_recv() {
        out.push((*notification).clone());
    }
    out
}

/// `(queue index, L1 block number, deposit amount)` of every `L1Message` notification.
fn messages(notifications: &[L1Notification]) -> Vec<(u64, u64, U256)> {
    notifications
        .iter()
        .filter_map(|n| match n {
            L1Notification::L1Message { message, block_info, .. } => {
                Some((message.queue_index, block_info.number, message.value))
            }
            _ => None,
        })
        .collect()
}

fn reorgs(notifications: &[L1Notification]) -> Vec<u64> {
    notifications
        .iter()
        .filter_map(|n| match n {
            L1Notification::Reorg(number) => Some(*number),
            _ => None,
        })
        .collect()
}

fn amount(variant: u64) -> U256 {
    U256::from(1_000 * (variant + 1))
}

/// The chain every test starts from: B0 (genesis), B1, B2, B3; B1, B2 and B3 carry deposits
/// with queue indices 0, 1 and 2.
struct Fixture {
    b0: Header,
    b1: Header,
    b2: Header,
    b3: Header,
}

impl Fixture {
    fn new() -> Self {
        let b0 = genesis();
        let b1 = header(1, 0, &b0);
        let b2 = header(2, 0, &b1);
        let b3 = header(3, 0, &b2);
        Self { b0, b1, b2, b3 }
    }

    fn chain(&self) -> Vec<Header> {
        vec![self.b0.clone(), self.b1.clone(), self.b2.clone(), self.b3.clone()]
    }

    fn logs(&self) -> Vec<Log> {
        vec![deposit(&self.b1, 0, 0), deposit(&self.b2, 1, 0), deposit(&self.b3, 2, 0)]
    }

    /// Polls the original chain once and checks the notifications of that first poll.
    async fn first_poll(&self) -> (SyntheticL1, L1Watcher<SyntheticL1>, L1WatcherHandle) {
        let l1 = SyntheticL1::new(self.chain(), self.logs());
        let (mut w, mut h) = watcher(l1.clone());
        w.step().await.expect("first poll");
        let first = drain(&mut h);
        // finalized == latest: the Finalized(3) notification precedes the logs of block 3.
        assert_eq!(first.first(), Some(&L1Notification::Finalized(3)));
        assert_eq!(first.get(1), Some(&L1Notification::NewBlock((&self.b3).into())));
        assert_eq!(messages(&first), vec![(0, 1, amount(0)), (1, 2, amount(0)), (2, 3, amount(0))]);
        assert_eq!(first.last(), Some(&L1Notification::Processed(3)));
        // Only the head is kept for reorg detection.
        assert_eq!(w.unfinalized_blocks.iter().cloned().collect::<Vec<_>>(), vec![self.b3.clone()]);
        (l1, w, h)
    }
}

/// Case 1(a): blocks 2 and 3 are replaced at the same height (fork point: block 1). The watcher
/// reports `Reorg(2)` (previous head - 1), so only block 3's message is unwound; block 2' is never
/// read and its replacement message (queue index 1, new amount) is never delivered.
#[tokio::test]
async fn rg41_two_block_replacement_reports_head_minus_one() {
    let f = Fixture::new();
    let (l1, mut w, mut h) = f.first_poll().await;

    let b2r = header(2, 1, &f.b1);
    let b3r = header(3, 1, &b2r);
    l1.serve(
        vec![f.b0.clone(), f.b1.clone(), b2r.clone(), b3r.clone()],
        vec![deposit(&b2r, 1, 1), deposit(&b3r, 2, 1)],
    );

    w.step().await.expect("second poll");
    let second = drain(&mut h);

    assert_eq!(reorgs(&second), vec![2], "fork point is block 1, the watcher unwinds from 2");
    assert_eq!(
        second,
        vec![
            L1Notification::Reorg(2),
            L1Notification::NewBlock((&b3r).into()),
            L1Notification::L1Message {
                message: QueueTransaction {
                    sender: Address::repeat_byte(0x11),
                    target: Address::repeat_byte(0x22),
                    value: amount(1),
                    queueIndex: 2,
                    gasLimit: U256::from(100_000),
                    data: Bytes::new(),
                }
                .into(),
                block_info: (&b3r).into(),
                block_timestamp: b3r.timestamp,
            },
            L1Notification::Processed(3),
        ]
    );
    // Block 2' (queue index 1 with the new amount) is never delivered.
    assert!(messages(&second).iter().all(|(queue_index, _, _)| *queue_index != 1));
}

/// Control for case 1(a): the same replacement against an L1 whose finalized block lags the head
/// by two blocks is reported at the fork point (`Reorg(1)`), and both replaced blocks are re-read.
/// The head-minus-one behaviour above comes from finalized == latest.
#[tokio::test]
async fn rg41_control_two_block_replacement_with_finality_lag_reports_fork_point() {
    let f = Fixture::new();
    let l1 = SyntheticL1::new(f.chain(), f.logs());
    l1.state().finalized_lag = 2;
    let (mut w, mut h) = watcher(l1.clone());
    w.step().await.expect("first poll");
    let first = drain(&mut h);
    assert_eq!(first.first(), Some(&L1Notification::Finalized(1)));
    assert_eq!(messages(&first).len(), 3);

    let b2r = header(2, 1, &f.b1);
    let b3r = header(3, 1, &b2r);
    l1.serve(
        vec![f.b0.clone(), f.b1.clone(), b2r.clone(), b3r.clone()],
        vec![deposit(&b2r, 1, 1), deposit(&b3r, 2, 1)],
    );
    w.step().await.expect("second poll");
    let second = drain(&mut h);
    assert_eq!(reorgs(&second), vec![1]);
    assert_eq!(messages(&second), vec![(1, 2, amount(1)), (2, 3, amount(1))]);
}

/// Case 1(b): the watcher polls while the served head is rolled back to block 1, then sees the
/// replacement blocks as a plain extension. The rollback poll reports `Reorg(2)` (previous head -
/// 1), which keeps block 2's message although block 2 is no longer served; the replacement for
/// queue index 1 is then delivered for block 2' and, in the database, dropped by
/// `insert_l1_message`'s on-conflict-do-nothing.
#[tokio::test]
async fn rg41_rollback_poll_then_regrowth_keeps_block_two_message() {
    let f = Fixture::new();
    let (l1, mut w, mut h) = f.first_poll().await;

    // Rollback: the served head drops to B1.
    l1.serve(vec![f.b0.clone(), f.b1.clone()], vec![]);
    w.step().await.expect("rollback poll");
    let rollback = drain(&mut h);
    assert_eq!(
        rollback,
        vec![
            L1Notification::Reorg(2),
            L1Notification::NewBlock((&f.b1).into()),
            L1Notification::Processed(1),
        ],
        "a drop from head 3 to head 1 still unwinds only from block 2"
    );

    // Regrowth with replaced blocks 2' and 3'.
    let b2r = header(2, 1, &f.b1);
    let b3r = header(3, 1, &b2r);
    l1.serve(vec![f.b0.clone(), f.b1.clone(), b2r.clone()], vec![deposit(&b2r, 1, 1)]);
    w.step().await.expect("regrowth poll 1");
    let regrow_1 = drain(&mut h);
    assert!(reorgs(&regrow_1).is_empty(), "2' extends the served head: no reorg");
    assert_eq!(messages(&regrow_1), vec![(1, 2, amount(1))]);

    l1.serve(vec![f.b0.clone(), f.b1.clone(), b2r.clone(), b3r.clone()], vec![deposit(&b3r, 2, 1)]);
    w.step().await.expect("regrowth poll 2");
    let regrow_2 = drain(&mut h);
    assert!(reorgs(&regrow_2).is_empty());
    assert_eq!(messages(&regrow_2), vec![(2, 3, amount(1))]);
}

/// Cases 1(b) of the task / 19: the replacement chain is already longer than the old head at
/// the next poll. `handle_finalized_block` clears the stored head (it is below the new finalized
/// block) and the new head is taken as a fresh start: no `Reorg` at all, and blocks 2' and 3'
/// are never read.
#[tokio::test]
async fn rg41_longer_replacement_reports_no_reorg() {
    let f = Fixture::new();
    let (l1, mut w, mut h) = f.first_poll().await;

    let b2r = header(2, 1, &f.b1);
    let b3r = header(3, 1, &b2r);
    let b4r = header(4, 1, &b3r);
    l1.serve(
        vec![f.b0.clone(), f.b1.clone(), b2r.clone(), b3r.clone(), b4r.clone()],
        vec![deposit(&b2r, 1, 1), deposit(&b3r, 2, 1), deposit(&b4r, 3, 1)],
    );

    w.step().await.expect("second poll");
    let second = drain(&mut h);
    assert!(reorgs(&second).is_empty(), "no reorg is reported: {second:?}");
    assert_eq!(second.first(), Some(&L1Notification::Finalized(4)));
    // Only block 4' is read; the replaced messages for queue indices 1 and 2 never arrive.
    assert_eq!(messages(&second), vec![(3, 4, amount(1))]);
    assert_eq!(second.last(), Some(&L1Notification::Processed(4)));
}

/// Case 1(d), fresh genesis: `step` reads `finalized` from the old chain (head B3) and `latest`
/// from the rolled-back chain (head B2'). `fetch_unfinalized_chain` walks down by number looking
/// for a block that is, or is a child of, the old finalized hash; none exists, and at block 0 the
/// walk asks for block `0.saturating_sub(1) == 0` again. The provider here stops serving block 0
/// after a budget, so the walk ends with `MissingBlock(0)`; against a real provider it does not
/// end (and the chain vector grows on every lookup).
#[cfg(not(feature = "test-utils"))]
#[tokio::test]
async fn rg45_finalized_latest_race_walk_never_terminates_on_fresh_genesis() {
    const GENESIS_BUDGET: u64 = 10_000;
    let f = Fixture::new();
    let (l1, mut w, mut h) = f.first_poll().await;

    let b2r = header(2, 1, &f.b1);
    {
        let mut s = l1.state();
        s.genesis_budget = Some(s.genesis_lookups + GENESIS_BUDGET);
        s.switch_after_finalized = Some(vec![f.b0.clone(), f.b1.clone(), b2r.clone()]);
        s.logs.entry(b2r.hash).or_default().push(deposit(&b2r, 1, 1));
    }
    let lookups_before = l1.state().by_number_lookups;
    let genesis_before = l1.state().genesis_lookups;

    let result = tokio::time::timeout(std::time::Duration::from_secs(60), w.step())
        .await
        .expect("bounded by the provider budget, not by the timeout");
    let err = result.expect_err("the walk cannot find a stopping point");
    assert!(
        matches!(err, L1WatcherError::EthRequest(EthRequestError::MissingBlock(0))),
        "unexpected error {err:?}"
    );

    let s = l1.state();
    // The walk re-requested block 0 until the provider refused: every one of the budgeted
    // lookups, plus the refused one.
    assert_eq!(s.genesis_lookups - genesis_before, GENESIS_BUDGET + 1);
    // Block 1 once, then block 0 over and over.
    assert_eq!(s.by_number_lookups - lookups_before, GENESIS_BUDGET + 2);
    assert_eq!(s.by_hash_lookups, 0, "the walk looks blocks up by number only");
    drop(s);
    assert!(drain(&mut h).is_empty(), "no Reorg, NewBlock or log notification was sent");
}

/// Case 1(d), history that starts above genesis: the same race fails the step with
/// `MissingBlock`, and the next poll adopts the new chain with no `Reorg` at all: the old
/// messages of blocks 2 and 3 are never unwound and block 2' is never read.
#[cfg(not(feature = "test-utils"))]
#[tokio::test]
async fn rg45_finalized_latest_race_on_truncated_history_skips_the_reorg() {
    let f = Fixture::new();
    let (l1, mut w, mut h) = f.first_poll().await;

    let b2r = header(2, 1, &f.b1);
    {
        let mut s = l1.state();
        s.floor = 1;
        s.switch_after_finalized = Some(vec![f.b0.clone(), f.b1.clone(), b2r.clone()]);
        s.logs.entry(b2r.hash).or_default().push(deposit(&b2r, 1, 1));
    }

    let err = w.step().await.expect_err("race poll fails");
    assert!(
        matches!(err, L1WatcherError::EthRequest(EthRequestError::MissingBlock(0))),
        "unexpected error {err:?}"
    );
    assert!(drain(&mut h).is_empty());
    // handle_finalized_block drained the old head before the walk failed.
    assert!(w.unfinalized_blocks.is_empty());

    // Next poll: no race; the watcher takes B2' as a fresh start.
    w.step().await.expect("next poll");
    let next = drain(&mut h);
    assert_eq!(
        next,
        vec![L1Notification::NewBlock((&b2r).into()), L1Notification::Processed(2)],
        "no Reorg, and block 2' is not read"
    );
}

/// Case 20: a restart with finalized = head starts from the block of the highest stored message
/// or batch (`L1BlockStartupInfo::FinalizedBlockNumber`, because the `l1_block` table stays empty)
/// and runs no unsafe-block check, so a replacement below that block is never seen.
#[tokio::test]
async fn rg41_restart_from_finalized_block_number_does_not_detect_replacement() {
    let f = Fixture::new();
    let b2r = header(2, 1, &f.b1);
    let b3r = header(3, 1, &b2r);
    let l1 = SyntheticL1::new(
        vec![f.b0.clone(), f.b1.clone(), b2r.clone(), b3r.clone()],
        vec![deposit(&f.b1, 0, 0), deposit(&b2r, 1, 1), deposit(&b3r, 2, 1)],
    );
    let config = NodeConfig { start_l1_block: 1, ..NodeConfig::mainnet() };

    // The database's highest stored message is the old block 3's: the node restarts from 3.
    let (_tx, mut handle) = L1Watcher::spawn(
        l1.clone(),
        L1BlockStartupInfo::FinalizedBlockNumber(3),
        Arc::new(config),
        500,
        60,
        12,
        #[cfg(feature = "test-utils")]
        false,
    )
    .await;

    let mut seen = Vec::new();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let next = tokio::time::timeout_at(deadline, handle.l1_notification_receiver().recv())
            .await
            .expect("watcher reaches Synced within the deadline")
            .expect("channel open");
        let done = matches!(*next, L1Notification::Synced);
        seen.push((*next).clone());
        if done {
            break;
        }
    }

    assert!(reorgs(&seen).is_empty(), "no reorg on restart: {seen:?}");
    // Only block 3' is re-read; block 2' (queue index 1, new amount) is not.
    assert_eq!(messages(&seen), vec![(2, 3, amount(1))]);
}

/// Case 32 (F13): the runtime signer refresh never fires. `handle_latest_block` sets
/// `l1_state.head = latest.number` before `handle_system_contract_update` compares
/// `latest.number != l1_state.head`, so the system contract is never read.
#[tokio::test]
async fn rg52_runtime_signer_refresh_never_reads_the_system_contract() {
    let old_signer = Address::repeat_byte(0xaa);
    let new_signer = Address::repeat_byte(0xbb);
    let f = Fixture::new();
    let l1 = SyntheticL1::new(f.chain(), f.logs());
    l1.state().signer = old_signer;
    let (mut w, mut h) = watcher(l1.clone());
    let mut all = Vec::new();

    w.step().await.expect("poll at head 3");
    all.extend(drain(&mut h));

    // The signer rotates; the chain extends by a block that carries a log, then by an empty
    // block, then block 5 is replaced at the same height.
    l1.state().signer = new_signer;
    let b4 = header(4, 0, &f.b3);
    let mut chain = f.chain();
    chain.push(b4.clone());
    l1.serve(chain.clone(), vec![deposit(&b4, 3, 0)]);
    w.step().await.expect("poll at head 4 (with a log)");
    all.extend(drain(&mut h));

    let b5 = header(5, 0, &b4);
    chain.push(b5.clone());
    l1.serve(chain.clone(), vec![]);
    w.step().await.expect("poll at head 5 (no logs): no InvalidNotificationCount either");
    all.extend(drain(&mut h));

    let b5r = header(5, 1, &b4);
    chain.pop();
    chain.push(b5r.clone());
    l1.serve(chain.clone(), vec![]);
    w.step().await.expect("poll at replaced head 5");
    let replaced = drain(&mut h);
    assert_eq!(reorgs(&replaced), vec![4]);
    all.extend(replaced);

    assert!(
        !all.iter().any(|n| matches!(n, L1Notification::Consensus(_))),
        "no AuthorizedSigner notification after the rotation: {all:?}"
    );
    assert_eq!(l1.state().storage_reads, 0, "slot 0x67 was never read");
    assert_eq!(w.l1_state.head, 5);

    // Control: the refresh itself works when the head comparison sees an older head.
    let b6 = header(6, 0, &b5r);
    let notification =
        w.handle_system_contract_update(&block(b6)).await.expect("storage read succeeds");
    assert_eq!(
        notification,
        Some(L1Notification::Consensus(ConsensusUpdate::AuthorizedSigner(new_signer)))
    );
    assert_eq!(l1.state().storage_reads, 1);
}
