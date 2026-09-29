//! Verification tests for REORG_FINDINGS F1 (queue-gap stop) and F9 (finality at zero depth)
//! at the orchestrator level. They call the orchestrator's handlers directly with an in-memory
//! database, a no-op network and an Engine client that panics if called. Assertions document
//! current behaviour.
//!
//! Tracker rows (Reorg Issue Tracker, Private Mainnet): RG-43 (message sync stops on a queue gap)
//! and RG-46 (L2 finalized at zero synthetic depth cannot be undone). Each test name starts with
//! the RG key of the row it pins. Every test passes today and documents behaviour that is not
//! fixed; when a row is fixed, change the matching assertion instead of deleting the test.

use super::*;
use alloy_primitives::{Address, U256};
use alloy_provider::ProviderBuilder;
use alloy_transport::mock::Asserter;
use dogeos_chainspec::{DogeosChainSpec, DOGEOS_DEV};
use dogeos_reth_consensus::DogeosConsensus;
use futures::FutureExt;
use reth_network_api::noop::NoopNetwork;
use reth_network_p2p::NoopFullBlockClient;
use rollup_node_providers::{test_utils::MockL1Provider, ScrollRootProvider};
use scroll_db::test_utils::setup_test_db;
use scroll_engine::{test_utils::PanicEngineClient, EngineError, FcsError, ForkchoiceState};
use scroll_network::{NetworkHandleMessage, ScrollNetworkHandle};

type TestNetwork = NoopNetwork<DogeosNetworkPrimitives>;
type TestL1Provider = MockL1Provider<Arc<Database>>;
type TestOrchestrator = ChainOrchestrator<
    TestNetwork,
    DogeosChainSpec,
    TestL1Provider,
    ScrollRootProvider,
    PanicEngineClient,
>;

fn info(number: u64, tag: u8) -> BlockInfo {
    BlockInfo { number, hash: B256::repeat_byte(tag) }
}

async fn test_scroll_network() -> ScrollNetwork<TestNetwork> {
    let (to_manager_tx, mut from_handle_rx) = mpsc::unbounded_channel();
    let handle =
        ScrollNetworkHandle::new(to_manager_tx, NoopNetwork::<DogeosNetworkPrimitives>::new());
    tokio::spawn(async move {
        let events = EventSender::new(16);
        while let Some(message) = from_handle_rx.recv().await {
            if let NetworkHandleMessage::EventListener(response) = message {
                let _ = response.send(events.new_listener());
            }
        }
    });
    handle.into_scroll_network().await
}

/// An orchestrator with no held batch, an Engine that panics if called and the given forkchoice
/// state and V2 message-queue start index.
async fn orchestrator(
    database: Arc<Database>,
    fcs: ForkchoiceState,
    v2_start: u64,
) -> TestOrchestrator {
    let engine = Engine::new(Arc::new(PanicEngineClient), fcs);
    let l2_provider =
        ProviderBuilder::<_, _, Scroll>::default().connect_mocked_client(Asserter::new());
    let l1_provider = MockL1Provider { db: database.clone(), blobs: Default::default() };
    let derivation_pipeline =
        DerivationPipeline::new(l1_provider.clone(), database.clone(), v2_start).await;
    let (watcher_command_tx, _watcher_command_rx) = mpsc::unbounded_channel();
    let (_notification_tx, notification_rx) = mpsc::channel(16);
    let l1_watcher = L1WatcherHandle::new(watcher_command_tx, notification_rx);
    let block_client = Arc::new(FullBlockClient::new(
        NoopFullBlockClient::<DogeosNetworkPrimitives>::default(),
        Arc::new(DogeosConsensus),
    ));
    let config = ChainOrchestratorConfig::<DogeosChainSpec>::new(DOGEOS_DEV.clone(), 1, v2_start);
    let (mut orchestrator, _handle) = ChainOrchestrator::new(
        database,
        config,
        block_client,
        l2_provider,
        l1_watcher,
        test_scroll_network().await,
        Box::new(NoopConsensus),
        engine,
        None::<Sequencer<TestL1Provider, DogeosChainSpec>>,
        None,
        derivation_pipeline,
    )
    .await
    .unwrap();
    orchestrator.sync_state.l1_mut().set_synced();
    orchestrator
}

fn deposit(queue_index: u64) -> TxL1Message {
    TxL1Message {
        queue_index,
        gas_limit: 100_000,
        to: Address::repeat_byte(0x22),
        value: U256::from(1_000 + queue_index),
        sender: Address::repeat_byte(0x11),
        input: Default::default(),
    }
}

async fn stored_queue_indices(database: &Database) -> Vec<u64> {
    database
        .get_n_l1_messages(Some(L1MessageKey::from_queue_index(0)), 100)
        .await
        .unwrap()
        .into_iter()
        .map(|m| m.transaction.queue_index)
        .collect()
}

/// F1 (`added_deposit_stops_message_sync`): a replacement adds a deposit at or below the unwind
/// point, the watcher never reads it, and the next message's queue index leaves a gap. The
/// orchestrator rejects that message and every later one (`L1MessageQueueGap` when the index is
/// below the V2 queue start, `L1MessageNotFound` from the queue-hash computation above it), so
/// message sync stops.
#[tokio::test]
async fn rg43_queue_gap_stops_l1_message_storage() {
    for v2_start in [1_000u64, 0] {
        let database = Arc::new(setup_test_db().await);
        let orchestrator =
            orchestrator(database.clone(), ForkchoiceState::from_genesis(B256::ZERO), v2_start)
                .await;

        orchestrator.handle_l1_message(deposit(0), info(1, 1)).await.unwrap();
        // Queue index 1 (the added deposit in the replaced lower block) is never delivered.
        for (queue_index, l1_block) in [(2u64, 2u64), (3, 3), (4, 4)] {
            let err = orchestrator
                .handle_l1_message(deposit(queue_index), info(l1_block, l1_block as u8))
                .await
                .unwrap_err();
            if v2_start == 1_000 {
                assert!(
                    matches!(err, ChainOrchestratorError::L1MessageQueueGap(q) if q == queue_index),
                    "v2_start={v2_start} q={queue_index}: {err:?}"
                );
            } else {
                assert!(
                    matches!(
                        err,
                        ChainOrchestratorError::DatabaseError(DatabaseError::L1MessageNotFound(
                            L1MessageKey::QueueIndex(q)
                        )) if q == queue_index - 1
                    ),
                    "v2_start={v2_start} q={queue_index}: {err:?}"
                );
            }
        }
        assert_eq!(stored_queue_indices(&database).await, vec![0], "v2_start={v2_start}");
    }
}

/// F9: L2 blocks 6..=10 of batch 2 were finalized at zero L1 depth. A synthetic L1 reorg below
/// batch 2's commit block deletes the batch row and (by cascade) its L2 block rows, then the
/// forkchoice update with the lowered safe block and no finalized value fails with
/// `SafeBelowFinalized`. `handle_l1_reorg` returns that error after the database unwind, without
/// the `L1Reorg` event, and the Engine keeps the old head, safe and finalized blocks.
#[tokio::test]
async fn rg46_l1_reorg_below_zero_depth_finalized_batch_leaves_db_and_engine_disagreeing() {
    let database = Arc::new(setup_test_db().await);
    let batch = |index: u64, tag: u8, l1_block: u64| BatchCommitData {
        hash: B256::repeat_byte(tag),
        index,
        block_number: l1_block,
        block_timestamp: 1_000 + l1_block,
        calldata: Arc::new(Default::default()),
        blob_versioned_hash: None,
        finalized_block_number: Some(l1_block),
        reverted_block_number: None,
    };
    let batch_1 = batch(1, 0xb1, 10);
    let batch_2 = batch(2, 0xb2, 20);
    database.insert_batch(batch_1.clone()).await.unwrap();
    database
        .insert_blocks((1..=5).map(|n| info(n, n as u8)).collect(), (&batch_1).into())
        .await
        .unwrap();
    database.insert_batch(batch_2.clone()).await.unwrap();
    database
        .insert_blocks((6..=10).map(|n| info(n, n as u8)).collect(), (&batch_2).into())
        .await
        .unwrap();

    let finalized = info(10, 10);
    let mut orchestrator =
        orchestrator(database.clone(), ForkchoiceState::new(finalized, finalized, finalized), 0)
            .await;
    let mut events = orchestrator.event_listener();

    let result = orchestrator.handle_l1_reorg(15).await;
    let err = result.as_ref().unwrap_err();
    assert!(
        matches!(
            err,
            ChainOrchestratorError::EngineError(EngineError::FcsError(
                FcsError::SafeBelowFinalized
            ))
        ),
        "got {err:?}"
    );

    // The database was unwound: batch 2 and its L2 blocks are gone; the safe block is 5.
    assert!(database.get_batch_by_index(2).await.unwrap().is_none());
    assert!(database.get_l2_block_info_by_number(8).await.unwrap().is_none());
    assert_eq!(database.get_latest_safe_l2_info().await.unwrap().0.number, 5);
    // The Engine did not move.
    assert_eq!(orchestrator.engine.fcs(), &ForkchoiceState::new(finalized, finalized, finalized));

    // The run loop only logs the error; no event is emitted.
    orchestrator.handle_outcome(result);
    assert!(events.next().now_or_never().is_none(), "no L1Reorg event");
}
