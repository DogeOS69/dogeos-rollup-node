//! Verification tests for `REORG_FINDINGS` F1 (queue-gap stop) and F9 (finality at zero depth)
//! at the orchestrator level. They call the orchestrator's handlers directly with an in-memory
//! database, a no-op network and an Engine client that panics if called. Assertions document
//! current behaviour.
//!
//! Tracker rows (Reorg Issue Tracker, Private Mainnet): RG-43 (message sync stops on a queue gap),
//! RG-46 (L2 finalized at zero synthetic depth cannot be undone) and RG-49 (a signed block that was
//! requested before an unwind is still persisted and announced after it, rollup-node issue #30).
//! Each test name starts with the RG key of the row it pins. Every test passes today and documents
//! behaviour that is not fixed; when a row is fixed, change the matching assertion instead of
//! deleting the test.

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
use rollup_node_watcher::L1WatcherCommand;
use scroll_db::test_utils::setup_test_db;
use scroll_engine::{
    test_utils::{PanicEngineClient, ScriptedEngineClient, ScriptedResponse},
    EngineError, FcsError, ForkchoiceState,
};
use scroll_network::{NetworkHandleMessage, ScrollNetworkHandle};

type TestNetwork = NoopNetwork<DogeosNetworkPrimitives>;
type TestL1Provider = MockL1Provider<Arc<Database>>;
type TestOrchestrator<EC = PanicEngineClient> =
    ChainOrchestrator<TestNetwork, DogeosChainSpec, TestL1Provider, ScrollRootProvider, EC>;

fn info(number: u64, tag: u8) -> BlockInfo {
    BlockInfo { number, hash: B256::repeat_byte(tag) }
}

/// A network handle backed by a no-op network. Every block announcement is forwarded to
/// `announced` when given.
async fn test_scroll_network(
    announced: Option<mpsc::UnboundedSender<(DogeosBlock, alloy_primitives::Signature)>>,
) -> ScrollNetwork<TestNetwork> {
    let (to_manager_tx, mut from_handle_rx) = mpsc::unbounded_channel();
    let handle =
        ScrollNetworkHandle::new(to_manager_tx, NoopNetwork::<DogeosNetworkPrimitives>::new());
    tokio::spawn(async move {
        let events = EventSender::new(16);
        while let Some(message) = from_handle_rx.recv().await {
            match message {
                NetworkHandleMessage::EventListener(response) => {
                    let _ = response.send(events.new_listener());
                }
                NetworkHandleMessage::AnnounceBlock { block, signature } => {
                    if let Some(announced) = &announced {
                        let _ = announced.send((block, signature));
                    }
                }
                _ => {}
            }
        }
    });
    handle.into_scroll_network().await
}

/// Observation points on the orchestrator's outputs. Each one is optional.
#[derive(Default)]
struct Probes {
    /// Receives every block the orchestrator announces to the network.
    announced: Option<mpsc::UnboundedSender<(DogeosBlock, alloy_primitives::Signature)>>,
    /// Receives every command the orchestrator sends to the L1 watcher.
    watcher_commands: Option<mpsc::UnboundedSender<L1WatcherCommand>>,
}

/// An orchestrator with no held batch, an Engine that panics if called and the given forkchoice
/// state and V2 message-queue start index.
async fn orchestrator(
    database: Arc<Database>,
    fcs: ForkchoiceState,
    v2_start: u64,
) -> TestOrchestrator {
    orchestrator_probed(database, fcs, v2_start, Probes::default()).await
}

/// Like [`orchestrator`], and reports the outputs named in `probes`.
async fn orchestrator_probed(
    database: Arc<Database>,
    fcs: ForkchoiceState,
    v2_start: u64,
    probes: Probes,
) -> TestOrchestrator {
    orchestrator_with_engine(database, fcs, v2_start, probes, Arc::new(PanicEngineClient)).await
}

/// Like [`orchestrator_probed`], with the given Engine client.
async fn orchestrator_with_engine<EC: ScrollEngineApi + Unpin + Send + Sync + 'static>(
    database: Arc<Database>,
    fcs: ForkchoiceState,
    v2_start: u64,
    probes: Probes,
    engine_client: Arc<EC>,
) -> TestOrchestrator<EC> {
    let engine = Engine::new(engine_client, fcs);
    let l2_provider =
        ProviderBuilder::<_, _, Scroll>::default().connect_mocked_client(Asserter::new());
    let l1_provider = MockL1Provider { db: database.clone(), blobs: Default::default() };
    let derivation_pipeline =
        DerivationPipeline::new(l1_provider.clone(), database.clone(), v2_start).await;
    let (watcher_command_tx, _watcher_command_rx) = mpsc::unbounded_channel();
    let watcher_command_tx = probes.watcher_commands.unwrap_or(watcher_command_tx);
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
        test_scroll_network(probes.announced).await,
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

/// Batch 1 (L2 blocks 1..=5, committed and finalized at L1 block 10) and batch 2 (L2 blocks
/// 6..=10, committed and finalized at L1 block 20). Both were finalized at zero L1 depth, so a
/// reorg below batch 2's commit block deletes batch 2 and its L2 blocks.
async fn seed_zero_depth_finalized_batches(database: &Database) {
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
}

/// F9: L2 blocks 6..=10 of batch 2 were finalized at zero L1 depth. A synthetic L1 reorg below
/// batch 2's commit block deletes the batch row and (by cascade) its L2 block rows, then the
/// forkchoice update with the lowered safe block and no finalized value fails with
/// `SafeBelowFinalized`. `handle_l1_reorg` returns that error after the database unwind, without
/// the `L1Reorg` event, and the Engine keeps the old head, safe and finalized blocks.
#[tokio::test]
async fn rg46_l1_reorg_below_zero_depth_finalized_batch_leaves_db_and_engine_disagreeing() {
    let database = Arc::new(setup_test_db().await);
    seed_zero_depth_finalized_batches(&database).await;

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

/// RG-49 (rollup-node issue #30): block signing is asynchronous, and the orchestrator has no way to
/// tell that an unwind happened between the sign request and the signed result.
///
/// The chain has L2 blocks 1..=5. Block 4 executed the L1 message from L1 block 5, and block 5 is
/// waiting for its signature. An L1 reorg to block 3 deletes the message, so the database head
/// moves back to L2 block 3 and block 5 belongs to a chain that no longer exists. The signed result
/// for block 5 then arrives. `handle_signer_event` persists it as the new head, stores its
/// signature and announces the block to the network.
///
/// The unwind is applied through `Database::unwind`, the operation `handle_l1_reorg` runs first.
/// The rest of `handle_l1_reorg` (L2 client lookup and forkchoice update) does not touch the signer
/// path, so it is left out.
///
/// Passes today and documents the defect. The fix in the issue tags each sign request with an
/// unwind generation and drops results from an older generation. Once the signer API carries that
/// tag, change the assertions to expect the head to stay at 3, no signature and no announcement.
#[tokio::test]
async fn rg49_signed_block_requested_before_an_unwind_is_still_persisted_and_announced() {
    let database = Arc::new(setup_test_db().await);
    let (announced_tx, mut announced_rx) = mpsc::unbounded_channel();
    let orchestrator = orchestrator_probed(
        database.clone(),
        ForkchoiceState::from_genesis(B256::ZERO),
        0,
        Probes { announced: Some(announced_tx), ..Default::default() },
    )
    .await;

    // Block 4 executed deposit 0 from L1 block 5. The head marker is at block 4 and block 5 is in
    // flight for signing.
    orchestrator.handle_l1_message(deposit(0), info(5, 5)).await.unwrap();
    database
        .update_l1_messages_from_l2_blocks(vec![L2BlockInfoWithL1Messages {
            block_info: info(4, 4),
            l1_messages: vec![deposit(0).tx_hash()],
        }])
        .await
        .unwrap();
    database.set_l2_head_block_number(4).await.unwrap();

    // The unwind removes deposit 0, so the head falls back to the block before it.
    let unwind = database.unwind(3).await.unwrap();
    assert_eq!(unwind.l2_head_block_number, Some(3));
    assert_eq!(database.get_l2_head_block_number().await.unwrap(), 3);

    // The signer now delivers the result requested before the unwind.
    let block = DogeosBlock {
        header: alloy_consensus::Header { number: 5, ..Default::default() },
        ..Default::default()
    };
    let hash = block.hash_slow();
    let signature = alloy_primitives::Signature::new(U256::from(1), U256::from(2), false);
    let event = orchestrator
        .handle_signer_event(SignerEvent::SignedBlock { block: block.clone(), signature })
        .await
        .unwrap();

    // Current behaviour: the stale result is accepted in full.
    assert!(
        matches!(event, Some(ChainOrchestratorEvent::SignedBlock { .. })),
        "the stale signed block is turned into a SignedBlock event, got {event:?}"
    );
    assert_eq!(
        database.get_l2_head_block_number().await.unwrap(),
        5,
        "the head marker jumps from the unwound head 3 to the stale block 5"
    );
    assert_eq!(database.get_signature(hash).await.unwrap(), Some(signature));
    let (announced, announced_signature) =
        announced_rx.try_recv().expect("the stale block was announced to the network");
    assert_eq!((announced.header.number, announced_signature), (5, signature));
}

/// RG-49: the administrative `RevertToL1Block` command is not atomic. It unwinds the database
/// first, then updates the forkchoice state, and only then tells the L1 watcher to go back. If the
/// forkchoice step fails, the command returns early, the database is already unwound and the
/// watcher is never reset, so it keeps delivering from its old position and the L1 data between
/// the revert point and that position is never fetched again.
///
/// The trigger is an Engine API failure on the safe-head update that follows the unwind (a
/// transport error here; an Engine that is restarting or slow produces the same result). The
/// unwind deletes batch 2, so the safe block moves and the Engine call is made.
///
/// The test asserts the invariant that holds under every fix: the database is unwound if and only
/// if the watcher was told to revert. Today the database is unwound and the watcher is not, so the
/// test is ignored. A fix may order the steps differently (forkchoice first, or a compensating
/// reset), which is why the assertion compares the two effects and does not pin either one.
#[tokio::test]
#[ignore = "RG-49: RevertToL1Block unwinds the database, then fails at the forkchoice step and never resets the L1 watcher"]
async fn rg49_admin_revert_unwinds_the_database_only_together_with_the_watcher_reset() {
    let database = Arc::new(setup_test_db().await);
    seed_zero_depth_finalized_batches(&database).await;

    let engine_client = Arc::new(ScriptedEngineClient::new());
    engine_client.push_fork_choice_updated(ScriptedResponse::TransportFailure);
    let (watcher_commands_tx, mut watcher_commands_rx) = mpsc::unbounded_channel();
    let finalized = info(10, 10);
    let mut orchestrator = orchestrator_with_engine(
        database.clone(),
        ForkchoiceState::new(finalized, finalized, finalized),
        0,
        Probes { watcher_commands: Some(watcher_commands_tx), ..Default::default() },
        engine_client.clone(),
    )
    .await;

    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    let result = orchestrator
        .handle_command(ChainOrchestratorCommand::RevertToL1Block((15, reply_tx)))
        .await;

    // The trigger: the forkchoice step fails after the database unwind.
    assert!(matches!(result, Err(ChainOrchestratorError::EngineError(_))), "got {result:?}");
    assert_eq!(engine_client.fork_choice_updated_calls(), 1);
    // The caller is told nothing: the reply channel is dropped without a value.
    assert!(reply_rx.await.is_err(), "the admin caller gets no reply");

    let database_unwound = database.get_batch_by_index(2).await.unwrap().is_none();
    let watcher_reset = matches!(
        watcher_commands_rx.try_recv(),
        Ok(L1WatcherCommand::ResetToBlock { block: 15, .. })
    );
    assert_eq!(
        database_unwound, watcher_reset,
        "database unwound: {database_unwound}, watcher reset: {watcher_reset}"
    );
}
