//! Orchestrator-level regression tests for the reorg tracker rows below. They call the
//! orchestrator's handlers directly with an in-memory database, a no-op network and an Engine
//! client that either panics if called or answers from a script.
//!
//! Tracker rows (Reorg Issue Tracker, Private Mainnet): RG-43 (message sync stops on a queue gap),
//! RG-46 (L2 finalized at zero synthetic depth cannot be undone) and RG-49 (unwind robustness:
//! a signed block requested before an unwind is still persisted and announced after it, an
//! administrative revert that stops half way, and forkchoice answers that are not checked).
//! Each test name starts with the RG key of the row it pins.
//!
//! RG-46 asserts that finalized conflicts roll back the database unwind. Other passing tests
//! document behaviour that is not fixed; when a row is fixed, change the matching assertion
//! instead of deleting the test. Tests marked `#[ignore]` assert the intended behaviour, fail
//! today and carry the reason in the ignore message; run them with `--ignored`.

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
    ForkchoiceState,
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

/// RG-43: a replacement adds a deposit at or below the unwind
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

/// Batch 1 (L2 blocks 1..=5, committed at L1 block 10) and batch 2 (L2 blocks 6..=10, committed at
/// L1 block 20). Both batches are only committed: `insert_batch` stores no finalization, so a test
/// that needs one sets it afterwards.
async fn seed_two_committed_batches(database: &Database) {
    let batch = |index: u64, tag: u8, l1_block: u64| BatchCommitData {
        hash: B256::repeat_byte(tag),
        index,
        block_number: l1_block,
        block_timestamp: 1_000 + l1_block,
        calldata: Arc::new(Default::default()),
        blob_versioned_hash: None,
        finalized_block_number: None,
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

/// RG-46: a reorg or administrative unwind below the Engine's finalized block must fail
/// before committing database changes. Clamping the new safe block to the old finalized block
/// would leave the database at 5 and the Engine at 10 while reporting a successful unwind.
#[tokio::test]
async fn rg46_finalized_conflict_rolls_back_l1_unwind() {
    for administrative in [false, true] {
        let database = Arc::new(setup_test_db().await);
        seed_two_committed_batches(&database).await;
        let (watcher_commands_tx, mut watcher_commands_rx) = mpsc::unbounded_channel();
        let finalized = info(10, 10);
        let mut orchestrator = orchestrator_probed(
            database.clone(),
            ForkchoiceState::new(finalized, finalized, finalized),
            0,
            Probes { watcher_commands: Some(watcher_commands_tx), ..Default::default() },
        )
        .await;
        let mut events = orchestrator.event_listener();

        let result = if administrative {
            let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
            let result = orchestrator
                .handle_command(ChainOrchestratorCommand::RevertToL1Block((15, reply_tx)))
                .await;
            assert!(reply_rx.await.is_err(), "a rejected unwind must not report success");
            result.map(|()| None)
        } else {
            orchestrator.handle_l1_reorg(15).await
        };
        assert!(
            matches!(
                &result,
                Err(ChainOrchestratorError::FinalizedFrontierConflict { target, observed })
                    if *target == info(5, 5) && *observed == finalized
            ),
            "administrative={administrative}: {result:?}"
        );

        // The transaction rolled back: both batches, their L2 rows, and the frontier survive.
        assert!(database.get_batch_by_index(2).await.unwrap().is_some());
        assert_eq!(database.get_l2_block_info_by_number(8).await.unwrap(), Some(info(8, 8)));
        assert_eq!(database.get_latest_safe_l2_info().await.unwrap().0, finalized);
        assert!(database.get_pending_frontier_transition().await.unwrap().is_none());
        assert_eq!(
            orchestrator.engine.fcs(),
            &ForkchoiceState::new(finalized, finalized, finalized)
        );
        assert!(watcher_commands_rx.try_recv().is_err(), "watcher must not reset on rejection");

        // The notification path treats the conflict as fatal and emits no successful reorg.
        assert!(orchestrator.handle_outcome(result).unwrap_err().is_frontier_fatal());
        assert!(events.next().now_or_never().is_none(), "no successful unwind event");
    }
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
/// reset), retry the forkchoice update, return the error or reply `false`, which is why the test
/// checks neither the result nor the reply, and compares the two effects without pinning either.
#[tokio::test]
#[ignore = "RG-49: RevertToL1Block unwinds the database, then fails at the forkchoice step and never resets the L1 watcher"]
async fn rg49_admin_revert_unwinds_the_database_only_together_with_the_watcher_reset() {
    let database = Arc::new(setup_test_db().await);
    seed_two_committed_batches(&database).await;

    let engine_client = Arc::new(ScriptedEngineClient::new());
    // Enough failures for a fix that retries a bounded number of times (the scripted client panics
    // once its queue is empty).
    for _ in 0..8 {
        engine_client.push_fork_choice_updated(ScriptedResponse::TransportFailure);
    }
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

    let (reply_tx, _reply_rx) = tokio::sync::oneshot::channel();
    let result = orchestrator
        .handle_command(ChainOrchestratorCommand::RevertToL1Block((15, reply_tx)))
        .await;

    // The trigger: the forkchoice step was attempted (and failed).
    assert!(
        engine_client.fork_choice_updated_calls() >= 1,
        "the forkchoice update reached the Engine, result: {result:?}"
    );

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

// RG-49, unchecked forkchoice answers. `Engine::update_fcs` returns `Ok` for an `INVALID` answer
// and leaves its own forkchoice state unchanged. The orchestrator call sites below write
// `update_fcs(..).await?` and drop the returned status, so an `INVALID` answer is reported as
// success although the Engine did not accept the new state.
//
// Each test drives one call site with an Engine whose forkchoice update answers `INVALID`. The
// assertion is the same in every test and holds under any fix: the handler either fails, or it
// reports success only when the Engine's forkchoice state holds what the handler reported. Each
// test also checks that the forkchoice update reached the Engine, so it cannot pass by never
// making the call.

/// A scripted Engine client whose forkchoice updates answer `INVALID`. Several answers are queued
/// so that a fix which retries or falls back to a second forkchoice update reaches the assertion
/// instead of the scripted client's panic on an empty queue.
fn engine_rejecting_forkchoice() -> Arc<ScriptedEngineClient> {
    let client = Arc::new(ScriptedEngineClient::new());
    for _ in 0..8 {
        client.push_fork_choice_updated(ScriptedResponse::Ok(
            alloy_rpc_types_engine::ForkchoiceUpdated {
                payload_status: alloy_rpc_types_engine::PayloadStatus {
                    status: alloy_rpc_types_engine::PayloadStatusEnum::Invalid {
                        validation_error: "rejected by the test Engine".to_string(),
                    },
                    latest_valid_hash: None,
                },
                payload_id: None,
            },
        ));
    }
    client
}

/// `handle_l1_reorg`: the unwind deletes batch 2, so the safe block moves back to L2 block 5. The
/// Engine answers `INVALID` and keeps safe at block 10. The handler still returns the `L1Reorg`
/// event with safe block 5.
#[tokio::test]
#[ignore = "RG-49: handle_l1_reorg drops an INVALID forkchoice answer and reports the reorg as done"]
async fn rg49_l1_reorg_reports_the_new_safe_block_only_if_the_engine_accepted_it() {
    let database = Arc::new(setup_test_db().await);
    seed_two_committed_batches(&database).await;
    let engine_client = engine_rejecting_forkchoice();
    let mut orchestrator = orchestrator_with_engine(
        database,
        ForkchoiceState::new(info(10, 10), info(10, 10), info(5, 5)),
        0,
        Probes::default(),
        engine_client.clone(),
    )
    .await;

    let result = orchestrator.handle_l1_reorg(15).await;

    assert!(engine_client.fork_choice_updated_calls() >= 1, "the update reached the Engine");
    if let Ok(Some(ChainOrchestratorEvent::L1Reorg { l2_safe_block_info, .. })) = &result {
        assert_eq!(
            *l2_safe_block_info,
            Some(*orchestrator.engine.fcs().safe_block_info()),
            "the L1Reorg event reports a safe block the Engine did not accept"
        );
    }
}

/// `handle_l1_finalized`: batch 1 is consolidated and its L1 block is now finalized, so the
/// finalized L2 block becomes 5. The Engine answers `INVALID` and keeps finalized at block 0. The
/// handler still returns the `L1BlockFinalized` event.
#[tokio::test]
#[ignore = "RG-49: handle_l1_finalized drops an INVALID forkchoice answer and reports the finalization as done"]
async fn rg49_l1_finalized_reports_the_new_finalized_block_only_if_the_engine_accepted_it() {
    let database = Arc::new(setup_test_db().await);
    seed_two_committed_batches(&database).await;
    // Batch 1 is finalized on L1 at block 10 and already consolidated on L2.
    database.finalize_batches_up_to_index(1, 10).await.unwrap();
    database.update_batch_status(B256::repeat_byte(0xb1), BatchStatus::Consolidated).await.unwrap();
    let engine_client = engine_rejecting_forkchoice();
    let mut orchestrator = orchestrator_with_engine(
        database,
        ForkchoiceState::new(info(10, 10), info(10, 10), info(0, 0)),
        0,
        Probes::default(),
        engine_client.clone(),
    )
    .await;

    let result = orchestrator.handle_l1_finalized(10).await;

    assert!(
        engine_client.fork_choice_updated_calls() >= 1,
        "the update reached the Engine, result: {result:?}"
    );
    if let Ok(Some(ChainOrchestratorEvent::L1BlockFinalized(..))) = &result {
        assert_eq!(
            orchestrator.engine.fcs().finalized_block_info().number,
            5,
            "the L1BlockFinalized event reports a finalized block the Engine did not accept"
        );
    }
}

/// `handle_batch_revert`: reverting batch 2 moves the safe block back to L2 block 5. The Engine
/// answers `INVALID` and keeps safe at block 10. The handler still returns the `BatchReverted`
/// event with safe head 5.
#[tokio::test]
#[ignore = "RG-49: handle_batch_revert drops an INVALID forkchoice answer and reports the revert as done"]
async fn rg49_batch_revert_reports_the_new_safe_head_only_if_the_engine_accepted_it() {
    let database = Arc::new(setup_test_db().await);
    seed_two_committed_batches(&database).await;
    let engine_client = engine_rejecting_forkchoice();
    let mut orchestrator = orchestrator_with_engine(
        database,
        ForkchoiceState::new(info(10, 10), info(10, 10), info(0, 0)),
        0,
        Probes::default(),
        engine_client.clone(),
    )
    .await;
    // The safe head is only pushed to the Engine when both L1 and L2 are synced.
    orchestrator.sync_state.l2_mut().set_synced();

    let result = orchestrator.handle_batch_revert(2, 2, info(30, 30)).await;

    assert!(engine_client.fork_choice_updated_calls() >= 1, "the update reached the Engine");
    if let Ok(Some(ChainOrchestratorEvent::BatchReverted { safe_head, .. })) = &result {
        assert_eq!(
            safe_head,
            orchestrator.engine.fcs().safe_block_info(),
            "the BatchReverted event reports a safe head the Engine did not accept"
        );
    }
}

/// `RevertToL1Block`: the unwind deletes batch 2, so the safe block moves back to L2 block 5. The
/// Engine answers `INVALID` and keeps safe at block 10. The command still replies `true`.
#[tokio::test]
#[ignore = "RG-49: RevertToL1Block drops an INVALID forkchoice answer and replies that the revert succeeded"]
async fn rg49_admin_revert_replies_success_only_if_the_engine_accepted_the_new_safe_block() {
    let database = Arc::new(setup_test_db().await);
    seed_two_committed_batches(&database).await;
    let engine_client = engine_rejecting_forkchoice();
    let mut orchestrator = orchestrator_with_engine(
        database,
        ForkchoiceState::new(info(10, 10), info(10, 10), info(0, 0)),
        0,
        Probes::default(),
        engine_client.clone(),
    )
    .await;

    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    let result = orchestrator
        .handle_command(ChainOrchestratorCommand::RevertToL1Block((15, reply_tx)))
        .await;

    assert!(engine_client.fork_choice_updated_calls() >= 1, "the update reached the Engine");
    if result.is_ok() && matches!(reply_rx.await, Ok(true)) {
        assert_eq!(
            orchestrator.engine.fcs().safe_block_info().number,
            5,
            "the admin caller is told the revert succeeded, but the Engine kept the old safe block"
        );
    }
}

/// `UpdateFcsHead`: the admin moves the head from L2 block 5 to block 8. The Engine answers
/// `INVALID` and keeps head at block 5. The command still stores head 8 in the database and
/// replies.
#[tokio::test]
#[ignore = "RG-49: UpdateFcsHead drops an INVALID forkchoice answer, stores the head and replies success"]
async fn rg49_update_fcs_head_replies_success_only_if_the_engine_accepted_the_head() {
    let database = Arc::new(setup_test_db().await);
    let engine_client = engine_rejecting_forkchoice();
    let mut orchestrator = orchestrator_with_engine(
        database.clone(),
        ForkchoiceState::new(info(5, 5), info(0, 0), info(0, 0)),
        0,
        Probes::default(),
        engine_client.clone(),
    )
    .await;

    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    let result = orchestrator
        .handle_command(ChainOrchestratorCommand::UpdateFcsHead((info(8, 8), reply_tx)))
        .await;

    assert!(engine_client.fork_choice_updated_calls() >= 1, "the update reached the Engine");
    if result.is_ok() && reply_rx.await.is_ok() {
        assert_eq!(
            orchestrator.engine.fcs().head_block_info().number,
            8,
            "the admin caller is told the head moved to 8 (database head {}), but the Engine kept head 5",
            database.get_l2_head_block_number().await.unwrap()
        );
    }
}
