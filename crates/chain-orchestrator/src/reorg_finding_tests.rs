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
//! RG-46 asserts that finalized conflicts roll back the database unwind, and the RG-49 forkchoice
//! tests for stale signatures, unwinds, reverts and finalization assert the fixed behaviour. Other
//! passing tests document behaviour that is not fixed; when a row is fixed, change the matching
//! assertion instead of deleting the test. Tests marked `#[ignore]` assert the intended behaviour,
//! fail today and carry the reason in the ignore message; run them with `--ignored`.

use super::*;
use alloy_primitives::{Address, U256};
use alloy_provider::ProviderBuilder;
use alloy_rpc_types_engine::{ForkchoiceUpdated, PayloadId, PayloadStatus};
use alloy_transport::mock::Asserter;
use dogeos_chainspec::{DogeosChainSpec, DOGEOS_DEV};
use dogeos_reth_consensus::DogeosConsensus;
use futures::FutureExt;
use reth_network_api::noop::NoopNetwork;
use reth_network_p2p::NoopFullBlockClient;
use rollup_node_providers::{test_utils::MockL1Provider, ScrollRootProvider};
use rollup_node_sequencer::{L1MessageInclusionMode, PayloadBuildingConfig, SequencerConfig};
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

#[tokio::test]
async fn administrative_frontier_failure_stops_run_loop_without_a_held_batch() {
    let database = Arc::new(setup_test_db().await);
    seed_two_committed_batches(&database).await;
    database.insert_genesis_block(B256::ZERO).await.unwrap();
    database.set_l2_head_block_number(10).await.unwrap();
    let client = engine_rejecting_forkchoice();
    let fcs = ForkchoiceState::new(info(10, 10), info(10, 10), info(0, 0));
    let mut orchestrator =
        orchestrator_with_engine(database.clone(), fcs, 0, Probes::default(), client.clone()).await;
    let asserter = Asserter::new();
    for block in [info(10, 10), info(10, 10), info(0, 0)] {
        let mut rpc_block =
            alloy_rpc_types_eth::Block::<dogeos_rpc_types::ScrollRpcTransaction>::default();
        rpc_block.header.inner.number = block.number;
        rpc_block.header.hash = block.hash;
        asserter.push_success(&Some(rpc_block));
    }
    orchestrator.l2_client =
        Arc::new(ProviderBuilder::<_, _, Scroll>::default().connect_mocked_client(asserter));
    let (commands, receiver) = mpsc::unbounded_channel();
    orchestrator.handle_rx = receiver;
    let (reply, response) = tokio::sync::oneshot::channel();
    commands.send(ChainOrchestratorCommand::RevertToL1Block((15, reply))).unwrap();
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        orchestrator.run_until_shutdown(std::future::pending()),
    )
    .await
    .expect("a frontier command failure must stop an idle run loop");
    assert!(result.unwrap_err().is_frontier_fatal());
    assert!(response.await.is_err(), "must not acknowledge a failed unwind");
    assert_eq!(client.fork_choice_updated_calls(), 1);
    assert!(database.get_pending_frontier_transition().await.unwrap().is_some());
}

#[tokio::test]
async fn finalized_catchup_does_not_rewind_a_later_finalized_batch() {
    let database = Arc::new(setup_test_db().await);
    seed_two_committed_batches(&database).await;
    database.finalize_batches_up_to_index(2, 30).await.unwrap();
    database.update_batch_status(B256::repeat_byte(0xb1), BatchStatus::Consolidated).await.unwrap();
    database.update_batch_status(B256::repeat_byte(0xb2), BatchStatus::Finalized).await.unwrap();
    let tip = info(10, 10);
    let mut orchestrator =
        orchestrator(database.clone(), ForkchoiceState::from_block_info(tip), 0).await;
    orchestrator.handle_l1_finalized(30).await.unwrap();
    assert_eq!(*orchestrator.engine.fcs().finalized_block_info(), tip);
    assert_eq!(
        database.get_batch_status_by_hash(B256::repeat_byte(0xb1)).await.unwrap(),
        Some(BatchStatus::Finalized)
    );
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
                Err(ChainOrchestratorError::FrontierRecovery(source))
                    if matches!(**source, ChainOrchestratorError::DatabaseError(scroll_db::DatabaseError::InvalidFrontierTransition(_)))
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

/// Captures a request through the production submission path and delays its signed result.
fn pending_signature<EC: ScrollEngineApi + Unpin + Send + Sync + 'static>(
    orchestrator: &mut TestOrchestrator<EC>,
    block: DogeosBlock,
) -> SignerEvent {
    let (request_tx, mut requests) = mpsc::unbounded_channel();
    let (_events, event_rx) = mpsc::unbounded_channel();
    orchestrator.signer = Some(SignerHandle::new(request_tx, event_rx.into(), Address::ZERO));
    orchestrator.request_block_signature(block).unwrap();
    let rollup_node_signer::SignerRequest::SignBlock { block, generation } =
        requests.try_recv().unwrap();
    SignerEvent::SignedBlock {
        block,
        generation,
        signature: alloy_primitives::Signature::test_signature(),
    }
}

fn signing_block(number: u64) -> DogeosBlock {
    DogeosBlock {
        header: alloy_consensus::Header { number, ..Default::default() },
        ..Default::default()
    }
}

fn push_signing_readback(asserter: &Asserter, block: &DogeosBlock) {
    let mut rpc_block =
        alloy_rpc_types_eth::Block::<dogeos_rpc_types::ScrollRpcTransaction>::default();
    rpc_block.header.inner = block.header.clone();
    rpc_block.header.hash = block.hash_slow();
    asserter.push_success(&Some(rpc_block));
}

fn accepting_engine() -> Arc<ScriptedEngineClient> {
    let engine = Arc::new(ScriptedEngineClient::new());
    engine.push_fork_choice_updated(ScriptedResponse::Ok(
        alloy_rpc_types_engine::ForkchoiceUpdated {
            payload_status: alloy_rpc_types_engine::PayloadStatus {
                status: alloy_rpc_types_engine::PayloadStatusEnum::Valid,
                latest_valid_hash: None,
            },
            payload_id: None,
        },
    ));
    engine
}

/// RG-49: both L1 reorg entry points reject orphan results before they can overwrite the
/// rewound head, persist, notify or announce, while preserving signatures for the retained chain.
#[tokio::test]
async fn rg49_signed_block_requested_before_an_unwind_is_discarded() {
    for administrative in [false, true] {
        let database = Arc::new(setup_test_db().await);
        database.insert_genesis_block(B256::ZERO).await.unwrap();
        let (announced_tx, mut announced_rx) = mpsc::unbounded_channel();
        let engine = accepting_engine();
        let mut orchestrator = orchestrator_with_engine(
            database.clone(),
            ForkchoiceState::new(info(5, 5), info(0, 0), info(0, 0)),
            0,
            Probes { announced: Some(announced_tx), ..Default::default() },
            engine.clone(),
        )
        .await;

        // Block 4 executed deposit 0 from L1 block 5. Block 5 is waiting to be signed.
        orchestrator.handle_l1_message(deposit(0), info(5, 5)).await.unwrap();
        database
            .update_l1_messages_from_l2_blocks(vec![L2BlockInfoWithL1Messages {
                block_info: info(4, 4),
                l1_messages: vec![deposit(0).tx_hash()],
            }])
            .await
            .unwrap();
        database.set_l2_head_block_number(4).await.unwrap();
        let orphan = signing_block(5);
        let stale = pending_signature(&mut orchestrator, orphan.clone());
        // Both a strict ancestor and the future rewound tip are already awaiting signatures.
        let preserved = [signing_block(2), signing_block(3)].map(|block| {
            let pending = pending_signature(&mut orchestrator, block.clone());
            (block, pending)
        });
        let asserter = Asserter::new();
        // Rewound head lookup, then collection of the two reverted blocks' transactions.
        for number in [3, 4, 5] {
            push_signing_readback(&asserter, &signing_block(number));
        }
        orchestrator.l2_client = Arc::new(
            ProviderBuilder::<_, _, Scroll>::default().connect_mocked_client(asserter.clone()),
        );
        if administrative {
            let (reply, response) = tokio::sync::oneshot::channel();
            orchestrator
                .handle_command(ChainOrchestratorCommand::RevertToL1Block((3, reply)))
                .await
                .unwrap();
            assert!(response.await.unwrap());
        } else {
            assert!(matches!(
                orchestrator.handle_l1_reorg(3).await.unwrap(),
                Some(ChainOrchestratorEvent::L1Reorg {
                    l2_head_block_info: Some(BlockInfo { number: 3, .. }),
                    ..
                })
            ));
        }
        assert_eq!(engine.fork_choice_updated_calls(), 1);
        assert_eq!(database.get_l2_head_block_number().await.unwrap(), 3);
        let mut events = orchestrator.event_listener();

        // No readback response is queued: stale ownership must be checked before even the RPC.
        let result = orchestrator.handle_signer_event(stale).await;
        assert!(matches!(result, Ok(None)), "{result:?}");
        orchestrator.handle_outcome(result).unwrap();
        assert_eq!(database.get_l2_head_block_number().await.unwrap(), 3);
        assert!(database.get_signature(orphan.hash_slow()).await.unwrap().is_none());
        assert!(events.next().now_or_never().is_none());
        // A channel round trip drains preceding network messages without a timing-based sleep.
        let _ = orchestrator.network.handle().event_listener().await;
        assert!(announced_rx.try_recv().is_err());

        // The older generation must not lose signatures for the preserved prefix or
        // matching tip. Canonical readback accepts them without regressing the DB head.
        for (block, pending) in preserved {
            let SignerEvent::SignedBlock { generation, .. } = &pending;
            assert_ne!(*generation, orchestrator.signing_generation);
            push_signing_readback(&asserter, &block);
            let result = orchestrator.handle_signer_event(pending).await;
            assert!(matches!(result, Ok(Some(ChainOrchestratorEvent::SignedBlock { .. }))));
            orchestrator.handle_outcome(result).unwrap();
            assert_eq!(database.get_l2_head_block_number().await.unwrap(), 3);
            assert_eq!(
                database.get_signature(block.hash_slow()).await.unwrap(),
                Some(alloy_primitives::Signature::test_signature())
            );
            assert!(matches!(events.next().await,
                Some(ChainOrchestratorEvent::SignedBlock { block: signed, .. }) if signed == block));
            let _ = orchestrator.network.handle().event_listener().await;
            assert_eq!(announced_rx.try_recv().unwrap().0, block);
            assert!(announced_rx.try_recv().is_err());
        }

        // Signing in the new generation still advances, persists, announces and notifies.
        let replacement = signing_block(4);
        let current = pending_signature(&mut orchestrator, replacement.clone());
        push_signing_readback(&asserter, &replacement);
        let result = orchestrator.handle_signer_event(current).await;
        assert!(matches!(result, Ok(Some(ChainOrchestratorEvent::SignedBlock { .. }))));
        orchestrator.handle_outcome(result).unwrap();
        assert_eq!(database.get_l2_head_block_number().await.unwrap(), 4);
        assert!(database.get_signature(replacement.hash_slow()).await.unwrap().is_some());
        assert!(matches!(events.next().await, Some(ChainOrchestratorEvent::SignedBlock { .. })));
        let _ = orchestrator.network.handle().event_listener().await;
        assert_eq!(announced_rx.try_recv().unwrap().0, replacement);
        assert!(announced_rx.try_recv().is_err());
    }
}

/// A same-generation result may be orphaned by peer sync or derived reconciliation. Canonical
/// absence, replacement and a failed readback all fail closed before any observable side effect.
#[tokio::test]
async fn rg49_signed_block_requires_successful_canonical_readback() {
    for readback in ["missing", "replacement", "error"] {
        let database = Arc::new(setup_test_db().await);
        database.set_l2_head_block_number(3).await.unwrap();
        let (announced_tx, mut announced_rx) = mpsc::unbounded_channel();
        let mut orchestrator = orchestrator_probed(
            database.clone(),
            ForkchoiceState::from_genesis(B256::ZERO),
            0,
            Probes { announced: Some(announced_tx), ..Default::default() },
        )
        .await;
        let block = signing_block(4);
        let result = pending_signature(&mut orchestrator, block.clone());
        let asserter = Asserter::new();
        match readback {
            "missing" => asserter.push_success(&Option::<()>::None),
            "replacement" => {
                let mut replacement = block.clone();
                replacement.header.extra_data = Bytes::from_static(b"replacement").into();
                push_signing_readback(&asserter, &replacement);
            }
            "error" => asserter.push_failure_msg("readback unavailable"),
            _ => unreachable!(),
        }
        orchestrator.l2_client =
            Arc::new(ProviderBuilder::<_, _, Scroll>::default().connect_mocked_client(asserter));
        let mut events = orchestrator.event_listener();
        let outcome = orchestrator.handle_signer_event(result).await;
        if readback == "error" {
            assert!(outcome.as_ref().unwrap_err().is_frontier_fatal());
            assert!(orchestrator.handle_outcome(outcome).unwrap_err().is_frontier_fatal());
        } else {
            assert!(matches!(outcome, Ok(None)), "{outcome:?}");
            orchestrator.handle_outcome(outcome).unwrap();
        }
        assert_eq!(database.get_l2_head_block_number().await.unwrap(), 3);
        assert!(database.get_signature(block.hash_slow()).await.unwrap().is_none());
        assert!(events.next().now_or_never().is_none());
        let _ = orchestrator.network.handle().event_listener().await;
        assert!(announced_rx.try_recv().is_err());
    }
}

/// A canonical ancestor can finish signing after an import advanced the persisted head. Its
/// signature is still useful, but acceptance must not move that newer head backwards.
#[tokio::test]
async fn rg49_delayed_canonical_signature_preserves_newer_database_head() {
    let database = Arc::new(setup_test_db().await);
    let (announced_tx, mut announced_rx) = mpsc::unbounded_channel();
    let mut orchestrator = orchestrator_probed(
        database.clone(),
        ForkchoiceState::from_genesis(B256::ZERO),
        0,
        Probes { announced: Some(announced_tx), ..Default::default() },
    )
    .await;
    let block = signing_block(4);
    let result = pending_signature(&mut orchestrator, block.clone());
    database.set_l2_head_block_number(8).await.unwrap();
    let asserter = Asserter::new();
    push_signing_readback(&asserter, &block);
    orchestrator.l2_client =
        Arc::new(ProviderBuilder::<_, _, Scroll>::default().connect_mocked_client(asserter));
    let mut events = orchestrator.event_listener();
    let result = orchestrator.handle_signer_event(result).await;
    assert!(matches!(result, Ok(Some(ChainOrchestratorEvent::SignedBlock { .. }))));
    orchestrator.handle_outcome(result).unwrap();
    assert_eq!(database.get_l2_head_block_number().await.unwrap(), 8);
    assert!(database.get_signature(block.hash_slow()).await.unwrap().is_some());
    assert!(matches!(events.next().await, Some(ChainOrchestratorEvent::SignedBlock { .. })));
    let _ = orchestrator.network.handle().event_listener().await;
    assert_eq!(announced_rx.try_recv().unwrap().0, block);
}

/// Reverting safe batches while retaining the unsafe head must keep its pending signatures.
#[tokio::test]
async fn rg49_safe_only_batch_revert_preserves_pending_signatures() {
    let database = Arc::new(setup_test_db().await);
    seed_two_committed_batches(&database).await;
    database.insert_genesis_block(B256::ZERO).await.unwrap();
    database.set_l2_head_block_number(10).await.unwrap();
    let mut orchestrator = orchestrator_with_engine(
        database.clone(),
        ForkchoiceState::new(info(10, 10), info(10, 10), info(0, 0)),
        0,
        Probes::default(),
        accepting_engine(),
    )
    .await;
    let block = signing_block(11);
    let pending = pending_signature(&mut orchestrator, block.clone());
    let generation = orchestrator.signing_generation;
    let asserter = Asserter::new();
    // The post-revert tags retain the unsafe head and only lower safe.
    for info in [info(10, 10), info(5, 5), info(0, 0)] {
        let mut rpc_block =
            alloy_rpc_types_eth::Block::<dogeos_rpc_types::ScrollRpcTransaction>::default();
        rpc_block.header.inner.number = info.number;
        rpc_block.header.hash = info.hash;
        asserter.push_success(&Some(rpc_block));
    }
    push_signing_readback(&asserter, &block);
    orchestrator.l2_client =
        Arc::new(ProviderBuilder::<_, _, Scroll>::default().connect_mocked_client(asserter));
    assert!(matches!(
        orchestrator.handle_batch_revert(2, 2, info(30, 30)).await.unwrap(),
        Some(ChainOrchestratorEvent::BatchReverted { .. })
    ));
    assert_eq!(orchestrator.signing_generation, generation);
    assert!(matches!(
        orchestrator.handle_signer_event(pending).await.unwrap(),
        Some(ChainOrchestratorEvent::SignedBlock { .. })
    ));
    assert_eq!(database.get_l2_head_block_number().await.unwrap(), 11);
    assert!(database.get_signature(block.hash_slow()).await.unwrap().is_some());
}

/// Administrative head rewinds (including same-height replacements) revoke queued results.
#[tokio::test]
async fn rg49_administrative_head_rewind_discards_pending_signatures() {
    for replacement in [info(3, 3), info(4, 44)] {
        let block = signing_block(4);
        let previous_head = BlockInfo { number: 4, hash: block.hash_slow() };
        let database = Arc::new(setup_test_db().await);
        database.set_l2_head_block_number(4).await.unwrap();
        let mut orchestrator = orchestrator_with_engine(
            database.clone(),
            ForkchoiceState::new(previous_head, info(0, 0), info(0, 0)),
            0,
            Probes::default(),
            accepting_engine(),
        )
        .await;
        // The queued result belongs to the actual old head, including its hash. A same-height
        // replacement must revoke it just as a lower head does.
        let pending = pending_signature(&mut orchestrator, block.clone());
        let SignerEvent::SignedBlock { generation, .. } = &pending;
        let generation = *generation;
        let asserter = Asserter::new();
        if replacement.number < 4 {
            push_signing_readback(&asserter, &block);
        }
        // A checked administrative FCU reads back latest, safe and finalized. The standalone
        // signing guard must discard the stale result without consuming these queued responses.
        for info in [replacement, info(0, 0), info(0, 0)] {
            let mut rpc_block =
                alloy_rpc_types_eth::Block::<dogeos_rpc_types::ScrollRpcTransaction>::default();
            rpc_block.header.inner.number = info.number;
            rpc_block.header.hash = info.hash;
            asserter.push_success(&Some(rpc_block));
        }
        orchestrator.l2_client = Arc::new(
            ProviderBuilder::<_, _, Scroll>::default().connect_mocked_client(asserter.clone()),
        );
        let (reply, response) = tokio::sync::oneshot::channel();
        orchestrator
            .handle_command(ChainOrchestratorCommand::UpdateFcsHead((replacement, reply)))
            .await
            .unwrap();
        response.await.unwrap();
        assert_eq!(
            orchestrator.signing_generation,
            generation + 1,
            "administrative head replacement must advance the signing generation: {replacement:?}"
        );
        // Unchecked FCU leaves the tag responses queued; checked FCU consumes them. Neither
        // path may read the provider while discarding this stale event.
        let queued_responses = asserter.read_q().len();
        assert!(orchestrator.handle_signer_event(pending).await.unwrap().is_none());
        assert_eq!(
            asserter.read_q().len(),
            queued_responses,
            "discarding a stale signature must not read the provider: {replacement:?}"
        );
        assert_eq!(database.get_l2_head_block_number().await.unwrap(), replacement.number);
        assert!(database.get_signature(block.hash_slow()).await.unwrap().is_none());
    }
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
// and leaves its own forkchoice state unchanged, so a call site that drops the returned status
// reports success although the Engine did not accept the new state.
//
// Each test drives one call site with an Engine whose forkchoice update answers `INVALID`, and
// checks that the update reached the Engine, so it cannot pass by never making the call. L1
// reorgs, batch reverts, finalization and the administrative revert are fixed: they save the
// move first, fail with `FrontierTransitionStatus`, which stops the run loop, and keep the saved
// move so a restart replays it. `UpdateFcsHead` confirms the provider before its DB commit.

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
/// Engine answers `INVALID` and keeps safe at block 10, so the handler fails instead of returning
/// the `L1Reorg` event.
#[tokio::test]
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
    assert!(
        matches!(
            &result,
            Err(error @ ChainOrchestratorError::FrontierTransitionStatus(
                alloy_rpc_types_engine::PayloadStatusEnum::Invalid { .. }
            )) if error.is_frontier_fatal()
        ),
        "got {result:?}"
    );
    if let Ok(Some(ChainOrchestratorEvent::L1Reorg { l2_safe_block_info, .. })) = &result {
        assert_eq!(
            *l2_safe_block_info,
            Some(*orchestrator.engine.fcs().safe_block_info()),
            "the L1Reorg event reports a safe block the Engine did not accept"
        );
    }
}

/// `handle_l1_finalized`: batch 1 is consolidated and its L1 block is now finalized, so the
/// finalized L2 block becomes 5. The Engine answers `INVALID` and keeps finalized at block 0, so
/// the handler fails instead of returning the `L1BlockFinalized` event.
#[tokio::test]
async fn rg49_l1_finalized_reports_the_new_finalized_block_only_if_the_engine_accepted_it() {
    let database = Arc::new(setup_test_db().await);
    seed_two_committed_batches(&database).await;
    // Batch 1 is finalized on L1 at block 10 and already consolidated on L2.
    database.finalize_batches_up_to_index(1, 10).await.unwrap();
    database.update_batch_status(B256::repeat_byte(0xb1), BatchStatus::Consolidated).await.unwrap();
    let engine_client = engine_rejecting_forkchoice();
    let mut orchestrator = orchestrator_with_engine(
        database.clone(),
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
    assert!(
        matches!(
            &result,
            Err(error @ ChainOrchestratorError::FrontierTransitionStatus(
                alloy_rpc_types_engine::PayloadStatusEnum::Invalid { .. }
            )) if error.is_frontier_fatal()
        ),
        "got {result:?}"
    );
    assert_eq!(*orchestrator.engine.fcs().finalized_block_info(), info(0, 0));
    let pending = database.get_pending_frontier_transition().await.unwrap();
    assert_eq!(pending.map(|transition| transition.target.finalized), Some(info(5, 5)));
}

/// `handle_batch_revert`: reverting batch 2 moves the safe block back to L2 block 5. The Engine
/// answers `INVALID` and keeps safe at block 10, so the handler fails instead of returning the
/// `BatchReverted` event.
#[tokio::test]
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
    assert!(
        matches!(
            &result,
            Err(error @ ChainOrchestratorError::FrontierTransitionStatus(
                alloy_rpc_types_engine::PayloadStatusEnum::Invalid { .. }
            )) if error.is_frontier_fatal()
        ),
        "got {result:?}"
    );
    if let Ok(Some(ChainOrchestratorEvent::BatchReverted { safe_head, .. })) = &result {
        assert_eq!(
            safe_head,
            orchestrator.engine.fcs().safe_block_info(),
            "the BatchReverted event reports a safe head the Engine did not accept"
        );
    }
}

/// `handle_batch_revert`: a `SYNCING` answer is not success either. The local forkchoice state
/// must stay at the block the Engine actually holds, or the next batch would be checked against a
/// safe block the Engine never adopted.
#[tokio::test]
async fn rg49_batch_revert_fails_on_a_syncing_forkchoice_answer() {
    let database = Arc::new(setup_test_db().await);
    seed_two_committed_batches(&database).await;
    let engine_client = Arc::new(ScriptedEngineClient::new());
    engine_client.push_fork_choice_updated(ScriptedResponse::Ok(
        alloy_rpc_types_engine::ForkchoiceUpdated {
            payload_status: alloy_rpc_types_engine::PayloadStatus {
                status: alloy_rpc_types_engine::PayloadStatusEnum::Syncing,
                latest_valid_hash: None,
            },
            payload_id: None,
        },
    ));
    let fcs = ForkchoiceState::new(info(10, 10), info(10, 10), info(0, 0));
    let mut orchestrator = orchestrator_with_engine(
        database.clone(),
        fcs.clone(),
        0,
        Probes::default(),
        engine_client.clone(),
    )
    .await;
    orchestrator.sync_state.l2_mut().set_synced();

    let result = orchestrator.handle_batch_revert(2, 2, info(30, 30)).await;

    assert_eq!(engine_client.fork_choice_updated_calls(), 1);
    assert!(
        matches!(
            &result,
            Err(ChainOrchestratorError::FrontierTransitionStatus(
                alloy_rpc_types_engine::PayloadStatusEnum::Syncing
            ))
        ),
        "got {result:?}"
    );
    assert_eq!(orchestrator.engine.fcs(), &fcs);
    let pending = database.get_pending_frontier_transition().await.unwrap();
    assert_eq!(pending.map(|transition| transition.target.safe), Some(info(5, 5)));
}

/// `RevertToL1Block`: the unwind deletes batch 2, so the safe block moves back to L2 block 5. The
/// Engine answers `INVALID` and keeps safe at block 10, so the command fails and never replies.
#[tokio::test]
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
    assert!(matches!(&result, Err(error) if error.is_frontier_fatal()), "got {result:?}");
    assert!(reply_rx.await.is_err(), "a rejected revert must not reply");
    assert_eq!(orchestrator.engine.fcs().safe_block_info().number, 10);
}

/// A rejected administrative head update must neither mutate local state nor acknowledge it.
#[tokio::test]
async fn rg49_update_fcs_head_replies_success_only_if_the_engine_accepted_the_head() {
    for status in [
        PayloadStatusEnum::Invalid { validation_error: "rejected by the test Engine".into() },
        PayloadStatusEnum::Syncing,
    ] {
        let (mut orchestrator, client, asserter) = admin_head_fixture().await;
        let database = orchestrator.database.clone();
        let previous = orchestrator.engine.fcs().clone();
        client.push_fork_choice_updated(fcu_response(status.clone()));
        let mut events = orchestrator.event_listener();
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        let result = orchestrator
            .handle_command(ChainOrchestratorCommand::UpdateFcsHead((info(4, 4), reply_tx)))
            .await;

        assert_eq!(client.fork_choice_updated_calls(), 1, "the update reached the Engine");
        assert!(
            matches!(&result, Err(ChainOrchestratorError::FrontierTransitionStatus(actual)) if actual == &status),
            "got {result:?}"
        );
        assert!(result.unwrap_err().is_frontier_fatal());
        assert!(reply_rx.await.is_err(), "a rejected update must not reply success");
        assert_eq!(orchestrator.engine.fcs(), &previous);
        assert_admin_db_unchanged(&database).await;
        assert!(events.next().now_or_never().is_none(), "no success event");
        assert!(asserter.read_q().is_empty());
    }
}

fn fcu_response(
    status: PayloadStatusEnum,
) -> ScriptedResponse<alloy_rpc_types_engine::ForkchoiceUpdated> {
    ScriptedResponse::Ok(alloy_rpc_types_engine::ForkchoiceUpdated {
        payload_status: alloy_rpc_types_engine::PayloadStatus { status, latest_valid_hash: None },
        payload_id: None,
    })
}

async fn admin_head_fixture(
) -> (TestOrchestrator<ScriptedEngineClient>, Arc<ScriptedEngineClient>, Asserter) {
    let database = Arc::new(setup_test_db().await);
    database.set_l2_head_block_number(5).await.unwrap();
    let message = L1MessageEnvelope::new(deposit(0), 1, Some(5), None);
    database.insert_l1_message(message).await.unwrap();
    let client = Arc::new(ScriptedEngineClient::new());
    let mut orchestrator = orchestrator_with_engine(
        database,
        ForkchoiceState::new(info(5, 5), info(0, 0), info(0, 0)),
        0,
        Probes::default(),
        client.clone(),
    )
    .await;
    let asserter = Asserter::new();
    // Collect the reverted block before FCU; no pool reinsertion may precede confirmation.
    push_admin_block(&asserter, info(5, 5));
    orchestrator.l2_client = Arc::new(
        ProviderBuilder::<_, _, Scroll>::default().connect_mocked_client(asserter.clone()),
    );
    (orchestrator, client, asserter)
}

async fn assert_admin_db_unchanged(database: &Database) {
    assert_eq!(database.get_l2_head_block_number().await.unwrap(), 5);
    let messages =
        database.get_n_l1_messages(Some(L1MessageKey::from_queue_index(0)), 1).await.unwrap();
    assert_eq!(messages[0].l2_block_number, Some(5), "do not purge mappings before confirmation");
    assert!(database.get_pending_frontier_transition().await.unwrap().is_none());
    assert!(database.get_confirmed_frontier_transition().await.unwrap().is_none());
}

fn push_admin_block(asserter: &Asserter, block: BlockInfo) {
    use alloy_rpc_types_eth::{Block, Header};
    use dogeos_rpc_types::ScrollRpcTransaction;
    // Provider observations are independent of the Engine's candidate cache.
    let mut header: Header = Header::default();
    header.inner.number = block.number;
    header.hash = block.hash;
    asserter.push_success(&Some(Block::<ScrollRpcTransaction>::empty(header)));
}

fn push_admin_tags(asserter: &Asserter, head: BlockInfo) {
    for block in [head, info(0, 0), info(0, 0)] {
        push_admin_block(asserter, block);
    }
}

fn payload_build_response(id: PayloadId) -> ScriptedResponse<ForkchoiceUpdated> {
    ScriptedResponse::Ok(ForkchoiceUpdated {
        payload_status: PayloadStatus { status: PayloadStatusEnum::Valid, latest_valid_hash: None },
        payload_id: Some(id),
    })
}

/// Install a real job and initialize its timer before any administrative command or wait.
async fn start_admin_payload_job(
    orchestrator: &mut TestOrchestrator<ScriptedEngineClient>,
    client: &ScriptedEngineClient,
) -> PayloadId {
    orchestrator.sequencer = Some(Sequencer::new(
        Arc::new(MockL1Provider { db: orchestrator.database.clone(), blobs: Default::default() }),
        SequencerConfig {
            chain_spec: DOGEOS_DEV.clone(),
            fee_recipient: Address::ZERO,
            auto_start: false,
            block_time: 100,
            payload_building_duration: 10,
            allow_empty_blocks: true,
            payload_building_config: PayloadBuildingConfig {
                block_gas_limit: 30_000_000,
                max_l1_messages_per_block: 10,
                l1_message_inclusion_mode: L1MessageInclusionMode::BlockDepth(0),
            },
        },
    ));
    let id = PayloadId::new([1; 8]);
    client.push_fork_choice_updated(payload_build_response(id));
    let sequencer = orchestrator.sequencer.as_mut().unwrap();
    sequencer.start_payload_building(&mut orchestrator.engine).await.unwrap();
    assert!(sequencer.next().now_or_never().is_none());
    assert_eq!(
        sequencer.payload_building_job().unwrap().parent(),
        *orchestrator.engine.fcs().head_block_info()
    );
    id
}

async fn assert_admin_payload_ready(
    orchestrator: &mut TestOrchestrator<ScriptedEngineClient>,
    id: PayloadId,
) {
    let sequencer = orchestrator.sequencer.as_mut().unwrap();
    assert!(sequencer.payload_building_job().is_some(), "the valid job must survive");
    let event =
        tokio::time::timeout(std::time::Duration::from_secs(1), sequencer.next()).await.unwrap();
    assert!(matches!(event, Some(SequencerEvent::PayloadReady(actual)) if actual == id));
}

async fn assert_admin_head_cancels_payload_job(head: BlockInfo) {
    for had_job in [true, false] {
        let (mut orchestrator, client, asserter) = admin_head_fixture().await;
        if head.number == 5 {
            asserter.write_q().clear(); // same-height replacement has no reverted suffix
        }
        start_admin_payload_job(&mut orchestrator, &client).await;
        if had_job {
            // The job is already ready when the command runs, but has not emitted its event.
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        } else {
            // Exercise a configured sequencer with no job, too.
            orchestrator.sequencer.as_mut().unwrap().cancel_payload_building_job();
        }
        let (request_tx, mut requests) = mpsc::unbounded_channel();
        let (_event_tx, event_rx) = mpsc::unbounded_channel();
        orchestrator.signer = Some(SignerHandle::new(request_tx, event_rx.into(), Address::ZERO));
        client.push_fork_choice_updated(fcu_response(PayloadStatusEnum::Valid));
        push_admin_tags(&asserter, head);
        let mut events = orchestrator.event_listener();
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        orchestrator
            .handle_command(ChainOrchestratorCommand::UpdateFcsHead((head, reply_tx)))
            .await
            .unwrap();
        reply_rx.await.unwrap();

        let sequencer = orchestrator.sequencer.as_mut().unwrap();
        assert!(sequencer.payload_building_job().is_none(), "old-head job must be cancelled");
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), sequencer.next())
                .await
                .is_err(),
            "the real sequencer must not emit the old PayloadReady"
        );
        if had_job {
            assert_eq!(events.next().await, Some(ChainOrchestratorEvent::BlockBuildingSkipped));
        }
        assert_eq!(events.next().await, Some(ChainOrchestratorEvent::FcsHeadUpdated(head)));
        assert!(events.next().now_or_never().is_none(), "no spurious skipped event");
        assert_eq!(*orchestrator.engine.fcs().head_block_info(), head);
        assert_eq!(orchestrator.database.get_l2_head_block_number().await.unwrap(), head.number);
        assert_eq!(client.fork_choice_updated_calls(), 2, "only initial build and admin FCU");
        assert_eq!(client.get_payload_calls(), 0, "no stale payload finalization");
        assert_eq!(client.new_payload_calls(), 0);
        assert!(requests.try_recv().is_err(), "no stale signature request");
        assert!(client.is_drained());
        assert!(asserter.read_q().is_empty());

        if had_job {
            let fresh_id = PayloadId::new([2; 8]);
            client.push_fork_choice_updated(payload_build_response(fresh_id));
            orchestrator.handle_command(ChainOrchestratorCommand::BuildBlock).await.unwrap();
            assert_eq!(
                orchestrator.sequencer.as_ref().unwrap().payload_building_job().unwrap().parent(),
                head
            );
            let (input, has_attributes) = client.fork_choice_inputs().last().copied().unwrap();
            assert_eq!(input.head_block_hash, head.hash);
            assert!(has_attributes, "fresh build must reach the Engine on the new head");
            assert_admin_payload_ready(&mut orchestrator, fresh_id).await;
        }
    }
}

#[tokio::test]
async fn rg49_update_fcs_head_rewind_cancels_payload_job() {
    assert_admin_head_cancels_payload_job(info(4, 4)).await;
}

#[tokio::test]
async fn rg49_update_fcs_head_replacement_cancels_payload_job() {
    assert_admin_head_cancels_payload_job(info(5, 0x55)).await;
}

#[tokio::test]
async fn rg49_update_fcs_head_same_head_preserves_payload_job() {
    let (mut orchestrator, client, asserter) = admin_head_fixture().await;
    asserter.write_q().clear();
    let id = start_admin_payload_job(&mut orchestrator, &client).await;
    let generation = orchestrator.signing_generation;
    client.push_fork_choice_updated(fcu_response(PayloadStatusEnum::Valid));
    push_admin_tags(&asserter, info(5, 5));
    let mut events = orchestrator.event_listener();
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    orchestrator
        .handle_command(ChainOrchestratorCommand::UpdateFcsHead((info(5, 5), reply_tx)))
        .await
        .unwrap();
    reply_rx.await.unwrap();
    assert_eq!(events.next().await, Some(ChainOrchestratorEvent::FcsHeadUpdated(info(5, 5))));
    assert!(events.next().now_or_never().is_none(), "no skipped event for a valid job");
    assert_eq!(orchestrator.signing_generation, generation);
    assert_admin_payload_ready(&mut orchestrator, id).await;
    assert_admin_db_unchanged(&orchestrator.database).await;
    assert_eq!(client.fork_choice_updated_calls(), 2);
    assert!(asserter.read_q().is_empty());
}

#[tokio::test]
async fn rg49_update_fcs_head_rejects_pending_frontier_before_any_side_effect() {
    let database = Arc::new(setup_test_db().await);
    let genesis = info(0, 0xa0);
    database.insert_genesis_block(genesis.hash).await.unwrap();
    database.set_l2_head_block_number(10).await.unwrap();
    for (index, number) in [(0, 5), (1, 10)] {
        database
            .insert_l1_message(L1MessageEnvelope::new(deposit(index), 1, Some(number), None))
            .await
            .unwrap();
    }
    let previous = ForkchoiceState::new(info(10, 10), genesis, genesis);
    let expected = frontier::stored_forkchoice(&previous);
    let pending = PendingFrontierTransition {
        expected,
        target: StoredForkchoiceState { safe: info(5, 5), ..expected },
    };
    let batch = BatchCommitData {
        hash: B256::repeat_byte(0xb1),
        index: 1,
        block_number: 10,
        block_timestamp: 1_010,
        calldata: Arc::new(Default::default()),
        blob_versioned_hash: None,
        finalized_block_number: None,
        reverted_block_number: None,
    };
    database.insert_batch(batch.clone()).await.unwrap();
    let batch_info = (&batch).into();
    // Consolidation committed safe 5 and its journal before the Engine accepted the move.
    database
        .tx_mut(move |tx| async move {
            tx.insert_blocks((1..=5).map(|n| info(n, n as u8)).collect(), batch_info).await?;
            tx.set_pending_frontier_transition(pending).await
        })
        .await
        .unwrap();
    let safe = database.get_latest_safe_l2_info().await.unwrap();
    assert_eq!(safe.0, info(5, 5));
    let mappings = database.get_n_l1_messages(None, 10).await.unwrap();
    let confirmed = database.get_confirmed_frontier_transition().await.unwrap();
    let client = Arc::new(ScriptedEngineClient::new());
    let mut orchestrator = orchestrator_with_engine(
        database.clone(),
        previous.clone(),
        0,
        Probes::default(),
        client.clone(),
    )
    .await;
    let asserter = Asserter::new();
    // Without the guard, the old path can confirm head 4 with cached safe at genesis,
    // then incorrectly commit DB head 4 below DB safe 5. Do not fail on mock exhaustion.
    for number in 5..=10 {
        push_admin_block(&asserter, info(number, number as u8));
    }
    for block in [info(4, 4), genesis, genesis] {
        push_admin_block(&asserter, block);
    }
    orchestrator.l2_client = Arc::new(
        ProviderBuilder::<_, _, Scroll>::default().connect_mocked_client(asserter.clone()),
    );
    let id = start_admin_payload_job(&mut orchestrator, &client).await;
    client.push_fork_choice_updated(fcu_response(PayloadStatusEnum::Valid));
    let mut events = orchestrator.event_listener();
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    let result = orchestrator
        .handle_command(ChainOrchestratorCommand::UpdateFcsHead((info(4, 4), reply_tx)))
        .await;
    assert!(
        matches!(&result, Err(ChainOrchestratorError::PendingFrontierTransition)),
        "{result:?}"
    );
    let error = result.unwrap_err();
    assert!(!error.is_frontier_fatal());
    assert!(orchestrator.handle_outcome(Err(error)).is_ok(), "the run loop must continue");
    assert_eq!(client.fork_choice_updated_calls(), 1, "only the existing payload build");
    assert_eq!(asserter.read_q().len(), 9, "no provider reads or pool reinsertion");
    assert_eq!(orchestrator.engine.fcs(), &previous);
    assert_eq!(database.get_l2_head_block_number().await.unwrap(), 10);
    assert_eq!(database.get_latest_safe_l2_info().await.unwrap(), safe);
    assert_eq!(database.get_n_l1_messages(None, 10).await.unwrap(), mappings);
    assert_eq!(database.get_pending_frontier_transition().await.unwrap(), Some(pending));
    assert_eq!(database.get_confirmed_frontier_transition().await.unwrap(), confirmed);
    assert!(reply_rx.await.is_err(), "no success reply");
    assert!(events.next().now_or_never().is_none(), "no success event");
    assert_admin_payload_ready(&mut orchestrator, id).await;
}

#[tokio::test]
async fn rg49_update_fcs_head_valid_requires_exact_provider_confirmation() {
    for observed in [info(5, 5), info(4, 0x44), info(6, 6)] {
        let (mut orchestrator, client, asserter) = admin_head_fixture().await;
        let database = orchestrator.database.clone();
        client.push_fork_choice_updated(fcu_response(PayloadStatusEnum::Valid));
        push_admin_tags(&asserter, observed);
        let mut events = orchestrator.event_listener();
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        let result = orchestrator
            .handle_command(ChainOrchestratorCommand::UpdateFcsHead((info(4, 4), reply_tx)))
            .await;
        assert!(
            matches!(&result, Err(ChainOrchestratorError::FrontierTransitionUnconfirmed { .. })),
            "{result:?}"
        );
        assert!(result.unwrap_err().is_frontier_fatal());
        assert!(reply_rx.await.is_err());
        assert_eq!(*orchestrator.engine.fcs().head_block_info(), observed);
        assert_admin_db_unchanged(&database).await;
        assert!(events.next().now_or_never().is_none());
        assert_eq!(client.fork_choice_updated_calls(), 1);
        assert!(asserter.read_q().is_empty(), "confirmation reads were consumed");
    }
}

#[tokio::test]
async fn rg49_update_fcs_head_confirmed_success_commits_then_notifies_and_replies() {
    let (mut orchestrator, client, asserter) = admin_head_fixture().await;
    asserter.write_q().clear(); // an advance has no reverted blocks to collect
    client.push_fork_choice_updated(fcu_response(PayloadStatusEnum::Valid));
    push_admin_tags(&asserter, info(8, 8));
    let mut events = orchestrator.event_listener();
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    orchestrator
        .handle_command(ChainOrchestratorCommand::UpdateFcsHead((info(8, 8), reply_tx)))
        .await
        .unwrap();
    reply_rx.await.unwrap();
    assert!(
        matches!(events.next().await, Some(ChainOrchestratorEvent::FcsHeadUpdated(head)) if head == info(8, 8))
    );
    assert_eq!(*orchestrator.engine.fcs().head_block_info(), info(8, 8));
    assert_eq!(orchestrator.database.get_l2_head_block_number().await.unwrap(), 8);
    let messages = orchestrator
        .database
        .get_n_l1_messages(Some(L1MessageKey::from_queue_index(0)), 1)
        .await
        .unwrap();
    assert_eq!(messages[0].l2_block_number, Some(5));
    assert!(orchestrator.database.get_confirmed_frontier_transition().await.unwrap().is_none());
    assert_eq!(client.fork_choice_updated_calls(), 1);
    assert!(asserter.read_q().is_empty());
}

#[tokio::test]
async fn rg49_update_fcs_head_timeout_is_fatal_without_local_commit_or_success() {
    let (mut orchestrator, client, asserter) = admin_head_fixture().await;
    let previous = orchestrator.engine.fcs().clone();
    client.push_fork_choice_updated(ScriptedResponse::RequestTimeout);
    let mut events = orchestrator.event_listener();
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    let result = orchestrator
        .handle_command(ChainOrchestratorCommand::UpdateFcsHead((info(4, 4), reply_tx)))
        .await;
    let error = result.unwrap_err();
    assert!(error.is_frontier_fatal(), "{error:?}");
    assert!(error.is_engine_unavailable(), "{error:?}");
    assert!(reply_rx.await.is_err());
    assert_eq!(orchestrator.engine.fcs(), &previous, "no candidate cached on ambiguity");
    assert_admin_db_unchanged(&orchestrator.database).await;
    assert!(events.next().now_or_never().is_none());
    assert_eq!(client.fork_choice_updated_calls(), 1);
    assert!(asserter.read_q().is_empty());
}

#[tokio::test]
async fn rg49_update_fcs_head_failed_readback_is_fatal_without_caching_candidate() {
    let (mut orchestrator, client, asserter) = admin_head_fixture().await;
    let previous = orchestrator.engine.fcs().clone();
    client.push_fork_choice_updated(fcu_response(PayloadStatusEnum::Valid));
    asserter.push_failure_msg("provider unavailable after FCU");
    let mut events = orchestrator.event_listener();
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    let result = orchestrator
        .handle_command(ChainOrchestratorCommand::UpdateFcsHead((info(4, 4), reply_tx)))
        .await;
    assert!(result.unwrap_err().is_frontier_fatal());
    assert!(reply_rx.await.is_err());
    assert_eq!(orchestrator.engine.fcs(), &previous);
    assert_admin_db_unchanged(&orchestrator.database).await;
    assert!(events.next().now_or_never().is_none());
    assert_eq!(client.fork_choice_updated_calls(), 1);
    assert!(asserter.read_q().is_empty());
}

/// The execution node may have applied an unanswered command. Restart follows #46's existing
/// DB-authoritative repair, without a durable guarantee to replay the requested admin intent.
#[tokio::test]
async fn rg49_update_fcs_head_ambiguous_application_recovers_from_database_on_restart() {
    use scroll_db::DatabaseConnectionProvider;
    let (mut orchestrator, client, asserter) = admin_head_fixture().await;
    let database = orchestrator.database.clone();
    database.insert_genesis_block(info(0, 0).hash).await.unwrap();
    let path = database.tmp_dir().unwrap().path().join("test.db");
    // The provider reports head 4 on restart despite the unanswered FCU.
    client.push_fork_choice_updated(ScriptedResponse::RequestTimeout);
    let mut events = orchestrator.event_listener();
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    let result = orchestrator
        .handle_command(ChainOrchestratorCommand::UpdateFcsHead((info(4, 4), reply_tx)))
        .await;
    assert!(result.unwrap_err().is_frontier_fatal());
    assert!(reply_rx.await.is_err());
    assert!(events.next().now_or_never().is_none());
    assert!(asserter.read_q().is_empty());
    assert_admin_db_unchanged(&database).await;
    database.inner().get_connection().clone().close().await.unwrap();
    let reopened = Database::new(path.to_str().unwrap()).await.unwrap();
    assert_admin_db_unchanged(&reopened).await;
    // The previous DB head is still available. Startup restores it; it does not replay the
    // failed admin intent, transaction reinsertion or notification.
    push_admin_tags(&asserter, info(4, 4));
    push_admin_block(&asserter, info(0, 0));
    push_admin_block(&asserter, info(5, 5));
    push_admin_tags(&asserter, info(5, 5));
    client.push_fork_choice_updated(fcu_response(PayloadStatusEnum::Valid));
    let mut restarted_engine =
        Engine::new(client.clone(), ForkchoiceState::from_genesis(B256::ZERO));
    frontier::recover_database_frontier(
        &*orchestrator.l2_client,
        &mut restarted_engine,
        &reopened,
        frontier::HeadSelection::StoredHead,
    )
    .await
    .unwrap();
    assert_eq!(*restarted_engine.fcs().head_block_info(), info(5, 5));
    assert_eq!(reopened.get_l2_head_block_number().await.unwrap(), 5);
    assert!(reopened.get_pending_frontier_transition().await.unwrap().is_none());
    assert_eq!(client.fork_choice_updated_calls(), 2);
    assert!(asserter.read_q().is_empty());
}

#[tokio::test]
async fn rg49_update_fcs_head_failure_stops_the_real_run_loop() {
    for response in [
        fcu_response(PayloadStatusEnum::Invalid { validation_error: "rejected".into() }),
        fcu_response(PayloadStatusEnum::Syncing),
        ScriptedResponse::RequestTimeout,
    ] {
        let (mut orchestrator, client, asserter) = admin_head_fixture().await;
        let database = orchestrator.database.clone();
        database.insert_genesis_block(B256::ZERO).await.unwrap();
        // The normal startup must complete before the queued command reaches the Engine.
        asserter.write_q().clear();
        push_admin_tags(&asserter, info(5, 5));
        push_admin_block(&asserter, info(0, 0));
        push_admin_block(&asserter, info(5, 5));
        push_admin_block(&asserter, info(5, 5)); // the command collects the reverted suffix
        client.push_fork_choice_updated(response);
        let (commands, receiver) = mpsc::unbounded_channel();
        orchestrator.handle_rx = receiver;
        let (reply, reply_rx) = tokio::sync::oneshot::channel();
        commands.send(ChainOrchestratorCommand::UpdateFcsHead((info(4, 4), reply))).unwrap();
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            orchestrator.run_until_shutdown(std::future::pending()),
        )
        .await
        .expect("the failed command must stop the run loop without a shutdown signal");
        assert!(result.unwrap_err().is_frontier_fatal());
        assert!(reply_rx.await.is_err());
        assert_eq!(database.get_l2_head_block_number().await.unwrap(), 5);
        let messages =
            database.get_n_l1_messages(Some(L1MessageKey::from_queue_index(0)), 1).await.unwrap();
        assert_eq!(messages[0].l2_block_number, Some(5));
        assert_eq!(client.fork_choice_updated_calls(), 1, "startup did not reject first");
        assert!(asserter.read_q().is_empty());
    }
}

#[tokio::test]
async fn rg49_update_fcs_head_success_reinserts_only_reverted_user_transactions() {
    use alloy_consensus::{transaction::Recovered, SignableTransaction, TxLegacy};
    use alloy_primitives::Signature;
    use alloy_rpc_types_eth::{Block, BlockTransactions, Header, Transaction};
    use dogeos_rpc_types::ScrollRpcTransaction;

    let (mut orchestrator, client, asserter) = admin_head_fixture().await;
    asserter.write_q().clear();
    let user_tx = ScrollTxEnvelope::Legacy(
        TxLegacy { gas_limit: 21_000, gas_price: 17, ..Default::default() }
            .into_signed(Signature::test_signature()),
    );
    let transactions = [user_tx, deposit(0).into()]
        .into_iter()
        .map(|tx| ScrollRpcTransaction {
            inner: Transaction {
                inner: Recovered::new_unchecked(tx, Address::repeat_byte(0x11)),
                block_hash: Some(info(5, 5).hash),
                block_number: Some(5),
                transaction_index: None,
                effective_gas_price: Some(17),
            },
        })
        .collect();
    let mut header: Header = Header::default();
    header.inner.number = 5;
    header.hash = info(5, 5).hash;
    asserter.push_success(&Some(Block::new(header, BlockTransactions::Full(transactions))));
    client.push_fork_choice_updated(fcu_response(PayloadStatusEnum::Valid));
    push_admin_tags(&asserter, info(4, 4));
    // Only the user transaction may consume a sendRawTransaction response after tag reads.
    asserter.push_success(&B256::repeat_byte(0x55));
    asserter.push_failure_msg("an L1 message must not be reinserted into the pool");
    let mut events = orchestrator.event_listener();
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    orchestrator
        .handle_command(ChainOrchestratorCommand::UpdateFcsHead((info(4, 4), reply_tx)))
        .await
        .unwrap();
    reply_rx.await.unwrap();
    assert!(
        matches!(events.next().await, Some(ChainOrchestratorEvent::FcsHeadUpdated(head)) if head == info(4, 4))
    );
    assert_eq!(orchestrator.database.get_l2_head_block_number().await.unwrap(), 4);
    assert_eq!(asserter.read_q().len(), 1, "exactly one user transaction was reinserted");
    assert_eq!(client.fork_choice_updated_calls(), 1);
}

#[tokio::test]
async fn rg49_update_fcs_head_local_validation_error_remains_nonfatal() {
    let (mut orchestrator, client, asserter) = admin_head_fixture().await;
    let previous = ForkchoiceState::new(info(5, 5), info(5, 5), info(0, 0));
    orchestrator.engine.replace_fcs_from_provider(previous.clone());
    let id = start_admin_payload_job(&mut orchestrator, &client).await;
    let mut events = orchestrator.event_listener();
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    let result = orchestrator
        .handle_command(ChainOrchestratorCommand::UpdateFcsHead((info(4, 4), reply_tx)))
        .await;
    let error = result.unwrap_err();
    assert!(!error.is_frontier_fatal(), "local validation must preserve the run loop: {error:?}");
    assert!(matches!(
        error,
        ChainOrchestratorError::EngineError(scroll_engine::EngineError::FcsError(
            scroll_engine::FcsError::HeadBelowSafe
        ))
    ));
    assert_eq!(client.fork_choice_updated_calls(), 1, "only the existing payload build");
    assert!(reply_rx.await.is_err());
    assert_eq!(orchestrator.engine.fcs(), &previous);
    assert_admin_db_unchanged(&orchestrator.database).await;
    assert!(events.next().now_or_never().is_none());
    assert!(asserter.read_q().is_empty());
    assert_admin_payload_ready(&mut orchestrator, id).await;
}
