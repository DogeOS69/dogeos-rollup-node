//! Readiness is published only after consolidation, and partial consolidation is fail-stop.

use super::*;
use alloy_consensus::transaction::Recovered;
use alloy_primitives::{Signature, U256};
use alloy_provider::ProviderBuilder;
use alloy_rpc_types_engine::{ForkchoiceUpdated, PayloadStatus};
use alloy_transport::mock::Asserter;
use dogeos_chainspec::{DogeosChainSpec, DOGEOS_DEV};
use dogeos_reth_consensus::DogeosConsensus;
use futures::FutureExt;
use reth_network_api::NetworkInfo;
use reth_network_p2p::sync::NetworkSyncUpdater;
use rollup_node_providers::{test_utils::MockL1Provider, ScrollRootProvider};
use scroll_db::test_utils::setup_test_db;
use scroll_engine::{
    test_utils::{ScriptedEngineClient, ScriptedResponse},
    ForkchoiceState,
};
use scroll_network::{NetworkConfigBuilder, ScrollNetworkManager, ScrollWireConfig};
use std::time::Duration;

fn info(block: &DogeosBlock) -> BlockInfo {
    BlockInfo { number: block.number, hash: block.hash_slow() }
}

fn block(number: u64, parent: B256, message: Option<TxL1Message>) -> DogeosBlock {
    let mut block = DogeosBlock::default();
    block.header.number = number;
    block.header.parent_hash = parent;
    block.body.transactions = message.into_iter().map(Into::into).collect();
    block
}

fn peer(block: DogeosBlock) -> NewBlockWithPeer {
    NewBlockWithPeer {
        peer_id: Default::default(),
        block,
        signature: Signature::new(U256::from(1), U256::from(2), false),
    }
}

fn push_block(asserter: &Asserter, block: &DogeosBlock) {
    let transactions = block
        .body
        .transactions
        .iter()
        .cloned()
        .map(|tx| {
            dogeos_rpc_types::ScrollRpcTransaction::from_transaction(
                Recovered::new_unchecked(tx, Default::default()),
                Default::default(),
            )
        })
        .collect();
    let mut rpc_block =
        alloy_rpc_types_eth::Block::<dogeos_rpc_types::ScrollRpcTransaction>::default();
    rpc_block.header.inner = block.header.clone();
    rpc_block.header.hash = block.hash_slow();
    rpc_block.transactions = alloy_rpc_types_eth::BlockTransactions::Full(transactions);
    asserter.push_success(&Some(rpc_block));
}

fn script_import(client: &ScriptedEngineClient) {
    let valid = PayloadStatus { status: PayloadStatusEnum::Valid, latest_valid_hash: None };
    client.push_new_payload(ScriptedResponse::Ok(valid.clone()));
    client.push_fork_choice_updated(ScriptedResponse::Ok(ForkchoiceUpdated {
        payload_status: valid,
        payload_id: None,
    }));
}

/// Use a real network handle so its initial-sync latch detects even a transient Idle update.
async fn orchestrator(
    database: Arc<Database>,
    asserter: Asserter,
    client: Arc<ScriptedEngineClient>,
    fcs: ForkchoiceState,
) -> (
    ChainOrchestrator<
        impl FullNetwork<Primitives = DogeosNetworkPrimitives>,
        DogeosChainSpec,
        MockL1Provider<Arc<Database>>,
        ScrollRootProvider,
        ScriptedEngineClient,
    >,
    mpsc::Sender<Arc<L1Notification>>,
) {
    database.insert_genesis_block(B256::ZERO).await.unwrap();
    database.set_l2_head_block_number(fcs.head_block_info().number).await.unwrap();
    let network_config = NetworkConfigBuilder::<DogeosNetworkPrimitives>::with_rng_secret_key(
        reth_tasks::Runtime::test(),
    )
    .disable_discovery()
    .with_unused_listener_port()
    .with_pow()
    .build_with_noop_provider(DOGEOS_DEV.clone());
    let (manager, network) = ScrollNetworkManager::new(
        DOGEOS_DEV.clone(),
        network_config,
        ScrollWireConfig::new(true),
        None,
        Default::default(),
        None,
    )
    .await;
    tokio::spawn(manager.run());
    network.inner().update_sync_state(RethSyncState::Syncing);
    let block_client = Arc::new(FullBlockClient::new(
        network.inner().fetch_client().await.unwrap(),
        Arc::new(DogeosConsensus),
    ));
    let network = network.into_scroll_network().await;
    let l1 = MockL1Provider { db: database.clone(), blobs: Default::default() };
    let pipeline = DerivationPipeline::new(l1.clone(), database.clone(), 0).await;
    let (watcher_tx, _watcher_rx) = mpsc::unbounded_channel();
    let (notification_tx, notification_rx) = mpsc::channel(16);
    let (orchestrator, _handle) = ChainOrchestrator::new(
        database,
        ChainOrchestratorConfig::new(DOGEOS_DEV.clone(), 1, 0),
        block_client,
        ProviderBuilder::<_, _, Scroll>::default().connect_mocked_client(asserter),
        L1WatcherHandle::new(watcher_tx, notification_rx),
        network,
        Box::new(NoopConsensus),
        Engine::new(client, fcs),
        None::<Sequencer<MockL1Provider<Arc<Database>>, DogeosChainSpec>>,
        None,
        pipeline,
    )
    .await
    .unwrap();
    (orchestrator, notification_tx)
}

#[derive(Clone, Copy, Debug)]
enum Completion {
    L1Last,
    L2Last,
}

#[derive(Clone, Copy, Debug)]
enum Outcome {
    Success,
    LateProviderError,
    LateValidationError,
}

/// Each chunk contains one block in tests: block 1 is committed before block 2 fails.
async fn check_completion(completion: Completion, outcome: Outcome) {
    let database = Arc::new(setup_test_db().await);
    let first_message = TxL1Message { queue_index: 0, gas_limit: 21_000, ..Default::default() };
    let second_message = TxL1Message { queue_index: 1, gas_limit: 21_000, ..Default::default() };
    database
        .insert_l1_message(L1MessageEnvelope::new(first_message.clone(), 1, None, None))
        .await
        .unwrap();
    // Deliberately omit the second message only for the late validation failure.
    if !matches!(outcome, Outcome::LateValidationError) {
        database
            .insert_l1_message(L1MessageEnvelope::new(second_message.clone(), 1, None, None))
            .await
            .unwrap();
    }
    let first = block(1, B256::ZERO, Some(first_message.clone()));
    let second = block(2, first.hash_slow(), Some(second_message));
    let asserter = Asserter::new();
    push_block(&asserter, &first);
    match outcome {
        Outcome::LateProviderError => asserter.push_failure_msg("late chunk provider failure"),
        _ => push_block(&asserter, &second),
    }
    let client = Arc::new(ScriptedEngineClient::new());
    let head = match completion {
        Completion::L1Last => info(&second),
        Completion::L2Last => info(&first),
    };
    let (mut orchestrator, _notifications) = orchestrator(
        database.clone(),
        asserter,
        client.clone(),
        ForkchoiceState::new(head, BlockInfo::default(), BlockInfo::default()),
    )
    .await;
    let mut events = orchestrator.event_listener();
    let result = match completion {
        Completion::L1Last => {
            orchestrator.handle_l1_notification(Arc::new(L1Notification::Synced)).await
        }
        Completion::L2Last => {
            orchestrator.sync_state.l1_mut().set_synced();
            orchestrator.sync_state.l2_mut().set_syncing();
            script_import(&client);
            // Exercise import_chain -> peer handler -> network handler, not just consolidation.
            orchestrator
                .handle_network_event(ScrollNetworkManagerEvent::NewBlock(peer(second)))
                .await
        }
    };
    let mut observed = Vec::new();
    while let Some(Some(event)) = events.next().now_or_never() {
        observed.push(event);
    }
    let success_events = observed
        .iter()
        .filter(|event| {
            matches!(
                event,
                ChainOrchestratorEvent::ChainConsolidated { .. } |
                    ChainOrchestratorEvent::L1Synced |
                    ChainOrchestratorEvent::ChainExtended(_)
            )
        })
        .count();
    let network = orchestrator.network.handle().inner();
    if matches!(outcome, Outcome::Success) {
        orchestrator.handle_outcome(result).unwrap();
        assert!(orchestrator.sync_state.is_synced(), "{completion:?}");
        assert!(!network.is_syncing());
        assert!(!network.is_initially_syncing());
        assert!(observed.iter().any(|event| matches!(
            event,
            ChainOrchestratorEvent::ChainConsolidated { from: 0, to: 2 }
        )));
        assert_eq!(success_events, 1 + usize::from(matches!(completion, Completion::L1Last)));
    } else {
        assert!(!orchestrator.sync_state.is_synced(), "{completion:?} {outcome:?}");
        match completion {
            Completion::L1Last => assert!(orchestrator.sync_state.l1().is_syncing()),
            Completion::L2Last => assert!(orchestrator.sync_state.l2().is_syncing()),
        }
        assert!(network.is_syncing(), "must not notify Idle");
        assert!(network.is_initially_syncing(), "must never have notified Idle");
        assert_eq!(success_events, 0, "{observed:?}");
        assert!(orchestrator.blocked_import.is_none(), "fatal failures must not be retried");
        let err = orchestrator.handle_outcome(result).expect_err("run loop must stop");
        let ChainOrchestratorError::ChainConsolidation(source) = err else {
            panic!("expected fatal consolidation error, got {err:?}")
        };
        match outcome {
            Outcome::LateProviderError => {
                assert!(matches!(*source, ChainOrchestratorError::InvalidBlock));
                let stored = database
                    .get_n_l1_messages(Some(L1MessageKey::from_queue_index(0)), 1)
                    .await
                    .unwrap();
                assert_eq!(
                    stored[0].l2_block_number,
                    Some(1),
                    "first chunk committed before failure"
                );
            }
            Outcome::LateValidationError => {
                assert!(matches!(*source, ChainOrchestratorError::L1MessageNotFound(_)))
            }
            Outcome::Success => unreachable!(),
        }
    }
}

#[tokio::test]
async fn l1_last_completion_waits_for_consolidation() {
    for outcome in [Outcome::Success, Outcome::LateProviderError, Outcome::LateValidationError] {
        check_completion(Completion::L1Last, outcome).await;
    }
}

#[tokio::test]
async fn l2_last_completion_waits_for_consolidation() {
    for outcome in [Outcome::Success, Outcome::LateProviderError, Outcome::LateValidationError] {
        check_completion(Completion::L2Last, outcome).await;
    }
}

#[tokio::test]
async fn duplicate_l1_synced_consolidation_failure_is_still_fatal() {
    let database = Arc::new(setup_test_db().await);
    let first = block(1, B256::ZERO, None);
    let asserter = Asserter::new();
    push_block(&asserter, &first);
    asserter.push_failure_msg("duplicate consolidation provider failure");
    let (mut orchestrator, _notifications) = orchestrator(
        database,
        asserter,
        Arc::new(ScriptedEngineClient::new()),
        ForkchoiceState::new(info(&first), BlockInfo::default(), BlockInfo::default()),
    )
    .await;
    orchestrator.handle_l1_notification(Arc::new(L1Notification::Synced)).await.unwrap();
    assert!(orchestrator.sync_state.is_synced());
    let mut events = orchestrator.event_listener();
    let result = tokio::time::timeout(
        Duration::from_secs(3),
        orchestrator.handle_l1_notification(Arc::new(L1Notification::Synced)),
    )
    .await
    .unwrap();
    assert!(orchestrator.handle_outcome(result).unwrap_err().is_fatal());
    assert!(events.next().now_or_never().is_none());
}

#[test]
fn consolidation_and_existing_frontier_failures_are_fatal() {
    let ordinary = ChainOrchestratorError::InvalidBlock;
    assert!(!ordinary.is_fatal());
    let consolidation = ChainOrchestratorError::ChainConsolidation(Box::new(ordinary));
    assert!(consolidation.is_fatal());
    assert!(!consolidation.is_frontier_fatal());
    for error in [
        ChainOrchestratorError::MissingEngineForkchoiceTags,
        ChainOrchestratorError::FrontierRecovery(Box::new(ChainOrchestratorError::InvalidBlock)),
        ChainOrchestratorError::FrontierTransitionStatus(PayloadStatusEnum::Syncing),
    ] {
        assert!(error.is_frontier_fatal());
        assert!(error.is_fatal());
    }
}

fn push_info(asserter: &Asserter, info: BlockInfo) {
    let mut rpc_block =
        alloy_rpc_types_eth::Block::<dogeos_rpc_types::ScrollRpcTransaction>::default();
    rpc_block.header.inner.number = info.number;
    rpc_block.header.hash = info.hash;
    asserter.push_success(&Some(rpc_block));
}

#[tokio::test]
async fn startup_polls_l1_notifications_while_l1_is_not_yet_synced() {
    let database = Arc::new(setup_test_db().await);
    let asserter = Asserter::new();
    for _ in 0..3 {
        push_info(&asserter, BlockInfo::default());
    }
    let (mut orchestrator, notifications) = orchestrator(
        database.clone(),
        asserter,
        Arc::new(ScriptedEngineClient::new()),
        ForkchoiceState::from_genesis(B256::ZERO),
    )
    .await;
    assert!(orchestrator.sync_state.l1().is_syncing());
    assert!(orchestrator.sync_state.l2().is_synced());
    let mut events = orchestrator.event_listener();
    notifications
        .send(Arc::new(L1Notification::NewBlock(BlockInfo { number: 1, hash: B256::ZERO })))
        .await
        .unwrap();
    notifications.send(Arc::new(L1Notification::Synced)).await.unwrap();
    let shutdown = Box::pin(async move {
        while let Some(event) = events.next().await {
            if matches!(event, ChainOrchestratorEvent::L1Synced) {
                return;
            }
        }
        panic!("L1Synced must be published before shutdown")
    });
    tokio::time::timeout(Duration::from_secs(3), orchestrator.run_until_shutdown(shutdown))
        .await
        .expect("startup must poll L1 before overall readiness")
        .unwrap();
    assert_eq!(database.get_latest_l1_block_number().await.unwrap(), 1);
}

#[tokio::test]
async fn import_command_consolidation_failure_stops_run_loop_and_replies_error() {
    let database = Arc::new(setup_test_db().await);
    let asserter = Asserter::new();
    for _ in 0..3 {
        push_info(&asserter, BlockInfo::default());
    }
    let first = block(1, B256::ZERO, None);
    let second = block(2, first.hash_slow(), None);
    push_block(&asserter, &first);
    asserter.push_failure_msg("late chunk provider failure");
    let client = Arc::new(ScriptedEngineClient::new());
    script_import(&client);
    let (mut orchestrator, _notifications) =
        orchestrator(database, asserter, client, ForkchoiceState::from_genesis(B256::ZERO)).await;
    orchestrator.sync_state.l1_mut().set_synced();
    orchestrator.sync_state.l2_mut().set_syncing();
    let (commands, receiver) = mpsc::unbounded_channel();
    orchestrator.handle_rx = receiver;
    let (reply, response) = tokio::sync::oneshot::channel();
    commands
        .send(ChainOrchestratorCommand::ImportBlock {
            block_with_peer: peer(second),
            response: reply,
        })
        .unwrap();
    let result = tokio::time::timeout(
        Duration::from_secs(3),
        orchestrator.run_until_shutdown(std::future::pending()),
    )
    .await
    .expect("fatal ImportBlock failure must terminate the command branch");
    assert!(matches!(result, Err(ChainOrchestratorError::ChainConsolidation(_))));
    assert!(response.await.unwrap().unwrap_err().contains("chain consolidation failed"));
}

#[tokio::test]
async fn l2_ready_before_l1_does_not_consolidate_or_block_l1_polling() {
    let database = Arc::new(setup_test_db().await);
    let client = Arc::new(ScriptedEngineClient::new());
    script_import(&client);
    let (mut orchestrator, _notifications) =
        orchestrator(database, Asserter::new(), client, ForkchoiceState::from_genesis(B256::ZERO))
            .await;
    orchestrator.sync_state.l2_mut().set_syncing();
    let mut events = orchestrator.event_listener();
    let imported = block(1, B256::ZERO, None);
    orchestrator.import_chain(vec![imported.clone()], peer(imported)).await.unwrap();
    assert!(orchestrator.sync_state.l2().is_synced());
    assert!(orchestrator.sync_state.l1().is_syncing());
    assert!(!orchestrator.sync_state.is_synced());
    assert!(l1_notification_receiver_may_poll(
        orchestrator.sync_state.l2().is_synced(),
        orchestrator.derivation_pipeline.is_empty(),
        orchestrator.derivation_driver.can_accept_batch(),
    ));
    assert!(orchestrator.network.handle().inner().is_initially_syncing());
    assert!(events.next().now_or_never().is_none());
}

#[tokio::test]
async fn batch_fetch_must_finish_before_network_idle() {
    let database = Arc::new(setup_test_db().await);
    let (mut orchestrator, _notifications) = orchestrator(
        database.clone(),
        Asserter::new(),
        Arc::new(ScriptedEngineClient::new()),
        ForkchoiceState::from_genesis(B256::ZERO),
    )
    .await;
    let network = orchestrator.network.handle().clone();
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let transaction = tokio::spawn({
        let entered = entered.clone();
        let release = release.clone();
        async move {
            database
                .tx_mut(move |tx| {
                    let entered = entered.clone();
                    let release = release.clone();
                    async move {
                        entered.notify_one();
                        release.notified().await;
                        drop(tx);
                        Ok::<_, ChainOrchestratorError>(())
                    }
                })
                .await
        }
    });
    tokio::time::timeout(Duration::from_secs(3), entered.notified()).await.unwrap();
    // With no unsafe blocks, consolidation's first database access is its batch fetch.
    let mut completion =
        Box::pin(orchestrator.handle_l1_notification(Arc::new(L1Notification::Synced)));
    let pending = tokio::time::timeout(Duration::from_millis(100), &mut completion).await;
    let remained_syncing = network.inner().is_initially_syncing();
    release.notify_one();
    tokio::time::timeout(Duration::from_secs(3), completion).await.unwrap().unwrap();
    transaction.await.unwrap().unwrap();
    assert!(pending.is_err(), "batch fetch must wait for the held transaction");
    assert!(remained_syncing, "must not publish Idle while the batch fetch is pending");
    assert!(orchestrator.sync_state.is_synced());
    assert!(!network.inner().is_syncing());
}
