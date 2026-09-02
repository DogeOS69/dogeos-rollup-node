//! Contains tests related to RN and EN sync.

use alloy_primitives::{b256, Address, Signature, B256, U256};
use dogeos_chainspec::{DOGEOS_CHIKYU, DOGEOS_DEV};
use dogeos_protocol_types::TxL1Message;
use futures::StreamExt;
use reqwest::Url;
use reth_provider::{BlockIdReader, BlockReader};
use reth_rpc_eth_api::helpers::EthTransactions;
use reth_tokio_util::EventStream;
use rollup_node::{
    test_utils::{
        default_test_scroll_rollup_node_config, generate_tx, setup_engine, EventAssertions,
        TestFixture,
    },
    BlobProviderArgs, ChainOrchestratorArgs, ConsensusArgs, EngineDriverArgs, L1ProviderArgs,
    PprofArgs, RollupNodeDatabaseArgs, RollupNodeGasPriceOracleArgs, RollupNodeNetworkArgs,
    RpcArgs, ScrollRollupNodeConfig, SequencerArgs, TestArgs,
};
use rollup_node_chain_orchestrator::ChainOrchestratorEvent;
use rollup_node_primitives::BlockInfo;
use rollup_node_sequencer::L1MessageInclusionMode;
use rollup_node_watcher::L1Notification;
use std::{path::PathBuf, sync::Arc};

#[tokio::test]
async fn test_should_consolidate_to_block_15k() -> eyre::Result<()> {
    reth_tracing::init_test_tracing();

    // Prepare the config for a L1 consolidation. GitHub Actions passes a
    // missing secret as an EMPTY string, so treat empty as unset. NOTE: with
    // sync.yaml dispatch-gated and test.yaml skipping this test, no CI lane
    // reaches this guard today — it protects local runs until issue #43
    // re-points the test at chikyu infrastructure.
    let alchemy_key = match std::env::var("ALCHEMY_KEY") {
        Ok(key) if !key.trim().is_empty() => key,
        _ => {
            eprintln!("ALCHEMY_KEY environment variable is not set or empty. Skipping test.");
            return Ok(());
        }
    };

    let node_config = ScrollRollupNodeConfig {
        test_args: TestArgs { test: false, skip_l1_synced: false },
        network_args: RollupNodeNetworkArgs {
            enable_eth_scroll_wire_bridge: false,
            enable_scroll_wire: false,
            sequencer_url: None,
            signer_address: None,
            legacy_geth_header_transform: false,
        },
        database_args: RollupNodeDatabaseArgs::default(),
        chain_orchestrator_args: ChainOrchestratorArgs {
            optimistic_sync_trigger: 100,
            ..Default::default()
        },
        l1_provider_args: L1ProviderArgs {
            url: Some(Url::parse(&format!("https://eth-sepolia.g.alchemy.com/v2/{alchemy_key}"))?),
            compute_units_per_second: 500,
            max_retries: 10,
            initial_backoff: 100,
            logs_query_block_range: 500,
            cache_max_items: 100,
            ..Default::default()
        },
        engine_driver_args: EngineDriverArgs { sync_at_startup: false },
        sequencer_args: SequencerArgs {
            sequencer_enabled: false,
            allow_empty_blocks: true,
            ..Default::default()
        },
        blob_provider_args: BlobProviderArgs {
            s3_url: Some(Url::parse(
                "https://scroll-sepolia-blob-data.s3.us-west-2.amazonaws.com/",
            )?),
            compute_units_per_second: 100,
            max_retries: 10,
            initial_backoff: 100,
            ..Default::default()
        },
        signer_args: Default::default(),
        gas_price_oracle_args: RollupNodeGasPriceOracleArgs::default(),
        consensus_args: ConsensusArgs::noop(),
        database: None,
        rpc_args: RpcArgs::default(),
        remote_block_source_args: Default::default(),
        pprof_args: PprofArgs::default(),
        require_l1_data_fee_buffer: false,
    };

    let chain_spec = (*DOGEOS_CHIKYU).clone();
    let (mut nodes, _dbs, _wallet) =
        setup_engine(node_config, 1, chain_spec.clone(), false, false, None, None).await?;
    let node = nodes.pop().unwrap();

    // We perform consolidation up to block 15k. This allows us to capture a batch revert event at
    // block 11419 (batch 1653).
    while node.inner.provider.safe_block_num_hash()?.map(|x| x.number).unwrap_or_default() < 15000 {
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await
    }

    let block_hash_15k = node.inner.provider.block(15000.into())?.unwrap();

    assert_eq!(
        block_hash_15k.hash_slow(),
        b256!("86901ebce1840ee45c1d5c70bf85ce6924f7a066ef11575d0f381858c83845d4")
    );

    Ok(())
}

#[allow(clippy::large_stack_frames)]
#[tokio::test]
async fn test_node_produces_block_on_startup() -> eyre::Result<()> {
    reth_tracing::init_test_tracing();

    // Start a sequencer and follower node.
    let mut fixture = TestFixture::builder()
        .sequencer()
        .followers(1)
        .auto_start(true)
        .allow_empty_blocks(false)
        .build()
        .await?;

    fixture.l1().sync().await?;

    // wait for both nodes to be synced.
    fixture.expect_event_on_all_nodes().chain_consolidated().await?;

    // construct a transaction and send it to the follower node.
    let wallet = fixture.wallet();
    let follower_rpc = fixture.follower(0).node.rpc.inner.clone();
    let handle = tokio::spawn(async move {
        loop {
            let tx = generate_tx(wallet.clone()).await;
            let _ = follower_rpc.eth_api().send_raw_transaction(tx).await;
        }
    });

    fixture.expect_event_on_followers().chain_extended(1).await?;
    drop(handle);

    Ok(())
}

/// We test if the syncing of the RN is correctly triggered and released when the EN reaches sync.
#[allow(clippy::large_stack_frames)]
#[tokio::test]
async fn test_should_trigger_pipeline_sync_for_execution_node() -> eyre::Result<()> {
    reth_tracing::init_test_tracing();

    const OPTIMISTIC_SYNC_TRIGGER: u64 = 100;
    let mut sequencer = TestFixture::builder()
        .sequencer()
        .block_time(40)
        .auto_start(true)
        .optimistic_sync_trigger(OPTIMISTIC_SYNC_TRIGGER)
        .build()
        .await?;

    let mut follower = TestFixture::builder()
        .followers(1)
        .optimistic_sync_trigger(OPTIMISTIC_SYNC_TRIGGER)
        .build()
        .await?;

    // Set the L1 to synced on the synced node to start block production.
    sequencer.l1().sync().await?;

    // Wait for the chain to be advanced by the sequencer.
    sequencer.expect_event().block_sequenced(OPTIMISTIC_SYNC_TRIGGER + 1).await?;

    // Connect the nodes together.
    sequencer.sequencer().node.connect(&mut follower.follower(0).node).await;

    // Assert that the unsynced node triggers optimistic sync.
    follower.expect_event().optimistic_sync().await?;

    // Verify the unsynced node syncs.
    let mut num = follower.get_block(0).await?.header.number;

    // Wall-clock deadline consistent with the other waiters: a ~2s retry
    // budget for a 101-block pipeline sync would flake on contended runners,
    // and this assertion gates merges.
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(30);
    while num <= OPTIMISTIC_SYNC_TRIGGER && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        num = follower.get_block(0).await?.header.number;
    }
    // Exhausting the deadline must FAIL, not fall through into the
    // >=-matched extension wait below with num still at 0.
    eyre::ensure!(
        num > OPTIMISTIC_SYNC_TRIGGER,
        "EN did not pipeline-sync past the optimistic-sync trigger: follower head {num}"
    );

    // Assert that the unsynced node triggers a chain extension on the optimistic chain.
    follower.expect_event().chain_extended(num).await?;

    Ok(())
}

#[allow(clippy::large_stack_frames)]
#[tokio::test]
async fn test_should_consolidate_after_optimistic_sync() -> eyre::Result<()> {
    reth_tracing::init_test_tracing();

    // The sequencer starts with automatic sequencing DISABLED (no
    // auto_start(true)): the setup loop below drives manual build_block()
    // calls with exact expected block numbers, and the 20ms build timer raced
    // those on slow runners — a timer-built block claimed the expected number,
    // its event was consumed by an interleaved waiter, and the exact-number
    // wait hung for 30s (issue #38). Automatic sequencing is enabled after the
    // loop, where the sync/consolidation phase relies on its continuous block
    // stream and no assertion depends on exact numbers.
    let mut sequencer = TestFixture::builder()
        .sequencer()
        .with_memory_db()
        .with_eth_scroll_bridge(true)
        .with_scroll_wire(true)
        .block_time(20)
        .with_l1_message_delay(0)
        .allow_empty_blocks(true)
        .build()
        .await?;

    let mut follower = TestFixture::builder().followers(1).with_memory_db().build().await?;

    // Send a notification to the sequencer node that the L1 watcher is synced.
    sequencer.l1().sync().await?;

    // Create a sequence of L1 messages to be added to the sequencer node.
    const L1_MESSAGES_COUNT: usize = 200;
    let mut l1_messages = Vec::with_capacity(L1_MESSAGES_COUNT);
    for i in 0..L1_MESSAGES_COUNT as u64 {
        let l1_message = TxL1Message {
            queue_index: i,
            gas_limit: 21000,
            sender: Address::random(),
            to: Address::random(),
            value: U256::from(1),
            input: Default::default(),
        };
        l1_messages.push(l1_message);
    }

    // Add the L1 messages to the sequencer node.
    for (i, l1_message) in l1_messages.iter().enumerate() {
        sequencer
            .l1()
            .add_message()
            .to(l1_message.to)
            .queue_index(l1_message.queue_index)
            .gas_limit(l1_message.gas_limit)
            .sender(l1_message.sender)
            .value(l1_message.value)
            .input(l1_message.input.clone())
            .at_block(i as u64)
            .send()
            .await?;
        sequencer.expect_event().l1_message_committed().await?;

        sequencer.l1().new_block(i as u64).await?;
        sequencer.expect_event().new_l1_block().await?;

        sequencer.build_block().expect_block_number((i + 1) as u64).build_and_await_block().await?;
    }

    // The exact-number assertions are done; from here the test needs a
    // continuous stream of sequenced blocks (to trigger the follower's
    // optimistic sync and later its consolidation), so enable the automatic
    // build timer now. The command returns false when no sequencer is
    // configured, which would surface much later as an unrelated timeout.
    eyre::ensure!(
        sequencer.sequencer().rollup_manager_handle.enable_automatic_sequencing().await?,
        "automatic sequencing was not enabled"
    );

    // Connect the nodes together.
    sequencer.sequencer().node.connect(&mut follower.follower(0).node).await;

    // trigger a new block on the sequencer node.
    sequencer.build_block().build_and_await_block().await?;

    // Assert that the unsynced node triggers optimistic sync.
    follower.expect_event().optimistic_sync().await?;

    // Let the unsynced node process the optimistic sync.
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    // Send all L1 messages to the unsynced node.
    for (i, l1_message) in l1_messages.iter().enumerate() {
        follower
            .l1()
            .add_message()
            .to(l1_message.to)
            .queue_index(l1_message.queue_index)
            .gas_limit(l1_message.gas_limit)
            .sender(l1_message.sender)
            .value(l1_message.value)
            .input(l1_message.input.clone())
            .at_block(i as u64)
            .send()
            .await?;
        follower.expect_event().l1_message_committed().await?;
    }

    // Send a notification to the unsynced node that the L1 watcher is synced.
    follower.l1().sync().await?;

    // Consolidation is triggered by whichever completes last: the Synced
    // notification (ChainConsolidated then L1Synced) or the L2 sync
    // transition on a later import (L1Synced then ChainConsolidated). This
    // waiter is safe under BOTH orderings — and deliberately NOT followed by
    // an l1_synced() wait, which the drain here could strand:
    // ChainConsolidated is only emitted when sync_state.is_synced(), which
    // already proves the Synced notification was processed. This is the
    // assertion the test is named for: the consolidated range must cover
    // the optimistically-synced blocks.
    //
    // Explicit budget, NOT the 30s default: automatic sequencing is running, so
    // the slower the runner, the further the sequencer's head has advanced by
    // the time the follower starts consolidating — and consolidate_chain makes
    // one full get_block_by_number round-trip per block over safe+1..=head. A
    // fixed default budget against that growing workload is a capacity race,
    // and this test runs in the merge gate at full parallelism and in the
    // nightly soak under four CPU spinners, where a capacity timeout is
    // auto-reported on issue #38 as a race regression. 120s makes the wait fail
    // on behaviour instead.
    let consolidations = follower
        .expect_event()
        .timeout(std::time::Duration::from_secs(120))
        .chain_consolidated()
        .await?;
    // Assert the RANGE: `to` alone merely restates the head the earlier
    // optimistic_sync wait guaranteed (ChainConsolidated is emitted even on
    // the head == safe early-out); from == 0 proves validation actually
    // covered the optimistically-synced blocks.
    let (from, to) = *consolidations.first().expect("one consolidation per waited node");
    eyre::ensure!(
        from == 0 && to >= L1_MESSAGES_COUNT as u64,
        "consolidation did not cover the optimistic range: from={from} to={to}"
    );

    // Let the unsynced node process the L1 messages.
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    // build a new block on the sequencer node to trigger consolidation on the unsynced node.
    sequencer.build_block().build_and_await_block().await?;

    // Assert that the unsynced node consolidates the chain.
    follower.expect_event().chain_extended((L1_MESSAGES_COUNT + 2) as u64).await?;

    // Now push a L1 message to the sequencer node and build a new block.
    sequencer
        .l1()
        .add_message()
        .queue_index(200)
        .sender(Address::random())
        .value(1)
        .at_block(200)
        .send()
        .await?;
    sequencer.expect_event().l1_message_committed().await?;

    sequencer.l1().new_block(201).await?;
    sequencer.expect_event().new_l1_block().await?;

    sequencer.build_block().build_and_await_block().await?;
    follower.expect_event().new_block_received().await?;

    // Assert that the follower node does not accept the new block as it does not have the L1
    // message.
    follower
        .expect_event()
        .where_event(|e| matches!(e, ChainOrchestratorEvent::L1MessageNotFoundInDatabase(_)))
        .await?;

    Ok(())
}

#[allow(clippy::large_stack_frames)]
#[tokio::test]
async fn test_consolidation() -> eyre::Result<()> {
    reth_tracing::init_test_tracing();

    let mut sequencer = TestFixture::builder()
        .sequencer()
        .with_eth_scroll_bridge(true)
        .with_scroll_wire(true)
        .auto_start(false)
        .block_time(10)
        .with_l1_message_delay(0)
        .allow_empty_blocks(true)
        .build()
        .await?;

    let mut follower = TestFixture::builder().followers(1).build().await?;

    // Connect the nodes together.
    sequencer.sequencer().node.connect(&mut follower.follower(0).node).await;

    // Create a L1 message and send it to both nodes.
    let sender = Address::random();
    let to = Address::random();

    sequencer.l1().add_message().sender(sender).to(to).value(1).queue_index(0).send().await?;
    sequencer.expect_event().l1_message_committed().await?;

    follower.l1().add_message().sender(sender).to(to).value(1).send().await?;
    follower.expect_event().l1_message_committed().await?;

    // Send a notification to both nodes that the L1 watcher is synced.
    sequencer.l1().sync().await?;
    follower.l1().sync().await?;

    // Assert that the unsynced node consolidates the chain.
    follower.expect_event().chain_consolidated().await?;

    // Build a new block on the sequencer node.
    sequencer.build_block().build_and_await_block().await?;

    // Now push a L1 message to the sequencer node and build a new block.
    sequencer
        .l1()
        .add_message()
        .sender(Address::random())
        .to(Address::random())
        .value(1)
        .queue_index(1)
        .at_block(1)
        .send()
        .await?;
    sequencer.expect_event().l1_message_committed().await?;

    sequencer.l1().new_block(5).await?;
    sequencer.expect_event().new_l1_block().await?;
    sequencer.build_block().build_and_await_block().await?;

    // Assert that the follower node rejects the new block as it hasn't received the L1 message.
    follower
        .expect_event()
        .where_event(|e| matches!(e, ChainOrchestratorEvent::L1MessageNotFoundInDatabase(_)))
        .await?;

    Ok(())
}

#[allow(clippy::large_stack_frames)]
#[tokio::test]
async fn test_chain_orchestrator_reorg_with_gap_above_head() -> eyre::Result<()> {
    test_chain_orchestrator_fork_choice(100, Some(95), 20, |e| {
        if let ChainOrchestratorEvent::ChainReorged(chain_import) = e {
            // Assert that the chain import is as expected.
            assert_eq!(chain_import.chain.len(), 21);
            true
        } else {
            false
        }
    })
    .await
}

#[allow(clippy::large_stack_frames)]
#[tokio::test]
async fn test_chain_orchestrator_reorg_with_gap_below_head() -> eyre::Result<()> {
    test_chain_orchestrator_fork_choice(100, Some(50), 20, |e| {
        if let ChainOrchestratorEvent::ChainReorged(chain_import) = e {
            // Assert that the chain import is as expected.
            assert_eq!(chain_import.chain.len(), 21);
            true
        } else {
            false
        }
    })
    .await
}

#[allow(clippy::large_stack_frames)]
#[tokio::test]
async fn test_chain_orchestrator_extension_with_gap() -> eyre::Result<()> {
    test_chain_orchestrator_fork_choice(100, None, 20, |e| {
        if let ChainOrchestratorEvent::ChainExtended(chain_import) = e {
            // Assert that the chain import is as expected.
            assert_eq!(chain_import.chain.len(), 21);
            true
        } else {
            false
        }
    })
    .await
}

#[allow(clippy::large_stack_frames)]
#[tokio::test]
async fn test_chain_orchestrator_extension_no_gap() -> eyre::Result<()> {
    test_chain_orchestrator_fork_choice(100, None, 0, |e| {
        if let ChainOrchestratorEvent::ChainExtended(chain_import) = e {
            // Assert that the chain import is as expected.
            assert_eq!(chain_import.chain.len(), 1);
            true
        } else {
            false
        }
    })
    .await
}

#[allow(clippy::large_stack_frames)]
async fn test_chain_orchestrator_fork_choice(
    initial_blocks: usize,
    reorg_block_number: Option<usize>,
    additional_blocks: usize,
    expected_final_event_predicate: impl Fn(&ChainOrchestratorEvent) -> bool,
) -> eyre::Result<()> {
    reth_tracing::init_test_tracing();

    let mut sequencer = TestFixture::builder()
        .sequencer()
        .with_scroll_wire(true)
        .with_eth_scroll_bridge(false)
        .auto_start(false)
        .block_time(10)
        .with_l1_message_delay(0)
        .allow_empty_blocks(true)
        .build()
        .await?;

    let mut follower = TestFixture::builder().followers(1).build().await?;

    // Connect the nodes together.
    sequencer.sequencer().node.connect(&mut follower.follower(0).node).await;

    // set both the sequencer and follower L1 watchers to synced
    sequencer.l1().sync().await?;
    follower.l1().sync().await?;

    // Initially the sequencer should build 100 empty blocks in each and the follower
    // should follow them
    let mut reorg_block_info: Option<BlockInfo> = None;
    for i in 0..initial_blocks {
        let num = (i + 1) as u64;
        let block = sequencer.build_block().build_and_await_block().await?;

        if Some(i) == reorg_block_number {
            reorg_block_info = Some((&block).into());
        }

        follower.expect_event().chain_extended(num).await?;
    }

    // Now reorg the sequencer and disable gossip so we can create fork
    let sequencer_handle = &sequencer.sequencer().rollup_manager_handle;
    sequencer_handle.set_gossip(false).await?;
    if let Some(block_info) = reorg_block_info {
        sequencer_handle
            .update_fcs_head(block_info)
            .await?
            .map_err(|refusal| eyre::eyre!("head update refused: {refusal}"))?;
    }

    // wait two seconds to ensure the timestamp of the new blocks is greater than the old ones
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Have the sequencer build 20 new blocks, containing new L1 messages.
    for _ in 0..additional_blocks {
        sequencer.build_block().build_and_await_block().await?;
    }

    // now build a final block
    let sequencer_handle = &sequencer.sequencer().rollup_manager_handle;
    sequencer_handle.set_gossip(true).await?;
    sequencer.build_block().build_and_await_block().await?;

    // Wait for the follower node to accept the new chain
    follower.expect_event().where_event(expected_final_event_predicate).await?;

    Ok(())
}

#[allow(clippy::large_stack_frames)]
#[tokio::test]
async fn test_chain_orchestrator_l1_reorg() -> eyre::Result<()> {
    reth_tracing::init_test_tracing();
    let node_config = default_test_scroll_rollup_node_config();
    let sequencer_node_config = ScrollRollupNodeConfig {
        test_args: TestArgs { test: true, skip_l1_synced: false },
        network_args: RollupNodeNetworkArgs {
            enable_eth_scroll_wire_bridge: false,
            enable_scroll_wire: true,
            ..Default::default()
        },
        database_args: RollupNodeDatabaseArgs {
            rn_db_path: Some(PathBuf::from("sqlite::memory:")),
        },
        l1_provider_args: L1ProviderArgs::default(),
        engine_driver_args: EngineDriverArgs::default(),
        chain_orchestrator_args: ChainOrchestratorArgs::default(),
        sequencer_args: SequencerArgs {
            sequencer_enabled: true,
            auto_start: false,
            block_time: 10,
            l1_message_inclusion_mode: L1MessageInclusionMode::BlockDepth(0),
            allow_empty_blocks: true,
            ..SequencerArgs::default()
        },
        blob_provider_args: BlobProviderArgs { mock: true, ..Default::default() },
        signer_args: Default::default(),
        gas_price_oracle_args: RollupNodeGasPriceOracleArgs::default(),
        consensus_args: ConsensusArgs::noop(),
        database: None,
        rpc_args: RpcArgs::default(),
        remote_block_source_args: Default::default(),
        pprof_args: PprofArgs::default(),
        require_l1_data_fee_buffer: false,
    };

    // Create the chain spec for scroll dev with Feynman activated and a test genesis.
    let chain_spec = (*DOGEOS_DEV).clone();

    // Create a sequencer node and an unsynced node.
    let (mut nodes, _dbs, _wallet) = setup_engine(
        sequencer_node_config.clone(),
        1,
        chain_spec.clone(),
        false,
        false,
        None,
        None,
    )
    .await
    .unwrap();
    let mut sequencer = nodes.pop().unwrap();
    let sequencer_handle = sequencer.rollup_manager_handle.clone();
    let mut sequencer_events = sequencer_handle.get_event_listener().await?;
    let sequencer_l1_watcher_tx = sequencer.rollup_manager_handle.l1_watcher_mock.clone().unwrap();

    let (mut nodes, _dbs, _wallet) =
        setup_engine(node_config.clone(), 1, chain_spec.clone(), false, false, None, None)
            .await
            .unwrap();
    let mut follower = nodes.pop().unwrap();
    let mut follower_events = follower.rollup_manager_handle.get_event_listener().await?;
    let follower_l1_watcher_tx = follower.rollup_manager_handle.l1_watcher_mock.clone().unwrap();

    // Connect the nodes together.
    sequencer.connect(&mut follower).await;

    // set both the sequencer and follower L1 watchers to synced
    sequencer_l1_watcher_tx.notification_tx.send(Arc::new(L1Notification::Synced)).await.unwrap();
    follower_l1_watcher_tx.notification_tx.send(Arc::new(L1Notification::Synced)).await.unwrap();

    // Initially the sequencer should build 100 blocks with 1 message in each and the follower
    // should follow them
    for i in 0..100 {
        let block_info = BlockInfo { number: i, hash: B256::random() };
        let l1_message = Arc::new(L1Notification::L1Message {
            message: TxL1Message {
                queue_index: i,
                gas_limit: 21000,
                sender: Address::random(),
                to: Address::random(),
                value: U256::from(1),
                input: Default::default(),
            },
            block_info,
            block_timestamp: i * 10,
        });
        let new_block = Arc::new(L1Notification::NewBlock(block_info));
        sequencer_l1_watcher_tx.notification_tx.send(l1_message.clone()).await.unwrap();
        sequencer_l1_watcher_tx.notification_tx.send(new_block.clone()).await.unwrap();
        wait_n_events(
            "sequencer NewL1Block",
            &mut sequencer_events,
            |e| matches!(e, ChainOrchestratorEvent::NewL1Block(_)),
            1,
        )
        .await;
        follower_l1_watcher_tx.notification_tx.send(l1_message).await.unwrap();
        follower_l1_watcher_tx.notification_tx.send(new_block).await.unwrap();
        wait_n_events(
            "follower NewL1Block",
            &mut follower_events,
            |e| matches!(e, ChainOrchestratorEvent::NewL1Block(_)),
            1,
        )
        .await;

        sequencer_handle.build_block();
        wait_n_events(
            "sequencer BlockSequenced",
            &mut sequencer_events,
            |e| matches!(e, ChainOrchestratorEvent::BlockSequenced(_)),
            1,
        )
        .await;
        wait_n_events(
            "follower ChainExtended (gossiped block)",
            &mut follower_events,
            |e| matches!(e, ChainOrchestratorEvent::ChainExtended(_)),
            1,
        )
        .await;
    }

    // send a reorg notification to the sequencer
    sequencer_l1_watcher_tx
        .notification_tx
        .send(Arc::new(L1Notification::Reorg(50)))
        .await
        .unwrap();
    wait_n_events(
        "sequencer L1Reorg(50)",
        &mut sequencer_events,
        |e| {
            matches!(
                e,
                ChainOrchestratorEvent::L1Reorg {
                    l1_block_number: 50,
                    queue_index: Some(51),
                    l2_head_block_info: _,
                    l2_safe_block_info: _
                }
            )
        },
        1,
    )
    .await;

    sequencer_handle.set_gossip(false).await.unwrap();

    // Have the sequencer build 20 new blocks, containing new L1 messages.
    let mut l1_notifications = vec![];
    for i in 0..20 {
        let block_info = BlockInfo { number: (51 + i), hash: B256::random() };
        let l1_message = Arc::new(L1Notification::L1Message {
            message: TxL1Message {
                queue_index: 51 + i,
                gas_limit: 21000,
                sender: Address::random(),
                to: Address::random(),
                value: U256::from(1),
                input: Default::default(),
            },
            block_info,
            block_timestamp: (51 + i) * 10,
        });
        let new_block = Arc::new(L1Notification::NewBlock(block_info));
        l1_notifications.extend([l1_message.clone(), new_block.clone()]);
        sequencer_l1_watcher_tx.notification_tx.send(l1_message.clone()).await.unwrap();
        sequencer_l1_watcher_tx.notification_tx.send(new_block.clone()).await.unwrap();
        wait_n_events(
            "sequencer NewL1Block",
            &mut sequencer_events,
            |e| matches!(e, ChainOrchestratorEvent::NewL1Block(_)),
            1,
        )
        .await;

        sequencer_handle.build_block();
        wait_n_events(
            "sequencer BlockSequenced",
            &mut sequencer_events,
            |e| matches!(e, ChainOrchestratorEvent::BlockSequenced(_)),
            1,
        )
        .await;
    }

    // wait two seconds to ensure the timestamp of the new blocks is greater than the old ones
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    // now build a final block
    sequencer_handle.set_gossip(true).await.unwrap();
    sequencer_handle.build_block();

    // The follower node should reject the new block as it has a different view of L1 data.
    wait_n_events(
        "follower L1MessageMismatch",
        &mut follower_events,
        |e| matches!(e, ChainOrchestratorEvent::L1MessageMismatch { .. }),
        1,
    )
    .await;

    // Now update the follower node with the new L1 data
    follower_l1_watcher_tx.notification_tx.send(Arc::new(L1Notification::Reorg(50))).await.unwrap();
    for notification in l1_notifications {
        follower_l1_watcher_tx.notification_tx.send(notification).await.unwrap();
    }
    wait_n_events(
        "follower NewL1Block x20 (post-reorg L1 update)",
        &mut follower_events,
        |e| matches!(e, ChainOrchestratorEvent::NewL1Block(_)),
        20,
    )
    .await;

    // Now build a new block on the sequencer to trigger the reorg on the follower
    sequencer_handle.build_block();

    // Wait for the follower node to accept the new chain
    wait_n_events(
        "follower ChainExtended (post-reorg)",
        &mut follower_events,
        |e| matches!(e, ChainOrchestratorEvent::ChainExtended(_)),
        1,
    )
    .await;

    Ok(())
}

/// Contract: a manual build request while a payload building job is in flight
/// coalesces with it — observable via the `BuildBlockCoalesced` event, which
/// the pre-fix replace semantics never emit — and numbering stays contiguous
/// (issue #38).
#[allow(clippy::large_stack_frames)]
#[tokio::test]
async fn test_manual_build_block_coalesces_with_inflight_job() -> eyre::Result<()> {
    reth_tracing::init_test_tracing();

    let mut fixture = TestFixture::builder()
        .sequencer()
        .with_memory_db()
        .payload_building_duration(2000) // long job so the second command lands mid-flight
        .allow_empty_blocks(true)
        .build()
        .await?;

    fixture.l1().sync().await?;

    // Fire two build commands; the second arrives while the 2s job from the
    // first is still in flight.
    fixture.sequencer().rollup_manager_handle.build_block();
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    fixture.sequencer().rollup_manager_handle.build_block();

    // The second command coalesced with the in-flight job. This event is
    // emitted before the job completes, so it must arrive ahead of the
    // BlockSequenced below and the waiter cannot discard that one.
    fixture
        .expect_event()
        .label("BuildBlockCoalesced with the in-flight manual job")
        .where_event(|e| matches!(e, ChainOrchestratorEvent::BuildBlockCoalesced))
        .await?;

    // Exactly one block results: number 1.
    fixture.expect_event().block_sequenced(1).await?;

    // Bite against a PARTIAL regression (emit the event but still spawn a
    // second job): the sequencer is quiescent here — no auto-start, no
    // outstanding command — so nothing may sequence within 2x the payload
    // duration unless the coalesced command leaked a phantom job.
    let phantom = fixture
        .expect_event()
        .timeout(std::time::Duration::from_secs(4))
        .label("no phantom second BlockSequenced after coalescing")
        .where_event(|e| matches!(e, ChainOrchestratorEvent::BlockSequenced(_)))
        .await;
    eyre::ensure!(
        phantom.is_err(),
        "a second job sequenced a block after the coalesced command: {phantom:?}"
    );
    // Behavioural anchor (the is_err above would also trip on a shut-down
    // node or closed stream): the chain must still sit exactly at block 1.
    eyre::ensure!(
        fixture.get_block(0).await?.header.number == 1,
        "unexpected head after the coalesced build"
    );

    // A follow-up build produces number 2. (`BuildBlockCoalesced` above is
    // what distinguishes coalescing from the old replace-semantics; a phantom
    // second job would also sequence 1 then 2, so contiguous numbering alone
    // proves nothing.)
    fixture.build_block().expect_block_number(2).build_and_await_block().await?;

    Ok(())
}

/// The coalescing guard's original target: a manual build request arriving
/// while a TIMER-triggered job is in flight must coalesce with it instead of
/// replacing it (issue #38 — the replace made numbering timing-dependent).
#[allow(clippy::large_stack_frames)]
#[tokio::test]
async fn test_manual_build_block_coalesces_with_timer_job() -> eyre::Result<()> {
    reth_tracing::init_test_tracing();

    let mut fixture = TestFixture::builder()
        .sequencer()
        .with_memory_db()
        .auto_start(true)
        .block_time(100)
        .payload_building_duration(3000)
        .allow_empty_blocks(true)
        .build()
        .await?;

    fixture.l1().sync().await?;
    // Observable precondition, not a guess: the sequencer arm's slot gate
    // opens only once the Synced notification is processed, while a manual
    // BuildBlock is ungated — sleeping blind here would race gate-open
    // latency and let the manual command start the job itself.
    fixture.expect_event().l1_synced().await?;

    // From gate-open, slots fire every 100ms and each job runs 3s
    // (comfortably inside the engine's ~12s payload-job deadline), so a job
    // is in flight essentially continuously and a manual request at t=500ms
    // lands mid-job with seconds of margin.
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Bounded retry instead of a single blind request: this test also runs
    // in the merge-gating lane at full parallelism, where a starved
    // orchestrator could let one request start the job itself. Every ping
    // either coalesces with an in-flight job (emitting the event) or starts
    // a job for the next ping to coalesce with — the contract under test is
    // coalesce-not-replace, whichever job is in flight.
    let pinger_handle = fixture.sequencer().rollup_manager_handle.clone();
    let pinger = tokio::spawn(async move {
        loop {
            pinger_handle.build_block();
            tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        }
    });
    let coalesced = fixture
        .expect_event()
        .label("BuildBlockCoalesced with the in-flight timer job")
        .where_event(|e| matches!(e, ChainOrchestratorEvent::BuildBlockCoalesced))
        .await;
    pinger.abort();
    coalesced?;

    // That slot still produces its block. The pinger's waiter above drained
    // events on the way, possibly including the first BlockSequenced, so
    // accept any height rather than pinning block 1 (which could hang 30s
    // when the coalesce lands on a later ping).
    fixture
        .expect_event()
        .where_event(|e| matches!(e, ChainOrchestratorEvent::BlockSequenced(_)))
        .await?;

    Ok(())
}

/// The head-move invariant: importing a chain must cancel an in-flight
/// payload building job — its attributes were fixed against the pre-import
/// head, and finalizing it later would reorg the imported block back out
/// (issue #38 review). The cancellation is observable as
/// `PayloadBuildingJobCancelled`.
#[allow(clippy::large_stack_frames)]
#[tokio::test]
async fn test_chain_import_cancels_inflight_payload_job() -> eyre::Result<()> {
    reth_tracing::init_test_tracing();

    // Node under test: a long payload building job will be in flight. The
    // duration must outlast the import below (the cancelled job never
    // completes — FIFO command ordering means milliseconds) with generous
    // margin for loaded runners, while staying well inside the engine's
    // ~12s payload-job deadline, which the follow-up build at the end
    // shares.
    let mut node_a = TestFixture::builder()
        .sequencer()
        .with_memory_db()
        .payload_building_duration(3000)
        .allow_empty_blocks(true)
        .build()
        .await?;
    // Same genesis; provides a valid block 1 to import.
    let mut node_b = TestFixture::builder()
        .sequencer()
        .with_memory_db()
        .allow_empty_blocks(true)
        .build()
        .await?;

    node_a.l1().sync().await?;
    node_b.l1().sync().await?;

    // Build B's block BEFORE starting A's job: it plays no part in A's
    // in-flight job, and building it after would burn up to ~2.5s of the 3s
    // job budget on a loaded runner before the import even lands.
    let block = node_b.build_block().expect_block_number(1).build_and_await_block().await?;

    // Start a long build on A, then import B's block 1 while it is in
    // flight. No sleep is needed: the command channel is FIFO, so the import
    // is processed after the build command.
    node_a.sequencer().rollup_manager_handle.build_block();

    // Prove the job slot is actually occupied — and drain any buffered
    // start-failure event — before importing: a second request must
    // coalesce with the in-flight job.
    node_a.sequencer().rollup_manager_handle.build_block();
    node_a
        .expect_event()
        .timeout(std::time::Duration::from_secs(2))
        .label("BuildBlockCoalesced before the import")
        .where_event(|e| matches!(e, ChainOrchestratorEvent::BuildBlockCoalesced))
        .await?;

    node_a
        .sequencer()
        .rollup_manager_handle
        .import_block(scroll_network::NewBlockWithPeer {
            peer_id: Default::default(),
            block,
            signature: Signature::new(Default::default(), Default::default(), false),
        })
        .await?
        .map_err(|e| eyre::eyre!("import failed: {e}"))?;

    // The import moved the head and must have cancelled the in-flight job.
    // Tightly bounded: this test cannot pre-build a block (node_a must stay
    // at genesis for the import to be a clean extension), so a loose wait
    // could be satisfied by a finalization-time emission from the 3s job
    // instead of the import's prompt cancellation.
    node_a
        .expect_event()
        .timeout(std::time::Duration::from_secs(2))
        .label("PayloadBuildingJobCancelled promptly after chain import")
        .where_event(|e| matches!(e, ChainOrchestratorEvent::PayloadBuildingJobCancelled))
        .await?;

    // Vacuous-pass guard: the window must OUTLAST the remaining payload
    // duration (an uncancelled 3s job sequences at ~t+3.0s; a 2s window
    // would close before it and could never fail).
    let phantom = node_a
        .expect_event()
        .timeout(std::time::Duration::from_secs(5))
        .label("no BlockSequenced after the import-cancelled job")
        .where_event(|e| matches!(e, ChainOrchestratorEvent::BlockSequenced(_)))
        .await;
    eyre::ensure!(
        phantom.is_err(),
        "the import-cancelled job still sequenced a block: {phantom:?}"
    );

    // A follow-up build proceeds cleanly on top of the imported block.
    node_a.build_block().expect_block_number(2).build_and_await_block().await?;

    Ok(())
}

/// An administrative FCS head update must cancel an in-flight payload
/// building job (its parent may no longer be the head) and emit
/// `PayloadBuildingJobCancelled` — otherwise finalizing the stale job would
/// silently undo the update (issue #38 review).
#[allow(clippy::large_stack_frames)]
#[tokio::test]
async fn test_update_fcs_head_cancels_inflight_payload_job() -> eyre::Result<()> {
    reth_tracing::init_test_tracing();

    let mut fixture = TestFixture::builder()
        .sequencer()
        .with_memory_db()
        // Long enough to be mid-flight at the head update under load (FIFO
        // ordering means milliseconds), short enough that the follow-up
        // build's payload survives the engine's ~12s payload-job deadline.
        .payload_building_duration(3000)
        .allow_empty_blocks(true)
        .build()
        .await?;

    fixture.l1().sync().await?;

    // NOTE: get_block takes a NODE INDEX and returns that node's latest
    // block — this is genesis only because nothing has been built yet.
    let genesis = fixture.get_block(0).await?;
    eyre::ensure!(genesis.header.number == 0, "expected genesis to still be the head");
    let genesis_info = BlockInfo { number: genesis.header.number, hash: genesis.header.hash };

    // Advance the head to block 1 so the administrative update below
    // genuinely MOVES the head (updating a genesis head to genesis would
    // exercise nothing).
    fixture.build_block().expect_block_number(1).build_and_await_block().await?;

    // Start a long build, then move the head back to genesis while it is in
    // flight. No sleep is needed: the command channel is FIFO, so the head
    // update is processed after the build command.
    fixture.sequencer().rollup_manager_handle.build_block();

    // Prove the job slot is actually occupied — and drain any buffered
    // start-failure emission — before the operation under test: a second
    // request must coalesce with the in-flight job.
    fixture.sequencer().rollup_manager_handle.build_block();
    fixture
        .expect_event()
        .timeout(std::time::Duration::from_secs(2))
        .label("BuildBlockCoalesced occupancy probe")
        .where_event(|e| matches!(e, ChainOrchestratorEvent::BuildBlockCoalesced))
        .await?;
    fixture
        .sequencer()
        .rollup_manager_handle
        .update_fcs_head(genesis_info)
        .await
        .expect("update_fcs_head reply channel dropped")
        .map_err(|refusal| eyre::eyre!("head update refused: {refusal}"))?;

    // The head update must have cancelled the in-flight job.
    // Bounded like the chain-import test: a post-finalization emission from
    // the 3s job must not be able to satisfy this wait.
    fixture
        .expect_event()
        .timeout(std::time::Duration::from_secs(2))
        .label("PayloadBuildingJobCancelled after UpdateFcsHead")
        .where_event(|e| matches!(e, ChainOrchestratorEvent::PayloadBuildingJobCancelled))
        .await?;

    // The event alone is not the cancellation (mirrors the sibling
    // cancellation tests): a regression that emits it but leaves the 3s job
    // in the slot would still sequence. Nothing may sequence within the
    // job's remaining lifetime, and the head must sit at the updated target.
    let phantom = fixture
        .expect_event()
        .timeout(std::time::Duration::from_secs(5))
        .label("no BlockSequenced after the cancelled job")
        .where_event(|e| matches!(e, ChainOrchestratorEvent::BlockSequenced(_)))
        .await;
    eyre::ensure!(phantom.is_err(), "the cancelled job still sequenced a block: {phantom:?}");
    eyre::ensure!(
        fixture.get_block(0).await?.header.number == 0,
        "unexpected head after the administrative head update"
    );

    // A follow-up build proceeds cleanly from the updated head.
    fixture.build_block().expect_block_number(1).build_and_await_block().await?;

    Ok(())
}

/// Disabling automatic sequencing must cancel an in-flight payload building
/// job through the observable path (`PayloadBuildingJobCancelled`), not clear
/// it silently (issue #38 review).
#[allow(clippy::large_stack_frames)]
#[tokio::test]
async fn test_disable_sequencing_cancels_inflight_payload_job() -> eyre::Result<()> {
    reth_tracing::init_test_tracing();

    let mut fixture = TestFixture::builder()
        .sequencer()
        .with_memory_db()
        .payload_building_duration(3000)
        .allow_empty_blocks(true)
        .build()
        .await?;

    fixture.l1().sync().await?;

    // Prove the sequencer actually builds before relying on a fire-and-forget
    // command: without this the test could pass via the start-failure
    // cancellation event with no job ever in flight.
    fixture.build_block().expect_block_number(1).build_and_await_block().await?;

    // Start a long build, then disable sequencing while it is in flight (the
    // command channel is FIFO, so the disable is processed after the build
    // command).
    fixture.sequencer().rollup_manager_handle.build_block();

    // Prove the job slot is actually occupied — and drain any buffered
    // start-failure emission — before the operation under test: a second
    // request must coalesce with the in-flight job.
    fixture.sequencer().rollup_manager_handle.build_block();
    fixture
        .expect_event()
        .timeout(std::time::Duration::from_secs(2))
        .label("BuildBlockCoalesced occupancy probe")
        .where_event(|e| matches!(e, ChainOrchestratorEvent::BuildBlockCoalesced))
        .await?;
    eyre::ensure!(
        fixture.sequencer().rollup_manager_handle.disable_automatic_sequencing().await?,
        "disable_automatic_sequencing should report success"
    );

    // Bounded like the sibling tests: only a prompt cancellation counts.
    fixture
        .expect_event()
        .timeout(std::time::Duration::from_secs(2))
        .label("PayloadBuildingJobCancelled after disabling sequencing")
        .where_event(|e| matches!(e, ChainOrchestratorEvent::PayloadBuildingJobCancelled))
        .await?;

    // The event alone is not the cancellation: a regression that emits it
    // but leaves the 3s job in the slot would still sequence. Nothing may
    // sequence within the job's remaining lifetime, and the head must still
    // sit at the pre-built block 1.
    let phantom = fixture
        .expect_event()
        .timeout(std::time::Duration::from_secs(5))
        .label("no BlockSequenced after the cancelled job")
        .where_event(|e| matches!(e, ChainOrchestratorEvent::BlockSequenced(_)))
        .await;
    eyre::ensure!(phantom.is_err(), "the cancelled job still sequenced a block: {phantom:?}");
    eyre::ensure!(
        fixture.get_block(0).await?.header.number == 1,
        "unexpected head after the cancelled job"
    );

    Ok(())
}

/// An administrative L1 unwind closes the sequencer gate for the whole
/// re-scan, so it must cancel an in-flight payload building job observably
/// (issue #38 review).
#[allow(clippy::large_stack_frames)]
#[tokio::test]
async fn test_revert_to_l1_block_cancels_inflight_payload_job() -> eyre::Result<()> {
    reth_tracing::init_test_tracing();

    let mut fixture = TestFixture::builder()
        .sequencer()
        .with_memory_db()
        .payload_building_duration(3000)
        .allow_empty_blocks(true)
        .build()
        .await?;

    fixture.l1().sync().await?;

    // Prove the sequencer actually builds before relying on a fire-and-forget
    // command: without this the test could pass via the start-failure
    // cancellation event with no job ever in flight.
    fixture.build_block().expect_block_number(1).build_and_await_block().await?;

    // Start a long build, then revert the L1 view while it is in flight (the
    // command channel is FIFO, so the revert is processed after the build
    // command). The `?` proves the revert command was fully processed — a
    // dropped reply channel would mean the handler bailed early; the
    // cancellation itself happens before any fallible unwind work, so the
    // event does not depend on the unwind's outcome.
    fixture.sequencer().rollup_manager_handle.build_block();

    // Prove the job slot is actually occupied — and drain any buffered
    // start-failure emission — before the operation under test: a second
    // request must coalesce with the in-flight job.
    fixture.sequencer().rollup_manager_handle.build_block();
    fixture
        .expect_event()
        .timeout(std::time::Duration::from_secs(2))
        .label("BuildBlockCoalesced occupancy probe")
        .where_event(|e| matches!(e, ChainOrchestratorEvent::BuildBlockCoalesced))
        .await?;
    eyre::ensure!(
        fixture.sequencer().rollup_manager_handle.revert_to_l1_block(0).await?,
        "administrative unwind was refused"
    );

    // Bounded like the sibling tests: only a prompt cancellation counts.
    fixture
        .expect_event()
        .timeout(std::time::Duration::from_secs(2))
        .label("PayloadBuildingJobCancelled after administrative L1 unwind")
        .where_event(|e| matches!(e, ChainOrchestratorEvent::PayloadBuildingJobCancelled))
        .await?;

    // The event alone is not the cancellation: a regression that emits it
    // but leaves the 3s job in the slot would still sequence. Nothing may
    // sequence within the job's remaining lifetime, and the head must still
    // sit at the pre-built block 1.
    let phantom = fixture
        .expect_event()
        .timeout(std::time::Duration::from_secs(5))
        .label("no BlockSequenced after the cancelled job")
        .where_event(|e| matches!(e, ChainOrchestratorEvent::BlockSequenced(_)))
        .await;
    eyre::ensure!(phantom.is_err(), "the cancelled job still sequenced a block: {phantom:?}");
    eyre::ensure!(
        fixture.get_block(0).await?.header.number == 1,
        "unexpected head after the cancelled job"
    );

    Ok(())
}

/// A build with an empty payload and empty blocks disabled is skipped, and
/// the skip event carries the head it sat on — the identity the remote block
/// source's outcome attribution rests on (issue #38 review).
#[allow(clippy::large_stack_frames)]
#[tokio::test]
async fn test_block_building_skipped_carries_head_number() -> eyre::Result<()> {
    reth_tracing::init_test_tracing();

    let mut fixture = TestFixture::builder()
        .sequencer()
        .with_memory_db()
        .allow_empty_blocks(false)
        .build()
        .await?;

    fixture.l1().sync().await?;

    // Give block 1 real content (an L1 message) so it is BUILT even with
    // empty blocks disallowed: asserting the skip identity at head 0 could
    // not distinguish a carried head from u64::default().
    fixture
        .l1()
        .add_message()
        .queue_index(0)
        .sender(Address::random())
        .value(1)
        .at_block(1)
        .send()
        .await?;
    fixture.expect_event().l1_message_committed().await?;
    fixture.l1().new_block(1).await?;
    fixture.expect_event().new_l1_block().await?;
    fixture.build_block().expect_block_number(1).build_and_await_block().await?;

    // No new messages: the next build is empty and skipped at head 1.
    fixture.sequencer().rollup_manager_handle.build_block();
    fixture
        .expect_event()
        .label("BlockBuildingSkipped at head 1")
        .where_event(|e| {
            matches!(
                e,
                ChainOrchestratorEvent::BlockBuildingSkipped { head_block_number } if *head_block_number == 1
            )
        })
        .await?;

    Ok(())
}

/// Waits for n events to be emitted.
///
/// Bounded: panics with a diagnosis after 60s instead of hanging the test
/// binary forever (a hung test is indistinguishable from a slow one in CI and
/// burns the whole job's timeout — issue #38).
async fn wait_n_events(
    label: &str,
    events: &mut EventStream<ChainOrchestratorEvent>,
    mut matches: impl FnMut(ChainOrchestratorEvent) -> bool,
    mut n: u64,
) {
    assert!(n > 0, "wait_n_events requires n > 0");
    let total = n;
    tokio::time::timeout(tokio::time::Duration::from_secs(60), async {
        while let Some(event) = events.next().await {
            if matches(event.clone()) {
                n -= 1;
            }
            if n == 0 {
                break
            }
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!("[{label}] Timeout (60s) waiting for {total} matching events ({n} still missing)")
    });
    // The stream ending early falls out of the while-let without the timeout
    // firing; that must not pass silently either.
    assert_eq!(n, 0, "[{label}] event stream ended with {n}/{total} matching events still missing");
}
