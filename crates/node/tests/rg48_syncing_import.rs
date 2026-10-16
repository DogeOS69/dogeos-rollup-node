//! RG-48 and RG-49: what the rollup node does when the Engine answers a forkchoice update with
//! `SYNCING`.
//!
//! Both rows share one mechanism: `Engine::update_fcs` commits the local forkchoice mirror on a
//! `SYNCING` verdict, while `import_chain` persists the database head marker and the L1-message
//! mappings only on `VALID`. These tests drive the real in-process Reth Engine and observe the
//! resulting state. They do not mock any Engine verdict.
//!
//! Every test passes today and documents behaviour that is not fixed (the open fix PR
//! rollup-node#49 covers only the sequenced-block case of RG-49). When a fix lands, change the
//! matching assertions instead of deleting the test.
//!
//! Natural-vs-arranged boundary, stated explicitly per test:
//! - The Engine verdicts observed here are the real Reth engine-tree answers.
//! - The trigger in every test is the production `import_block` command (the entry point used by
//!   `RemoteBlockSourceAddOn`), fed a real sequencer block whose parent the receiving Engine has
//!   not seen. The gap itself is arranged by the test (gossip is paused on the sequencer).
//!
//! Run these tests with nextest, as CI does (`cargo nextest run -E 'kind(test) and not
//! test(docker)'`), or with `--test-threads=1`. `TestFixture` names each node's data directory
//! after the process id, the node index and the current time in nanoseconds, so two tests running
//! in the same process can pick the same directory; plain `cargo test` then fails on macOS with an
//! MDBX "Resource temporarily unavailable" error.

use alloy_primitives::{Address, Signature, U256};
use alloy_rpc_types_engine::PayloadStatusEnum;
use reth_network_api::PeerId;
use rollup_node::test_utils::{
    DatabaseOperations, EventAssertions, NetworkHelperProvider, TestFixture,
};
use rollup_node_chain_orchestrator::ChainOrchestratorEvent;
use scroll_db::{DatabaseReadOperations, L1MessageKey};
use scroll_network::NewBlockWithPeer;
use std::time::Duration;

/// Polls the Engine's latest block on `node` until it reaches `target` or `wait` elapses.
/// Returns the last observed latest block number.
async fn engine_latest_after(
    fixture: &TestFixture,
    node: usize,
    target: u64,
    wait: Duration,
) -> eyre::Result<u64> {
    let deadline = tokio::time::Instant::now() + wait;
    loop {
        let latest = fixture.get_block(node).await?.header.number;
        if latest >= target || tokio::time::Instant::now() >= deadline {
            return Ok(latest);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Imported-head confirmation, follower side.
///
/// A synced follower receives, through the production `import_block` command, a real sequencer
/// block whose parent it never saw. The real Engine answers `SYNCING` for both `newPayload` and
/// the forkchoice update. The ordinary `update_fcs` wrapper commits the mirror, `import_chain`
/// returns `Ok`, and the database head marker plus the L1-message mapping are skipped.
#[tokio::test]
async fn rg48_synced_follower_import_on_syncing_fcu_advances_mirror_but_not_database(
) -> eyre::Result<()> {
    tokio::time::timeout(Duration::from_secs(180), async {
        reth_tracing::init_test_tracing();

        let mut fixture = TestFixture::builder()
            .sequencer()
            .followers(1)
            .block_time(0)
            .allow_empty_blocks(true)
            .payload_building_duration(100)
            .build()
            .await?;

        fixture.l1().sync().await?;
        fixture.expect_event_on_all_nodes().l1_synced().await?;

        let sender = Address::random();
        let recipient = Address::random();

        // L1 message queue index 0, known to both nodes, included by the sequencer in block 1 and
        // received by the follower through ordinary gossip. This is the healthy baseline.
        for node_index in 0..=1 {
            fixture
                .l1()
                .for_node(node_index)
                .add_message()
                .queue_index(0)
                .gas_limit(21_000)
                .sender(sender)
                .to(recipient)
                .value(1u64)
                .at_block(1)
                .send()
                .await?;
            fixture.expect_event_on(node_index).l1_message_committed().await?;
            fixture.l1().for_node(node_index).new_block(1).await?;
            fixture.expect_event_on(node_index).new_l1_block().await?;
        }
        fixture
            .build_block()
            .expect_block_number(1)
            .expect_l1_message_count(1)
            .build_and_await_block()
            .await?;
        fixture.expect_event_on(1).chain_extended(1).await?;

        let follower_db = fixture.nodes[1]
            .as_ref()
            .expect("follower is running")
            .rollup_manager_handle
            .get_database_handle()
            .await?;
        assert_eq!(follower_db.get_l2_head_block_number().await?, 1);
        assert_eq!(fixture.get_status(1).await?.l2.fcs.head_block_info().number, 1);
        assert_eq!(fixture.get_block(1).await?.header.number, 1);
        println!("RG[baseline]: follower mirror=1 db_head=1 engine_latest=1 after gossip import of block 1");

        // Arrange the gap: pause sequencer gossip so the follower sees neither block 2 nor 3.
        fixture.nodes[0]
            .as_ref()
            .expect("sequencer is running")
            .rollup_manager_handle
            .set_gossip(false)
            .await?;

        let block_2 = fixture
            .build_block()
            .expect_block_number(2)
            .expect_l1_message_count(0)
            .build_and_await_block()
            .await?;

        // L1 message queue index 1, known to both nodes, included by the sequencer in block 3.
        for node_index in 0..=1 {
            fixture
                .l1()
                .for_node(node_index)
                .add_message()
                .queue_index(1)
                .gas_limit(21_000)
                .sender(sender)
                .to(recipient)
                .value(1u64)
                .at_block(2)
                .send()
                .await?;
            fixture.expect_event_on(node_index).l1_message_committed().await?;
            fixture.l1().for_node(node_index).new_block(2).await?;
            fixture.expect_event_on(node_index).new_l1_block().await?;
        }
        let block_3 = fixture
            .build_block()
            .expect_block_number(3)
            .expect_l1_message_count(1)
            .build_and_await_block()
            .await?;

        // The follower is still at 1 on every observable and still considers itself synced.
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(fixture.get_block(1).await?.header.number, 1, "gossip pause did not hold");
        assert_eq!(fixture.get_status(1).await?.l2.fcs.head_block_info().number, 1);
        assert!(fixture.get_status(1).await?.l2.status.is_synced());
        let q1_before = follower_db
            .get_n_l1_messages(Some(L1MessageKey::from_queue_index(1)), 1)
            .await?
            .pop()
            .expect("follower indexed queue index 1");
        assert_eq!(q1_before.l2_block_number, None);

        // Production entry point: the same command RemoteBlockSourceAddOn uses. Block 3 is a real
        // sequencer block; its parent (block 2) is unknown to the follower's Engine.
        let block_3_hash = block_3.hash_slow();
        let import = fixture.nodes[1]
            .as_ref()
            .expect("follower is running")
            .rollup_manager_handle
            .import_block(NewBlockWithPeer {
                peer_id: PeerId::default(),
                block: block_3.clone(),
                signature: Signature::new(U256::ZERO, U256::ZERO, false),
            })
            .await?;

        // (1) No error is surfaced: the import is reported as success.
        let chain_import = import.expect("import_chain must not return an error on SYNCING");
        let fcu_status = chain_import.result.payload_status.status.clone();
        println!("RG[import]: import_chain returned Ok; real Engine FCU status = {fcu_status:?}");
        assert!(
            matches!(fcu_status, PayloadStatusEnum::Syncing),
            "expected the real Engine to answer SYNCING for a head whose parent it lacks, got {fcu_status:?}"
        );
        assert!(!chain_import.result.is_valid());

        // (2) The local forkchoice mirror has advanced to the unconfirmed head.
        let status = fixture.get_status(1).await?;
        assert_eq!(status.l2.fcs.head_block_info().number, 3);
        assert_eq!(status.l2.fcs.head_block_info().hash, block_3_hash);
        assert!(status.l2.status.is_synced(), "sync state stays Synced");

        // (3) The persisted head marker and the L1-message mapping did not advance.
        assert_eq!(follower_db.get_l2_head_block_number().await?, 1);
        let q1_after = follower_db
            .get_n_l1_messages(Some(L1MessageKey::from_queue_index(1)), 1)
            .await?
            .pop()
            .expect("follower retained queue index 1");
        assert_eq!(q1_after.l2_block_number, None);

        // (4) The Engine's own view. Immediately after the import the Engine is still at 1. Reth
        // reacts to a missing FCU head by requesting the missing ancestors from its peers, so we
        // also record whether it catches up on its own.
        let engine_now = fixture.get_block(1).await?.header.number;
        println!(
            "RG[divergence]: mirror_head=3 db_head=1 q1_mapping=None engine_latest={engine_now} sync_state=Synced error_surfaced=false"
        );
        let engine_later = engine_latest_after(&fixture, 1, 3, Duration::from_secs(60)).await?;
        println!("RG[engine-self-heal]: engine_latest after <=60s = {engine_later} (3 means Reth downloaded block 2 from its peer and canonicalized the FCU target on its own)");
        assert_eq!(
            engine_later, 3,
            "Reth downloads block 2 from its peer and adopts the forkchoice target on its own"
        );
        // Whatever the Engine did, the rollup-node database is unchanged.
        assert_eq!(follower_db.get_l2_head_block_number().await?, 1);
        assert_eq!(
            follower_db
                .get_n_l1_messages(Some(L1MessageKey::from_queue_index(1)), 1)
                .await?
                .pop()
                .expect("follower retained queue index 1")
                .l2_block_number,
            None
        );

        // Recovery attempt through the normal gossip path: resume gossip and build block 4.
        fixture.nodes[0]
            .as_ref()
            .expect("sequencer is running")
            .rollup_manager_handle
            .set_gossip(true)
            .await?;
        let block_4 = fixture
            .build_block()
            .expect_block_number(4)
            .expect_l1_message_count(0)
            .build_and_await_block()
            .await?;
        let gossip_status = fixture
            .expect_event_on(1)
            .extract(|e| {
                if let ChainOrchestratorEvent::ChainExtended(ci) = e {
                    (ci.chain.last().map(|b| b.header.number) == Some(4))
                        .then(|| ci.result.payload_status.status.clone())
                } else {
                    None
                }
            })
            .await?
            .pop()
            .expect("follower handled block 4");
        println!("RG[gossip-after]: follower import of block 4 via gossip -> FCU status {gossip_status:?}");

        let db_head_after_4 = follower_db.get_l2_head_block_number().await?;
        let q1_after_4 = follower_db
            .get_n_l1_messages(Some(L1MessageKey::from_queue_index(1)), 1)
            .await?
            .pop()
            .expect("follower retained queue index 1")
            .l2_block_number;
        let mirror_after_4 = fixture.get_status(1).await?.l2.fcs.head_block_info().number;
        let engine_after_4 = fixture.get_block(1).await?.header.number;
        println!(
            "RG[after-block-4]: mirror_head={mirror_after_4} db_head={db_head_after_4} engine_latest={engine_after_4} q1_mapping={q1_after_4:?}"
        );
        assert_eq!(mirror_after_4, 4);
        // By now Reth has fetched block 2 from its peer on its own, so block 4 connects and is
        // VALID. The head marker catches up as a number, but the mapping skipped at block 3 is not
        // rewritten by a later import: the mapping gap stays until the node restarts (below), on a
        // node that reports healthy.
        assert!(
            matches!(gossip_status, PayloadStatusEnum::Valid),
            "expected block 4 to be VALID once the Engine caught up, got {gossip_status:?}"
        );
        assert_eq!(db_head_after_4, 4);
        assert_eq!(engine_after_4, 4);
        assert_eq!(fixture.get_block(1).await?.header.hash, block_4.hash_slow());
        assert_eq!(q1_after_4, None, "later VALID import does not repair the skipped mapping");
        drop(follower_db);

        // Restart: startup walks the persisted head down to a block the Engine has and
        // consolidation re-derives the mappings from the Engine chain.
        fixture.shutdown_node(1).await?;
        fixture.start_node(1).await?;
        fixture.l1().for_node(1).sync().await?;
        let consolidated = fixture.expect_event_on(1).chain_consolidated().await?;
        let restarted_db = fixture.nodes[1]
            .as_ref()
            .expect("follower restarted")
            .rollup_manager_handle
            .get_database_handle()
            .await?;
        let q1_restart = restarted_db
            .get_n_l1_messages(Some(L1MessageKey::from_queue_index(1)), 1)
            .await?
            .pop()
            .expect("follower retained queue index 1 after restart")
            .l2_block_number;
        let db_head_restart = restarted_db.get_l2_head_block_number().await?;
        let mirror_restart = fixture.get_status(1).await?.l2.fcs.head_block_info().number;
        let engine_restart = fixture.get_block(1).await?.header.number;
        println!(
            "RG[restart]: consolidated={consolidated:?} mirror_head={mirror_restart} db_head={db_head_restart} engine_latest={engine_restart} q1_mapping={q1_restart:?}"
        );
        assert_eq!(mirror_restart, db_head_restart);
        assert_eq!(engine_restart, db_head_restart);
        assert_eq!(engine_restart, 4, "the Engine kept block 4 across the restart");
        assert_eq!(q1_restart, Some(3), "restart consolidation repairs the skipped mapping");
        assert_eq!(block_2.header.number, 2);

        Ok::<_, eyre::Report>(())
    })
    .await
    .map_err(|_| eyre::eyre!("test timed out after 180 seconds"))??;

    Ok(())
}

/// Control for the follower test: the same two-block gap delivered through the ordinary gossip
/// path. `handle_block_from_peer` fetches the missing parent itself before calling
/// `import_chain`, so the chain connects to the Engine head, both verdicts are `VALID`, and the
/// database head marker and mapping advance together with the mirror. This is why the gossip path
/// cannot seed the divergence on a synced node: the arranged gap is closed before the FCU.
#[tokio::test]
async fn rg48_gossip_path_closes_gap_and_engine_confirms_head() -> eyre::Result<()> {
    tokio::time::timeout(Duration::from_secs(150), async {
        reth_tracing::init_test_tracing();

        let mut fixture = TestFixture::builder()
            .sequencer()
            .followers(1)
            .block_time(0)
            .allow_empty_blocks(true)
            .payload_building_duration(100)
            .build()
            .await?;

        fixture.l1().sync().await?;
        fixture.expect_event_on_all_nodes().l1_synced().await?;

        let sender = Address::random();
        let recipient = Address::random();
        for node_index in 0..=1 {
            fixture
                .l1()
                .for_node(node_index)
                .add_message()
                .queue_index(0)
                .gas_limit(21_000)
                .sender(sender)
                .to(recipient)
                .value(1u64)
                .at_block(1)
                .send()
                .await?;
            fixture.expect_event_on(node_index).l1_message_committed().await?;
            fixture.l1().for_node(node_index).new_block(1).await?;
            fixture.expect_event_on(node_index).new_l1_block().await?;
        }
        fixture
            .build_block()
            .expect_block_number(1)
            .expect_l1_message_count(1)
            .build_and_await_block()
            .await?;
        fixture.expect_event_on(1).chain_extended(1).await?;

        fixture.nodes[0]
            .as_ref()
            .expect("sequencer is running")
            .rollup_manager_handle
            .set_gossip(false)
            .await?;
        fixture
            .build_block()
            .expect_block_number(2)
            .expect_l1_message_count(0)
            .build_and_await_block()
            .await?;
        for node_index in 0..=1 {
            fixture
                .l1()
                .for_node(node_index)
                .add_message()
                .queue_index(1)
                .gas_limit(21_000)
                .sender(sender)
                .to(recipient)
                .value(1u64)
                .at_block(2)
                .send()
                .await?;
            fixture.expect_event_on(node_index).l1_message_committed().await?;
            fixture.l1().for_node(node_index).new_block(2).await?;
            fixture.expect_event_on(node_index).new_l1_block().await?;
        }
        let block_3 = fixture
            .build_block()
            .expect_block_number(3)
            .expect_l1_message_count(1)
            .build_and_await_block()
            .await?;
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(fixture.get_block(1).await?.header.number, 1, "gossip pause did not hold");
        assert_eq!(fixture.db_on(1).get_l2_head_block_number().await?, 1);

        // Same gap, ordinary path: the sequencer gossips block 3 to the follower.
        fixture.nodes[0]
            .as_ref()
            .expect("sequencer is running")
            .rollup_manager_handle
            .set_gossip(true)
            .await?;
        fixture
            .network()
            .announce_block(block_3.clone(), Signature::new(U256::from(1), U256::from(1), false))
            .await?;
        let gossip_status = fixture
            .expect_event_on(1)
            .extract(|e| {
                if let ChainOrchestratorEvent::ChainExtended(ci) = e {
                    (ci.chain.last().map(|b| b.header.number) == Some(3)).then(|| {
                        (ci.chain.len(), ci.result.payload_status.status.clone())
                    })
                } else {
                    None
                }
            })
            .await?
            .pop()
            .expect("follower handled block 3");
        println!(
            "RG[gossip-control]: follower imported a {}-block chain ending at 3 via gossip -> FCU status {:?}",
            gossip_status.0, gossip_status.1
        );
        assert_eq!(gossip_status.0, 2, "handle_block_from_peer fetched block 2 to close the gap");
        assert!(matches!(gossip_status.1, PayloadStatusEnum::Valid));

        let follower_db = fixture.nodes[1]
            .as_ref()
            .expect("follower is running")
            .rollup_manager_handle
            .get_database_handle()
            .await?;
        let q1 = follower_db
            .get_n_l1_messages(Some(L1MessageKey::from_queue_index(1)), 1)
            .await?
            .pop()
            .expect("follower retained queue index 1")
            .l2_block_number;
        let status = fixture.get_status(1).await?;
        println!(
            "RG[gossip-control]: mirror_head={} db_head={} engine_latest={} q1_mapping={q1:?}",
            status.l2.fcs.head_block_info().number,
            follower_db.get_l2_head_block_number().await?,
            fixture.get_block(1).await?.header.number
        );
        assert_eq!(status.l2.fcs.head_block_info().number, 3);
        assert_eq!(status.l2.fcs.head_block_info().hash, block_3.hash_slow());
        assert_eq!(follower_db.get_l2_head_block_number().await?, 3);
        assert_eq!(fixture.get_block(1).await?.header.number, 3);
        assert_eq!(q1, Some(3));

        Ok::<_, eyre::Report>(())
    })
    .await
    .map_err(|_| eyre::eyre!("test timed out after 150 seconds"))??;

    Ok(())
}

/// Sequencer head confirmation, shown from its only reachable side.
///
/// On the ordinary path a sequencer's forkchoice update for its own freshly built block is
/// `VALID`: Reth inserts every built payload into the engine tree (`InsertExecutedBlock`) when
/// `getPayload` resolves, before the payload is even returned, so `on_new_head` finds the block.
/// The first part of this test observes that ordinary path on a real in-process node.
///
/// CONTROLLED SETUP for the second part: the same production `import_block` command moves the
/// sequencer-capable node's mirror to a head its Engine has not adopted (real `SYNCING` verdict,
/// gap arranged by the test). The next slot then shows what an unconfirmed mirror head does to
/// sequencing: no block is sequenced or skipped within 8 seconds while status still reports
/// Synced. The sequencer does not sign anything on the unconfirmed head; it stalls. The stall is
/// inferred from the absence of a `BlockSequenced` or `BlockBuildingSkipped` event: the cause
/// (`build_payload` gets `SYNCING` with no payload id, so the slot fails with `MissingPayloadId`)
/// is only logged and is not asserted here.
///
/// Recovery after a restart is not part of this test: after the restart the remote source switches
/// to node 0's live RPC and imports and builds in the background, so the node's state keeps
/// moving and no snapshot of it is stable.
#[tokio::test]
async fn rg49_sequencer_ordinary_fcu_is_valid_and_unconfirmed_mirror_head_stalls_building(
) -> eyre::Result<()> {
    tokio::time::timeout(Duration::from_secs(180), async {
        reth_tracing::init_test_tracing();

        // Keep the remote-source add-on inert: connections to the "remote" are accepted and
        // dropped, which the add-on logs and retries at poll cadence without importing anything.
        // This gives us a second, sequencer-capable node that is not peered with node 0.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let dead_port = listener.local_addr()?.port();
        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((inbound, _)) => drop(inbound),
                    Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
                }
            }
        });

        let mut fixture = TestFixture::builder()
            .sequencer()
            .remote_source_node()
            .remote_source_url(format!("http://127.0.0.1:{dead_port}").parse()?)
            .block_time(0)
            .allow_empty_blocks(true)
            .payload_building_duration(100)
            .build()
            .await?;

        fixture.l1().sync().await?;
        fixture.expect_event_on_all_nodes().l1_synced().await?;

        // Ordinary path on node 1 (sequencer-capable): build two blocks and check that mirror,
        // database head and Engine head agree after each. The FCU inside
        // finalize_payload_building is not inspected by the sequencer; the agreement below is what
        // a VALID verdict looks like from the outside.
        let mut node_1_blocks = Vec::new();
        for n in 1..=2u64 {
            fixture.nodes[1].as_ref().expect("node 1 running").rollup_manager_handle.build_block();
            fixture.expect_event_on(1).block_sequenced(n).await?;
            // The head marker is written by the signer event that follows BlockSequenced.
            let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
            while fixture.db_on(1).get_l2_head_block_number().await? < n {
                assert!(tokio::time::Instant::now() < deadline, "db head did not reach {n}");
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            let status = fixture.get_status(1).await?;
            let engine = fixture.get_block(1).await?;
            assert_eq!(status.l2.fcs.head_block_info().number, n);
            assert_eq!(engine.header.number, n);
            assert_eq!(status.l2.fcs.head_block_info().hash, engine.header.hash);
            assert_eq!(fixture.db_on(1).get_l2_head_block_number().await?, n);
            node_1_blocks.push((engine.header.hash, engine.header.timestamp));
            println!("RG[sequencer-ordinary]: node1 built block {n}: mirror=db=engine={n}, hash agrees");
        }

        // Node 0 builds its own chain 1..3; node 1 never sees it (no p2p link, add-on inert).
        // Both nodes share one config and blocks 1 and 2 are empty, so two blocks built in the same
        // second are identical (timestamps are whole seconds). Wait until the clock is past node
        // 1's newest block timestamp so node 0's chain differs from its first block on; otherwise
        // node 1 already has block 3's parent and the import is VALID.
        let last_node_1_timestamp = node_1_blocks.last().expect("node 1 built blocks").1;
        while std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_secs() <=
            last_node_1_timestamp
        {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let mut foreign_block_3 = None;
        for n in 1..=3u64 {
            let block = fixture.build_block().expect_block_number(n).build_and_await_block().await?;
            if n == 1 {
                assert_ne!(
                    block.hash_slow(),
                    node_1_blocks[0].0,
                    "node 0's block 1 must differ from node 1's block 1"
                );
            }
            foreign_block_3 = Some(block);
        }
        let foreign_block_3 = foreign_block_3.expect("node 0 built block 3");
        assert_eq!(foreign_block_3.header.number, 3);
        let foreign_hash = foreign_block_3.hash_slow();

        // CONTROLLED: move node 1's mirror to a head its Engine has not adopted.
        let import = fixture.nodes[1]
            .as_ref()
            .expect("node 1 running")
            .rollup_manager_handle
            .import_block(NewBlockWithPeer {
                peer_id: PeerId::default(),
                block: foreign_block_3,
                signature: Signature::new(U256::ZERO, U256::ZERO, false),
            })
            .await?
            .expect("import_chain returns Ok on SYNCING");
        let fcu_status = import.result.payload_status.status.clone();
        println!("RG[controlled-import]: node1 import of foreign block 3 -> real Engine FCU status {fcu_status:?}");
        assert!(matches!(fcu_status, PayloadStatusEnum::Syncing));
        let status = fixture.get_status(1).await?;
        assert_eq!(status.l2.fcs.head_block_info().number, 3);
        assert_eq!(status.l2.fcs.head_block_info().hash, foreign_hash);
        assert!(status.is_synced(), "status reports fully synced with an unconfirmed head");
        assert_eq!(fixture.get_block(1).await?.header.number, 2, "Engine still at its own block 2");
        assert_eq!(fixture.db_on(1).get_l2_head_block_number().await?, 2);

        // Next slot: the sequencer asks the Engine to build on the mirror head.
        fixture.nodes[1].as_ref().expect("node 1 running").rollup_manager_handle.build_block();
        let outcome = fixture
            .expect_event_on(1)
            .timeout(Duration::from_secs(8))
            .where_event(|e| {
                matches!(
                    e,
                    ChainOrchestratorEvent::BlockSequenced(_) |
                        ChainOrchestratorEvent::BlockBuildingSkipped
                )
            })
            .await;
        println!(
            "RG[stall]: build_block on unconfirmed mirror head -> sequencing outcome within 8s: {}",
            match &outcome {
                Ok(events) => format!("{events:?}"),
                Err(e) => format!("none ({e})"),
            }
        );
        assert!(outcome.is_err(), "no block may be sequenced on an unconfirmed head");
        let status = fixture.get_status(1).await?;
        assert_eq!(status.l2.fcs.head_block_info().number, 3, "mirror stays on the unconfirmed head");
        assert!(status.is_synced());
        assert_eq!(fixture.get_block(1).await?.header.number, 2);
        assert_eq!(fixture.db_on(1).get_l2_head_block_number().await?, 2);


        Ok::<_, eyre::Report>(())
    })
    .await
    .map_err(|_| eyre::eyre!("test timed out after 180 seconds"))??;

    Ok(())
}
