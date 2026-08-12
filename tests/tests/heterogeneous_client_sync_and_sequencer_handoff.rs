use eyre::Result;
use std::sync::{atomic::AtomicBool, Arc};
use tests::*;

/// Tests the one-way Testnet crossover of block production from `l2geth` to the rollup node (Reth).
///
/// This integration test validates cross-client block propagation between heterogeneous nodes
/// (l2geth and rollup-node) up to and including the single, one-way sequencer handoff. It models
/// the approved Tsuki Testnet contract: after the crossover, sequencing never returns to geth and
/// there is no geth rollback or recovery path. The test exercises:
///
/// 1. **Isolated Network Segments**: Initially runs l2geth nodes in isolation, verifying they can
///    produce and sync blocks independently
///    - Topology: `l2geth_follower -> l2geth_sequencer`
///    - l2geth_sequencer produces blocks, l2geth_follower syncs
///    - Rollup nodes remain disconnected at block 0
///
/// 2. **Cross-Client Synchronization**: Connects rollup nodes to the l2geth network, ensuring the
///    lagging Reth nodes can catch up to the current chain state
///    - Topology: `[rn_follower, rn_sequencer, l2geth_follower] -> l2geth_sequencer`
///    - All nodes connect to l2geth_sequencer as the single source of truth
///    - Rollup nodes sync from block 0 to current height
///
/// 3. **One-Way Sequencer Handoff**: Freezes l2geth sequencing at its final head, proves the Reth
///    nodes have reached that frozen head, and transitions block production to the rollup node.
///    Production never returns to l2geth.
///    - Topology remains: `[rn_follower, rn_sequencer, l2geth_follower] -> l2geth_sequencer`
///    - l2geth sequencing is frozen; all nodes converge on the frozen final head
///    - Block production switches from l2geth_sequencer to rn_sequencer for the remainder
///    - A rollup follower is restarted mid-test to verify Reth state recovery
///
/// The test validates that both client implementations maintain consensus through the cross-client
/// sync and the one-way sequencer handoff.
#[tokio::test]
async fn docker_test_heterogeneous_client_sync_and_sequencer_handoff() -> Result<()> {
    reth_tracing::init_test_tracing();

    tracing::info!("=== STARTING docker_test_heterogeneous_client_sync_and_sequencer_handoff ===");
    let env = DockerComposeEnv::new("docker_test_heterogeneous_client_sync_and_sequencer_handoff")
        .await?;

    let rn_sequencer = env.get_rn_sequencer_provider().await?;
    let rn_follower = env.get_rn_follower_provider().await?;
    let l2geth_sequencer = env.get_l2geth_sequencer_provider().await?;
    let l2geth_follower = env.get_l2geth_follower_provider().await?;

    let rn_nodes = [&rn_sequencer, &rn_follower];
    let l2geth_nodes = [&l2geth_sequencer, &l2geth_follower];
    let nodes = [&rn_sequencer, &rn_follower, &l2geth_sequencer, &l2geth_follower];

    // Connect only l2geth nodes first
    // l2geth_follower -> l2geth_sequencer
    utils::admin_add_peer(&l2geth_follower, &env.l2geth_sequencer_enode()?).await?;
    tracing::info!("✅ Connected l2geth follower to l2geth sequencer");

    // Enable block production on l2geth sequencer
    utils::miner_start(&l2geth_sequencer).await?;

    // Start single continuous transaction sender for entire test
    let stop = Arc::new(AtomicBool::new(false));
    let stop_clone = stop.clone();
    let rn_follower_clone = env.get_rn_follower_provider().await.unwrap();
    let l2geth_follower_clone = env.get_l2geth_follower_provider().await.unwrap();
    let tx_sender = tokio::spawn(async move {
        utils::run_continuous_tx_sender(stop_clone, &[&rn_follower_clone, &l2geth_follower_clone])
            .await
    });
    let stop_clone = stop.clone();
    let l1_message_sender =
        tokio::spawn(async move { utils::run_continuous_l1_message_sender(stop_clone).await });

    tracing::info!("🔄 Started continuous L1 message and L2 transaction sender for entire test");

    // Wait for at least 10 blocks to be produced
    let target_block = 10;
    utils::wait_for_block(&[&l2geth_sequencer], target_block).await?;
    utils::miner_stop(&l2geth_sequencer).await?;

    let latest_block = l2geth_sequencer.get_block_number().await?;

    // Wait for all l2geth nodes to reach the latest block
    utils::wait_for_block(&l2geth_nodes, latest_block).await?;
    utils::assert_blocks_match(&l2geth_nodes, latest_block).await?;
    tracing::info!("✅ All l2geth nodes reached block {}", latest_block);

    // Assert rollup nodes are still at block 0
    utils::assert_latest_block(&rn_nodes, 0).await?;

    // Connect rollup nodes to l2geth sequencer
    // topology:
    //  l2geth_follower -> l2geth_sequencer
    //  rn_follower -> l2geth_sequencer
    //  rn_sequencer -> l2geth_sequencer
    utils::admin_add_peer(&rn_follower, &env.l2geth_sequencer_enode()?).await?;
    utils::admin_add_peer(&rn_sequencer, &env.l2geth_sequencer_enode()?).await?;
    tracing::info!("✅ Connected rollup nodes to l2geth sequencer");

    // Continue block production on l2geth sequencer
    utils::miner_start(&l2geth_sequencer).await?;

    // Wait for all nodes to reach target block
    let target_block = latest_block + 10;
    utils::wait_for_block(&nodes, target_block).await?;

    // Freeze l2geth sequencing at its final head and prove the Reth nodes reached it.
    utils::miner_stop(&l2geth_sequencer).await?;
    let latest_block = l2geth_sequencer.get_block_number().await?;
    utils::wait_for_block(&nodes, latest_block).await?;
    utils::assert_blocks_match(&nodes, latest_block).await?;
    tracing::info!("✅ All nodes reached geth's frozen final block {}", latest_block);

    // One-way handoff: enable sequencing on the Reth sequencer. Production never returns to geth.
    tracing::info!("Enabling sequencing on RN sequencer");
    utils::enable_automatic_sequencing(&rn_sequencer).await?;
    let target_block = latest_block + 10;

    // restart RN follower to test it can recover its state after a restart
    tracing::info!("Restarting RN follower");
    utils::admin_remove_peer(&rn_follower, &env.l2geth_sequencer_enode()?).await?;
    let latest_block_before_restart = rn_follower.get_block_number().await?;
    let chain_status_before_restart = utils::rollup_node_status(&rn_follower).await?;
    env.restart_container(&rn_follower).await?;
    let rn_follower = env.get_rn_follower_provider().await?; // without this line rn_follower isn't always reachable after restart
    utils::assert_latest_block(&[&rn_follower], latest_block_before_restart).await?;
    let chain_status_after_restart = utils::rollup_node_status(&rn_follower).await?;
    assert!(
        chain_status_after_restart.l2 == chain_status_before_restart.l2,
        "L2 Chain status after restart does not match the one before restart {:?} != {:?}",
        chain_status_after_restart.l2,
        chain_status_before_restart.l2
    );
    utils::admin_add_peer(&rn_follower, &env.l2geth_sequencer_enode()?).await?;

    utils::wait_for_block(&nodes, target_block).await?;

    // Reth is now the sole sequencer; confirm the full heterogeneous network agrees on its chain.
    let latest_block = rn_sequencer.get_block_number().await?;
    utils::wait_for_block(&nodes, latest_block).await?;
    utils::assert_blocks_match(&nodes, latest_block).await?;
    tracing::info!("✅ All nodes agree on the Reth-sequenced chain at block {}", latest_block);

    utils::stop_continuous_tx_sender(stop.clone(), tx_sender).await?;
    utils::stop_continuous_l1_message_sender(stop, l1_message_sender).await?;

    // Make sure l1 message queue is processed on all l2geth nodes
    let q = utils::get_l1_message_index_at_finalized().await?;
    utils::wait_for_l1_message_queue_index_reached(&[&l2geth_sequencer, &l2geth_follower], q)
        .await?;

    Ok(())
}
