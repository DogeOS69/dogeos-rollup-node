use eyre::Result;
use std::sync::{atomic::AtomicBool, Arc};
use tests::*;

/// One-way Testnet crossover: sequencing transitions from `l2geth` to the rollup node (Reth) once
/// and never returns.
///
/// This models the approved Tsuki Testnet contract: a lagging Reth node syncs from the last geth
/// sequencer, geth sequencing is frozen, Reth is proven to have reached geth's frozen final head,
/// and only then does Reth begin sequencing. There is no geth rollback or recovery path — geth is
/// retired after the cutover.
#[tokio::test]
async fn docker_test_migrate_sequencer() -> Result<()> {
    reth_tracing::init_test_tracing();

    tracing::info!("=== STARTING docker_test_migrate_sequencer (one-way geth -> Reth) ===");
    let env = DockerComposeEnv::new("docker_test_migrate_sequencer").await?;

    let rn_sequencer = env.get_rn_sequencer_provider().await?;
    let rn_follower = env.get_rn_follower_provider().await?;
    let l2geth_sequencer = env.get_l2geth_sequencer_provider().await?;
    let l2geth_follower = env.get_l2geth_follower_provider().await?;

    let nodes = [&rn_sequencer, &rn_follower, &l2geth_sequencer, &l2geth_follower];

    // Connect all nodes so the rollup nodes can sync from the geth sequencer during the crossover
    // and follow the Reth sequencer afterwards.
    // topology:
    //  l2geth_follower -> l2geth_sequencer
    //  l2geth_follower -> rn_sequencer
    //  rn_follower -> l2geth_sequencer
    //  rn_follower -> rn_sequencer
    //  rn_sequencer -> l2geth_sequencer
    utils::admin_add_peer(&l2geth_follower, &env.l2geth_sequencer_enode()?).await?;
    utils::admin_add_peer(&l2geth_follower, &env.rn_sequencer_enode()?).await?;
    utils::admin_add_peer(&rn_follower, &env.l2geth_sequencer_enode()?).await?;
    utils::admin_add_peer(&rn_follower, &env.rn_sequencer_enode()?).await?;
    utils::admin_add_peer(&rn_sequencer, &env.l2geth_sequencer_enode()?).await?;

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

    // Phase 1: geth is the sequencer. Ensure Reth is a passive follower during this phase.
    utils::disable_automatic_sequencing(&rn_sequencer).await?;
    utils::miner_start(&l2geth_sequencer).await?;
    utils::wait_for_block(&nodes, 20).await?;

    // Phase 2: freeze geth sequencing at its final head.
    tracing::info!("Freezing geth sequencing");
    utils::miner_stop(&l2geth_sequencer).await?;
    let frozen_final_block = l2geth_sequencer.get_block_number().await?;

    // Phase 3: prove the lagging Reth reached geth's frozen final head/hash before it sequences.
    utils::wait_for_block(&nodes, frozen_final_block).await?;
    utils::assert_blocks_match(&nodes, frozen_final_block).await?;
    tracing::info!("✅ Reth reached geth's frozen final block {}", frozen_final_block);

    // Phase 4: hand sequencing to Reth. Sequencing never returns to geth.
    tracing::info!("Enabling sequencing on the Reth sequencer");
    utils::enable_automatic_sequencing(&rn_sequencer).await?;
    let target_block = frozen_final_block + 20;
    utils::wait_for_block(&nodes, target_block).await?;

    let latest_block = rn_sequencer.get_block_number().await?;
    utils::wait_for_block(&nodes, latest_block).await?;
    utils::assert_blocks_match(&nodes, latest_block).await?;
    tracing::info!("✅ All nodes agree on the Reth-sequenced chain at block {}", latest_block);

    utils::stop_continuous_tx_sender(stop.clone(), tx_sender).await?;
    utils::stop_continuous_l1_message_sender(stop, l1_message_sender).await?;

    // Make sure the L1 message queue is processed on all l2geth nodes.
    let q = utils::get_l1_message_index_at_finalized().await?;
    utils::wait_for_l1_message_queue_index_reached(&[&l2geth_sequencer, &l2geth_follower], q)
        .await?;

    Ok(())
}
