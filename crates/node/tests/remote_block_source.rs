//! Integration tests for the `RemoteBlockSourceAddOn` feature.
//!
//! These tests verify that a node configured with `RemoteBlockSourceAddOn` can:
//! - Import blocks from a remote L2 node (the sequencer)
//! - Build new blocks on top of each imported block

use alloy_signer::Signer;
use alloy_signer_local::PrivateKeySigner;
use dogeos_chainspec::DOGEOS_DEV;
use reth_chainspec::EthChainSpec;
use rollup_node::test_utils::{EventAssertions, TestFixture};
use std::time::Duration;

#[allow(clippy::large_stack_frames)]
#[tokio::test]
async fn test_remote_block_source() -> eyre::Result<()> {
    reth_tracing::init_test_tracing();

    let mut fixture = TestFixture::builder().sequencer().remote_source_node().build().await?;

    fixture.l1().sync().await?;
    fixture.expect_event().l1_synced().await?;

    // Sequencer produces blocks 1-5
    for i in 1..=5 {
        fixture.build_block().expect_block_number(i).build_and_await_block().await?;
        fixture.expect_event_on(1).block_sequenced(i + 1).await?;
    }

    Ok(())
}

/// Test that the remote block source correctly determines its resume point on restart.
///
/// The remote source's local chain has blocks 1-3 (imported from sequencer) plus
/// block 4 (built locally). The sequencer goes on to produce blocks 4-6. On restart,
/// the highest-common-block walk must identify block 3 (locally-built block 4 diverges
/// from sequencer's block 4) and import only blocks 4-6.
///
/// If the detection were broken (e.g. always returning 0), the remote source would try
/// to re-import blocks 1-6, producing 6 `BlockSequenced` events before reaching blocks
/// 5, 6, 7. This test asserts exactly three events in the correct order, confirming
/// the resume point is block 3.
#[allow(clippy::large_stack_frames)]
#[tokio::test]
async fn test_remote_block_source_resumes_from_correct_head() -> eyre::Result<()> {
    reth_tracing::init_test_tracing();

    let mut fixture = TestFixture::builder().sequencer().remote_source_node().build().await?;

    fixture.l1().sync().await?;
    fixture.expect_event().l1_synced().await?;

    // Sequencer produces blocks 1-3; remote source imports each and builds on top.
    // After this phase the remote source local chain is: 1, 2, 3 (sequencer) + 4 (local).
    for i in 1..=3u64 {
        fixture.build_block().expect_block_number(i).build_and_await_block().await?;
        fixture.expect_event_on(1).block_sequenced(i + 1).await?;
    }

    // Shut down the remote source node (index 1).
    fixture.shutdown_node(1).await?;

    // Sequencer produces blocks 4-6 while the remote source is offline.
    for i in 4..=6u64 {
        fixture.build_block().expect_block_number(i).build_and_await_block().await?;
    }

    // Restart the remote source.
    // Expected detection: local_head=4, remote_head=6, min=4.
    //   Block 4: local hash (locally built) ≠ remote hash (sequencer's) → walk back.
    //   Block 3: local hash == remote hash → last_imported_block = 3.
    // The add-on should therefore import blocks 4, 5, 6 and build 5, 6, 7 on top.
    fixture.start_node(1).await?;

    // Synchronise L1 state on the restarted remote source node.
    fixture.l1().for_node(1).sync().await?;

    // Verify the remote source catches up with the 3 missed sequencer blocks.
    fixture.expect_event_on(1).block_sequenced(5).await?;
    fixture.expect_event_on(1).block_sequenced(6).await?;
    fixture.expect_event_on(1).block_sequenced(7).await?;

    Ok(())
}

/// A build rejected or cancelled by an authorization transition must not strand the remote source
/// in its completion wait; it must resume polling and import the next remote block.
#[allow(clippy::large_stack_frames)]
#[tokio::test]
async fn test_remote_block_source_resumes_after_build_cancellation() -> eyre::Result<()> {
    reth_tracing::init_test_tracing();

    let chain_spec = (*DOGEOS_DEV).clone();
    let signer = PrivateKeySigner::random().with_chain_id(Some(chain_spec.chain().id()));
    let signer_address = signer.address();
    let mut fixture = TestFixture::builder()
        .sequencer()
        .remote_source_node()
        .with_test(false)
        .with_consensus_system_contract(Some(signer_address))
        .with_signer(signer)
        .with_eth_scroll_bridge(false)
        .payload_building_duration(2_000)
        .build()
        .await?;

    fixture.l1().sync().await?;
    fixture.expect_event().l1_synced().await?;

    fixture.build_block().expect_block_number(1).build_and_await_block().await?;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if fixture.get_block(1).await?.header.number >= 1 {
                return Ok::<_, eyre::Report>(())
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??;

    // Depending on scheduling, phase one either rejects the queued BuildBlock or cancels the
    // accepted long-running payload job. Both outcomes must release the add-on's completion path.
    fixture.l1().for_node(1).signer_update(signer_address).await?;

    let remote_block = fixture.build_block().expect_block_number(2).build_and_await_block().await?;
    let remote_hash = remote_block.header.hash_slow();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let local = fixture.get_block(1).await?;
            if local.header.number >= 2 && local.header.hash_slow() == remote_hash {
                return Ok::<_, eyre::Report>(())
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??;

    Ok(())
}
