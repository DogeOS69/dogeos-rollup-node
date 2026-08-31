//! Integration tests for the `RemoteBlockSourceAddOn` feature.
//!
//! These tests verify that a node configured with `RemoteBlockSourceAddOn` can:
//! - Import blocks from a remote L2 node (the sequencer)
//! - Build new blocks on top of each imported block

use rollup_node::test_utils::{EventAssertions, TestFixture};

#[allow(clippy::large_stack_frames)]
#[tokio::test]
async fn test_remote_block_source() -> eyre::Result<()> {
    reth_tracing::init_test_tracing();

    let mut fixture = TestFixture::builder().sequencer().remote_source_node().build().await?;

    fixture.l1().sync().await?;

    // Sequencer produces blocks 1-5
    for i in 1..=5 {
        fixture.build_block().expect_block_number(i).build_and_await_block().await?;
        fixture.expect_event_on(1).block_sequenced(i + 1).await?;
    }

    Ok(())
}

/// Node launch must not depend on the remote block source being reachable.
///
/// Before the fix for issue #38 (defect 2), `RemoteBlockSourceAddOn::new()` probed
/// the remote during `launch_add_ons`, and a connection-refused error (which
/// alloy's retry layer does not retry) aborted the entire node — in the Docker
/// tests the remote-source container raced the sequencer container and died at
/// startup, never exposing its own RPC. The node must come up with the remote
/// down, then import and build once the remote appears.
#[allow(clippy::large_stack_frames)]
#[tokio::test]
async fn test_remote_source_node_launches_when_remote_unreachable() -> eyre::Result<()> {
    reth_tracing::init_test_tracing();

    // Reserve a port and free it again: connections to it are now refused.
    // (Small inherent race: another process could grab the port before the
    // proxy below binds it; acceptable for a test.)
    let placeholder = std::net::TcpListener::bind("127.0.0.1:0")?;
    let proxy_port = placeholder.local_addr()?.port();
    drop(placeholder);

    // Build sequencer + remote-source fixture with the remote URL pointed at
    // the dead port. Pre-fix this call failed inside launch_add_ons.
    let mut fixture = TestFixture::builder()
        .sequencer()
        .remote_source_node()
        .remote_source_url(format!("http://127.0.0.1:{proxy_port}").parse()?)
        .build()
        .await?;

    fixture.l1().sync().await?;

    // Sequencer produces blocks 1-2 while the remote-source add-on can only
    // log connection errors.
    for i in 1..=2 {
        fixture.build_block().expect_block_number(i).build_and_await_block().await?;
    }

    // Bring the "remote" up: forward the reserved port to the sequencer RPC.
    let sequencer_port =
        fixture.sequencer().node.rpc_url().port().expect("sequencer rpc url has a port");
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", proxy_port)).await?;
    tokio::spawn(async move {
        loop {
            let Ok((mut inbound, _)) = listener.accept().await else { break };
            let Ok(mut outbound) =
                tokio::net::TcpStream::connect(("127.0.0.1", sequencer_port)).await
            else {
                continue;
            };
            tokio::spawn(async move {
                let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
            });
        }
    });

    // Remote source recovers: imports blocks 1-2 and ends up building block 3
    // on top (same event pattern as test_remote_block_source).
    fixture.expect_event_on(1).block_sequenced(3).await?;

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
