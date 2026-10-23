//! Materialized L1 RPC state -> production watcher -> running Reth signer adoption.
//!
//! The RPC fixture models `l1_interface`'s replay output: `SystemConfig` storage changes
//! with a new synthetic L1 head, and latest/safe/finalized coincide. It does not
//! execute `RotateSequencerSigner` proofs or replay. No consensus notifications are
//! injected, and all three nodes remain running throughout the handoff.
//!
//! The test asserts the intended behaviour and fails today, so it is `#[ignore]`d (RG-52);
//! run it with `--ignored`. A running watcher never re-reads the signer slot:
//! `handle_latest_block` stores the new L1 head before `handle_system_contract_update`
//! compares against it, so the standby never sequences block 4. Passing needs two watcher
//! changes: re-read the signer on every new L1 head, and keep that update out of the
//! watcher's notification-count check, which today counts it against the block's logs.
//! Either change alone still fails. Remove the `#[ignore]` once both are fixed. Until then,
//! followers pick up a rotation by exiting and restarting
//! (`--consensus.exit-on-signer-rotation`).

use alloy_primitives::{Address, B256, U256};
use alloy_rpc_types_eth::Block;
use alloy_signer_local::PrivateKeySigner;
use dogeos_chainspec::DOGEOS_DEV;
use eyre::{ensure, Context};
use jsonrpsee::{
    server::{ServerBuilder, ServerHandle},
    types::ErrorObjectOwned,
    RpcModule,
};
use reth_rpc_api::EthApiServer;
use rollup_node::test_utils::{EventAssertions, TestFixture, TestFixtureBuilder};
use rollup_node_chain_orchestrator::ChainOrchestratorEvent;
use rollup_node_primitives::{sig_encode_hash, NodeConfig};
use scroll_db::DatabaseReadOperations;
use serde_json::Value;
use std::{
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::time::{sleep, timeout};

const PHASE_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Debug)]
struct MaterializedL1 {
    blocks: Vec<Block>,
    signers: Vec<Address>,
}

impl MaterializedL1 {
    fn new(signer: Address) -> Self {
        let mut state = Self { blocks: Vec::new(), signers: Vec::new() };
        // Serve blocks 0 and 1: the watcher starts at L1 block 0 and reports a block as
        // processed only once the head is above it.
        state.advance(signer);
        state.advance(signer);
        state
    }

    fn advance(&mut self, signer: Address) {
        let mut block = Block::<alloy_rpc_types_eth::Transaction>::default();
        block.header.inner.number = self.blocks.len() as u64;
        block.header.inner.timestamp =
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        block.header.inner.parent_hash =
            self.blocks.last().map(|b| b.header.hash).unwrap_or_default();
        block.header.hash = block.header.inner.hash_slow();
        self.blocks.push(block);
        self.signers.push(signer);
    }
}

/// Each node gets a distinct HTTP endpoint, so startup and refresh reads are
/// attributable to that node rather than to an aggregate request count.
#[derive(Debug)]
struct L1Rpc {
    url: reqwest::Url,
    handle: ServerHandle,
    reads: Arc<Mutex<Vec<Address>>>,
}

impl Drop for L1Rpc {
    fn drop(&mut self) {
        let _ = self.handle.stop();
    }
}

impl L1Rpc {
    async fn start(state: Arc<Mutex<MaterializedL1>>) -> eyre::Result<Self> {
        let server = ServerBuilder::default().build("127.0.0.1:0").await?;
        let url = format!("http://{}", server.local_addr()?).parse()?;
        let reads = Arc::new(Mutex::new(Vec::new()));
        let mut rpc = RpcModule::new(state);
        rpc.register_method("eth_getBlockByNumber", |params, state, _| {
            let (tag, full): (String, bool) = params.parse()?;
            if full {
                return Err(rpc_error("fixture only serves transaction-free headers"));
            }
            let state = state.lock().unwrap();
            let number = match tag.as_str() {
                "latest" | "safe" | "finalized" => state.blocks.len() - 1,
                "earliest" => 0,
                _ => usize::from_str_radix(tag.trim_start_matches("0x"), 16)
                    .map_err(|_| rpc_error("invalid block tag"))?,
            };
            Ok::<_, ErrorObjectOwned>(state.blocks.get(number).cloned())
        })?;
        let storage_reads = reads.clone();
        rpc.register_method("eth_getStorageAt", move |params, state, _| {
            let (address, slot, tag): (Address, U256, String) = params.parse()?;
            let system_contract = NodeConfig::from_chainspec(&*DOGEOS_DEV)
                .unwrap()
                .address_book
                .system_contract_address;
            if address != system_contract || slot != U256::from(0x67) || tag != "latest" {
                return Err(rpc_error("unexpected SystemConfig address, slot, or block tag"));
            }
            let signer = *state.lock().unwrap().signers.last().unwrap();
            storage_reads.lock().unwrap().push(signer);
            Ok::<_, ErrorObjectOwned>(B256::left_padding_from(signer.as_slice()))
        })?;
        rpc.register_method("eth_getLogs", |params, _, _| {
            let (_filter,): (Value,) = params.parse()?;
            Ok::<_, ErrorObjectOwned>(Vec::<Value>::new())
        })?;
        Ok(Self { url, handle: server.start(rpc), reads })
    }

    fn reads(&self) -> Vec<Address> {
        self.reads.lock().unwrap().clone()
    }

    fn configure(&self, builder: TestFixtureBuilder) -> TestFixtureBuilder {
        builder
            .with_test(false)
            .with_consensus_system_contract(None)
            .with_network_valid_signer(None)
            .with_eth_scroll_bridge(false)
            .with_scroll_wire(true)
            .allow_empty_blocks(true)
            .config(|config| {
                config.l1_provider_args.url = Some(self.url.clone());
                config.consensus_args.exit_on_signer_rotation = false;
            })
    }
}

fn rpc_error(message: &str) -> ErrorObjectOwned {
    ErrorObjectOwned::owned(-32602, message, None::<()>)
}

async fn processed(fixture: &TestFixture) -> eyre::Result<u64> {
    let db = fixture.nodes[0].as_ref().unwrap().rollup_manager_handle.get_database_handle().await?;
    Ok(db.get_processed_l1_block_number().await?)
}

async fn wait_processed(fixture: &TestFixture, height: u64, label: &str) -> eyre::Result<()> {
    timeout(PHASE_TIMEOUT, async {
        loop {
            if processed(fixture).await? >= height {
                return Ok::<_, eyre::Report>(());
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .wrap_err_with(|| format!("{label}: production watcher did not process L1 {height}"))?
}

async fn expect_import(
    fixture: &mut TestFixture,
    height: u64,
    hash: B256,
    signer: Address,
) -> eyre::Result<()> {
    let signatures = fixture
        .expect_event_on(0)
        .timeout(PHASE_TIMEOUT)
        .extract(|event| {
            if let ChainOrchestratorEvent::NewBlockReceived(received) = event {
                if received.block.hash_slow() == hash {
                    return Some(
                        received
                            .signature
                            .recover_address_from_prehash(&sig_encode_hash(&received.block.header))
                            .map_err(|err| err.to_string()),
                    );
                }
            }
            None
        })
        .await?;
    ensure!(
        signatures[0].as_ref().is_ok_and(|address| *address == signer),
        "received block {height} was not signed by {signer}: {signatures:?}"
    );
    fixture.expect_event_on(0).timeout(PHASE_TIMEOUT).chain_extended(height).await?;
    ensure!(
        canonical_hash(fixture, height).await? == hash,
        "follower imported a different block at {height}, expected {hash}"
    );
    Ok(())
}

async fn canonical_hash(fixture: &TestFixture, height: u64) -> eyre::Result<B256> {
    let block = fixture.nodes[0]
        .as_ref()
        .unwrap()
        .node
        .rpc
        .inner
        .eth_api()
        .block_by_number(height.into(), false)
        .await?
        .ok_or_else(|| eyre::eyre!("missing canonical block {height}"))?;
    Ok(block.header.hash)
}

#[tokio::test]
#[ignore = "RG-52: a running L1 watcher never re-reads the SystemConfig signer, so the standby never sequences"]
async fn running_nodes_discover_materialized_signer_rotation() -> eyre::Result<()> {
    reth_tracing::init_test_tracing();
    timeout(Duration::from_secs(120), rotation())
        .await
        .wrap_err("live signer rotation exceeded whole-test deadline")?
}

async fn rotation() -> eyre::Result<()> {
    let old_signer = PrivateKeySigner::random();
    let new_signer = PrivateKeySigner::random();
    let old_address = old_signer.address();
    let new_address = new_signer.address();
    let state = Arc::new(Mutex::new(MaterializedL1::new(old_address)));
    let old_rpc = L1Rpc::start(state.clone()).await?;
    let new_rpc = L1Rpc::start(state.clone()).await?;
    let follower_rpc = L1Rpc::start(state.clone()).await?;
    let reads = || {
        format!(
            "storage reads={:?}; old={old_address}, new={new_address}",
            [old_rpc.reads(), new_rpc.reads(), follower_rpc.reads()]
        )
    };

    let mut old = old_rpc
        .configure(TestFixture::builder().sequencer())
        .with_signer(old_signer)
        .block_time(0)
        .payload_building_duration(100)
        .build()
        .await?;
    // The standby's automatic slots consult production consensus. It must not
    // sequence until the watcher discovers its authorization from the RPC slot.
    let mut new = new_rpc
        .configure(TestFixture::builder().sequencer())
        .with_signer(new_signer)
        .block_time(250)
        .with_sequencer_auto_start(true)
        .payload_building_duration(100)
        .build()
        .await?;
    let mut follower = follower_rpc.configure(TestFixture::builder().followers(1)).build().await?;

    old.sequencer().node.connect(&mut new.sequencer().node).await;
    old.sequencer().node.connect(&mut follower.nodes[0].as_mut().unwrap().node).await;
    new.sequencer().node.connect(&mut follower.nodes[0].as_mut().unwrap().node).await;
    for (fixture, label) in [(&old, "old"), (&new, "standby"), (&follower, "follower")] {
        wait_processed(fixture, 1, label).await.wrap_err_with(reads)?;
    }
    let mut common_parent = B256::ZERO;
    for height in 1..=3 {
        let block = old.build_block().expect_block_number(height).build_and_await_block().await?;
        common_parent = block.hash_slow();
        expect_import(&mut new, height, common_parent, old_address)
            .await
            .wrap_err("standby failed to import old-signer block")?;
        expect_import(&mut follower, height, common_parent, old_address)
            .await
            .wrap_err("prestarted follower failed to import old-signer block")?;
    }
    for rpc in [&old_rpc, &new_rpc, &follower_rpc] {
        ensure!(rpc.reads().contains(&old_address), "startup did not read old signer from RPC");
    }
    tracing::info!(%common_parent, "all three running nodes imported old-signer block 3");

    // One atomic materialization: no node config, process, notification sender,
    // consensus object, or network signer is changed by the test.
    state.lock().unwrap().advance(new_address);
    for (fixture, label) in [(&old, "old"), (&new, "standby"), (&follower, "follower")] {
        wait_processed(fixture, 2, label).await.wrap_err_with(reads)?;
    }

    let outcome = new.expect_event_on(0).timeout(PHASE_TIMEOUT).block_sequenced(4).await;
    let progress = [processed(&old).await?, processed(&new).await?, processed(&follower).await?];
    let block = outcome.wrap_err_with(|| {
        format!(
            "new signer did not sequence after L1 rotation; L1 processed [old, standby, follower]=\
         {progress:?}; {}",
            reads()
        )
    })?;
    ensure!(block.header.parent_hash == common_parent, "standby did not extend common parent");
    for (label, rpc) in [("old", &old_rpc), ("standby", &new_rpc), ("follower", &follower_rpc)] {
        ensure!(rpc.reads().contains(&new_address), "{label}: watcher did not refresh signer");
    }
    expect_import(&mut follower, 4, block.hash_slow(), new_address)
        .await
        .wrap_err("prestarted follower did not adopt new signer")?;
    ensure!(
        canonical_hash(&new, 4).await? == canonical_hash(&follower, 4).await?,
        "new sequencer and continuously running follower disagree"
    );
    Ok(())
}
