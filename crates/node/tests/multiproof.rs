//! Local-only integration coverage for the opt-in ordinary EIP-1186 multiproof RPC.

use alloy_eips::eip2935::HISTORY_STORAGE_CODE;
use alloy_genesis::GenesisAccount;
use alloy_primitives::{address, Address, B256, U256};
use dogeos_chainspec::{DogeosChainSpec, DOGEOS_DEV, DOGEOS_MAINNET};
use dogeos_hardforks::DogeosHardforks;
use jsonrpsee::{core::client::ClientT, rpc_params};
use reth_chainspec::EthChainSpec;
use reth_node_core::args::RpcServerArgs;
use reth_rpc_builder::RethRpcModule;
use rollup_node::{
    test_utils::{
        default_sequencer_test_scroll_rollup_node_config,
        fixture::{NodeHandle, NodeType},
        setup_engine_with_rpc, EventAssertions, TestFixture,
    },
    RpcArgs,
};
use serde_json::{json, Value};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::Mutex,
};

const GPO: Address = address!("5300000000000000000000000000000000000002");
const NATIVE: Address = address!("530000000000000000000000000000000000d09e");
const QUEUE: Address = address!("5300000000000000000000000000000000000000");
const HISTORY: Address = address!("0000f90827f1c53a10cb7a02335b175320002935");
const EMPTY: Address = address!("0000000000000000000000000000000000000100");
const ABSENT: Address = address!("0000000000000000000000000000000000000101");

fn key(value: u64) -> B256 {
    B256::from(U256::from(value))
}

fn targets(parent_number: u64) -> Value {
    json!([
        {"address": GPO, "storageKeys": [key(9), key(10), key(11), key(12)]},
        {"address": NATIVE, "storageKeys": [key(0)]},
        {"address": QUEUE, "storageKeys": [key(0), key(1)]},
        {"address": HISTORY, "storageKeys": [key(parent_number % 8191)]}
    ])
}

/// Serialize the exact schedule used by the node so core can consume the same genesis.
fn chain_spec(tsuki: bool) -> Arc<DogeosChainSpec> {
    let mut genesis = DOGEOS_DEV.genesis().clone();
    genesis.config.chain_id = DOGEOS_DEV.chain().id();
    // Core retains EuclidV2 as the predecessor implied by compact Reth's Feynman baseline.
    for fork in ["euclidV2Time", "feynmanTime", "galileoTime", "galileoV2Time"] {
        genesis.config.extra_fields.insert(fork.into(), json!(0));
    }
    if tsuki {
        genesis.config.extra_fields.insert("tsukiTime".into(), json!(0));
        // Use the current deployed queue definition, not a synthetic RPC witness stub.
        genesis.alloc.insert(QUEUE, DOGEOS_MAINNET.genesis().alloc[&QUEUE].clone());
        genesis.alloc.insert(
            HISTORY,
            GenesisAccount {
                nonce: Some(1),
                code: Some(HISTORY_STORAGE_CODE.clone()),
                ..Default::default()
            },
        );
    } else {
        genesis.config.extra_fields.remove("tsukiTime");
    }
    genesis
        .config
        .extra_fields
        .insert("scroll".into(), serde_json::to_value(DOGEOS_DEV.config).unwrap());
    if !tsuki {
        // Synthetic values exercise proof shapes only. The live Tsuki fixture keeps
        // deployed system initialization intact and lets the executor install NativeDogeToken.
        for (address, slots) in [
            (GPO, vec![9, 10, 11, 12]),
            (NATIVE, vec![0]),
            (QUEUE, vec![0, 1]),
            (HISTORY, vec![0, 1, 2]),
        ] {
            let account = genesis.alloc.entry(address).or_default();
            account.balance = U256::from(1);
            let storage = account.storage.get_or_insert_with(BTreeMap::new);
            for slot in slots {
                // Preserve the initialized dev system-contract values when present.
                storage.entry(key(slot)).or_insert(key(slot + 1));
            }
        }
        genesis
            .alloc
            .insert(EMPTY, GenesisAccount { balance: U256::from(1), ..Default::default() });
        genesis.alloc.remove(&ABSENT);
    }
    let spec = DogeosChainSpec::from_custom_genesis(genesis);
    assert_eq!(spec.is_tsuki_active_at_timestamp(spec.genesis().timestamp), tsuki);
    Arc::new(spec)
}

async fn fixture(enabled: bool, rpc: RpcServerArgs) -> eyre::Result<TestFixture> {
    fixture_with_tsuki(enabled, rpc, false).await
}

async fn fixture_with_tsuki(
    enabled: bool,
    rpc: RpcServerArgs,
    tsuki: bool,
) -> eyre::Result<TestFixture> {
    let mut config = default_sequencer_test_scroll_rollup_node_config();
    // Deliberately disable both rollup namespaces: the experiment must be independent.
    config.rpc_args = RpcArgs { experimental_multiproof: enabled, ..Default::default() };
    let chain_spec = chain_spec(tsuki);
    let (nodes, dbs, wallet) =
        setup_engine_with_rpc(config.clone(), 1, chain_spec.clone(), false, true, None, None, rpc)
            .await?;
    let mut handles = Vec::new();
    for node in nodes {
        handles.push(Some(NodeHandle::new(node, NodeType::Sequencer).await?));
    }
    Ok(TestFixture {
        nodes: handles,
        dbs,
        wallet: Arc::new(Mutex::new(wallet)),
        chain_spec,
        l1_provider: None,
        anvil: None,
        config,
        has_remote_source_node: false,
    })
}

fn rpc_args(http_eth: bool, ws_eth: bool) -> RpcServerArgs {
    let mut args = RpcServerArgs::default()
        .with_http()
        .with_http_api(
            if http_eth {
                vec![RethRpcModule::Eth, RethRpcModule::Debug]
            } else {
                vec![RethRpcModule::Net]
            }
            .into(),
        )
        .with_ws()
        .with_ws_api(vec![if ws_eth { RethRpcModule::Eth } else { RethRpcModule::Net }].into());
    // Both ports become zero in setup_engine. Distinct loopback addresses keep Reth
    // from coalescing HTTP/WS into one server, which requires identical API selections.
    args.ws_addr = std::net::Ipv4Addr::new(127, 0, 0, 2).into();
    // The production default is latest-only (zero). This fixture deliberately retains
    // and queries the first two historical states without changing that default.
    args.rpc_eth_proof_window = 16;
    args
}

async fn assert_error(client: &impl ClientT, request: Value, code: i32) {
    let result: Result<Value, _> = client.request("dogeos_getProofs", rpc_params![request]).await;
    let error = result.expect_err("request should fail");
    match error {
        jsonrpsee::core::client::Error::Call(error) => assert_eq!(error.code(), code, "{error}"),
        error => panic!("expected JSON-RPC error, got {error}"),
    }
}

#[cfg(unix)]
async fn ipc_request(endpoint: String, request: Value) -> eyre::Result<Value> {
    let mut stream = tokio::net::UnixStream::connect(endpoint).await?;
    stream
        .write_all(&serde_json::to_vec(&json!({
            "jsonrpc": "2.0", "id": 1, "method": "dogeos_getProofs", "params": [request]
        }))?)
        .await?;
    stream.write_all(b"\n").await?;
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut response = Vec::new();
        loop {
            let mut chunk = [0; 1024];
            let len = stream.read(&mut chunk).await?;
            eyre::ensure!(len != 0, "IPC closed without a JSON response");
            response.extend_from_slice(&chunk[..len]);
            eyre::ensure!(response.len() <= 64 * 1024, "unexpectedly large IPC error response");
            match serde_json::from_slice(&response) {
                Ok(value) => return Ok(value),
                Err(error) if error.is_eof() => {}
                Err(error) => return Err(error.into()),
            }
        }
    })
    .await?
}

#[tokio::test]
async fn multiproof_opt_in_and_transport_selection() -> eyre::Result<()> {
    // Different HTTP/WS selections catch accidental merge_configured exposure. IPC's
    // existing default selection includes Eth; the authenticated endpoint is separate.
    for (enabled, http_eth, ws_eth) in
        [(false, true, true), (true, true, false), (true, false, true)]
    {
        let mut fixture = fixture(enabled, rpc_args(http_eth, ws_eth)).await?;
        let handles = fixture.sequencer().node.inner.add_ons_handle.rpc_server_handles.clone();
        let request = json!({"blockHash": fixture.chain_spec.genesis_hash(), "targets": []});
        assert_error(
            &handles.rpc.http_client().unwrap(),
            request.clone(),
            if enabled && http_eth { -32602 } else { -32601 },
        )
        .await;
        assert_error(
            &handles.rpc.ws_client().await.unwrap(),
            request.clone(),
            if enabled && ws_eth { -32602 } else { -32601 },
        )
        .await;
        assert_error(&handles.auth.http_client(), request.clone(), -32601).await;
        #[cfg(unix)]
        {
            let response = ipc_request(handles.rpc.ipc_endpoint().unwrap(), request).await?;
            assert_eq!(response["error"]["code"], if enabled { -32602 } else { -32601 });
        }
    }
    Ok(())
}

async fn assert_equivalent(
    client: &impl ClientT,
    hash: B256,
    targets: Value,
) -> eyre::Result<Value> {
    let mut individual = Vec::new();
    for target in targets.as_array().unwrap() {
        let proof: Value = client
            .request(
                "eth_getProof",
                rpc_params![
                    target["address"].clone(),
                    target["storageKeys"].clone(),
                    json!({"blockHash": hash, "requireCanonical": true})
                ],
            )
            .await?;
        individual.push(proof);
    }
    let shared: Value = client
        .request("dogeos_getProofs", rpc_params![json!({"blockHash": hash, "targets": targets})])
        .await?;
    // Full JSON value equality includes all metadata and ordered proof nodes and slots.
    assert_eq!(shared, Value::Array(individual));
    Ok(shared)
}

#[tokio::test]
async fn multiproof_matches_individual_proofs() -> eyre::Result<()> {
    let mut fixture = fixture(true, rpc_args(true, false)).await?;
    let client = fixture.sequencer().node.rpc_client().unwrap();
    let hash = fixture.chain_spec.genesis_hash();
    let shared = assert_equivalent(&client, hash, targets(0)).await?;
    assert_eq!(shared.as_array().unwrap().len(), 4);
    assert_equivalent(
        &client,
        hash,
        json!([
            {"address": ABSENT, "storageKeys": [key(0)]},
            {"address": EMPTY, "storageKeys": [key(0)]},
            // Nonempty storage with zero requested keys is a mandatory provider gate.
            {"address": GPO, "storageKeys": []},
            {"address": NATIVE, "storageKeys": [key(99)]}
        ]),
    )
    .await?;
    // Reversing targets and slot order must not expose internal map ordering.
    assert_equivalent(
        &client,
        hash,
        json!([
            {"address": HISTORY, "storageKeys": [key(2), key(0)]},
            {"address": GPO, "storageKeys": [key(12), key(9), key(11), key(10)]}
        ]),
    )
    .await?;

    // Move beyond genesis; the same hash now selects retained historical state.
    fixture.l1().sync().await?;
    fixture.expect_event().l1_synced().await?;
    fixture.build_block().build_and_await_block().await?;
    assert_equivalent(&client, hash, targets(0)).await?;
    let parent = fixture.get_sequencer_block().await?;
    fixture.build_block().build_and_await_block().await?;
    assert_equivalent(&client, parent.header.hash_slow(), targets(parent.header.number)).await?;
    Ok(())
}

#[tokio::test]
async fn multiproof_tsuki_fixture_mines_real_blocks() -> eyre::Result<()> {
    let mut fixture = fixture_with_tsuki(true, rpc_args(true, false), true).await?;
    let client = fixture.sequencer().node.rpc_client().unwrap();
    // The emitted genesis must recreate both the schedule and genesis hash in another process.
    let serialized = serde_json::to_vec(fixture.chain_spec.genesis())?;
    let exported: Value = serde_json::from_slice(&serialized)?;
    // This is core's complete ordered genesis schedule, including the predecessor
    // which compact Reth no longer exposes as a separate fork enum variant.
    for fork in ["euclidV2Time", "feynmanTime", "galileoTime", "galileoV2Time", "tsukiTime"] {
        assert_eq!(exported["config"][fork], 0, "missing active predecessor {fork}");
    }
    let reparsed = DogeosChainSpec::from_custom_genesis(serde_json::from_slice(&serialized)?);
    assert_eq!(reparsed.genesis_hash(), fixture.chain_spec.genesis_hash());
    assert!(reparsed.is_tsuki_active_at_timestamp(0));
    fixture.l1().sync().await?;
    fixture.expect_event().l1_synced().await?;
    fixture.build_block().build_and_await_block().await?;
    let parent = fixture.get_sequencer_block().await?;
    fixture.build_block().build_and_await_block().await?;
    let child = fixture.get_sequencer_block().await?;
    assert_eq!(parent.header.number, 1);
    assert_eq!(child.header.number, 2);
    assert_eq!(child.header.parent_hash, parent.header.hash_slow());
    assert!(parent.transactions.is_empty());
    assert!(child.transactions.is_empty());
    let proofs = assert_equivalent(&client, parent.header.hash_slow(), targets(1)).await?;
    // Block 1 installs the real Tsuki token, so block 2 witnesses an existing predeploy.
    let code: String = client
        .request(
            "eth_getCode",
            rpc_params![
                NATIVE,
                json!({"blockHash": parent.header.hash_slow(), "requireCanonical": true})
            ],
        )
        .await?;
    assert_ne!(code, "0x", "Tsuki must install the NativeDogeToken predeploy");
    assert_ne!(proofs[1]["storageProof"][0]["value"], "0x1", "Tsuki installs the total supply");
    // A real execution witness is mandatory for core; never substitute fixture JSON here.
    let witness: Value = client
        .request("debug_executionWitness", rpc_params![format!("0x{:x}", child.header.number)])
        .await?;
    assert!(witness.is_object(), "node must provide an execution witness for the real block");
    Ok(())
}

#[tokio::test]
async fn multiproof_raw_params_limit_is_method_scoped() -> eyre::Result<()> {
    let mut fixture = fixture(true, rpc_args(true, false)).await?;
    let url = fixture.sequencer().node.rpc_url();
    let request = json!({"blockHash": fixture.chain_spec.genesis_hash(), "targets": targets(0)});
    let padding = " ".repeat(64 * 1024);
    let client = reqwest::Client::new();
    let response: Value = client.post(url.clone()).header("content-type", "application/json")
        .body(format!(r#"{{"jsonrpc":"2.0","id":1,"method":"dogeos_getProofs","params":[{request}{padding}]}}"#))
        .send().await?.json().await?;
    assert_eq!(response["error"]["code"], -32602);
    // The same whitespace remains legal for another method under unchanged global limits.
    let response: Value = client
        .post(url)
        .header("content-type", "application/json")
        .body(format!(r#"{{"jsonrpc":"2.0","id":2,"method":"eth_chainId","params":[{padding}]}}"#))
        .send()
        .await?
        .json()
        .await?;
    assert!(response.get("result").is_some(), "{response}");
    Ok(())
}

#[tokio::test]
async fn multiproof_invalid_requests_do_not_fall_back() -> eyre::Result<()> {
    let mut fixture = fixture(true, rpc_args(true, false)).await?;
    let client = fixture.sequencer().node.rpc_client().unwrap();
    let hash = fixture.chain_spec.genesis_hash();
    for request in [
        json!({"blockHash": hash, "targets": []}),
        json!({"blockHash": hash, "targets": [
            {"address": GPO, "storageKeys": []}, {"address": GPO, "storageKeys": []}
        ]}),
        json!({"blockHash": hash, "targets": [{"address": GPO, "storageKeys": [key(0), key(0)]}]}),
        json!({"blockHash": hash, "targets": [{"address": GPO, "storageKeys": [key(0), key(1), key(2), key(3), key(4)]}]}),
        json!({"blockHash": hash, "targets": [
            {"address": GPO, "storageKeys": []}, {"address": NATIVE, "storageKeys": []},
            {"address": QUEUE, "storageKeys": []}, {"address": HISTORY, "storageKeys": []},
            {"address": EMPTY, "storageKeys": []}
        ]}),
        json!({"blockHash": hash, "targets": [
            {"address": GPO, "storageKeys": [key(0), key(1), key(2), key(3)]},
            {"address": NATIVE, "storageKeys": [key(0), key(1), key(2), key(3)]},
            {"address": QUEUE, "storageKeys": [key(0)]}
        ]}),
        json!({"blockHash": "latest", "targets": targets(0)}),
        json!({"blockHash": 0, "targets": targets(0)}),
    ] {
        assert_error(&client, request, -32602).await;
    }
    let result: Result<Value, _> = client
        .request(
            "dogeos_getProofs",
            rpc_params![json!({"blockHash": B256::repeat_byte(0xff), "targets": targets(0)})],
        )
        .await;
    match result.expect_err("unknown hash must fail") {
        jsonrpsee::core::client::Error::Call(error) => assert_ne!(error.code(), -32601),
        error => panic!("expected provider JSON-RPC error, got {error}"),
    }
    Ok(())
}

/// A bounded local endpoint for the root's external comparison harness. Run explicitly
/// with --ignored --exact serve_multiproof_fixture --nocapture. The manifest and genesis
/// are written only when MULTIPROOF_FIXTURE_DIR is supplied; the node lives for at most
/// 15 minutes, or until a `stop` file appears in that directory.
#[tokio::test]
#[ignore = "starts a temporary RPC endpoint for the external experiment harness"]
async fn serve_multiproof_fixture() -> eyre::Result<()> {
    serve_fixture(false).await
}

/// Live Tsuki variant for core's one-shot comparison against the same generated genesis.
#[tokio::test]
#[ignore = "starts a temporary Tsuki RPC endpoint for the core artifact gate"]
async fn serve_tsuki_multiproof_fixture() -> eyre::Result<()> {
    serve_fixture(true).await
}

async fn serve_fixture(tsuki: bool) -> eyre::Result<()> {
    let output = std::path::PathBuf::from(std::env::var("MULTIPROOF_FIXTURE_DIR")?);
    std::fs::create_dir_all(&output)?;
    eyre::ensure!(!output.join("stop").exists(), "remove the old stop file before starting");
    let mut fixture = fixture_with_tsuki(true, rpc_args(true, false), tsuki).await?;
    let client = fixture.sequencer().node.rpc_client().unwrap();
    fixture.l1().sync().await?;
    fixture.expect_event().l1_synced().await?;
    fixture.build_block().build_and_await_block().await?;
    let parent = fixture.get_sequencer_block().await?;
    fixture.build_block().build_and_await_block().await?;
    let child = fixture.get_sequencer_block().await?;
    let parent_hash = parent.header.hash_slow();
    assert_equivalent(&client, parent_hash, targets(parent.header.number)).await?;
    if tsuki {
        let code: String = client
            .request(
                "eth_getCode",
                rpc_params![NATIVE, json!({"blockHash": parent_hash, "requireCanonical": true})],
            )
            .await?;
        eyre::ensure!(code != "0x", "Tsuki did not install NativeDogeToken");
        let witness: Value = client
            .request("debug_executionWitness", rpc_params![format!("0x{:x}", child.header.number)])
            .await?;
        eyre::ensure!(witness.is_object(), "node did not return a real execution witness");
    }
    let manifest = json!({
        "rpcUrl": fixture.sequencer().node.rpc_url(),
        "blockHash": parent_hash,
        "blockNumber": parent.header.number,
        "stateRoot": parent.header.state_root,
        "childBlockHash": child.header.hash_slow(),
        "childBlockNumber": child.header.number,
        "childStateRoot": child.header.state_root,
        "genesisHash": fixture.chain_spec.genesis_hash(),
        "targets": targets(parent.header.number),
        "tsukiEnabled": tsuki,
        "scope": if tsuki { "live Tsuki input; core artifact equivalence must be checked separately" } else { "raw EIP-1186 proof equality; not core Tsuki artifact equivalence" }
    });
    std::fs::write(
        output.join("genesis.json"),
        serde_json::to_vec_pretty(fixture.chain_spec.genesis())?,
    )?;
    std::fs::write(output.join("manifest.json"), serde_json::to_vec_pretty(&manifest)?)?;
    println!("MULTIPROOF_FIXTURE {}", serde_json::to_string(&manifest)?);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15 * 60);
    while tokio::time::Instant::now() < deadline && !output.join("stop").exists() {
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    Ok(())
}
