//! Deterministic executed workload; no fabricated bridge transitions.
use super::*;
use alloy_eips::eip2718::Encodable2718;
use alloy_primitives::{Bytes, TxKind};
use alloy_rpc_types_eth::{TransactionInput, TransactionRequest};
use dogeos_rpc_types::ScrollTransactionRequest;
use reth_rpc_eth_api::SignableTxRequest;

async fn inject(f: &mut TestFixture, to: TxKind, data: Bytes, value: u64) -> eyre::Result<B256> {
    let mut wallet = f.wallet.lock().await;
    let req: ScrollTransactionRequest = TransactionRequest {
        nonce: Some(wallet.inner_nonce),
        to: Some(to),
        gas: Some(200_000),
        gas_price: Some(1_000_000_000),
        chain_id: Some(wallet.chain_id),
        value: Some(U256::from(value)),
        input: TransactionInput::new(data),
        ..Default::default()
    }
    .into();
    let signed = req.try_build_and_sign(wallet.inner.clone()).await?;
    wallet.inner_nonce += 1;
    drop(wallet);
    Ok(f.sequencer().node.rpc.inject_tx(signed.encoded_2718().into()).await?)
}

async fn workload_block(
    f: &mut TestFixture,
    client: &impl ClientT,
    index: u64,
    count: u64,
    profile: &str,
    seed: u64,
    contract: Address,
) -> eyre::Result<Value> {
    let mut hashes = Vec::new();
    for tx in 0..count {
        let slot = if profile == "state-churn" { seed + index * count + tx } else { tx };
        let mut data = Vec::from(key(slot).as_slice());
        data.extend_from_slice(key(seed + index + tx).as_slice());
        hashes.push(inject(f, TxKind::Call(contract), data.into(), 0).await?);
    }
    hashes.push(
        inject(f, TxKind::Call(Address::from_word(key(seed + index))), Bytes::new(), 1).await?,
    );
    f.build_block().expect_tx_count(hashes.len()).build_and_await_block().await?;
    for hash in hashes {
        let receipt: Value = client.request("eth_getTransactionReceipt", rpc_params![hash]).await?;
        eyre::ensure!(receipt["status"] == "0x1", "workload reverted: {receipt}");
    }
    let block: Value = client
        .request("eth_getBlockByNumber", rpc_params![format!("0x{:x}", index + 2), true])
        .await?;
    Ok(block)
}

async fn persisted_through(f: &TestFixture, expected: u64) -> eyre::Result<u64> {
    use reth_db::{database::Database, transaction::DbTx};
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let checkpoint =
                f.dbs[0].tx()?.get::<reth_db::tables::StageCheckpoints>("Execution".into())?;
            if let Some(checkpoint) = checkpoint {
                if checkpoint.block_number >= expected {
                    return Ok::<_, eyre::Report>(checkpoint.block_number);
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await?
}

fn option(name: &str, default: u64) -> eyre::Result<u64> {
    Ok(std::env::var(name).ok().map(|s| s.parse()).transpose()?.unwrap_or(default))
}

#[tokio::test]
#[ignore = "explicit retained real-node workload service"]
async fn serve_tsuki_retained_range() -> eyre::Result<()> {
    let output = std::path::PathBuf::from(std::env::var("MULTIPROOF_FIXTURE_DIR")?);
    std::fs::create_dir(&output)?;
    let length = option("RANGE_BLOCKS", 4)?;
    let depth = option("RANGE_ADVANCE", 0)?;
    let chunk_blocks = option("RANGE_CHUNK_BLOCKS", 2)?;
    eyre::ensure!((1..=8).contains(&chunk_blocks), "chunk block limit is 8");
    let seed = option("RANGE_SEED", 1066)?;
    let profile = std::env::var("RANGE_PROFILE").unwrap_or("quiet".into());
    let count = match profile.as_str() {
        "quiet" => 1,
        "transaction-heavy" => 16,
        "state-churn" => 8,
        _ => eyre::bail!("unknown profile"),
    };
    eyre::ensure!(length >= 4 && length <= 512 && depth <= 2048, "bounded local range/depth");
    let deferred = std::env::var("RANGE_DEFER_ADVANCE").as_deref() == Ok("1") && depth > 0;
    let mut rpc = rpc_args(true, false);
    let ordinary_proof_permits = rpc.rpc_proof_permits;
    let blocking_io_requests = rpc.rpc_max_blocking_io_requests;
    rpc.rpc_eth_proof_window = length + depth + 32;
    let mut f = fixture_with_tsuki(true, rpc, true).await?;
    let client = f.sequencer().node.rpc_client().unwrap();
    f.l1().sync().await?;
    f.expect_event().l1_synced().await?;
    f.build_block().build_and_await_block().await?;
    let parent = f.get_sequencer_block().await?;
    // init copies a runtime that stores calldata word 1 in slot calldata word 0.
    let creation = inject(
        &mut f,
        TxKind::Create,
        Bytes::from_static(&[
            0x60, 0x09, 0x60, 0x0c, 0x60, 0x00, 0x39, 0x60, 0x09, 0x60, 0x00, 0xf3, 0x60, 0x20,
            0x35, 0x60, 0x00, 0x35, 0x55, 0x00, 0x00,
        ]),
        0,
    )
    .await?;
    f.build_block().expect_tx(creation).build_and_await_block().await?;
    let receipt: Value = client.request("eth_getTransactionReceipt", rpc_params![creation]).await?;
    eyre::ensure!(receipt["status"] == "0x1", "creation reverted: {receipt}");
    let contract: Address = serde_json::from_value(receipt["contractAddress"].clone())?;
    let mut blocks = Vec::new();
    let first: Value = client.request("eth_getBlockByNumber", rpc_params!["0x2", true]).await?;
    blocks.push(first);
    for index in 1..length + if deferred { 0 } else { depth } {
        blocks.push(workload_block(&mut f, &client, index, count, &profile, seed, contract).await?);
    }
    // Frozen sequencer: no automatic ingestion; all blocks are explicitly triggered.
    let persisted = persisted_through(&f, length + 1).await?;
    let mut manifest = json!({"schema": 1, "rpcUrl": f.sequencer().node.rpc_url(),
        "nodePid": std::process::id(), "nodeBinary": std::env::current_exe()?,
        "database": f.dbs[0].path(), "genesisHash": f.chain_spec.genesis_hash(),
        "parent": {"number": parent.header.number, "hash": parent.header.hash_slow(), "stateRoot": parent.header.state_root},
        "blocks": &blocks[..length as usize], "advanceBlocks": &blocks[length as usize..],
        "profile": profile, "seed": seed, "advanceDepth": if deferred {0} else {depth},
        "phase": if deferred {"near"} else {"ready"},
        "ordinaryProofPermits": ordinary_proof_permits, "blockingIoRequests": blocking_io_requests,
        "multiproofAdmission": 2, "multiproofDeadlineSeconds": 30, "multiproofSharedPermitWaitSeconds": 1,
        "proofWindow": length + depth + 32, "executionCheckpointAtReady": persisted, "history": "archive: NodeConfig default prune modes; persistence threshold 0",
        "limits": {"maxBlocks": 8, "maxGas": 6000000, "maxBytes": 122880},
        "chunks": (0..length).step_by(chunk_blocks as usize).map(|i| json!({"start": i + 2, "end": (i + chunk_blocks + 1).min(length + 1)})).collect::<Vec<_>>(),
        "bridgeCoverage": false, "cache": "warm/uncontrolled OS page cache", "contract": contract});
    if let Ok(path) = std::env::var("RANGE_NODE_IDENTITY") {
        manifest["identity"] = serde_json::from_slice(&std::fs::read(path)?)?;
    }
    std::fs::write(
        output.join("genesis.json"),
        serde_json::to_vec_pretty(f.chain_spec.genesis())?,
    )?;
    std::fs::write(output.join("manifest.json"), serde_json::to_vec_pretty(&manifest)?)?;
    println!("RETAINED_RANGE_READY {}", output.display());
    let deadline = tokio::time::Instant::now() +
        Duration::from_secs(option("RANGE_SERVE_SECONDS", 3600)?.min(7200));
    while tokio::time::Instant::now() < deadline && !output.join("stop").exists() {
        if deferred && manifest["phase"] == "near" && output.join("advance").exists() {
            for index in length..length + depth {
                blocks.push(
                    workload_block(&mut f, &client, index, count, "state-churn", seed, contract)
                        .await?,
                );
            }
            manifest["executionCheckpointAtReady"] =
                json!(persisted_through(&f, length + depth + 1).await?);
            manifest["advanceBlocks"] = json!(&blocks[length as usize..]);
            manifest["advanceDepth"] = json!(depth);
            manifest["phase"] = json!("historical");
            std::fs::write(
                output.join("manifest.next.json"),
                serde_json::to_vec_pretty(&manifest)?,
            )?;
            std::fs::rename(output.join("manifest.next.json"), output.join("manifest.json"))?;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    f.shutdown_node(0).await?;
    // Retain only DB handles after graceful node shutdown. TempDatabase otherwise
    // removes evidence on drop. Process exit closes MDBX without deleting the DB.
    let db_path = f.dbs[0].path().to_path_buf();
    for db in f.dbs.drain(..) {
        std::mem::forget(db);
    }
    drop(f);
    eyre::ensure!(db_path.is_dir(), "retained DB vanished");
    std::fs::write(
        output.join("shutdown.json"),
        serde_json::to_vec_pretty(&json!({
            "cleanShutdown": true, "database": db_path, "existsBeforeProcessExit": true
        }))?,
    )?;
    Ok(())
}

#[tokio::test]
#[ignore = "opens only explicitly supplied stopped fixture DB read-only"]
async fn verify_retained_range_db() -> eyre::Result<()> {
    use reth_db::{database::Database, transaction::DbTx};
    let output = std::path::PathBuf::from(std::env::var("MULTIPROOF_FIXTURE_DIR")?);
    let stopped: Value = serde_json::from_slice(&std::fs::read(output.join("shutdown.json"))?)?;
    eyre::ensure!(stopped["cleanShutdown"] == true, "clean shutdown marker required");
    let db = reth_db::open_db_read_only(stopped["database"].as_str().unwrap(), Default::default())?;
    let checkpoint = db.tx()?.get::<reth_db::tables::StageCheckpoints>("Execution".into())?;
    let number = checkpoint.ok_or_else(|| eyre::eyre!("execution checkpoint absent"))?.block_number;
    let manifest: Value = serde_json::from_slice(&std::fs::read(output.join("manifest.json"))?)?;
    let last = manifest["blocks"].as_array().unwrap().last().unwrap()["number"].as_str().unwrap();
    let expected = u64::from_str_radix(last.trim_start_matches("0x"), 16)? +
        manifest["advanceDepth"].as_u64().unwrap();
    eyre::ensure!(number >= expected, "DB checkpoint {number} below workload head {expected}");
    std::fs::write(
        output.join("retained-db-verified.json"),
        serde_json::to_vec_pretty(
            &json!({"readOnlyReopen":true,"executionCheckpoint":number,"expectedHead":expected,"database":stopped["database"]}),
        )?,
    )?;
    Ok(())
}
