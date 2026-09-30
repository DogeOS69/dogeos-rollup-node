//! Verification test for the reorg findings F1 (fullnode halt): a derivation that fails because the
//! node lacks the batch's L1 messages is retried without limit, and after an L1 unwind deletes
//! the batch row the same request keeps failing (`UnknownBatch`); the pipeline has no reset, so
//! it never yields and never becomes empty. Assertions document current behaviour.
//!
//! Tracker rows (Reorg Issue Tracker, Private Mainnet): RG-43 (a fullnode halts derivation after a
//! queue gap). Each test name starts with the RG key of the row it pins. Every test passes today
//! and documents behaviour that is not fixed; when a row is fixed, change the matching assertion
//! instead of deleting the test.

use super::*;
use alloy_primitives::{b256, U256};
use dogeos_protocol_types::TxL1Message;
use futures::StreamExt;
use rollup_node_providers::test_utils::MockL1Provider;
use scroll_codec::decoding::test_utils::read_to_bytes;
use scroll_db::{test_utils::setup_test_db, DatabaseWriteOperations};
use std::{collections::HashMap, time::Duration};

fn message(queue_index: u64) -> L1MessageEnvelope {
    L1MessageEnvelope::new(
        TxL1Message {
            queue_index,
            gas_limit: 168_000,
            to: Address::repeat_byte(0x22),
            value: U256::ZERO,
            sender: Address::repeat_byte(0x11),
            input: Bytes::new(),
        },
        717,
        None,
        None,
    )
}

#[tokio::test]
async fn rg43_missing_messages_then_unwind_leave_the_pipeline_stuck() -> eyre::Result<()> {
    // The batch from `test_should_retry_on_derivation_error`: it includes L1 messages 33 and 34.
    let db = Arc::new(setup_test_db().await);
    let batch_data = BatchCommitData {
        hash: b256!("7f26edf8e3decbc1620b4d2ba5f010a6bdd10d6bb16430c4f458134e36ab3961"),
        index: 12,
        block_number: 18319648,
        block_timestamp: 1696935971,
        calldata: Arc::new(read_to_bytes("./testdata/calldata_v0.bin")?),
        blob_versioned_hash: None,
        finalized_block_number: None,
        reverted_block_number: None,
    };
    db.insert_batch(batch_data.clone()).await?;

    let provider = MockL1Provider { db: db.clone(), blobs: HashMap::new() };
    let mut pipeline = DerivationPipeline::new(provider.clone(), db.clone(), u64::MAX).await;
    pipeline
        .push_batch(BatchInfo { index: 12, hash: batch_data.hash }, BatchStatus::Consolidated)
        .await;

    // The node never stored messages 33 and 34 (the queue gap): InvalidL1MessagesCount, retried.
    assert!(tokio::time::timeout(Duration::from_secs(2), pipeline.next()).await.is_err());
    assert_eq!(pipeline.len(), 1);
    // The worker only logs the error it retries on, so derive the same batch directly to show the
    // cause: the two messages are missing.
    let cache = PreFetchCache::new(db.clone(), 100, Duration::from_secs(60), 10).await?;
    let err = derive(batch_data.clone(), BatchStatus::Consolidated, provider, cache, u64::MAX)
        .await
        .expect_err("the batch cannot be derived without messages 33 and 34");
    assert!(
        matches!(err, DerivationPipelineError::InvalidL1MessagesCount { expected: 2, got: 0 }),
        "got {err:?}"
    );

    // `revertToL1Block` below the batch's commit block deletes the batch row.
    db.unwind(batch_data.block_number - 1).await?;
    assert!(db.get_batch_by_index(12).await?.is_none());

    // Even if the messages now arrive, the queued request fails with UnknownBatch forever.
    db.insert_l1_message(message(33)).await?;
    db.insert_l1_message(message(34)).await?;
    assert!(tokio::time::timeout(Duration::from_secs(2), pipeline.next()).await.is_err());
    assert_eq!(pipeline.len(), 1, "the pipeline never becomes empty");
    assert!(!pipeline.is_empty());

    // Control: with the batch row back, the same request derives (the retry loop is live).
    db.insert_batch(batch_data.clone()).await?;
    let result = tokio::time::timeout(Duration::from_secs(10), pipeline.next())
        .await
        .expect("derives once the batch row exists again")
        .expect("pipeline open");
    assert_eq!(result.batch_info.index, 12);
    assert!(pipeline.is_empty());
    Ok(())
}
