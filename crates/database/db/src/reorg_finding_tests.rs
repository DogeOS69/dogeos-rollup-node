//! Verification tests for the reorg findings F1 (database side; `REORG_RUST_TEST_CASES.md` cases 1,
//! 18 and 20). They pin how the database reacts to what the L1 watcher reports when the synthetic
//! L1's finalized block is its head. Assertions document current behaviour.
//!
//! Tracker rows (Reorg Issue Tracker, Private Mainnet): RG-41 (a lower replaced message is kept;
//! the `l1_block` table stays empty), RG-43 (the queue-gap check) and RG-44 (two batch rows for one
//! index). Each test name starts with the RG key of the row it pins. Every test passes today and
//! documents behaviour that is not fixed; when a row is fixed, change the matching assertion
//! instead of deleting the test.

use crate::{
    models,
    operations::{DatabaseReadOperations, DatabaseWriteOperations},
    test_utils::setup_test_db,
    DatabaseConnectionProvider, L1MessageKey,
};
use alloy_primitives::{Address, Bytes, B256, U256};
use dogeos_protocol_types::TxL1Message;
use rollup_node_primitives::{BatchCommitData, BlockInfo, L1BlockStartupInfo, L1MessageEnvelope};
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
use std::sync::Arc;

fn message(queue_index: u64, l1_block: u64, amount: u64) -> L1MessageEnvelope {
    L1MessageEnvelope::new(
        TxL1Message {
            queue_index,
            gas_limit: 100_000,
            to: Address::repeat_byte(0x22),
            value: U256::from(amount),
            sender: Address::repeat_byte(0x11),
            input: Bytes::new(),
        },
        l1_block,
        None,
        None,
    )
}

fn batch(index: u64, tag: u8, l1_block: u64) -> BatchCommitData {
    BatchCommitData {
        hash: B256::repeat_byte(tag),
        index,
        block_number: l1_block,
        block_timestamp: 1_000 + l1_block,
        calldata: Arc::new(Bytes::from(vec![tag])),
        blob_versioned_hash: None,
        finalized_block_number: None,
        reverted_block_number: None,
    }
}

/// F1: `Reorg(head - 1)` unwinds only the head block's messages. A replacement message for a
/// lower replaced block (same queue index, new content) is then dropped by `insert_l1_message`
/// (`on_conflict_do_nothing`), so the old content stays.
#[tokio::test]
async fn rg41_head_minus_one_unwind_keeps_lower_replaced_message() {
    let db = setup_test_db().await;
    for (queue_index, l1_block) in [(0, 1), (1, 2), (2, 3)] {
        db.insert_l1_message(message(queue_index, l1_block, 1_000)).await.unwrap();
    }

    // The watcher reports Reorg(2) for a replacement of blocks 2 and 3 (fork point: block 1).
    db.unwind(2).await.unwrap();
    let remaining =
        db.get_n_l1_messages(Some(L1MessageKey::from_queue_index(0)), 10).await.unwrap();
    assert_eq!(
        remaining
            .iter()
            .map(|m| (m.transaction.queue_index, m.l1_block_number))
            .collect::<Vec<_>>(),
        vec![(0, 1), (1, 2)],
        "block 2's message survives the unwind"
    );

    // The replacement for queue index 1 (block 2', new amount) is silently ignored.
    db.insert_l1_message(message(1, 2, 2_000)).await.unwrap();
    let stored = db.get_n_l1_messages(Some(L1MessageKey::from_queue_index(1)), 1).await.unwrap();
    assert_eq!(stored[0].transaction.value, U256::from(1_000), "old content kept");
    assert_eq!(stored[0].transaction.queue_index, 1);
}

/// The orchestrator's gap check asks for "one message at or above `q - 1`", not for `q - 1`
/// itself.
#[tokio::test]
async fn rg43_gap_check_query_is_at_or_above() {
    let db = setup_test_db().await;
    db.insert_l1_message(message(0, 1, 1_000)).await.unwrap();
    // Queue index 1 is missing: the check for q = 2 finds nothing at or above 1.
    assert!(db
        .get_n_l1_messages(Some(L1MessageKey::from_queue_index(1)), 1)
        .await
        .unwrap()
        .is_empty());
    // A stored message above the gap satisfies the same check.
    db.insert_l1_message(message(5, 4, 1_000)).await.unwrap();
    let found = db.get_n_l1_messages(Some(L1MessageKey::from_queue_index(1)), 1).await.unwrap();
    assert_eq!(found[0].transaction.queue_index, 5);
}

/// Case 18: the batch table's `index` is not unique (only `hash` is). A replacement batch with
/// the same index and a new hash is inserted as a second row; `get_batch_by_index` (no ORDER BY)
/// returns one of them (`SQLite` returns the older row) and `finalize_batches_up_to_index`
/// finalizes both.
#[tokio::test]
async fn rg44_replaced_batch_with_same_index_keeps_both_rows() {
    let db = setup_test_db().await;
    let old = batch(1, 0xaa, 10);
    let replacement = batch(1, 0xbb, 11);
    db.insert_batch(old.clone()).await.unwrap();
    db.insert_batch(replacement.clone()).await.unwrap();

    let rows = models::batch_commit::Entity::find()
        .filter(models::batch_commit::Column::Index.eq(1i64))
        .all(db.inner().get_connection())
        .await
        .unwrap();
    assert_eq!(rows.len(), 2, "both rows for batch index 1 are stored");

    let by_index = db.get_batch_by_index(1).await.unwrap().unwrap();
    assert_eq!(by_index.hash, old.hash, "get_batch_by_index returns the old row");

    db.finalize_batches_up_to_index(1, 12).await.unwrap();
    for hash in [old.hash, replacement.hash] {
        let row = db.get_batch_by_hash(hash).await.unwrap().unwrap();
        assert_eq!(row.finalized_block_number, Some(12), "row {hash} finalized");
    }
}

/// Case 20: with finalized = head, `insert_l1_block_info` skips every block (they are all at or
/// below the finalized block), so the `l1_block` table stays empty and a restart starts from the
/// block of the highest stored message or batch, with no unsafe-block reorg check.
#[tokio::test]
async fn rg41_finalized_head_leaves_l1_block_table_empty() {
    let db = setup_test_db().await;
    // The watcher's Finalized(5) is handled before block 5's logs.
    db.set_finalized_l1_block_number(5).await.unwrap();
    db.insert_l1_block_info(BlockInfo { number: 5, hash: B256::repeat_byte(5) }).await.unwrap();
    db.insert_l1_block_info(BlockInfo { number: 4, hash: B256::repeat_byte(4) }).await.unwrap();
    assert!(db.get_l1_block_info().await.unwrap().is_empty());

    db.insert_l1_message(message(0, 3, 1_000)).await.unwrap();
    db.insert_batch(batch(1, 0xaa, 5)).await.unwrap();
    assert_eq!(
        db.prepare_l1_watcher_start_info().await.unwrap(),
        L1BlockStartupInfo::FinalizedBlockNumber(5)
    );
}
