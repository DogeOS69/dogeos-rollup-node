//! A node syncing from genesis derives a history of batches below codec 12 that name their L1
//! messages by the legacy (masked) queue hash, then codec 12 batches that use the full-width one.

use super::*;
use alloy_eips::Encodable2718;
use dogeos_protocol_types::TxL1Message;
use rollup_node_providers::test_utils::MockL1Provider;
use scroll_db::{test_utils::setup_test_db, DatabaseWriteOperations};
use std::{collections::HashMap, path::PathBuf};

const BLOB_SIZE: usize = 131_072;

/// Encodes an uncompressed codec 7+ blob: one block per entry of `l1_messages_per_block`, no L2
/// transactions.
fn blob(
    version: u8,
    prev: B256,
    post: B256,
    first_block: u64,
    l1_messages_per_block: &[u16],
) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(prev.as_slice());
    payload.extend_from_slice(post.as_slice());
    payload.extend_from_slice(&first_block.to_be_bytes());
    payload.extend_from_slice(&(l1_messages_per_block.len() as u16).to_be_bytes());
    for (i, &l1_messages) in l1_messages_per_block.iter().enumerate() {
        payload.extend_from_slice(&(1_000 + first_block + i as u64).to_be_bytes()); // timestamp
        payload.extend_from_slice(&U256::from(1u64).to_be_bytes::<32>()); // base fee
        payload.extend_from_slice(&30_000_000u64.to_be_bytes()); // gas limit
        payload.extend_from_slice(&l1_messages.to_be_bytes()); // transactions
        payload.extend_from_slice(&l1_messages.to_be_bytes()); // of which L1 messages
    }
    let mut envelope = vec![version];
    envelope.extend_from_slice(&(payload.len() as u32).to_be_bytes()[1..]);
    envelope.push(0); // uncompressed
    envelope.extend_from_slice(&payload);

    // 31 data bytes per 32-byte field element.
    let mut blob = Vec::with_capacity(BLOB_SIZE);
    for chunk in envelope.chunks(31) {
        blob.push(0);
        blob.extend_from_slice(chunk);
        blob.resize(blob.len() + 31 - chunk.len(), 0);
    }
    blob.resize(BLOB_SIZE, 0);
    blob
}

struct Harness {
    db: Arc<Database>,
    blobs: HashMap<B256, PathBuf>,
    dir: PathBuf,
}

impl Harness {
    /// A committed batch whose blob is written to disk for the mock provider.
    fn batch(&mut self, index: u64, version: u8, blob: Vec<u8>) -> BatchCommitData {
        let blob_hash = B256::left_padding_from(&[version, index as u8]);
        let path = self.dir.join(format!("blob_{index}.bin"));
        std::fs::write(&path, blob).unwrap();
        self.blobs.insert(blob_hash, path);
        let mut calldata = vec![0u8; 4 + 32];
        calldata[4 + 31] = version;
        BatchCommitData {
            hash: B256::repeat_byte(index as u8),
            index,
            block_number: index,
            block_timestamp: 0,
            calldata: Arc::new(calldata.into()),
            blob_versioned_hash: Some(blob_hash),
            finalized_block_number: None,
            reverted_block_number: None,
        }
    }

    async fn derive(
        &self,
        batch: BatchCommitData,
    ) -> Result<BatchDerivationResult, DerivationPipelineError> {
        let provider = MockL1Provider { db: self.db.clone(), blobs: self.blobs.clone() };
        let cache =
            PreFetchCache::new(self.db.clone(), 100, Duration::from_secs(60), 10).await.unwrap();
        derive(batch, BatchStatus::Committed, provider, cache, 0).await
    }
}

fn l1_transactions(result: &BatchDerivationResult) -> Vec<Bytes> {
    result.attributes.iter().flat_map(|a| a.attributes.transactions.clone().unwrap()).collect()
}

#[tokio::test]
async fn fresh_sync_derives_legacy_hashed_batches_then_codec_12_batches() {
    let db = Arc::new(setup_test_db().await);
    let messages: Vec<TxL1Message> = (0..6u64)
        .map(|queue_index| TxL1Message {
            queue_index,
            gas_limit: 100_000,
            value: U256::from(queue_index + 1),
            ..Default::default()
        })
        .collect();
    for message in &messages {
        db.insert_l1_message(L1MessageEnvelope::new(message.clone(), 1, None, None)).await.unwrap();
    }
    // The node's rule on a chain whose TsukiV2 activates after genesis: both chains.
    db.recompute_l1_message_queue_hashes(0, true).await.unwrap();
    let stored = db.get_n_l1_messages(Some(L1MessageKey::from_queue_index(0)), 6).await.unwrap();
    let full = |i: usize| stored[i].queue_hash.unwrap();
    let legacy = |i: usize| stored[i].legacy_queue_hash.unwrap();
    let encoded = |range: std::ops::Range<usize>| -> Vec<Bytes> {
        messages[range].iter().map(|m| m.encoded_2718().into()).collect()
    };

    let dir = std::env::temp_dir().join(format!("queue-hash-derive-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let mut h = Harness { db, blobs: HashMap::new(), dir: dir.clone() };

    // Codec 11, first batch: the zero hash starts at the V2 queue start index.
    let batch = h.batch(1, 11, blob(11, B256::ZERO, legacy(1), 1, &[1, 1]));
    assert_eq!(l1_transactions(&h.derive(batch).await.unwrap()), encoded(0..2));
    // Codec 11 names its messages by the masked hash.
    let batch = h.batch(2, 11, blob(11, legacy(1), legacy(3), 3, &[2]));
    assert_eq!(l1_transactions(&h.derive(batch).await.unwrap()), encoded(2..4));
    // Codec 12 (TsukiV2) names them by the full-width hash.
    let batch = h.batch(3, 12, blob(12, full(3), full(5), 4, &[1, 1]));
    assert_eq!(l1_transactions(&h.derive(batch).await.unwrap()), encoded(4..6));

    // Each codec looks only in its own rule's column.
    let batch = h.batch(4, 12, blob(12, legacy(3), full(5), 4, &[2]));
    let err = h.derive(batch).await.unwrap_err();
    assert!(format!("{err:?}").contains("L1MessageNotFound(QueueHash("), "{err:?}");
    let batch = h.batch(5, 11, blob(11, full(3), legacy(5), 4, &[2]));
    let err = h.derive(batch).await.unwrap_err();
    assert!(format!("{err:?}").contains("L1MessageNotFound(LegacyQueueHash("), "{err:?}");

    std::fs::remove_dir_all(dir).unwrap();
}
