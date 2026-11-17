//! Shared fixture setup for the benchmark and its local-blob regression.

use alloy_primitives::{Bytes, B256};
use dogeos_protocol_types::TxL1Message;
use futures::StreamExt;
use rollup_node_primitives::{BatchCommitData, L1MessageEnvelope};
use rollup_node_providers::{test_utils::MockL1Provider, L1Provider};
use scroll_db::{
    test_utils::setup_test_db, Database, DatabaseConnectionProvider, DatabaseWriteOperations,
    EntityTrait,
};
use scroll_derivation_pipeline::DerivationPipeline;
use std::{collections::HashMap, future::Future, path::PathBuf, pin::Pin, sync::Arc};

fn testdata() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("benches/testdata")
}

pub(super) const BATCHES_START_INDEX: u64 = 414261;
pub(super) const BATCHES_STOP_INDEX: u64 = 414513;

/// Set up a mock provider instance.
pub(super) fn setup_mock_provider(
    db: Arc<Database>,
) -> Pin<Box<dyn Future<Output = MockL1Provider<Arc<Database>>> + Send>> {
    Box::pin(async {
        let mut blobs = HashMap::new();
        let db_inner = db.inner();
        let mut batches = scroll_db::batch_commit::Entity::find()
            .stream(db_inner.get_connection())
            .await
            .expect("failed to get batches stream");

        while let Some(Ok(batch)) = batches.next().await {
            let batch = Into::<BatchCommitData>::into(batch);
            if let Some(blob_hash) = batch.blob_versioned_hash {
                blobs.insert(blob_hash, testdata().join(format!("blob/blob_{}.bin", batch.index)));
            }
        }

        MockL1Provider { db, blobs }
    })
}

/// The L1 provider factory function.
type L1ProviderFactory<P> =
    Box<dyn Fn(Arc<Database>) -> Pin<Box<dyn Future<Output = P> + Send>> + Send>;

/// Returns a pipeline with a provider initiated from the factory function.
pub(super) async fn setup_pipeline<P: L1Provider + Clone + Send + Sync + 'static>(
    factory: L1ProviderFactory<P>,
) -> DerivationPipeline {
    // load batch data in the db.
    let db = Arc::new(setup_test_db().await);
    let blob_hashes: Vec<B256> =
        serde_json::from_str(&std::fs::read_to_string(testdata().join("batch_info.json")).unwrap())
            .unwrap();

    for (index, hash) in (BATCHES_START_INDEX..=BATCHES_STOP_INDEX).zip(blob_hashes.into_iter()) {
        let raw_calldata =
            std::fs::read(testdata().join(format!("calldata/calldata_batch_{index}.bin"))).unwrap();
        let batch_data = BatchCommitData {
            hash: B256::random(),
            index,
            block_number: 18319648 + index,
            block_timestamp: 1696935971 + index,
            calldata: Arc::new(raw_calldata.into()),
            blob_versioned_hash: Some(hash),
            finalized_block_number: None,
            reverted_block_number: None,
        };
        db.insert_batch(batch_data).await.unwrap();
    }

    // These codec-8 fixture hashes are masked, and this is only a queue slice: preserve the
    // supplied legacy chain rather than recomputing either chain from zero.
    let l1_messages: Vec<_> = serde_json::from_str::<Vec<(Bytes, B256)>>(
        &std::fs::read_to_string(testdata().join("l1_messages.json")).unwrap(),
    )
    .unwrap()
    .into_iter()
    .map(|(bytes, queue_hash)| (TxL1Message::rlp_decode(&mut bytes.as_ref()).unwrap(), queue_hash))
    .map(|(tx, queue_hash)| {
        L1MessageEnvelope::new(tx, 0, None, None).with_legacy_queue_hash(Some(queue_hash))
    })
    .collect();

    for message in l1_messages {
        db.insert_l1_message(message).await.unwrap();
    }

    // construct the pipeline.
    let l1_provider = factory(db.clone()).await;
    DerivationPipeline::new(l1_provider, db, u64::MAX).await
}
