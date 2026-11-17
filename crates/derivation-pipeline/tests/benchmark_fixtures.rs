//! Exercise the benchmark's exact setup with local codec-8 blobs under normal cargo test.

#[path = "../benches/support/mod.rs"]
mod support;

use dogeos_protocol_types::L1_MESSAGE_TX_TYPE_ID;
use futures::StreamExt;
use rollup_node_primitives::{BatchInfo, BatchStatus};
use std::time::Duration;
use support::{setup_mock_provider, setup_pipeline, BATCHES_START_INDEX, BATCHES_STOP_INDEX};

#[tokio::test]
async fn benchmark_fixtures_derive_all_local_batches() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let mut pipeline = setup_pipeline(Box::new(setup_mock_provider)).await;
        for index in BATCHES_START_INDEX..=BATCHES_STOP_INDEX {
            pipeline
                .push_batch(BatchInfo { index, hash: Default::default() }, BatchStatus::Committed)
                .await;
        }
        let mut blocks = 0;
        let mut messages = 0;
        for index in BATCHES_START_INDEX..=BATCHES_STOP_INDEX {
            let result = pipeline.next().await.expect("pipeline must derive each fixture batch");
            assert_eq!(result.batch_info.index, index);
            assert_eq!(result.target_status, BatchStatus::Committed);
            blocks += result.attributes.len();
            for block in result.attributes {
                messages += block
                    .attributes
                    .transactions
                    .iter()
                    .flatten()
                    .filter(|tx| tx.first() == Some(&L1_MESSAGE_TX_TYPE_ID))
                    .count();
            }
        }
        assert!(pipeline.is_empty());
        assert!(blocks > 0);
        assert!(messages > 0, "local fixtures must exercise L1 message lookup");
    })
    .await
    .expect("benchmark fixtures must derive without retrying missing queue hashes");
}
