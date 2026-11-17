//! Benchmarks for the derivation pipeline.

#![allow(missing_docs)]

mod support;

use criterion::{criterion_group, criterion_main, Criterion};
use futures::StreamExt;
use rollup_node_primitives::{BatchInfo, BatchStatus};
use rollup_node_providers::{FullL1Provider, S3BlobProvider};
use scroll_db::Database;
use std::{future::Future, pin::Pin, sync::Arc};
use support::{setup_mock_provider, setup_pipeline, BATCHES_START_INDEX, BATCHES_STOP_INDEX};
use tokio::runtime::{Handle, Runtime};

/// Set up a full provider instance
fn setup_full_provider(
    db: Arc<Database>,
) -> Pin<Box<dyn Future<Output = FullL1Provider<Arc<Database>, S3BlobProvider>> + Send>> {
    Box::pin(async {
        let blob_provider = S3BlobProvider::new_http(
            reqwest::Url::parse("https://scroll-mainnet-blob-data.s3.us-west-2.amazonaws.com")
                .unwrap(),
        );
        FullL1Provider::new(blob_provider, db).await
    })
}

/// Benchmark the derivation pipeline with blobs fetched from file. This does not bench the network
/// call to the AWS S3 blob storage.
fn benchmark_pipeline_derivation_in_file_blobs(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    c.bench_function("pipeline_derive_in_file_blobs", |b| {
        b.to_async(&rt).iter_batched(
            || {
                let (tx, rx) = std::sync::mpsc::channel();
                Handle::current().spawn(async move {
                    // setup (not measured): create fresh pipeline with 253 committed batches
                    let mut pipeline = setup_pipeline(Box::new(setup_mock_provider)).await;

                    // commit 253 batches.
                    for index in BATCHES_START_INDEX..=BATCHES_STOP_INDEX {
                        let batch_info = BatchInfo { index, hash: Default::default() };
                        pipeline.push_batch(batch_info, BatchStatus::Committed).await;
                    }

                    tx.send(pipeline).unwrap();
                });
                rx.recv().unwrap()
            },
            |mut pipeline| async move {
                // measured work.
                for _ in BATCHES_START_INDEX..=BATCHES_STOP_INDEX {
                    let _ = pipeline.next().await;
                }
            },
            criterion::BatchSize::SmallInput,
        )
    });
}

/// Benchmark the derivation pipeline with blobs fetched from S3.
fn benchmark_pipeline_derivation_s3_blobs(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("pipeline_derive_s3_blobs");
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(20));

    group.bench_function("pipeline_derive_s3_blobs", |b| {
        b.to_async(&rt).iter_batched(
            || {
                let (tx, rx) = std::sync::mpsc::channel();
                Handle::current().spawn(async move {
                    // setup (not measured): create fresh pipeline with 15 committed batches
                    let mut pipeline = setup_pipeline(Box::new(setup_full_provider)).await;

                    // commit 15 batches.
                    for index in BATCHES_START_INDEX..=BATCHES_START_INDEX + 15 {
                        let batch_info = BatchInfo { index, hash: Default::default() };
                        pipeline.push_batch(batch_info, BatchStatus::Committed).await;
                    }

                    tx.send(pipeline).unwrap();
                });
                rx.recv().unwrap()
            },
            |mut pipeline| async move {
                // measured work.
                for _ in BATCHES_START_INDEX..=BATCHES_START_INDEX + 15 {
                    let _ = pipeline.next().await;
                }
            },
            criterion::BatchSize::LargeInput,
        )
    });
}

criterion_group!(
    benches,
    benchmark_pipeline_derivation_in_file_blobs,
    benchmark_pipeline_derivation_s3_blobs
);
criterion_main!(benches);
