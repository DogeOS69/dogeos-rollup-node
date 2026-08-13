//! Benchmarks for the codec decoding.

#![allow(missing_docs)]

use alloy_primitives::Bytes;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use scroll_codec::decoding::{
    v0::decode_v0, v1::decode_v1, v2::decode_v2, v4::decode_v4, v7::decode_v7,
};
use std::{hint::black_box, path::Path, str::FromStr};

/// Reads the hex encoded file at `path` and returns the corresponding [`Bytes`].
fn read_to_bytes<P: AsRef<Path>>(path: P) -> Bytes {
    let path = path.as_ref();
    let content = std::fs::read_to_string(path)
        .unwrap_or_else(|err| panic!("failed to read {}: {err}", path.display()));
    Bytes::from_str(content.trim()).expect("failed to parse hex encoded test data")
}

/// Benchmark the decoding of the calldata only codec version.
fn bench_decode_v0(c: &mut Criterion) {
    let mut group = c.benchmark_group("codec_decode_v0");

    for name in ["calldata_v0", "calldata_v0_with_l1_messages"] {
        let calldata = read_to_bytes(format!("./testdata/{name}.bin"));
        group.bench_with_input(BenchmarkId::from_parameter(name), &calldata, |b, calldata| {
            b.iter(|| black_box(decode_v0(black_box(calldata)).unwrap()))
        });
    }

    group.finish();
}

/// Benchmark the decoding of the blob based codec versions which don't use compression.
fn bench_decode_v1(c: &mut Criterion) {
    let mut group = c.benchmark_group("codec_decode_v1");

    for name in ["v1", "v1_with_l1_messages"] {
        let calldata = read_to_bytes(format!("./testdata/calldata_{name}.bin"));
        let blob = read_to_bytes(format!("./testdata/blob_{name}.bin"));
        group.bench_with_input(
            BenchmarkId::from_parameter(name),
            &(calldata, blob),
            |b, (calldata, blob)| {
                b.iter(|| black_box(decode_v1(black_box(calldata), black_box(blob)).unwrap()))
            },
        );
    }

    group.finish();
}

/// Benchmark the decoding of the zstd compressed blob based codec version.
fn bench_decode_v2(c: &mut Criterion) {
    let calldata = read_to_bytes("./testdata/calldata_v2.bin");
    let blob = read_to_bytes("./testdata/blob_v2.bin");

    c.bench_function("codec_decode_v2", |b| {
        b.iter(|| black_box(decode_v2(black_box(&calldata), black_box(&blob)).unwrap()))
    });
}

/// Benchmark the decoding of the codec version supporting both compressed and uncompressed blobs.
fn bench_decode_v4(c: &mut Criterion) {
    let mut group = c.benchmark_group("codec_decode_v4");

    for name in ["compressed", "uncompressed"] {
        let calldata = read_to_bytes(format!("./testdata/calldata_v4_{name}.bin"));
        let blob = read_to_bytes(format!("./testdata/blob_v4_{name}.bin"));
        group.bench_with_input(
            BenchmarkId::from_parameter(name),
            &(calldata, blob),
            |b, (calldata, blob)| {
                b.iter(|| black_box(decode_v4(black_box(calldata), black_box(blob)).unwrap()))
            },
        );
    }

    group.finish();
}

/// Benchmark the decoding of the codec versions where all the data lives in the blob.
fn bench_decode_v7(c: &mut Criterion) {
    let mut group = c.benchmark_group("codec_decode_v7");

    for name in ["blob_v7_compressed", "blob_v7_uncompressed", "blob_v8_compressed"] {
        let blob = read_to_bytes(format!("./testdata/{name}.bin"));
        group.bench_with_input(BenchmarkId::from_parameter(name), &blob, |b, blob| {
            b.iter(|| black_box(decode_v7(black_box(blob)).unwrap()))
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_decode_v0,
    bench_decode_v1,
    bench_decode_v2,
    bench_decode_v4,
    bench_decode_v7
);
criterion_main!(benches);
