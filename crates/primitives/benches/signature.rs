//! Benchmarks for the block signature primitives.

#![allow(missing_docs)]

use alloy_consensus::Header;
use alloy_primitives::{address, b256, Bloom, Bytes, B64, U256};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use rollup_node_primitives::sig_encode_hash;
use std::hint::black_box;

/// Returns a header representative of a Scroll L2 block header.
fn header() -> Header {
    Header {
        parent_hash: b256!("d4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3"),
        ommers_hash: b256!("1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347"),
        beneficiary: address!("5300000000000000000000000000000000000005"),
        state_root: b256!("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421"),
        transactions_root: b256!(
            "56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421"
        ),
        receipts_root: b256!("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421"),
        logs_bloom: Bloom::repeat_byte(0x11),
        difficulty: U256::from(1u64),
        number: 12_345_678,
        gas_limit: 10_000_000,
        gas_used: 8_123_456,
        timestamp: 1_696_935_971,
        extra_data: Bytes::from_static(&[0x0d; 97]),
        mix_hash: b256!("0000000000000000000000000000000000000000000000000000000000000000"),
        nonce: B64::ZERO,
        base_fee_per_gas: Some(1_000_000_000),
        ..Default::default()
    }
}

/// Benchmark the signature encoding and hashing of a block header, which is performed for every
/// block that is signed or verified by the node.
fn bench_sig_encode_hash(c: &mut Criterion) {
    let mut group = c.benchmark_group("primitives_sig_encode_hash");

    let with_base_fee = header();
    group.bench_with_input(
        BenchmarkId::from_parameter("with_base_fee"),
        &with_base_fee,
        |b, header| b.iter(|| black_box(sig_encode_hash(black_box(header)))),
    );

    let without_base_fee = Header { base_fee_per_gas: None, ..header() };
    group.bench_with_input(
        BenchmarkId::from_parameter("without_base_fee"),
        &without_base_fee,
        |b, header| b.iter(|| black_box(sig_encode_hash(black_box(header)))),
    );

    group.finish();
}

criterion_group!(benches, bench_sig_encode_hash);
criterion_main!(benches);
