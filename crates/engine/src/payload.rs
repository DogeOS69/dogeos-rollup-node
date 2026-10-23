use alloy_primitives::{B64, U256};
use dogeos_reth_engine::ScrollPayloadAttributes;
use reth_primitives_traits::{AlloyBlockHeader, Block, BlockBody};

use tracing::debug;

/// Returns true if the [`Block`] matches the [`ScrollPayloadAttributes`]:
///    - all transactions match.
///    - timestamps are equal.
///    - gas limits are equal when the attributes specify a gas limit.
///    - `prev_randaos` are equal.
///    - `block_data_hint` matches the block data if present.
///    - the base fee equals `base_fee`, the value the batch recorded for this block.
///
/// `base_fee` is carried next to the attributes rather than inside them, because the Engine API
/// does not take a base fee: the payload builder derives it from the parent and the system
/// config. A follower whose block disagrees with the batch must reorg to the DA version, so a
/// base-fee difference is a mismatch like any other.
pub fn block_matches_attributes<B: Block>(
    attributes: &ScrollPayloadAttributes,
    base_fee: U256,
    block: &B,
) -> bool {
    let header = block.header();

    if header.base_fee_per_gas().map(U256::from) != Some(base_fee) {
        debug!(
            target: "scroll::engine::driver",
            expected = ?base_fee,
            got = ?header.base_fee_per_gas(),
            "reorg: mismatch in base_fee"
        );
        return false;
    }

    if attributes.gas_limit.is_some_and(|gas_limit| gas_limit != header.gas_limit()) {
        debug!(
            target: "scroll::engine::driver",
            expected = ?attributes.gas_limit,
            got = header.gas_limit(),
            "reorg: mismatch in gas_limit"
        );
        return false;
    }

    let payload_transactions = &block.body().encoded_2718_transactions();
    let matching_transactions =
        attributes.transactions.as_ref().is_some_and(|v| v == payload_transactions);

    if !matching_transactions {
        debug!(
            target: "scroll::engine::driver",
            expected = ?attributes.transactions,
            got = ?payload_transactions,
            "reorg: mismatch in transactions"
        );
        return false;
    }

    if header.timestamp() != attributes.payload_attributes.timestamp {
        debug!(
            target: "scroll::engine::driver",
            expected = ?attributes.payload_attributes.timestamp,
            got = ?header.timestamp(),
            "reorg: mismatch in timestamp"
        );
        return false;
    }

    if header.mix_hash().unwrap_or_default() != attributes.payload_attributes.prev_randao {
        debug!(
            target: "scroll::engine::driver",
            expected = ?attributes.payload_attributes.prev_randao,
            got = ?header.mix_hash().unwrap_or_default(),
            "reorg: mismatch in prev_randao"
        );
        return false;
    }

    let block_data = &attributes.block_data_hint;
    if block_data.extra_data.as_ref().is_some_and(|ex| ex != header.extra_data()) {
        debug!(
            target: "scroll::engine::driver",
            expected = ?block_data.extra_data,
            got = ?header.extra_data(),
            "reorg: mismatch in extra_data"
        );
        return false;
    }
    if block_data.state_root.is_some_and(|d| d != header.state_root()) {
        debug!(
            target: "scroll::engine::driver",
            expected = ?block_data.state_root,
            got = ?header.state_root(),
            "reorg: mismatch in state_root"
        );
        return false;
    }
    if block_data.coinbase.is_some_and(|d| d != header.beneficiary()) {
        debug!(
            target: "scroll::engine::driver",
            expected = ?block_data.coinbase,
            got = ?header.beneficiary(),
            "reorg: mismatch in coinbase"
        );
        return false;
    }

    // nonce defaults to `Some(0x0000000000000000)` for `DogeosBlock`.
    if B64::from(block_data.nonce.unwrap_or_default()) != header.nonce().unwrap_or_default() {
        debug!(
            target: "scroll::engine::driver",
            expected = ?block_data.nonce,
            got = ?header.nonce(),
            "reorg: mismatch in nonce"
        );
        return false;
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;

    use alloy_consensus::Header;
    use alloy_eips::Encodable2718;
    use alloy_primitives::{Bytes, B256, U256};
    use alloy_rpc_types_engine::PayloadAttributes;
    use arbitrary::{Arbitrary, Unstructured};
    use dogeos_protocol_types::ScrollTxEnvelope;
    use dogeos_reth_engine::BlockDataHint;
    use dogeos_reth_primitives::DogeosBlock;
    use reth_testing_utils::{generators, generators::Rng};

    #[test]
    fn test_matching_payloads() -> eyre::Result<()> {
        let mut bytes = [0u8; 1024];
        generators::rng().fill(bytes.as_mut_slice());
        let mut unstructured = Unstructured::new(&bytes);

        let parent_hash = B256::arbitrary(&mut unstructured)?;
        let transactions = Vec::<ScrollTxEnvelope>::arbitrary(&mut unstructured)?;
        let encoded_transactions = transactions
            .clone()
            .into_iter()
            .map(|tx| tx.encoded_2718().into())
            .collect::<Vec<Bytes>>();
        let prev_randao = B256::arbitrary(&mut unstructured)?;
        let timestamp = u64::arbitrary(&mut unstructured)?;
        let base_fee = u64::arbitrary(&mut unstructured)?;
        let block_data_hint = BlockDataHint::default();

        let attributes = ScrollPayloadAttributes {
            payload_attributes: PayloadAttributes { timestamp, prev_randao, ..Default::default() },
            transactions: Some(encoded_transactions),
            block_data_hint: block_data_hint.clone(),
            ..Default::default()
        };

        let block = DogeosBlock {
            header: Header {
                parent_hash,
                timestamp,
                difficulty: block_data_hint.difficulty.unwrap_or_default(),
                nonce: block_data_hint.nonce.unwrap_or_default().into(),
                beneficiary: block_data_hint.coinbase.unwrap_or_default(),
                extra_data: block_data_hint.extra_data.unwrap_or_default(),
                state_root: block_data_hint.state_root.unwrap_or_default(),
                mix_hash: prev_randao,
                base_fee_per_gas: Some(base_fee),
                ..Default::default()
            },
            body: alloy_consensus::BlockBody { transactions, ..Default::default() },
        };

        assert!(block_matches_attributes(&attributes, U256::from(base_fee), &block));

        Ok(())
    }

    #[test]
    fn test_payload_gas_limit_equivalence() {
        let gas_limit = 30_000_000;
        let base_fee = U256::from(1_015_680_000u64);
        let block = DogeosBlock {
            header: Header {
                gas_limit,
                timestamp: 1_700_000_001,
                base_fee_per_gas: Some(base_fee.to()),
                ..Default::default()
            },
            body: Default::default(),
        };
        let mut attributes = ScrollPayloadAttributes {
            payload_attributes: PayloadAttributes {
                timestamp: block.header.timestamp,
                ..Default::default()
            },
            transactions: Some(vec![]),
            no_tx_pool: true,
            ..Default::default()
        };

        // Equal state roots cannot identify a header-only gas-limit difference.
        for state_root in [None, Some(block.header.state_root)] {
            attributes.block_data_hint.state_root = state_root;
            for (gas_limit, expected_match) in [
                (None, true),
                (Some(gas_limit), true),
                (Some(gas_limit - 1), false),
                (Some(gas_limit + 1), false),
            ] {
                attributes.gas_limit = gas_limit;
                assert_eq!(
                    block_matches_attributes(&attributes, base_fee, &block),
                    expected_match,
                    "gas_limit={gas_limit:?}, state_root={state_root:?}"
                );
            }
        }
    }

    /// The transaction lists differ, so the block is not the one the batch describes. Split
    /// from the timestamp case below: with the base fee compared too, a single test that got
    /// everything wrong at once would no longer say which comparison caught it.
    #[test]
    fn test_transaction_mismatch() -> eyre::Result<()> {
        let mut bytes = [0u8; 1024];
        generators::rng().fill(bytes.as_mut_slice());
        let mut unstructured = Unstructured::new(&bytes);

        let transactions = vec![ScrollTxEnvelope::arbitrary(&mut unstructured)?];

        // Everything else agrees: the attributes carry no transactions, the block carries some.
        let attributes =
            ScrollPayloadAttributes { transactions: Some(vec![]), ..Default::default() };
        let block = DogeosBlock {
            header: Header { base_fee_per_gas: Some(0), ..Default::default() },
            body: alloy_consensus::BlockBody { transactions, ..Default::default() },
        };

        assert!(!block_matches_attributes(&attributes, U256::ZERO, &block));
        Ok(())
    }

    #[test]
    fn test_timestamp_mismatch() {
        let attributes = ScrollPayloadAttributes {
            payload_attributes: PayloadAttributes { timestamp: 1, ..Default::default() },
            transactions: Some(vec![]),
            ..Default::default()
        };
        let block = |timestamp| DogeosBlock {
            header: Header { timestamp, base_fee_per_gas: Some(0), ..Default::default() },
            body: Default::default(),
        };

        assert!(block_matches_attributes(&attributes, U256::ZERO, &block(1)));
        assert!(!block_matches_attributes(&attributes, U256::ZERO, &block(2)));
    }

    /// A block that agrees with the batch in every other respect, but not on the base fee, is
    /// not a match: the follower has to reorg to the DA version.
    #[test]
    fn test_base_fee_mismatch_is_not_a_match() {
        let base_fee = U256::from(1_015_680_000u64);
        let attributes = ScrollPayloadAttributes {
            payload_attributes: PayloadAttributes { timestamp: 1, ..Default::default() },
            transactions: Some(vec![]),
            no_tx_pool: true,
            ..Default::default()
        };
        let block = |base_fee_per_gas| DogeosBlock {
            header: Header { timestamp: 1, base_fee_per_gas, ..Default::default() },
            body: Default::default(),
        };

        assert!(block_matches_attributes(&attributes, base_fee, &block(Some(base_fee.to()))));
        for wrong in [Some(base_fee.to::<u64>() - 1), Some(base_fee.to::<u64>() + 1), Some(0), None]
        {
            assert!(
                !block_matches_attributes(&attributes, base_fee, &block(wrong)),
                "base_fee_per_gas={wrong:?} was treated as a match"
            );

            // A batch whose decoded base fee does not fit in a header's `u64` cannot match any
            // block. The comparison is in `U256`, so it says so rather than truncating; whether
            // such a batch should be rejected as malformed earlier, at decode time, is a separate
            // question from this predicate.
            for out_of_range in [U256::from(u64::MAX) + U256::from(1), U256::MAX] {
                assert!(
                    !block_matches_attributes(&attributes, out_of_range, &block(Some(u64::MAX))),
                    "an out-of-range batch base fee {out_of_range} was treated as a match"
                );
            }
        }
    }
}
