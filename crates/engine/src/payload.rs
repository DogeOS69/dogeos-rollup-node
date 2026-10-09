use alloy_primitives::B64;
use alloy_rpc_types_engine::ExecutionPayloadV1;
use dogeos_reth_engine::ScrollPayloadAttributes;
use reth_primitives_traits::{AlloyBlockHeader, Block, BlockBody};
use rollup_node_primitives::BlockInfo;

use tracing::debug;

/// Returns true if the [`Block`] matches the [`ScrollPayloadAttributes`]:
///    - all transactions match.
///    - timestamps are equal.
///    - `prev_randaos` are equal.
///    - `block_data_hint` matches the block data if present.
pub fn block_matches_attributes<B: Block>(attributes: &ScrollPayloadAttributes, block: &B) -> bool {
    let header = block.header();

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

/// Returns true when an Engine-built payload is the exact child requested by derived attributes.
/// This check runs before `engine_newPayload` so a builder operating on the wrong parent cannot
/// silently produce a different-but-valid state transition.
pub fn payload_matches_attributes(
    expected_parent: BlockInfo,
    expected_block_number: u64,
    attributes: &ScrollPayloadAttributes,
    payload: &ExecutionPayloadV1,
) -> bool {
    if payload.parent_hash != expected_parent.hash ||
        expected_parent.number.checked_add(1) != Some(payload.block_number) ||
        payload.block_number != expected_block_number
    {
        debug!(
            target: "scroll::engine::driver",
            ?expected_parent,
            expected_block_number,
            actual_parent = ?payload.parent_hash,
            actual_block_number = payload.block_number,
            "built payload has an unexpected parent or block number"
        );
        return false
    }

    if attributes.transactions.as_ref().is_none_or(|txs| txs != &payload.transactions) {
        debug!(
            target: "scroll::engine::driver",
            expected = ?attributes.transactions,
            got = ?payload.transactions,
            "built payload has unexpected transactions"
        );
        return false
    }

    let expected_fee_recipient = attributes
        .block_data_hint
        .coinbase
        .unwrap_or(attributes.payload_attributes.suggested_fee_recipient);
    if payload.timestamp != attributes.payload_attributes.timestamp ||
        payload.prev_randao != attributes.payload_attributes.prev_randao ||
        payload.fee_recipient != expected_fee_recipient
    {
        debug!(
            target: "scroll::engine::driver",
            "built payload does not match base payload attributes"
        );
        return false
    }

    let hint = &attributes.block_data_hint;
    if hint.extra_data.as_ref().is_some_and(|value| value != &payload.extra_data) ||
        hint.state_root.is_some_and(|value| value != payload.state_root) ||
        attributes.gas_limit.is_some_and(|value| value != payload.gas_limit)
    {
        debug!(
            target: "scroll::engine::driver",
            "built payload does not match derived block data"
        );
        return false
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
                ..Default::default()
            },
            body: alloy_consensus::BlockBody { transactions, ..Default::default() },
        };

        assert!(block_matches_attributes(&attributes, &block));

        Ok(())
    }

    #[test]
    fn test_mismatched_payloads() -> eyre::Result<()> {
        let mut bytes = [0u8; 1024];
        generators::rng().fill(bytes.as_mut_slice());
        let mut unstructured = Unstructured::new(&bytes);

        let parent_hash = B256::arbitrary(&mut unstructured)?;
        let transactions = Vec::<ScrollTxEnvelope>::arbitrary(&mut unstructured)?;
        let prev_randao = B256::arbitrary(&mut unstructured)?;
        let timestamp = u64::arbitrary(&mut unstructured)?;
        let difficulty = U256::arbitrary(&mut unstructured)?;
        let extra_data = Bytes::arbitrary(&mut unstructured)?;

        let attributes = ScrollPayloadAttributes::default();
        let block = DogeosBlock {
            header: Header {
                parent_hash,
                timestamp,
                difficulty,
                extra_data,
                mix_hash: prev_randao,
                ..Default::default()
            },
            body: alloy_consensus::BlockBody { transactions, ..Default::default() },
        };

        assert!(!block_matches_attributes(&attributes, &block));

        Ok(())
    }
}
