//! L1 message queue hash rules.
//!
//! Every L1 message from the V2 queue start index onward has a rolling queue hash:
//! `hash_i = fold(hash_{i-1}, tx_hash_i)`, starting from zero. A committed batch names the L1
//! messages it consumes by the queue hash before its first message.
//!
//! Two rules exist:
//! - Full width (`DogeOS`): keep all 256 bits at every step. Batches of codec version 12
//!   (`TsukiV2`) and later use it.
//! - Legacy (Scroll's upstream rule): clear the last 4 bytes at every step. Batches below codec
//!   version 12 use it.
//!
//! Testnet-only: the legacy rule exists because `Chikyu` (`DogeOS` testnet) committed its batches
//! below codec 12 with masked queue hashes before `TsukiV2`, and a node that syncs from genesis
//! must still find their L1 messages. It is computed only on a chain whose `TsukiV2` activates
//! after genesis, so it is inert on mainnet (`TsukiV2` at genesis). Added by dogeos-rollup-node
//! #107. Remove it when no supported network has batches below codec 12,
//! together with: the `legacy_queue_hash` column and envelope field,
//! `L1MessageKey::LegacyQueueHash` and `from_batch_queue_hash`, the orchestrator's
//! `with_legacy_queue_hash` config and the legacy part of `recompute_l1_message_queue_hashes`.

use alloy_primitives::{b256, keccak256, B256};

/// The first codec version whose batches name L1 messages by the full-width queue hash.
pub const FIRST_FULL_WIDTH_QUEUE_HASH_CODEC_VERSION: u8 = 12;

/// The legacy mask: Scroll's `L1MessageQueueV2` clears the last 4 bytes of the queue hash,
/// where it stores the enqueue timestamp.
const LEGACY_QUEUE_HASH_MASK: B256 =
    b256!("ffffffffffffffffffffffffffffffffffffffffffffffffffffffff00000000");

/// Returns true if a batch of the given codec version names its L1 messages by the legacy
/// (masked) queue hash.
pub const fn uses_legacy_queue_hash(codec_version: u8) -> bool {
    codec_version < FIRST_FULL_WIDTH_QUEUE_HASH_CODEC_VERSION
}

/// Folds a transaction hash into the full-width queue hash.
pub fn fold_queue_hash(previous: B256, tx_hash: B256) -> B256 {
    let mut input = [0u8; 64];
    input[..32].copy_from_slice(previous.as_slice());
    input[32..].copy_from_slice(tx_hash.as_slice());
    keccak256(input)
}

/// Folds a transaction hash into the legacy (masked) queue hash.
///
/// See the module docs for why this exists and when it can be removed. The upstream Solidity
/// rule: <https://github.com/scroll-tech/scroll-contracts/blob/67c1bde19c1d3462abf8c175916a2bb3c89530e4/src/L1/rollup/L1MessageQueueV2.sol#L379-L403>
pub fn fold_legacy_queue_hash(previous: B256, tx_hash: B256) -> B256 {
    fold_queue_hash(previous, tx_hash) & LEGACY_QUEUE_HASH_MASK
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_rule_masks_the_last_four_bytes_and_chains_on_the_masked_value() {
        let tx_a = B256::repeat_byte(0xaa);
        let tx_b = B256::repeat_byte(0xbb);

        let full_a = fold_queue_hash(B256::ZERO, tx_a);
        let legacy_a = fold_legacy_queue_hash(B256::ZERO, tx_a);
        assert_eq!(&legacy_a[..28], &full_a[..28]);
        assert_eq!(&legacy_a[28..], &[0u8; 4]);
        assert_ne!(legacy_a, full_a);

        // The legacy chain folds the masked previous hash, not the full-width one.
        let legacy_b = fold_legacy_queue_hash(legacy_a, tx_b);
        assert_eq!(legacy_b, fold_queue_hash(legacy_a, tx_b) & LEGACY_QUEUE_HASH_MASK);
        assert_ne!(&legacy_b[..28], &fold_queue_hash(full_a, tx_b)[..28]);
    }

    /// Expected values computed outside this code with `cast keccak` (foundry): full width
    /// `keccak(prev || tx)`, legacy the same with the last 4 bytes cleared at every step.
    #[test]
    fn folds_match_independently_computed_vectors() {
        let tx_a = B256::repeat_byte(0xaa);
        let tx_b = B256::repeat_byte(0xbb);
        let full_1 = b256!("e60eb41b492e32a28ec6d7ba5db66f1206fcdf79cf8042e94129c92deaf6ffbe");
        let full_2 = b256!("5a3796172fe281d0caa0bacaeb99d54d09ef721cf75a7f455f89a117425c07f0");
        let legacy_1 = b256!("e60eb41b492e32a28ec6d7ba5db66f1206fcdf79cf8042e94129c92d00000000");
        let legacy_2 = b256!("d8609d3f6a6926a698428efe07f18f7ace012755973eef0b18e4836a00000000");
        assert_eq!(fold_queue_hash(B256::ZERO, tx_a), full_1);
        assert_eq!(fold_queue_hash(full_1, tx_b), full_2);
        assert_eq!(fold_legacy_queue_hash(B256::ZERO, tx_a), legacy_1);
        assert_eq!(fold_legacy_queue_hash(legacy_1, tx_b), legacy_2);
    }

    #[test]
    fn codec_twelve_is_the_first_full_width_version() {
        assert!(uses_legacy_queue_hash(7));
        assert!(uses_legacy_queue_hash(11));
        assert!(!uses_legacy_queue_hash(12));
        assert!(!uses_legacy_queue_hash(13));
    }
}
