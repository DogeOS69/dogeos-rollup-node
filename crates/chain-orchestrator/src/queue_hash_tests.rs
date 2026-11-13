//! L1 message queue hashes stored by the orchestrator: the full-width chain always, the legacy
//! (masked) chain only when configured, and both kept consistent across an L1 reorg.

use super::{
    reorg_finding_tests::{deposit, info, orchestrator_probed, Probes},
    *,
};
use alloy_primitives::U256;
use scroll_db::test_utils::setup_test_db;
use scroll_engine::ForkchoiceState;

async fn stored(database: &Database) -> Vec<L1MessageEnvelope> {
    database.get_n_l1_messages(Some(L1MessageKey::from_queue_index(0)), 100).await.unwrap()
}

/// Recomputes both chains from zero over the stored messages and checks every stored hash.
fn assert_chains(messages: &[L1MessageEnvelope], with_legacy: bool) {
    let (mut full, mut legacy) = (B256::ZERO, B256::ZERO);
    for (i, message) in messages.iter().enumerate() {
        assert_eq!(message.transaction.queue_index, i as u64);
        let tx_hash = message.transaction.tx_hash();
        full = fold_queue_hash(full, tx_hash);
        legacy = fold_legacy_queue_hash(legacy, tx_hash);
        assert_eq!(message.queue_hash, Some(full), "full-width hash of message {i}");
        assert_eq!(
            message.legacy_queue_hash,
            with_legacy.then_some(legacy),
            "legacy hash of message {i}"
        );
    }
}

#[tokio::test]
async fn live_messages_store_both_chains_and_an_l1_reorg_keeps_them_consistent() {
    let database = Arc::new(setup_test_db().await);
    let probes = Probes { with_legacy_queue_hash: true, ..Default::default() };
    let orchestrator =
        orchestrator_probed(database.clone(), ForkchoiceState::from_genesis(B256::ZERO), 0, probes)
            .await;

    // Messages 0..=4 in L1 blocks 1..=5.
    for queue_index in 0..5u64 {
        orchestrator
            .handle_l1_message(deposit(queue_index), info(queue_index + 1, queue_index as u8 + 1))
            .await
            .unwrap();
    }
    let before = stored(&database).await;
    assert_eq!(before.len(), 5);
    assert_chains(&before, true);

    // An L1 reorg to block 3 removes messages 3 and 4; the replacement chain carries different
    // messages at the same indices.
    database.unwind(3).await.unwrap();
    assert_eq!(stored(&database).await.len(), 3);
    for queue_index in 3..5u64 {
        let replacement =
            TxL1Message { value: U256::from(7_777 + queue_index), ..deposit(queue_index) };
        orchestrator
            .handle_l1_message(replacement, info(queue_index + 1, 0x40 + queue_index as u8))
            .await
            .unwrap();
    }
    let after = stored(&database).await;
    assert_eq!(after.len(), 5);
    assert_chains(&after, true);
    assert_eq!(after[..3], before[..3]);
    assert_ne!(after[3].queue_hash, before[3].queue_hash);
    assert_ne!(after[3].legacy_queue_hash, before[3].legacy_queue_hash);

    // The removed messages' hashes are gone from both columns; the replacements are found.
    for (old, new) in before[3..].iter().zip(&after[3..]) {
        for key in [
            L1MessageKey::QueueHash(old.queue_hash.unwrap()),
            L1MessageKey::LegacyQueueHash(old.legacy_queue_hash.unwrap()),
        ] {
            assert!(database.get_n_l1_messages(Some(key.clone()), 1).await.is_err(), "{key}");
        }
        for key in [
            L1MessageKey::QueueHash(new.queue_hash.unwrap()),
            L1MessageKey::LegacyQueueHash(new.legacy_queue_hash.unwrap()),
        ] {
            assert_eq!(database.get_n_l1_messages(Some(key), 1).await.unwrap()[0], *new);
        }
    }

    // The startup recompute derives the same values as the live path.
    assert_eq!(database.recompute_l1_message_queue_hashes(0, true).await.unwrap(), Some(5));
    assert_eq!(stored(&database).await, after);
}

#[tokio::test]
async fn legacy_chain_is_not_stored_unless_configured() {
    let database = Arc::new(setup_test_db().await);
    let orchestrator = orchestrator_probed(
        database.clone(),
        ForkchoiceState::from_genesis(B256::ZERO),
        0,
        Probes::default(),
    )
    .await;
    for queue_index in 0..3u64 {
        orchestrator
            .handle_l1_message(deposit(queue_index), info(queue_index + 1, queue_index as u8 + 1))
            .await
            .unwrap();
    }
    let messages = stored(&database).await;
    assert_eq!(messages.len(), 3, "every message lands with a NULL legacy hash");
    assert_chains(&messages, false);
}

/// With a non-zero V2 start index, messages below it have no queue hash in either column, and
/// the message at the start index folds zero in both chains. The startup recompute agrees.
#[tokio::test]
async fn messages_below_the_v2_start_index_have_no_hash_and_the_chains_start_at_it() {
    let v2_start = 2;
    let database = Arc::new(setup_test_db().await);
    let probes = Probes { with_legacy_queue_hash: true, ..Default::default() };
    let orchestrator = orchestrator_probed(
        database.clone(),
        ForkchoiceState::from_genesis(B256::ZERO),
        v2_start,
        probes,
    )
    .await;
    for queue_index in 0..5u64 {
        orchestrator
            .handle_l1_message(deposit(queue_index), info(queue_index + 1, queue_index as u8 + 1))
            .await
            .unwrap();
    }
    let messages = stored(&database).await;
    assert_eq!(messages.len(), 5);
    for message in &messages[..2] {
        assert_eq!((message.queue_hash, message.legacy_queue_hash), (None, None));
    }
    let tx_2 = messages[2].transaction.tx_hash();
    assert_eq!(messages[2].queue_hash, Some(fold_queue_hash(B256::ZERO, tx_2)));
    assert_eq!(messages[2].legacy_queue_hash, Some(fold_legacy_queue_hash(B256::ZERO, tx_2)));
    let tx_3 = messages[3].transaction.tx_hash();
    assert_eq!(
        messages[3].legacy_queue_hash,
        Some(fold_legacy_queue_hash(fold_legacy_queue_hash(B256::ZERO, tx_2), tx_3))
    );

    assert_eq!(database.recompute_l1_message_queue_hashes(v2_start, true).await.unwrap(), Some(3));
    assert_eq!(stored(&database).await, messages);
}

/// If the legacy chain is on but the previous message has no legacy hash (a database the
/// startup recompute did not cover), the message is refused instead of restarting the legacy
/// chain from zero.
#[tokio::test]
async fn a_missing_previous_legacy_hash_is_an_error_not_a_restart() {
    let database = Arc::new(setup_test_db().await);
    let without = orchestrator_probed(
        database.clone(),
        ForkchoiceState::from_genesis(B256::ZERO),
        0,
        Probes::default(),
    )
    .await;
    without.handle_l1_message(deposit(0), info(1, 1)).await.unwrap();

    let probes = Probes { with_legacy_queue_hash: true, ..Default::default() };
    let with =
        orchestrator_probed(database.clone(), ForkchoiceState::from_genesis(B256::ZERO), 0, probes)
            .await;
    let err = with.handle_l1_message(deposit(1), info(2, 2)).await.unwrap_err();
    assert!(matches!(err, ChainOrchestratorError::MissingLegacyQueueHash(1)), "{err:?}");
    assert_eq!(stored(&database).await.len(), 1);
}
