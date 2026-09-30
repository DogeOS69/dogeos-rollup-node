//! RG-106: how a genesis file becomes a fork schedule, and what the devp2p
//! fork ID says about it.
//!
//! Three facts about the pinned `dogeos-chainspec` are pinned here, using the
//! repository's own e2e genesis (`tests/l2reth-genesis-e2e.json`, which sets
//! `galileoTime` but no `tsukiTime`):
//!
//! 1. A genesis without `tsukiTime` never activates Tsuki. The chainspec treats a missing hardfork
//!    time as `ForkCondition::Never` on purpose.
//! 2. A `tsukiTime` in the genesis schedules Tsuki at that timestamp.
//! 3. The fork ID and the fork filter leave timestamp forks out (inherited Scroll behaviour). Two
//!    nodes whose genesis files differ only in `tsukiTime` therefore accept each other at the
//!    handshake, and they split at the first block whose timestamp reaches the earlier activation.
//!
//! These are characterization tests for a design choice. They do not say the
//! choice is wrong; they make the consequence visible so a deployment can be
//! checked for matching fork times on every node before genesis.

use alloy_genesis::Genesis;
use dogeos_chainspec::DogeosChainSpec;
use dogeos_hardforks::{DogeosHardfork, DogeosHardforks};
use reth_chainspec::{ForkCondition, Hardforks, Head};

/// The e2e genesis: Galileo at 0, no `tsukiTime`.
const BASE_GENESIS: &str = include_str!("../../../tests/l2reth-genesis-e2e.json");

fn chain_spec(tsuki_time: Option<u64>) -> DogeosChainSpec {
    let mut genesis: Genesis = serde_json::from_str(BASE_GENESIS).expect("valid base genesis");
    assert!(
        !genesis.config.extra_fields.contains_key("tsukiTime"),
        "the base genesis must not set tsukiTime"
    );
    if let Some(time) = tsuki_time {
        genesis.config.extra_fields.insert("tsukiTime".to_string(), serde_json::json!(time));
    }
    DogeosChainSpec::from_custom_genesis(genesis)
}

fn head_at(timestamp: u64) -> Head {
    Head { number: 0, timestamp, ..Default::default() }
}

/// A missing `tsukiTime` is `Never`, however late the timestamp.
#[test]
fn rg106_a_genesis_without_tsuki_time_never_activates_tsuki() {
    let spec = chain_spec(None);
    assert_eq!(spec.dogeos_fork_activation(DogeosHardfork::Tsuki), ForkCondition::Never);
    assert!(!spec.is_tsuki_active_at_timestamp(u64::MAX));
    // The same genesis does schedule the forks it names.
    assert_eq!(spec.dogeos_fork_activation(DogeosHardfork::Galileo), ForkCondition::Timestamp(0));
}

/// Control: a `tsukiTime` schedules Tsuki at exactly that timestamp.
#[test]
fn rg106_a_tsuki_time_in_the_genesis_schedules_tsuki() {
    let spec = chain_spec(Some(1_000));
    assert_eq!(spec.dogeos_fork_activation(DogeosHardfork::Tsuki), ForkCondition::Timestamp(1_000));
    assert!(!spec.is_tsuki_active_at_timestamp(999));
    assert!(spec.is_tsuki_active_at_timestamp(1_000));
}

/// Two genesis files that differ only in `tsukiTime` have the same fork ID at
/// every head, and each node's fork filter accepts the other's fork ID. The
/// handshake cannot tell them apart, yet they disagree about Tsuki from
/// timestamp 1000 on.
#[test]
fn rg106_peers_with_different_tsuki_times_pass_the_fork_id_check() {
    let never = chain_spec(None);
    let at_1000 = chain_spec(Some(1_000));
    assert_ne!(
        never.dogeos_fork_activation(DogeosHardfork::Tsuki),
        at_1000.dogeos_fork_activation(DogeosHardfork::Tsuki),
        "the two schedules really differ"
    );

    for timestamp in [0, 999, 1_000, 5_000] {
        let head = head_at(timestamp);
        assert_eq!(
            never.fork_id(&head),
            at_1000.fork_id(&head),
            "fork ID must not depend on the Tsuki time (timestamp {timestamp})"
        );
        never
            .fork_filter(head)
            .validate(at_1000.fork_id(&head))
            .expect("the node without Tsuki accepts the peer that schedules it");
        at_1000
            .fork_filter(head)
            .validate(never.fork_id(&head))
            .expect("the node that schedules Tsuki accepts the peer without it");
    }

    // From timestamp 1000 the two nodes apply different rules.
    assert!(at_1000.is_tsuki_active_at_timestamp(1_000));
    assert!(!never.is_tsuki_active_at_timestamp(1_000));
}
