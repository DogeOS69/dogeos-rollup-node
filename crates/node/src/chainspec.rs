//! `DogeOS` chain spec parsing for the rollup-node CLI.

use dogeos_chainspec::{DogeosChainSpec, DOGEOS_CHIKYU, DOGEOS_DEV, DOGEOS_MAINNET};
use reth_cli::chainspec::{parse_genesis, ChainSpecParser};
use std::sync::Arc;

/// Built-in chain names accepted by the rollup node.
pub const SUPPORTED_CHAINS: &[&str] = &["dogeos-mainnet", "dogeos-chikyu", "dev"];

/// Parses built-in `DogeOS` networks and custom genesis JSON files or strings.
#[derive(Clone, Debug, Default)]
pub struct DogeosChainSpecParser;

impl ChainSpecParser for DogeosChainSpecParser {
    type ChainSpec = DogeosChainSpec;

    const SUPPORTED_CHAINS: &'static [&'static str] = SUPPORTED_CHAINS;

    fn parse(value: &str) -> eyre::Result<Arc<Self::ChainSpec>> {
        Ok(match value {
            "dogeos-mainnet" => DOGEOS_MAINNET.clone(),
            "dogeos-chikyu" => DOGEOS_CHIKYU.clone(),
            "dev" => DOGEOS_DEV.clone(),
            _ => Arc::new(DogeosChainSpec::from_custom_genesis(parse_genesis(value)?)),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::b256;
    use dogeos_hardforks::DogeosHardforks;
    use reth_chainspec::EthChainSpec;

    #[test]
    fn chikyu_cli_matches_custom_genesis_loading() {
        let builtin = DogeosChainSpecParser::parse("dogeos-chikyu").unwrap();
        let json = serde_json::to_string(builtin.genesis()).unwrap();
        let custom = DogeosChainSpecParser::parse(&json).unwrap();
        assert_eq!(builtin, custom);
        assert_eq!(
            builtin.genesis_hash(),
            b256!("f9f7c524dce38b51a4d28ec2f18680773e5ba9d3f5f430d0e05f92cfeb65b1bc")
        );
        assert_eq!(builtin.config.l1_config.l1_chain_id, 111_111);
        assert!(!builtin.is_tsuki_active_at_timestamp(1_790_030_899));
        assert!(builtin.is_tsuki_active_at_timestamp(1_790_030_900));
    }
}
