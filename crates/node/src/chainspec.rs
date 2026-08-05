//! DogeOS chain spec parsing for the rollup-node CLI.

use dogeos_chainspec::{DogeosChainSpec, DOGEOS_CHIKYU, DOGEOS_DEV, DOGEOS_MAINNET};
use reth_cli::chainspec::{parse_genesis, ChainSpecParser};
use std::sync::Arc;

/// Built-in chain names accepted by the rollup node.
pub const SUPPORTED_CHAINS: &[&str] = &["dogeos-mainnet", "dogeos-chikyu", "dev"];

/// Parses built-in DogeOS networks and custom genesis JSON files or strings.
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
