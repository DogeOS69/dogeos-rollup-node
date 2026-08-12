//! Scroll binary

use reth_node_builder::TreeConfig;

const DEFAULT_PERSISTENCE_THRESHOLD: u64 = 0;
const DEFAULT_PERSISTENCE_BACKPRESSURE_THRESHOLD: u64 = 16;

#[global_allocator]
static ALLOC: reth_cli_util::allocator::Allocator = reth_cli_util::allocator::new_allocator();

const fn with_rollup_tree_overrides(tree_config: TreeConfig) -> TreeConfig {
    tree_config
        .with_always_process_payload_attributes_on_canonical_head(true)
        .with_unwind_canonical_header(true)
        // Use the legacy processor due to performance issues with Reth's state root task.
        .with_legacy_state_root(true)
}

fn main() {
    use clap::Parser;
    use dogeos_reth_consensus::DogeosConsensus;
    use dogeos_reth_evm::ScrollEvmConfig;
    use reth_ethereum_cli::Cli;
    use reth_node_builder::EngineNodeLauncher;
    use reth_node_core::args::DefaultEngineValues;
    use rollup_node::{DogeosChainSpecParser, ScrollRollupNode, ScrollRollupNodeConfig};
    use std::sync::Arc;
    use tracing::info;

    DefaultEngineValues::default()
        .with_persistence_threshold(DEFAULT_PERSISTENCE_THRESHOLD)
        .with_persistence_backpressure_threshold(DEFAULT_PERSISTENCE_BACKPRESSURE_THRESHOLD)
        .try_init()
        .expect("engine defaults must be initialized before parsing CLI arguments");

    // set default log level to info if RUST_LOG is not set
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }

    reth_cli_util::sigsegv_handler::install();

    // Enable backtraces unless a RUST_BACKTRACE value has already been explicitly provided.
    if std::env::var_os("RUST_BACKTRACE").is_none() {
        std::env::set_var("RUST_BACKTRACE", "1");
    }

    if let Err(err) = Cli::<DogeosChainSpecParser, ScrollRollupNodeConfig>::parse()
        .run_with_components::<ScrollRollupNode>(
        |chain_spec| (
            ScrollEvmConfig::dogeos(chain_spec),
            Arc::new(DogeosConsensus),
        ),
        async move |builder, args| {
            info!(target: "reth::cli", "Launching node");

            // Modify the chain spec based on the CLI args.
            let config = builder.config().clone();
            let mut chain_spec = (*config.chain).clone();
            chain_spec.config.l1_data_fee_buffer_check = args.require_l1_data_fee_buffer;
            let config = config.with_chain(chain_spec);

            // Launch the node.
            let handle = builder
                .node(ScrollRollupNode::new(args, config).await)
                .launch_with_fn(|builder| {
                    info!(target: "reth::cli", config = ?builder.config().chain.config, "Running with config");

                    // We must use `always_process_payload_attributes_on_canonical_head` in order to
                    // be able to build payloads with the forkchoice state API
                    // on top of heads part of the canonical state. Not
                    // providing this argument leads the `EngineTree` to ignore
                    // the payload building attributes: <https://github.com/scroll-tech/reth/blob/4271872fdcbe7ff96520825e38f5e36ef923fcca/crates/engine/tree/src/tree/mod.rs#L898>
                    let tree_config =
                        with_rollup_tree_overrides(builder.config().engine.tree_config());
                    info!(
                        target: "reth::cli",
                        persistence_threshold = tree_config.persistence_threshold(),
                        persistence_backpressure_threshold =
                            tree_config.persistence_backpressure_threshold(),
                        memory_block_buffer_target = tree_config.memory_block_buffer_target(),
                        "Engine persistence configured"
                    );
                    let launcher = EngineNodeLauncher::new(
                        builder.task_executor().clone(),
                        builder.config().datadir(),
                        tree_config,
                    );
                    builder.launch_with(launcher)
                })
                .await?;
            handle.node_exit_future.await
        },
    ) {
        eprintln!("Error: {err:?}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rollup_tree_overrides_preserve_persistence_settings() {
        let tree_config = TreeConfig::default()
            .with_persistence_backpressure_threshold(24)
            .with_persistence_threshold(8)
            .with_memory_block_buffer_target(0);

        let tree_config = with_rollup_tree_overrides(tree_config);

        assert_eq!(tree_config.persistence_threshold(), 8);
        assert_eq!(tree_config.persistence_backpressure_threshold(), 24);
        assert_eq!(tree_config.memory_block_buffer_target(), 0);
        assert!(tree_config.always_process_payload_attributes_on_canonical_head());
        assert!(tree_config.unwind_canonical_header());
        assert!(tree_config.legacy_state_root());
    }
}
