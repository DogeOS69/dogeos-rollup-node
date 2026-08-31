//! Shared rendering for rollup node status outputs.

use colored::Colorize;
use rollup_node_chain_orchestrator::{ChainOrchestratorStatus, DerivationStatus};

/// Print derivation and held-batch progress.
pub(crate) fn print_derivation_status(status: &ChainOrchestratorStatus) {
    println!("{}", "Derivation:".underline());
    match &status.derivation {
        DerivationStatus::Idle => println!("  State:     {}", "idle".green()),
        DerivationStatus::Deriving { queued } => {
            println!("  State:     {}", "deriving".yellow());
            println!("  Queued:    {queued}");
        }
        DerivationStatus::Held(batch) => {
            println!("  State:     {}", "held".yellow());
            println!(
                "  Batch:     #{} ({:.12}...)",
                batch.batch_index,
                format!("{:?}", batch.batch_hash)
            );
            println!("  Attempt:   {}", batch.attempts_started);
            println!("  Held:      {}ms", batch.held_duration_ms);
            if let (Some(method), Some(engine_status)) =
                (&batch.last_engine_method, &batch.last_engine_status)
            {
                println!("  Engine:    {method} -> {engine_status}");
            }
            if let Some(error) = &batch.last_engine_error {
                println!("  Error:     {error}");
            }
            if let Some(backoff_ms) = batch.current_backoff_ms {
                println!("  Backoff:   {backoff_ms}ms");
            }
            if batch.queued_behind > 0 {
                println!("  Queued:    {}", batch.queued_behind);
            }
        }
    }
}

/// Print L2/L1 overview sections used by `status`.
pub(crate) fn print_status_overview(status: &ChainOrchestratorStatus) {
    let fcs = &status.l2.fcs;

    println!("{}", "L2:".underline());
    println!(
        "  Head:      #{} ({:.12}...)",
        fcs.head_block_info().number.to_string().green(),
        format!("{:?}", fcs.head_block_info().hash)
    );
    println!(
        "  Safe:      #{} ({:.12}...)",
        fcs.safe_block_info().number.to_string().yellow(),
        format!("{:?}", fcs.safe_block_info().hash)
    );
    println!(
        "  Finalized: #{} ({:.12}...)",
        fcs.finalized_block_info().number.to_string().blue(),
        format!("{:?}", fcs.finalized_block_info().hash)
    );
    println!(
        "  Synced:    {}",
        if status.l2.status.is_synced() { "true".green() } else { "false".red() }
    );

    println!("{}", "L1:".underline());
    println!("  Head:      #{}", status.l1.latest.to_string().cyan());
    println!("  Finalized: #{}", status.l1.finalized);
    println!("  Processed: #{}", status.l1.processed);
    println!(
        "  Synced:    {}",
        if status.l1.status.is_synced() { "true".green() } else { "false".red() }
    );

    print_derivation_status(status);
}

/// Print detailed sync status used by `sync-status`.
pub(crate) fn print_sync_status(status: &ChainOrchestratorStatus) {
    println!("{}", "Sync Status:".bold());
    println!();
    println!("{}", "L1 Sync:".underline());
    println!(
        "  Status:    {}",
        if status.l1.status.is_synced() {
            "SYNCED".green()
        } else {
            format!("{:?}", status.l1.status).yellow().to_string().into()
        }
    );
    println!("  Latest:    #{}", status.l1.latest.to_string().cyan());
    println!("  Finalized: #{}", status.l1.finalized);
    println!("  Processed: #{}", status.l1.processed);
    println!();

    println!("{}", "L2 Sync:".underline());
    println!(
        "  Status:    {}",
        if status.l2.status.is_synced() {
            "SYNCED".green()
        } else {
            format!("{:?}", status.l2.status).yellow().to_string().into()
        }
    );
    println!();
    print_derivation_status(status);
    println!();
    println!("{}", "Forkchoice:".underline());

    let fcs = &status.l2.fcs;
    println!(
        "  Head:      #{} ({:.12}...)",
        fcs.head_block_info().number.to_string().green(),
        format!("{:?}", fcs.head_block_info().hash)
    );
    println!(
        "  Safe:      #{} ({:.12}...)",
        fcs.safe_block_info().number.to_string().yellow(),
        format!("{:?}", fcs.safe_block_info().hash)
    );
    println!(
        "  Finalized: #{} ({:.12}...)",
        fcs.finalized_block_info().number.to_string().blue(),
        format!("{:?}", fcs.finalized_block_info().hash)
    );
}

/// Print forkchoice section used by `fcs`.
pub(crate) fn print_forkchoice(status: &ChainOrchestratorStatus) {
    let fcs = &status.l2.fcs;
    println!("{}", "Forkchoice State:".bold());
    println!("  Head:");
    println!("    Number: {}", fcs.head_block_info().number);
    println!("    Hash:   {:?}", fcs.head_block_info().hash);
    println!("  Safe:");
    println!("    Number: {}", fcs.safe_block_info().number);
    println!("    Hash:   {:?}", fcs.safe_block_info().hash);
    println!("  Finalized:");
    println!("    Number: {}", fcs.finalized_block_info().number);
    println!("    Hash:   {:?}", fcs.finalized_block_info().hash);
}
