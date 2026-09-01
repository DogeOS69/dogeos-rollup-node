//! Crash-safe synchronization of the rollup database frontier and Engine forkchoice.
//!
//! Canonical L1 history persisted by the rollup database is authoritative for the desired safe
//! and finalized frontier. Provider tags describe what Reth actually applied, while the Engine's
//! in-memory forkchoice is only a cache of the last confirmed operation. A durable single-row
//! intent bridges database transactions and the external Engine API; normal processing never
//! proceeds while that intent is unresolved.

use crate::ChainOrchestratorError;
use alloy_provider::Provider;
use alloy_rpc_types_engine::PayloadStatusEnum;
use dogeos_rpc_types::Scroll;
use scroll_db::{
    Database, DatabaseReadOperations, DatabaseWriteOperations, FrontierTransitionKind,
    PendingFrontierTransition, StoredForkchoiceState,
};
use scroll_engine::{Engine, EngineError, ForkchoiceState, ScrollEngineApi};

pub(crate) const fn stored_forkchoice(value: &ForkchoiceState) -> StoredForkchoiceState {
    StoredForkchoiceState {
        head: *value.head_block_info(),
        safe: *value.safe_block_info(),
        finalized: *value.finalized_block_info(),
    }
}

async fn observe_forkchoice<P: Provider<Scroll>>(
    provider: &P,
) -> Result<ForkchoiceState, ChainOrchestratorError> {
    ForkchoiceState::try_from_provider(provider)
        .await?
        .ok_or(ChainOrchestratorError::MissingEngineForkchoiceTags)
}

async fn observe_forkchoice_for_recovery<P: Provider<Scroll>>(
    provider: &P,
) -> Result<ForkchoiceState, ChainOrchestratorError> {
    observe_forkchoice(provider)
        .await
        .map_err(|error| ChainOrchestratorError::FrontierObservation(Box::new(error)))
}

fn conflict(
    transition: PendingFrontierTransition,
    observed: StoredForkchoiceState,
) -> ChainOrchestratorError {
    ChainOrchestratorError::frontier_transition_conflict(
        transition.kind,
        transition.expected,
        transition.target,
        observed,
    )
}

/// Applies the durable frontier transition, resolving ambiguous Engine errors by observing the
/// execution provider before returning.
pub(crate) async fn apply_pending_frontier_transition<P, EC>(
    provider: &P,
    engine: &mut Engine<EC>,
    database: &Database,
) -> Result<bool, ChainOrchestratorError>
where
    P: Provider<Scroll>,
    EC: ScrollEngineApi + Sync + Send + 'static,
{
    let Some(mut transition) = database.get_pending_frontier_transition().await? else {
        return Ok(false)
    };
    tracing::warn!(
        target: "scroll::chain_orchestrator",
        kind = ?transition.kind,
        expected = ?transition.expected,
        target_frontier = ?transition.target,
        "Reconciling a durable database/Engine frontier transition"
    );

    let mut observed = stored_forkchoice(engine.fcs());
    // Head is not part of the L1-backed authority decision when a transition changes only
    // safe/finalized. On restart, node startup may already have selected a database-backed unsafe
    // head. Carry that head through the durable transition instead of treating it as a conflict,
    // while still requiring it to remain at or above the target safe block.
    if transition.target.head == transition.expected.head &&
        observed.head != transition.expected.head &&
        observed.head.number >= transition.target.safe.number
    {
        transition.expected.head = observed.head;
        transition.target.head = observed.head;
        database.set_pending_frontier_transition(transition).await?;
    }
    if observed == transition.target {
        database.clear_pending_frontier_transition().await?;
        return Ok(true)
    }

    if observed != transition.expected {
        let observed_fcs = observe_forkchoice_for_recovery(provider).await?;
        observed = stored_forkchoice(&observed_fcs);
        engine.replace_fcs_from_provider(observed_fcs);

        if observed == transition.target {
            database.clear_pending_frontier_transition().await?;
            return Ok(true)
        }
        if observed != transition.expected {
            return Err(conflict(transition, observed))
        }
    }

    if transition.target.finalized.number < observed.finalized.number ||
        (transition.target.finalized.number == observed.finalized.number &&
            transition.target.finalized != observed.finalized)
    {
        return Err(ChainOrchestratorError::FinalizedFrontierConflict {
            target: transition.target.finalized,
            observed: observed.finalized,
        })
    }

    let response = match engine
        .update_fcs_checked(
            Some(transition.target.head),
            Some(transition.target.safe),
            Some(transition.target.finalized),
        )
        .await
    {
        Ok(response) => response,
        Err(source @ EngineError::TransportError(_)) => {
            // A transport error is ambiguous: the Engine may have applied the FCU before the
            // response was lost. Observe remote tags before deciding that the operation failed.
            let observed_fcs = observe_forkchoice_for_recovery(provider).await?;
            let observed = stored_forkchoice(&observed_fcs);
            engine.replace_fcs_from_provider(observed_fcs);
            if observed == transition.target {
                database.clear_pending_frontier_transition().await?;
                return Ok(true)
            }
            return Err(ChainOrchestratorError::FrontierTransitionEngineRequest {
                kind: transition.kind,
                source,
            })
        }
        Err(source @ EngineError::FcsError(_)) => {
            return Err(ChainOrchestratorError::FrontierTransitionEngineRequest {
                kind: transition.kind,
                source,
            })
        }
    };

    match response.payload_status.status {
        PayloadStatusEnum::Valid => {
            database.clear_pending_frontier_transition().await?;
            Ok(true)
        }
        PayloadStatusEnum::Syncing => Err(ChainOrchestratorError::FrontierTransitionStatus {
            kind: transition.kind,
            status: "SYNCING",
        }),
        PayloadStatusEnum::Accepted => Err(ChainOrchestratorError::FrontierTransitionStatus {
            kind: transition.kind,
            status: "ACCEPTED",
        }),
        PayloadStatusEnum::Invalid { .. } => {
            Err(ChainOrchestratorError::FrontierTransitionStatus {
                kind: transition.kind,
                status: "INVALID",
            })
        }
    }
}

/// Ensures the database-backed safe frontier and Engine safe tag agree before normal processing.
/// A legacy or crash-induced mismatch is converted into a durable startup-repair transition.
pub(crate) async fn ensure_database_frontier<P, EC>(
    provider: &P,
    engine: &mut Engine<EC>,
    database: &Database,
) -> Result<(), ChainOrchestratorError>
where
    P: Provider<Scroll>,
    EC: ScrollEngineApi + Sync + Send + 'static,
{
    if database.get_pending_frontier_transition().await?.is_some() {
        apply_pending_frontier_transition(provider, engine, database).await?;
    }

    let (database_safe, _) = database.get_latest_safe_l2_info().await?;
    if *engine.fcs().safe_block_info() == database_safe {
        return Ok(())
    }

    tracing::warn!(
        target: "scroll::chain_orchestrator",
        ?database_safe,
        cached_engine_safe = ?engine.fcs().safe_block_info(),
        "Database and cached Engine safe frontiers differ"
    );

    let observed_fcs = observe_forkchoice_for_recovery(provider).await?;
    let observed = stored_forkchoice(&observed_fcs);
    engine.replace_fcs_from_provider(observed_fcs);
    if observed.safe == database_safe {
        return Ok(())
    }

    tracing::warn!(
        target: "scroll::chain_orchestrator",
        ?database_safe,
        observed_engine_safe = ?observed.safe,
        "Persisting repair for a database/Engine safe-frontier mismatch"
    );

    if database_safe.number < observed.finalized.number ||
        (database_safe.number == observed.finalized.number &&
            database_safe != observed.finalized)
    {
        return Err(ChainOrchestratorError::FinalizedFrontierConflict {
            target: database_safe,
            observed: observed.finalized,
        })
    }

    let target = StoredForkchoiceState {
        head: database_safe,
        safe: database_safe,
        finalized: observed.finalized,
    };
    database
        .tx_mut(move |tx| async move {
            tx.set_l2_head_block_number(database_safe.number).await?;
            tx.set_pending_frontier_transition(PendingFrontierTransition {
                kind: FrontierTransitionKind::StartupRepair,
                expected: observed,
                target,
                batch_hash: None,
            })
            .await?;
            Ok::<_, ChainOrchestratorError>(())
        })
        .await?;
    apply_pending_frontier_transition(provider, engine, database).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_consensus::Header as ConsensusHeader;
    use alloy_primitives::{Bytes, Sealable, B256};
    use alloy_provider::ProviderBuilder;
    use alloy_rpc_types_engine::{ForkchoiceUpdated, PayloadStatus, PayloadStatusEnum};
    use alloy_rpc_types_eth::{Block as RpcBlock, Header as RpcHeader};
    use alloy_transport::mock::Asserter;
    use dogeos_rpc_types::ScrollRpcTransaction;
    use rollup_node_primitives::{BatchInfo, BlockInfo};
    use scroll_db::test_utils::setup_test_db;
    use scroll_engine::{
        test_utils::{ScriptedEngineClient, ScriptedResponse},
        ForkchoiceState,
    };
    use std::sync::Arc;

    fn rpc_block(
        number: u64,
        parent_hash: B256,
        tag: u8,
    ) -> (RpcBlock<ScrollRpcTransaction>, BlockInfo) {
        let header = ConsensusHeader {
            parent_hash,
            number,
            extra_data: Bytes::from(vec![tag]),
            ..Default::default()
        };
        let sealed = header.seal_slow();
        let block_info = BlockInfo { number, hash: sealed.hash() };
        let block = RpcBlock::empty(RpcHeader::from_consensus(sealed, None, None));
        (block, block_info)
    }

    fn valid_fcu() -> ForkchoiceUpdated {
        ForkchoiceUpdated {
            payload_status: PayloadStatus {
                status: PayloadStatusEnum::Valid,
                latest_valid_hash: None,
            },
            payload_id: None,
        }
    }

    #[tokio::test]
    async fn same_height_different_hash_repairs_to_database_frontier() {
        let database = setup_test_db().await;
        let database_safe = BlockInfo { number: 100, hash: B256::repeat_byte(0xa1) };
        database.insert_blocks(vec![database_safe], BatchInfo::new(0, B256::ZERO)).await.unwrap();

        let (finalized_block, finalized) = rpc_block(0, B256::ZERO, 0xf0);
        let (engine_safe_block, engine_safe) = rpc_block(100, finalized.hash, 0xb1);
        let asserter = Asserter::new();
        asserter.push_success(&Some(engine_safe_block.clone()));
        asserter.push_success(&Some(engine_safe_block));
        asserter.push_success(&Some(finalized_block));
        let provider = ProviderBuilder::<_, _, Scroll>::default().connect_mocked_client(asserter);

        let client = Arc::new(ScriptedEngineClient::new());
        client.push_fork_choice_updated(ScriptedResponse::Ok(valid_fcu()));
        let mut engine =
            Engine::new(client.clone(), ForkchoiceState::new(engine_safe, engine_safe, finalized));

        ensure_database_frontier(&provider, &mut engine, &database).await.unwrap();

        assert_eq!(*engine.fcs().head_block_info(), database_safe);
        assert_eq!(*engine.fcs().safe_block_info(), database_safe);
        assert_eq!(database.get_pending_frontier_transition().await.unwrap(), None);
        let inputs = client.fork_choice_inputs();
        assert_eq!(inputs.len(), 1);
        assert!(!inputs[0].1);
        assert_eq!(inputs[0].0.head_block_hash, database_safe.hash);
        assert_eq!(inputs[0].0.safe_block_hash, database_safe.hash);
    }

    #[tokio::test]
    async fn ambiguous_engine_timeout_observes_target_and_clears_intent() {
        let database = setup_test_db().await;
        let (finalized_block, finalized) = rpc_block(0, B256::ZERO, 0xf0);
        let expected_block = BlockInfo { number: 100, hash: B256::repeat_byte(0xa1) };
        let (target_block, target) = rpc_block(101, expected_block.hash, 0xc1);
        let expected =
            StoredForkchoiceState { head: expected_block, safe: expected_block, finalized };
        let target_state = StoredForkchoiceState { head: target, safe: target, finalized };
        database
            .set_pending_frontier_transition(PendingFrontierTransition {
                kind: FrontierTransitionKind::ConsolidateBatch,
                expected,
                target: target_state,
                batch_hash: Some(B256::repeat_byte(1)),
            })
            .await
            .unwrap();

        // The Engine applied the target before the HTTP response was lost, so provider tags
        // already expose the target when recovery observes them.
        let asserter = Asserter::new();
        asserter.push_success(&Some(target_block.clone()));
        asserter.push_success(&Some(target_block));
        asserter.push_success(&Some(finalized_block));
        let provider = ProviderBuilder::<_, _, Scroll>::default().connect_mocked_client(asserter);
        let client = Arc::new(ScriptedEngineClient::new());
        client.push_fork_choice_updated(ScriptedResponse::TransportFailure);
        let mut engine = Engine::new(
            client,
            ForkchoiceState::new(expected.head, expected.safe, expected.finalized),
        );

        assert!(apply_pending_frontier_transition(&provider, &mut engine, &database)
            .await
            .unwrap());
        assert_eq!(stored_forkchoice(engine.fcs()), target_state);
        assert_eq!(database.get_pending_frontier_transition().await.unwrap(), None);
    }

    #[tokio::test]
    async fn restart_carries_database_selected_head_through_safe_rewind() {
        let database = setup_test_db().await;
        let finalized = BlockInfo { number: 0, hash: B256::repeat_byte(0xf0) };
        let old_safe = BlockInfo { number: 101, hash: B256::repeat_byte(0xb1) };
        let database_head = BlockInfo { number: 100, hash: B256::repeat_byte(0xa1) };
        let expected = StoredForkchoiceState { head: old_safe, safe: old_safe, finalized };
        database
            .set_pending_frontier_transition(PendingFrontierTransition {
                kind: FrontierTransitionKind::UnwindL1,
                expected,
                target: StoredForkchoiceState { head: old_safe, safe: database_head, finalized },
                batch_hash: None,
            })
            .await
            .unwrap();

        let asserter = Asserter::new();
        let provider = ProviderBuilder::<_, _, Scroll>::default().connect_mocked_client(asserter);
        let client = Arc::new(ScriptedEngineClient::new());
        client.push_fork_choice_updated(ScriptedResponse::Ok(valid_fcu()));
        // Node startup has already selected the persisted DB head in its local FCS, while Reth's
        // safe tag still needs the journaled rewind.
        let mut engine =
            Engine::new(client.clone(), ForkchoiceState::new(database_head, old_safe, finalized));

        assert!(apply_pending_frontier_transition(&provider, &mut engine, &database)
            .await
            .unwrap());
        assert_eq!(*engine.fcs().head_block_info(), database_head);
        assert_eq!(*engine.fcs().safe_block_info(), database_head);
        let inputs = client.fork_choice_inputs();
        assert_eq!(inputs[0].0.head_block_hash, database_head.hash);
        assert_eq!(inputs[0].0.safe_block_hash, database_head.hash);
        assert_eq!(database.get_pending_frontier_transition().await.unwrap(), None);
    }

    #[tokio::test]
    async fn startup_rejects_database_frontier_conflicting_with_finalized() {
        let database = setup_test_db().await;
        let database_safe = BlockInfo { number: 100, hash: B256::repeat_byte(0xa1) };
        database.insert_blocks(vec![database_safe], BatchInfo::new(0, B256::ZERO)).await.unwrap();

        let (finalized_block, finalized) = rpc_block(100, B256::ZERO, 0xf0);
        let asserter = Asserter::new();
        asserter.push_success(&Some(finalized_block.clone()));
        asserter.push_success(&Some(finalized_block.clone()));
        asserter.push_success(&Some(finalized_block));
        let provider = ProviderBuilder::<_, _, Scroll>::default().connect_mocked_client(asserter);
        let client = Arc::new(ScriptedEngineClient::new());
        let mut engine = Engine::new(client, ForkchoiceState::from_block_info(finalized));

        let error = ensure_database_frontier(&provider, &mut engine, &database).await.unwrap_err();
        assert!(matches!(error, ChainOrchestratorError::FinalizedFrontierConflict { .. }));
        assert_eq!(database.get_pending_frontier_transition().await.unwrap(), None);
    }
}
