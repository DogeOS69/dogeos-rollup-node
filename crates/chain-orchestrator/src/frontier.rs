//! Crash-safe synchronization of the rollup database frontier and Engine forkchoice.
//!
//! Canonical L1 history determines the desired frontier. A pending intent bridges database
//! commits and Engine calls; an acknowledged safe intent also explains a lost Engine tag write.
//! Local forkchoice is only a cache and cannot acknowledge a durable intent on restart.

use crate::ChainOrchestratorError;
use alloy_provider::Provider;
use alloy_rpc_types_engine::PayloadStatusEnum;
use dogeos_rpc_types::Scroll;
use rollup_node_primitives::BlockInfo;
use scroll_db::{
    Database, DatabaseReadOperations, DatabaseWriteOperations, PendingFrontierTransition,
    StoredForkchoiceState,
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

/// Both endpoints must belong to the provider's canonical chain. Height alone is not proof.
async fn canonical_ancestor<P: Provider<Scroll>>(
    provider: &P,
    ancestor: BlockInfo,
    descendant: BlockInfo,
) -> Result<bool, ChainOrchestratorError> {
    if ancestor.number >= descendant.number {
        return Ok(ancestor == descendant)
    }
    for block in [ancestor, descendant] {
        if provider.get_block(block.number.into()).await?.map(|b| BlockInfo::from(&b.header)) !=
            Some(block)
        {
            return Ok(false)
        }
    }
    Ok(true)
}

/// Applies an immutable durable intent. Even a cached target requires a checked Engine call.
pub(crate) async fn apply_pending_frontier_transition<P, EC>(
    provider: &P,
    engine: &mut Engine<EC>,
    database: &Database,
) -> Result<bool, ChainOrchestratorError>
where
    P: Provider<Scroll>,
    EC: ScrollEngineApi + Sync + Send + 'static,
{
    async {
        let Some(transition) = database.get_pending_frontier_transition().await? else {
            return Ok(false)
        };
        let mut observed = stored_forkchoice(engine.fcs());
        if observed != transition.expected && observed != transition.target {
            let observed_fcs = observe_forkchoice(provider).await?;
            observed = stored_forkchoice(&observed_fcs);
            engine.replace_fcs_from_provider(observed_fcs);
            // Unsafe head can advance independently; safe must be explained by this intent or
            // by a delayed persistence write on the same canonical chain.
            if observed.safe != transition.expected.safe &&
                observed.safe != transition.target.safe &&
                !canonical_ancestor(provider, observed.safe, transition.expected.safe).await?
            {
                return Err(ChainOrchestratorError::FrontierConflict {
                    tag: "safe",
                    target: transition.target.safe,
                    observed: observed.safe,
                })
            }
        }
        // The journal already validates expected -> target; any other finalized tag must
        // be a canonical ancestor of the target (never ahead or a same-height fork).
        if observed.finalized != transition.expected.finalized &&
            !canonical_ancestor(provider, observed.finalized, transition.target.finalized)
                .await?
        {
            return Err(ChainOrchestratorError::FrontierConflict {
                tag: "finalized",
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
            Err(source) => {
                if matches!(source, EngineError::TransportError(_)) {
                    // The Engine may have applied FCU before losing the HTTP response.
                    let observed_fcs = observe_forkchoice(provider).await?;
                    let observed = stored_forkchoice(&observed_fcs);
                    engine.replace_fcs_from_provider(observed_fcs);
                    if observed == transition.target {
                        database.clear_pending_frontier_transition().await?;
                        return Ok(true)
                    }
                }
                return Err(ChainOrchestratorError::EngineError(source))
            }
        };
        match response.payload_status.status {
            PayloadStatusEnum::Valid => {
                database.clear_pending_frontier_transition().await?;
                Ok(true)
            }
            status => Err(ChainOrchestratorError::FrontierTransitionStatus(status)),
        }
    }
    .await
    .map_err(ChainOrchestratorError::frontier_recovery)
}

/// Reconcile all durable frontiers at startup, including finalized when safe already agrees.
/// Unexplained Engine safe forks/ahead states are fail-stop, not permission to overwrite tags.
pub(crate) async fn recover_database_frontier<P, EC>(
    provider: &P,
    engine: &mut Engine<EC>,
    database: &Database,
) -> Result<(), ChainOrchestratorError>
where
    P: Provider<Scroll>,
    EC: ScrollEngineApi + Sync + Send + 'static,
{
    async {
        engine.replace_fcs_from_provider(observe_forkchoice(provider).await?);
        apply_pending_frontier_transition(provider, engine, database).await?;
        let observed = stored_forkchoice(engine.fcs());
        observed.validate()?;
        let (safe, _) = database.get_latest_safe_l2_info().await?;
        let finalized = database.get_latest_finalized_l2_info().await?;
        if !canonical_ancestor(provider, observed.finalized, finalized).await? {
            return Err(ChainOrchestratorError::FrontierConflict {
                tag: "finalized",
                target: finalized,
                observed: observed.finalized,
            })
        }
        if observed.safe != safe {
            let confirmed = database.get_confirmed_frontier_transition().await?;
            let known_transition = confirmed.is_some_and(|transition| {
                transition.expected.safe == observed.safe && transition.target.safe == safe
            });
            if !known_transition && !canonical_ancestor(provider, observed.safe, safe).await? {
                return Err(ChainOrchestratorError::FrontierConflict {
                    tag: "safe",
                    target: safe,
                    observed: observed.safe,
                })
            }
        }

        // Select the persisted unsafe head only after resolving a possible safe rewind. If the
        // execution node lost recent blocks, walk back to the durable safe boundary and purge
        // mappings for the discarded unsafe suffix in the same transaction as the repair intent.
        let mut head = safe;
        let mut number = database.get_l2_head_block_number().await?;
        if number > safe.number &&
            provider.get_block(safe.number.into()).await?.map(|b| BlockInfo::from(&b.header)) ==
                Some(safe)
        {
            while number > safe.number {
                if let Some(block) = provider.get_block(number.into()).await? {
                    head = BlockInfo::from(&block.header);
                    break
                }
                number -= 1;
            }
        }
        let target = StoredForkchoiceState { head, safe, finalized };
        target.validate()?;
        database
            .tx_mut(move |tx| async move {
                tx.set_l2_head_block_number(head.number).await?;
                tx.purge_l1_message_to_l2_block_mappings(Some(head.number.saturating_add(1)))
                    .await?;
                tx.set_pending_frontier_transition(PendingFrontierTransition {
                    expected: observed,
                    target,
                })
                .await?;
                Ok::<_, ChainOrchestratorError>(())
            })
            .await?;
        apply_pending_frontier_transition(provider, engine, database).await?;
        Ok(())
    }
    .await
    .map_err(ChainOrchestratorError::frontier_recovery)
}

/// Cheap runtime guard. Full startup recovery must run once before entering the event loop.
pub(crate) async fn ensure_database_frontier<P, EC>(
    provider: &P,
    engine: &mut Engine<EC>,
    database: &Database,
) -> Result<(), ChainOrchestratorError>
where
    P: Provider<Scroll>,
    EC: ScrollEngineApi + Sync + Send + 'static,
{
    async {
        apply_pending_frontier_transition(provider, engine, database).await?;
        let (safe, _) = database.get_latest_safe_l2_info().await?;
        if *engine.fcs().safe_block_info() != safe {
            recover_database_frontier(provider, engine, database).await?;
        }
        Ok::<_, ChainOrchestratorError>(())
    }
    .await
    .map_err(ChainOrchestratorError::frontier_recovery)
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
    async fn cached_target_requires_engine_confirmation() {
        let database = setup_test_db().await;
        let finalized = BlockInfo::new(0, B256::ZERO);
        let old = BlockInfo::new(101, B256::repeat_byte(1));
        let safe = BlockInfo::new(100, B256::repeat_byte(2));
        let expected = StoredForkchoiceState { head: old, safe: old, finalized };
        let target = StoredForkchoiceState { head: old, safe, finalized };
        database
            .set_pending_frontier_transition(PendingFrontierTransition { expected, target })
            .await
            .unwrap();
        let provider =
            ProviderBuilder::<_, _, Scroll>::default().connect_mocked_client(Asserter::new());
        let client = Arc::new(ScriptedEngineClient::new());
        client.push_fork_choice_updated(ScriptedResponse::Ok(valid_fcu()));
        let mut engine = Engine::new(client.clone(), ForkchoiceState::new(old, safe, finalized));
        apply_pending_frontier_transition(&provider, &mut engine, &database).await.unwrap();
        assert_eq!(
            client.fork_choice_updated_calls(),
            1,
            "a local target is not remote confirmation"
        );
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
                expected,
                target: target_state,
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

    async fn seed_frontier(database: &Database, genesis: BlockInfo, safe: BlockInfo) {
        database.insert_genesis_block(genesis.hash).await.unwrap();
        database.insert_blocks(vec![safe], BatchInfo::new(0, B256::ZERO)).await.unwrap();
        // These synthetic safe blocks are not finalized just because the fixture uses batch 0.
        database
            .update_batch_status(B256::ZERO, rollup_node_primitives::BatchStatus::Consolidated)
            .await
            .unwrap();
        database.set_l2_head_block_number(safe.number).await.unwrap();
    }

    fn push_tags(
        asserter: &Asserter,
        head: &RpcBlock<ScrollRpcTransaction>,
        safe: &RpcBlock<ScrollRpcTransaction>,
        finalized: &RpcBlock<ScrollRpcTransaction>,
    ) {
        for block in [head, safe, finalized] {
            asserter.push_success(&Some(block.clone()));
        }
    }

    #[tokio::test]
    async fn unexplained_safe_fork_or_ahead_state_is_fatal() {
        for engine_number in [100, 101] {
            let database = setup_test_db().await;
            let (genesis_block, genesis) = rpc_block(0, B256::ZERO, 0xf0);
            let (_, safe) = rpc_block(100, genesis.hash, 0xa1);
            seed_frontier(&database, genesis, safe).await;
            let (other_block, other) = rpc_block(engine_number, genesis.hash, 0xb1);
            let asserter = Asserter::new();
            push_tags(&asserter, &other_block, &other_block, &genesis_block);
            let provider =
                ProviderBuilder::<_, _, Scroll>::default().connect_mocked_client(asserter);
            let client = Arc::new(ScriptedEngineClient::new());
            let mut engine =
                Engine::new(client.clone(), ForkchoiceState::new(other, other, genesis));
            let error =
                recover_database_frontier(&provider, &mut engine, &database).await.unwrap_err();
            assert!(matches!(error, ChainOrchestratorError::FrontierConflict { tag: "safe", .. }));
            assert_eq!(client.fork_choice_updated_calls(), 0);
            assert_eq!(database.get_pending_frontier_transition().await.unwrap(), None);
            assert_eq!(database.get_l2_head_block_number().await.unwrap(), safe.number);
        }
    }

    #[tokio::test]
    async fn startup_recovers_canonical_ancestor_and_preserves_persisted_head() {
        let database = setup_test_db().await;
        let (genesis_block, genesis) = rpc_block(0, B256::ZERO, 0xf0);
        let (old_block, old) = rpc_block(99, genesis.hash, 0xb1);
        let (safe_block, safe) = rpc_block(100, old.hash, 0xa1);
        let (head_block, head) = rpc_block(101, safe.hash, 0xc1);
        seed_frontier(&database, genesis, safe).await;
        database.set_l2_head_block_number(head.number).await.unwrap();
        let asserter = Asserter::new();
        push_tags(&asserter, &head_block, &old_block, &genesis_block);
        // Canonical ancestry proof, then persisted head selection.
        for block in [&old_block, &safe_block, &safe_block, &head_block] {
            asserter.push_success(&Some(block.clone()));
        }
        let provider = ProviderBuilder::<_, _, Scroll>::default().connect_mocked_client(asserter);
        let client = Arc::new(ScriptedEngineClient::new());
        client.push_fork_choice_updated(ScriptedResponse::Ok(valid_fcu()));
        let mut engine = Engine::new(client.clone(), ForkchoiceState::new(head, old, genesis));
        recover_database_frontier(&provider, &mut engine, &database).await.unwrap();
        assert_eq!(
            stored_forkchoice(engine.fcs()),
            StoredForkchoiceState { head, safe, finalized: genesis }
        );
        assert_eq!(client.fork_choice_updated_calls(), 1);
    }

    #[tokio::test]
    async fn startup_recovers_finalized_even_when_safe_matches() {
        let database = setup_test_db().await;
        let (genesis_block, genesis) = rpc_block(0, B256::ZERO, 0xf0);
        let (safe_block, safe) = rpc_block(100, genesis.hash, 0xa1);
        seed_frontier(&database, genesis, safe).await;
        database
            .update_batch_status(B256::ZERO, rollup_node_primitives::BatchStatus::Finalized)
            .await
            .unwrap();
        let asserter = Asserter::new();
        push_tags(&asserter, &safe_block, &safe_block, &genesis_block);
        asserter.push_success(&Some(genesis_block));
        asserter.push_success(&Some(safe_block));
        let provider = ProviderBuilder::<_, _, Scroll>::default().connect_mocked_client(asserter);
        let client = Arc::new(ScriptedEngineClient::new());
        client.push_fork_choice_updated(ScriptedResponse::Ok(valid_fcu()));
        let mut engine = Engine::new(client.clone(), ForkchoiceState::new(safe, safe, genesis));
        recover_database_frontier(&provider, &mut engine, &database).await.unwrap();
        assert_eq!(engine.fcs(), &ForkchoiceState::from_block_info(safe));
        assert_eq!(client.fork_choice_updated_calls(), 1);
        assert_eq!(database.get_pending_frontier_transition().await.unwrap(), None);
    }

    #[tokio::test]
    async fn restart_reopens_database_and_recovers_each_rewind_checkpoint() {
        use scroll_db::DatabaseConnectionProvider;
        // 0: DB committed, no FCU. 1: FCU applied, no acknowledgement.
        // 2: acknowledged, but Reth lost its deferred safe-tag write. 3: fully persisted.
        for checkpoint in 0..4 {
            let original = setup_test_db().await;
            let (genesis_block, genesis) = rpc_block(0, B256::ZERO, 0xf0);
            let (safe_block, safe) = rpc_block(100, genesis.hash, 0xa1);
            let (old_block, old) = rpc_block(101, safe.hash, 0xb1);
            seed_frontier(&original, genesis, safe).await;
            let expected = StoredForkchoiceState { head: old, safe: old, finalized: genesis };
            let target = StoredForkchoiceState { head: old, safe, finalized: genesis };
            let transition = PendingFrontierTransition { expected, target };
            original.set_pending_frontier_transition(transition).await.unwrap();
            if checkpoint >= 2 {
                original.clear_pending_frontier_transition().await.unwrap();
            }
            let path = original.tmp_dir().unwrap().path().join("test.db");
            original.inner().get_connection().clone().close().await.unwrap();
            let database = Database::new(path.to_str().unwrap()).await.unwrap();
            let asserter = Asserter::new();
            push_tags(
                &asserter,
                &old_block,
                if checkpoint % 2 == 0 { &old_block } else { &safe_block },
                &genesis_block,
            );
            let provider =
                ProviderBuilder::<_, _, Scroll>::default().connect_mocked_client(asserter);
            let client = Arc::new(ScriptedEngineClient::new());
            for _ in 0..2 {
                client.push_fork_choice_updated(ScriptedResponse::Ok(valid_fcu()));
            }
            // A new Engine cache must learn actual provider tags before recovery.
            let mut engine = Engine::new(client.clone(), ForkchoiceState::from_block_info(genesis));
            recover_database_frontier(&provider, &mut engine, &database).await.unwrap();
            assert_eq!(
                engine.fcs(),
                &ForkchoiceState::new(safe, safe, genesis),
                "checkpoint {checkpoint}"
            );
            assert_eq!(database.get_pending_frontier_transition().await.unwrap(), None);
            assert_eq!(client.fork_choice_updated_calls(), if checkpoint < 2 { 2 } else { 1 });
            // The safe-only journal is replayed exactly, then startup separately selects DB head.
            let inputs = client.fork_choice_inputs();
            if checkpoint < 2 {
                assert_eq!(inputs[0].0.head_block_hash, old.hash);
            }
            assert_eq!(inputs.last().unwrap().0.head_block_hash, safe.hash);
            let confirmed = database.get_confirmed_frontier_transition().await.unwrap().unwrap();
            assert_eq!(confirmed.expected.safe, old);
            assert_eq!(confirmed.target.safe, safe);
        }
    }

    #[tokio::test]
    async fn startup_rejects_database_frontier_conflicting_with_finalized() {
        let database = setup_test_db().await;
        let (genesis_block, genesis) = rpc_block(0, B256::ZERO, 0xf0);
        let (safe_block, safe) = rpc_block(100, genesis.hash, 0xa1);
        seed_frontier(&database, genesis, safe).await;
        let asserter = Asserter::new();
        push_tags(&asserter, &safe_block, &safe_block, &safe_block);
        let provider = ProviderBuilder::<_, _, Scroll>::default().connect_mocked_client(asserter);
        let client = Arc::new(ScriptedEngineClient::new());
        let mut engine = Engine::new(client.clone(), ForkchoiceState::from_block_info(genesis));
        let error = recover_database_frontier(&provider, &mut engine, &database).await.unwrap_err();
        assert!(matches!(error, ChainOrchestratorError::FrontierConflict { tag: "finalized", .. }));
        assert_eq!(client.fork_choice_updated_calls(), 0);
        assert_eq!(database.get_pending_frontier_transition().await.unwrap(), None);
        drop(genesis_block);
    }

    #[tokio::test]
    async fn absent_tags_are_only_initialized_at_observed_genesis() {
        for number in [0, 1] {
            let (block, info) = rpc_block(number, B256::ZERO, 0xf0);
            let asserter = Asserter::new();
            asserter.push_success(&Some(block));
            asserter.push_success(&Option::<RpcBlock<ScrollRpcTransaction>>::None);
            asserter.push_success(&Option::<RpcBlock<ScrollRpcTransaction>>::None);
            let provider =
                ProviderBuilder::<_, _, Scroll>::default().connect_mocked_client(asserter);
            let observed = ForkchoiceState::try_from_provider(&provider).await.unwrap();
            assert_eq!(observed, (number == 0).then_some(ForkchoiceState::from_block_info(info)));
        }
        let database = setup_test_db().await;
        let provider =
            ProviderBuilder::<_, _, Scroll>::default().connect_mocked_client(Asserter::new());
        let mut engine = Engine::new(
            Arc::new(ScriptedEngineClient::new()),
            ForkchoiceState::from_genesis(B256::ZERO),
        );
        let error = recover_database_frontier(&provider, &mut engine, &database).await.unwrap_err();
        assert!(matches!(error, ChainOrchestratorError::FrontierRecovery(source)
            if matches!(*source, ChainOrchestratorError::RpcError(_))));
    }

    #[tokio::test]
    async fn corrupt_journal_and_missing_safe_frontier_are_fatal() {
        use rollup_node_primitives::Metadata;
        use scroll_db::{DatabaseConnectionProvider, EntityTrait};
        let database = setup_test_db().await;
        let row: scroll_db::metadata::ActiveModel =
            Metadata { key: "pending_frontier_transition".into(), value: "{".into() }.into();
        scroll_db::metadata::Entity::insert(row)
            .exec(database.inner().get_connection())
            .await
            .unwrap();
        let provider =
            ProviderBuilder::<_, _, Scroll>::default().connect_mocked_client(Asserter::new());
        let client = Arc::new(ScriptedEngineClient::new());
        let mut engine = Engine::new(client.clone(), ForkchoiceState::from_genesis(B256::ZERO));
        assert!(ensure_database_frontier(&provider, &mut engine, &database)
            .await
            .unwrap_err()
            .is_frontier_fatal());
        scroll_db::metadata::Entity::delete_by_id("pending_frontier_transition")
            .exec(database.inner().get_connection())
            .await
            .unwrap();
        scroll_db::l2_block::Entity::delete_many()
            .exec(database.inner().get_connection())
            .await
            .unwrap();
        assert!(ensure_database_frontier(&provider, &mut engine, &database)
            .await
            .unwrap_err()
            .is_frontier_fatal());
        assert_eq!(client.fork_choice_updated_calls(), 0);
    }
}
