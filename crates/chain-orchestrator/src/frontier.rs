//! Crash-safe synchronization of the rollup database frontier and Engine forkchoice.
//!
//! Canonical L1 history determines the desired frontier. A pending intent bridges database
//! commits and Engine calls; acknowledged safe history also explains lost Engine tag writes.
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

/// How frontier recovery chooses the unsafe head when Reth's head is above the stored L2 head.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum HeadSelection {
    #[default]
    /// Use the stored L2 head: the newest block Reth kept up to it. A sequencer builds on the
    /// head, and its L1 message state ends at the stored head.
    StoredHead,
    /// Keep Reth's head when the stored head is its canonical ancestor. A follower's Reth can run
    /// far ahead of the stored head (P2P sync); moving the head down to the stored head makes Reth
    /// unwind every block above it inside the forkchoice update.
    KeepEngineHead,
}

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

/// Whether acknowledged moves connected to `safe` explain a lost Engine safe-tag write:
/// any retained start, or a block below it on the canonical chain.
async fn explained_by_acknowledged_move<P: Provider<Scroll>>(
    provider: &P,
    database: &Database,
    observed: BlockInfo,
    safe: BlockInfo,
) -> Result<bool, ChainOrchestratorError> {
    let Some(confirmed) = database.get_confirmed_frontier_transition().await? else {
        return Ok(false)
    };
    if confirmed.transition.target.safe != safe {
        return Ok(false)
    }
    // Check all exact witnesses before querying ancestry: a retained block may no longer be
    // canonical after a reorg, but its acknowledged transition still explains that exact tag.
    if confirmed.safe_starts().any(|start| start == observed) {
        return Ok(true)
    }
    for start in confirmed.safe_starts() {
        if canonical_ancestor(provider, observed, start).await? {
            return Ok(true)
        }
    }
    Ok(false)
}

/// Returns the newest block Reth kept from `safe` up to height `number`, or `safe` itself when
/// Reth no longer has `safe` on its canonical chain.
async fn kept_head<P: Provider<Scroll>>(
    provider: &P,
    safe: BlockInfo,
    mut number: u64,
) -> Result<BlockInfo, ChainOrchestratorError> {
    if number <= safe.number ||
        provider.get_block(safe.number.into()).await?.map(|b| BlockInfo::from(&b.header)) !=
            Some(safe)
    {
        return Ok(safe)
    }
    while number > safe.number {
        if let Some(block) = provider.get_block(number.into()).await? {
            return Ok(BlockInfo::from(&block.header))
        }
        number -= 1;
    }
    Ok(safe)
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
            engine.replace_fcs_from_provider(observe_forkchoice(provider).await?);
            observed = stored_forkchoice(engine.fcs());
            // Unsafe head can advance independently. Safe must be explained by this intent, by the
            // acknowledged safe history whose tag writes Reth lost, or by a delayed tag write on
            // the canonical chain below either end of this intent.
            if observed.safe != transition.expected.safe &&
                observed.safe != transition.target.safe &&
                !explained_by_acknowledged_move(
                    provider,
                    database,
                    observed.safe,
                    transition.expected.safe,
                )
                .await? &&
                !canonical_ancestor(provider, observed.safe, transition.expected.safe).await? &&
                !canonical_ancestor(provider, observed.safe, transition.target.safe).await?
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
        match engine
            .update_fcs_checked(
                Some(transition.target.head),
                Some(transition.target.safe),
                Some(transition.target.finalized),
            )
            .await
        {
            Ok(response) => match response.payload_status.status {
                PayloadStatusEnum::Valid => {}
                status => {
                    tracing::error!(target: "scroll::chain_orchestrator", ?transition, ?observed, %status, "Engine rejected a saved frontier move");
                    return Err(ChainOrchestratorError::FrontierTransitionStatus(status))
                }
            },
            Err(source @ EngineError::TransportError(_)) => {
                // The Engine may have applied FCU before losing the HTTP response.
                engine.replace_fcs_from_provider(observe_forkchoice(provider).await?);
                if stored_forkchoice(engine.fcs()) != transition.target {
                    return Err(ChainOrchestratorError::EngineError(source))
                }
            }
            Err(source) => return Err(ChainOrchestratorError::EngineError(source)),
        }
        database.clear_pending_frontier_transition().await?;
        Ok(true)
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
    head_selection: HeadSelection,
) -> Result<(), ChainOrchestratorError>
where
    P: Provider<Scroll>,
    EC: ScrollEngineApi + Sync + Send + 'static,
{
    async {
        engine.replace_fcs_from_provider(observe_forkchoice(provider).await?);
        // Reth may have lost the unsafe head a saved move names. Replay the move's safe and
        // finalized blocks on the newest block Reth kept instead of failing on every restart.
        if let Some(mut transition) = database.get_pending_frontier_transition().await? {
            let head = transition.target.head;
            if provider.get_block(head.number.into()).await?.map(|b| BlockInfo::from(&b.header)) !=
                Some(head)
            {
                transition.target.head =
                    kept_head(provider, transition.target.safe, head.number).await?;
                if transition.target == transition.expected {
                    database.clear_pending_frontier_transition().await?;
                } else {
                    database.set_pending_frontier_transition(transition).await?;
                }
            }
        }
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
        if observed.safe != safe &&
            !explained_by_acknowledged_move(provider, database, observed.safe, safe).await? &&
            !canonical_ancestor(provider, observed.safe, safe).await?
        {
            return Err(ChainOrchestratorError::FrontierConflict {
                tag: "safe",
                target: safe,
                observed: observed.safe,
            })
        }

        // Select the persisted unsafe head only after resolving a possible safe rewind. If the
        // execution node lost recent blocks, walk back to the durable safe boundary and purge
        // mappings for the discarded unsafe suffix in the same transaction as the repair intent.
        // A follower whose Reth ran ahead of the stored head (P2P sync) keeps Reth's head when the
        // stored head is its canonical ancestor: moving the head down would make Reth unwind every
        // block above it inside the forkchoice update. The stored head and its mapping purge are
        // unchanged; consolidation maps the blocks above it once the node is synced.
        let stored_head =
            kept_head(provider, safe, database.get_l2_head_block_number().await?).await?;
        let head = if head_selection == HeadSelection::KeepEngineHead &&
            observed.head.number > stored_head.number &&
            canonical_ancestor(provider, stored_head, observed.head).await?
        {
            tracing::info!(target: "scroll::chain_orchestrator", ?stored_head, engine_head = ?observed.head, "Keeping Reth's head above the stored L2 head");
            observed.head
        } else {
            stored_head
        };
        let target = StoredForkchoiceState { head, safe, finalized };
        target.validate()?;
        database
            .tx_mut(move |tx| async move {
                tx.set_l2_head_block_number(stored_head.number).await?;
                tx.purge_l1_message_to_l2_block_mappings(Some(
                    stored_head.number.saturating_add(1),
                ))
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

/// Startup recovery that waits for an unavailable Engine instead of failing: a forkchoice update
/// that times out or loses its connection is retried from fresh state with the derived-batch hold
/// backoff (2 s doubling to 30 s), logged at error level each time. Any other error fails startup.
pub(crate) async fn recover_database_frontier_retrying<P, EC>(
    provider: &P,
    engine: &mut Engine<EC>,
    database: &Database,
    head_selection: HeadSelection,
) -> Result<(), ChainOrchestratorError>
where
    P: Provider<Scroll>,
    EC: ScrollEngineApi + Sync + Send + 'static,
{
    let mut attempt = 0u64;
    loop {
        match recover_database_frontier(provider, engine, database, head_selection).await {
            Err(error) if error.is_engine_unavailable() => {
                attempt = attempt.saturating_add(1);
                let backoff = crate::derivation::hold_backoff(attempt);
                tracing::error!(
                    target: "scroll::chain_orchestrator",
                    attempt,
                    backoff_ms = u64::try_from(backoff.as_millis()).unwrap_or(u64::MAX),
                    error = %error,
                    "Engine did not answer during startup frontier recovery; retrying"
                );
                tokio::time::sleep(backoff).await;
            }
            result => return result,
        }
    }
}

/// Reconciles and returns the database safe frontier. Startup recovery must run before the loop.
pub(crate) async fn ensure_database_frontier<P, EC>(
    provider: &P,
    engine: &mut Engine<EC>,
    database: &Database,
    head_selection: HeadSelection,
) -> Result<BlockInfo, ChainOrchestratorError>
where
    P: Provider<Scroll>,
    EC: ScrollEngineApi + Sync + Send + 'static,
{
    async {
        apply_pending_frontier_transition(provider, engine, database).await?;
        let (safe, _) = database.get_latest_safe_l2_info().await?;
        if *engine.fcs().safe_block_info() != safe {
            recover_database_frontier(provider, engine, database, head_selection).await?;
        }
        Ok::<_, ChainOrchestratorError>(safe)
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

    fn mock_engine(
        asserter: Asserter,
        fcs: ForkchoiceState,
    ) -> (impl Provider<Scroll>, Arc<ScriptedEngineClient>, Engine<ScriptedEngineClient>) {
        let provider = ProviderBuilder::<_, _, Scroll>::default().connect_mocked_client(asserter);
        let client = Arc::new(ScriptedEngineClient::new());
        let engine = Engine::new(client.clone(), fcs);
        (provider, client, engine)
    }

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
        let (provider, client, mut engine) =
            mock_engine(Asserter::new(), ForkchoiceState::new(old, safe, finalized));
        client.push_fork_choice_updated(ScriptedResponse::Ok(valid_fcu()));
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
        let (provider, client, mut engine) = mock_engine(
            asserter,
            ForkchoiceState::new(expected.head, expected.safe, expected.finalized),
        );
        client.push_fork_choice_updated(ScriptedResponse::TransportFailure);

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
            let (provider, client, mut engine) =
                mock_engine(asserter, ForkchoiceState::new(other, other, genesis));
            let error = recover_database_frontier(
                &provider,
                &mut engine,
                &database,
                HeadSelection::StoredHead,
            )
            .await
            .unwrap_err();
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
        let (provider, client, mut engine) =
            mock_engine(asserter, ForkchoiceState::new(head, old, genesis));
        client.push_fork_choice_updated(ScriptedResponse::Ok(valid_fcu()));
        recover_database_frontier(&provider, &mut engine, &database, HeadSelection::StoredHead)
            .await
            .unwrap();
        assert_eq!(
            stored_forkchoice(engine.fcs()),
            StoredForkchoiceState { head, safe, finalized: genesis }
        );
        assert_eq!(client.fork_choice_updated_calls(), 1);
    }

    /// Reth's tip about a million blocks above the stored rollup head, as on the testnet datadir
    /// where the startup forkchoice update timed out while Reth unwound to the stored head.
    const STORED_HEAD: u64 = 100_000;
    const RETH_TIP: u64 = 1_100_000;

    /// Runs startup recovery with Reth's tip far above the stored head and returns the heads the
    /// forkchoice updates sent, the local forkchoice head and the stored L2 head afterwards.
    async fn recover_with_reth_far_ahead(
        head_selection: HeadSelection,
    ) -> (Vec<B256>, BlockInfo, u64, BlockInfo, BlockInfo) {
        let database = setup_test_db().await;
        let (genesis_block, genesis) = rpc_block(0, B256::ZERO, 0xf0);
        let (safe_block, safe) = rpc_block(100, genesis.hash, 0xa1);
        let (stored_block, stored) = rpc_block(STORED_HEAD, B256::repeat_byte(0x51), 0xb1);
        let (tip_block, tip) = rpc_block(RETH_TIP, B256::repeat_byte(0x52), 0xc1);
        seed_frontier(&database, genesis, safe).await;
        database.set_l2_head_block_number(STORED_HEAD).await.unwrap();
        let asserter = Asserter::new();
        push_tags(&asserter, &tip_block, &safe_block, &genesis_block);
        // Stored head selection, then (keeping Reth's head) its canonical ancestry proof.
        for block in [&safe_block, &stored_block, &stored_block, &tip_block] {
            asserter.push_success(&Some(block.clone()));
        }
        let (provider, client, mut engine) =
            mock_engine(asserter, ForkchoiceState::new(tip, safe, genesis));
        client.push_fork_choice_updated(ScriptedResponse::Ok(valid_fcu()));

        recover_database_frontier(&provider, &mut engine, &database, head_selection).await.unwrap();
        let inputs = client.fork_choice_inputs();
        assert!(inputs.iter().all(|(fcs, _)| fcs.safe_block_hash == safe.hash));
        assert_eq!(database.get_pending_frontier_transition().await.unwrap(), None);
        (
            inputs.iter().map(|(fcs, _)| fcs.head_block_hash).collect(),
            *engine.fcs().head_block_info(),
            database.get_l2_head_block_number().await.unwrap(),
            stored,
            tip,
        )
    }

    /// A follower keeps Reth's head: the startup forkchoice update must not move the head about a
    /// million blocks down to the stored head, which makes Reth unwind them inside the call. The
    /// stored head and its message-mapping purge are unchanged.
    #[tokio::test]
    async fn follower_startup_keeps_reth_head_far_above_the_stored_head() {
        let (sent_heads, head, stored_number, _, tip) =
            recover_with_reth_far_ahead(HeadSelection::KeepEngineHead).await;
        // Reth's tags already match the target, so no forkchoice update is needed at all; any
        // that is sent must keep Reth's head.
        assert!(sent_heads.iter().all(|sent| *sent == tip.hash), "{sent_heads:?}");
        assert_eq!(head, tip);
        assert_eq!(stored_number, STORED_HEAD);
    }

    /// A sequencer keeps moving the head to the stored head.
    #[tokio::test]
    async fn sequencer_startup_moves_the_head_to_the_stored_head() {
        let (sent_heads, head, stored_number, stored, _) =
            recover_with_reth_far_ahead(HeadSelection::StoredHead).await;
        assert_eq!(sent_heads, vec![stored.hash]);
        assert_eq!(head, stored);
        assert_eq!(stored_number, STORED_HEAD);
    }

    /// The startup forkchoice update times out (Reth busy); recovery retries after the backoff
    /// and completes instead of failing startup.
    #[tokio::test]
    async fn startup_recovery_retries_after_an_engine_request_timeout() {
        let database = setup_test_db().await;
        let (genesis_block, genesis) = rpc_block(0, B256::ZERO, 0xf0);
        let (old_block, old) = rpc_block(99, genesis.hash, 0xb1);
        let (safe_block, safe) = rpc_block(100, old.hash, 0xa1);
        let (head_block, head) = rpc_block(101, safe.hash, 0xc1);
        seed_frontier(&database, genesis, safe).await;
        database.set_l2_head_block_number(head.number).await.unwrap();
        let asserter = Asserter::new();
        // First attempt: tags, ancestry proof, head selection; after the timeout, tags again.
        push_tags(&asserter, &head_block, &old_block, &genesis_block);
        for block in [&old_block, &safe_block, &safe_block, &head_block] {
            asserter.push_success(&Some(block.clone()));
        }
        push_tags(&asserter, &head_block, &old_block, &genesis_block);
        // Second attempt: tags, the saved move's head (still in Reth), then head selection. The
        // replayed move makes the tags match the target, so no further update is needed.
        push_tags(&asserter, &head_block, &old_block, &genesis_block);
        for block in [&head_block, &safe_block, &head_block] {
            asserter.push_success(&Some(block.clone()));
        }
        let (provider, client, mut engine) =
            mock_engine(asserter, ForkchoiceState::new(head, old, genesis));
        client.push_fork_choice_updated(ScriptedResponse::RequestTimeout);
        client.push_fork_choice_updated(ScriptedResponse::Ok(valid_fcu()));

        recover_database_frontier_retrying(
            &provider,
            &mut engine,
            &database,
            HeadSelection::StoredHead,
        )
        .await
        .unwrap();
        assert_eq!(client.fork_choice_updated_calls(), 2);
        assert_eq!(
            stored_forkchoice(engine.fcs()),
            StoredForkchoiceState { head, safe, finalized: genesis }
        );
        assert_eq!(database.get_pending_frontier_transition().await.unwrap(), None);
    }

    /// Any other startup recovery failure still fails startup.
    #[tokio::test]
    async fn startup_recovery_fails_on_an_engine_error_response() {
        let database = setup_test_db().await;
        let (genesis_block, genesis) = rpc_block(0, B256::ZERO, 0xf0);
        let (old_block, old) = rpc_block(99, genesis.hash, 0xb1);
        let (safe_block, safe) = rpc_block(100, old.hash, 0xa1);
        let (head_block, head) = rpc_block(101, safe.hash, 0xc1);
        seed_frontier(&database, genesis, safe).await;
        database.set_l2_head_block_number(head.number).await.unwrap();
        let asserter = Asserter::new();
        push_tags(&asserter, &head_block, &old_block, &genesis_block);
        for block in [&old_block, &safe_block, &safe_block, &head_block] {
            asserter.push_success(&Some(block.clone()));
        }
        push_tags(&asserter, &head_block, &old_block, &genesis_block);
        let (provider, client, mut engine) =
            mock_engine(asserter, ForkchoiceState::new(head, old, genesis));
        client.push_fork_choice_updated(ScriptedResponse::RpcErrorResponse(-32000));

        let error = recover_database_frontier_retrying(
            &provider,
            &mut engine,
            &database,
            HeadSelection::StoredHead,
        )
        .await
        .unwrap_err();
        assert!(!error.is_engine_unavailable(), "{error:?}");
        assert_eq!(client.fork_choice_updated_calls(), 1);
    }

    #[tokio::test]
    async fn startup_replays_a_saved_move_on_the_newest_block_reth_kept() {
        // The safe move was saved with head 101, then the crash lost block 101 in Reth.
        let database = setup_test_db().await;
        let (genesis_block, genesis) = rpc_block(0, B256::ZERO, 0xf0);
        let (old_safe_block, old_safe) = rpc_block(99, genesis.hash, 0xb1);
        let (safe_block, safe) = rpc_block(100, old_safe.hash, 0xa1);
        let (_, lost) = rpc_block(101, safe.hash, 0xc1);
        seed_frontier(&database, genesis, safe).await;
        database.set_l2_head_block_number(lost.number).await.unwrap();
        // A deposit in the lost block must become includable again; one in a kept block must not.
        for (queue_index, l2_block) in [(0, safe.number), (1, lost.number)] {
            let message = dogeos_protocol_types::TxL1Message { queue_index, ..Default::default() };
            database
                .insert_l1_message(rollup_node_primitives::L1MessageEnvelope::new(
                    message,
                    0,
                    Some(l2_block),
                    None,
                ))
                .await
                .unwrap();
        }
        let expected = StoredForkchoiceState { head: lost, safe: old_safe, finalized: genesis };
        let target = StoredForkchoiceState { safe, ..expected };
        database
            .set_pending_frontier_transition(PendingFrontierTransition { expected, target })
            .await
            .unwrap();
        let asserter = Asserter::new();
        let absent = || asserter.push_success(&Option::<RpcBlock<ScrollRpcTransaction>>::None);
        push_tags(&asserter, &safe_block, &old_safe_block, &genesis_block);
        // The saved head is gone; the newest kept block is the safe block.
        absent();
        asserter.push_success(&Some(safe_block.clone()));
        absent();
        // The replay re-reads the tags, then startup selects the head again.
        push_tags(&asserter, &safe_block, &old_safe_block, &genesis_block);
        asserter.push_success(&Some(safe_block));
        absent();
        let (provider, client, mut engine) =
            mock_engine(asserter, ForkchoiceState::new(lost, old_safe, genesis));
        client.push_fork_choice_updated(ScriptedResponse::Ok(valid_fcu()));

        recover_database_frontier(&provider, &mut engine, &database, HeadSelection::StoredHead)
            .await
            .unwrap();
        assert_eq!(client.fork_choice_updated_calls(), 1);
        assert_eq!(client.fork_choice_inputs()[0].0.head_block_hash, safe.hash);
        assert_eq!(engine.fcs(), &ForkchoiceState::new(safe, safe, genesis));
        assert_eq!(database.get_pending_frontier_transition().await.unwrap(), None);
        assert_eq!(database.get_l2_head_block_number().await.unwrap(), safe.number);
        let messages = database
            .get_n_l1_messages(Some(scroll_db::L1MessageKey::from_queue_index(0)), 2)
            .await
            .unwrap();
        let mapped: Vec<_> = messages.iter().map(|message| message.l2_block_number).collect();
        assert_eq!(mapped, [Some(safe.number), None]);
    }

    #[tokio::test]
    async fn startup_replays_a_rewind_when_reth_lost_its_old_safe_block() {
        // A rewind from safe 101 to 100 was saved with head 102. The crash left Reth with blocks
        // through 100 and its safe tag at 99.
        let database = setup_test_db().await;
        let (genesis_block, genesis) = rpc_block(0, B256::ZERO, 0xf0);
        let (old_block, old) = rpc_block(99, genesis.hash, 0xb1);
        let (safe_block, safe) = rpc_block(100, old.hash, 0xa1);
        let (_, rewound) = rpc_block(101, safe.hash, 0xc1);
        let (_, head) = rpc_block(102, rewound.hash, 0xc2);
        seed_frontier(&database, genesis, safe).await;
        database.set_l2_head_block_number(head.number).await.unwrap();
        let expected = StoredForkchoiceState { head, safe: rewound, finalized: genesis };
        let target = StoredForkchoiceState { safe, ..expected };
        database
            .set_pending_frontier_transition(PendingFrontierTransition { expected, target })
            .await
            .unwrap();
        let asserter = Asserter::new();
        let absent = || asserter.push_success(&Option::<RpcBlock<ScrollRpcTransaction>>::None);
        push_tags(&asserter, &safe_block, &old_block, &genesis_block);
        // Blocks 101 and 102 are gone, so the replay moves to block 100.
        absent();
        asserter.push_success(&Some(safe_block.clone()));
        absent();
        absent();
        // The replay re-reads the tags. Tag 99 is no ancestor of the lost 101, but is of 100.
        push_tags(&asserter, &safe_block, &old_block, &genesis_block);
        asserter.push_success(&Some(old_block.clone()));
        absent();
        asserter.push_success(&Some(old_block));
        asserter.push_success(&Some(safe_block.clone()));
        // Startup then selects the head again.
        asserter.push_success(&Some(safe_block));
        absent();
        absent();
        let (provider, client, mut engine) =
            mock_engine(asserter, ForkchoiceState::from_block_info(genesis));
        client.push_fork_choice_updated(ScriptedResponse::Ok(valid_fcu()));

        recover_database_frontier(&provider, &mut engine, &database, HeadSelection::StoredHead)
            .await
            .unwrap();
        assert_eq!(client.fork_choice_updated_calls(), 1);
        assert_eq!(client.fork_choice_inputs()[0].0.head_block_hash, safe.hash);
        assert_eq!(engine.fcs(), &ForkchoiceState::new(safe, safe, genesis));
        assert_eq!(database.get_pending_frontier_transition().await.unwrap(), None);
    }

    #[tokio::test]
    async fn saved_move_replay_accepts_a_safe_tag_the_last_acknowledged_move_explains() {
        // A rewind to safe 100 was acknowledged, but Reth lost its tag write, and in the second
        // case also the write of the advance before it, so Reth's safe tag is 101 in both. A later
        // finalized-only move to 100 is still saved.
        for lagging in [false, true] {
            let database = setup_test_db().await;
            let (genesis_block, genesis) = rpc_block(0, B256::ZERO, 0xf0);
            let (safe_block, safe) = rpc_block(100, genesis.hash, 0xa1);
            let (tag_block, tag) = rpc_block(101, safe.hash, 0xc1);
            let (head_block, head) =
                if lagging { rpc_block(102, tag.hash, 0xc2) } else { (tag_block.clone(), tag) };
            seed_frontier(&database, genesis, safe).await;
            database
                .update_batch_status(B256::ZERO, rollup_node_primitives::BatchStatus::Finalized)
                .await
                .unwrap();
            database.set_l2_head_block_number(head.number).await.unwrap();
            let rewind = PendingFrontierTransition {
                expected: StoredForkchoiceState { head, safe: head, finalized: genesis },
                target: StoredForkchoiceState { head, safe, finalized: genesis },
            };
            database.set_pending_frontier_transition(rewind).await.unwrap();
            database.clear_pending_frontier_transition().await.unwrap();
            let finalize = PendingFrontierTransition {
                expected: rewind.target,
                target: StoredForkchoiceState { finalized: safe, ..rewind.target },
            };
            database.set_pending_frontier_transition(finalize).await.unwrap();
            let asserter = Asserter::new();
            push_tags(&asserter, &head_block, &tag_block, &genesis_block);
            // The saved head is still there, then the replay re-reads the tags.
            asserter.push_success(&Some(head_block.clone()));
            push_tags(&asserter, &head_block, &tag_block, &genesis_block);
            if lagging {
                // Tag 101 is a canonical ancestor of the rewind's start.
                asserter.push_success(&Some(tag_block.clone()));
                asserter.push_success(&Some(head_block.clone()));
            }
            // Startup then selects the head again.
            asserter.push_success(&Some(safe_block));
            asserter.push_success(&Some(head_block));
            let (provider, client, mut engine) =
                mock_engine(asserter, ForkchoiceState::from_block_info(genesis));
            client.push_fork_choice_updated(ScriptedResponse::Ok(valid_fcu()));

            recover_database_frontier(&provider, &mut engine, &database, HeadSelection::StoredHead)
                .await
                .unwrap();
            assert_eq!(client.fork_choice_updated_calls(), 1, "lagging={lagging}");
            assert_eq!(stored_forkchoice(engine.fcs()), finalize.target);
            assert_eq!(database.get_pending_frontier_transition().await.unwrap(), None);
        }
    }

    #[tokio::test]
    async fn startup_accepts_a_safe_tag_below_the_start_of_the_acknowledged_rewind() {
        // Reth persisted safe 2, then lost the tag writes of an advance to 3 and of the
        // acknowledged rewind from 3 to 1.
        let database = setup_test_db().await;
        let (genesis_block, genesis) = rpc_block(0, B256::ZERO, 0xf0);
        let (safe_block, safe) = rpc_block(1, genesis.hash, 0xa1);
        let (lagging_block, lagging) = rpc_block(2, safe.hash, 0xa2);
        let (head_block, head) = rpc_block(3, lagging.hash, 0xa3);
        seed_frontier(&database, genesis, safe).await;
        database.set_l2_head_block_number(head.number).await.unwrap();
        let expected = StoredForkchoiceState { head, safe: head, finalized: genesis };
        database
            .set_pending_frontier_transition(PendingFrontierTransition {
                expected,
                target: StoredForkchoiceState { safe, ..expected },
            })
            .await
            .unwrap();
        database.clear_pending_frontier_transition().await.unwrap();
        let asserter = Asserter::new();
        push_tags(&asserter, &head_block, &lagging_block, &genesis_block);
        // Tag 2 is a canonical ancestor of the rewind's start, then startup selects the head.
        for block in [&lagging_block, &head_block, &safe_block, &head_block] {
            asserter.push_success(&Some(block.clone()));
        }
        let (provider, client, mut engine) =
            mock_engine(asserter, ForkchoiceState::from_block_info(genesis));
        client.push_fork_choice_updated(ScriptedResponse::Ok(valid_fcu()));

        recover_database_frontier(&provider, &mut engine, &database, HeadSelection::StoredHead)
            .await
            .unwrap();
        assert_eq!(client.fork_choice_updated_calls(), 1);
        assert_eq!(engine.fcs(), &ForkchoiceState::new(head, safe, genesis));
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
        let (provider, client, mut engine) =
            mock_engine(asserter, ForkchoiceState::new(safe, safe, genesis));
        client.push_fork_choice_updated(ScriptedResponse::Ok(valid_fcu()));
        recover_database_frontier(&provider, &mut engine, &database, HeadSelection::StoredHead)
            .await
            .unwrap();
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
            if checkpoint < 2 {
                // Reth still has the head the saved move names.
                asserter.push_success(&Some(old_block.clone()));
            }
            // A new Engine cache must learn actual provider tags before recovery.
            let (provider, client, mut engine) =
                mock_engine(asserter, ForkchoiceState::from_block_info(genesis));
            for _ in 0..2 {
                client.push_fork_choice_updated(ScriptedResponse::Ok(valid_fcu()));
            }
            recover_database_frontier(&provider, &mut engine, &database, HeadSelection::StoredHead)
                .await
                .unwrap();
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
            assert!(confirmed.safe_starts().any(|start| start == old));
            assert_eq!(confirmed.transition.target.safe, safe);
        }
    }

    #[tokio::test]
    async fn restart_recovers_two_acknowledged_safe_rewinds_after_tag_loss() {
        use scroll_db::DatabaseConnectionProvider;

        let original = setup_test_db().await;
        let (genesis_block, genesis) = rpc_block(0, B256::ZERO, 0xf0);
        let (safe_block, safe) = rpc_block(1, genesis.hash, 0xa1);
        let (_, intermediate) = rpc_block(2, safe.hash, 0xa2);
        let (head_block, head) = rpc_block(3, intermediate.hash, 0xa3);
        seed_frontier(&original, genesis, safe).await;
        original.set_l2_head_block_number(head.number).await.unwrap();

        // Reth acknowledges both safe rewinds while keeping the same canonical head.
        let (provider, client, mut engine) =
            mock_engine(Asserter::new(), ForkchoiceState::new(head, head, genesis));
        for (from, to) in [(head, intermediate), (intermediate, safe)] {
            let expected = StoredForkchoiceState { head, safe: from, finalized: genesis };
            let target = StoredForkchoiceState { safe: to, ..expected };
            original
                .set_pending_frontier_transition(PendingFrontierTransition { expected, target })
                .await
                .unwrap();
            client.push_fork_choice_updated(ScriptedResponse::Ok(valid_fcu()));
            assert!(apply_pending_frontier_transition(&provider, &mut engine, &original)
                .await
                .unwrap());
        }

        let path = original.tmp_dir().unwrap().path().join("test.db");
        original.inner().get_connection().clone().close().await.unwrap();
        let database = Database::new(path.to_str().unwrap()).await.unwrap();

        // Both safe-tag writes were lost. The database must still explain the original tag.
        let asserter = Asserter::new();
        push_tags(&asserter, &head_block, &head_block, &genesis_block);
        asserter.push_success(&Some(safe_block));
        asserter.push_success(&Some(head_block));
        let (provider, client, mut engine) =
            mock_engine(asserter, ForkchoiceState::from_block_info(genesis));
        client.push_fork_choice_updated(ScriptedResponse::Ok(valid_fcu()));

        recover_database_frontier(&provider, &mut engine, &database, HeadSelection::StoredHead)
            .await
            .unwrap();
        assert_eq!(engine.fcs(), &ForkchoiceState::new(head, safe, genesis));
        assert_eq!(database.get_pending_frontier_transition().await.unwrap(), None);
    }

    #[tokio::test]
    async fn safe_history_recovers_after_an_advance_pending_rewind_and_repeated_tag_loss() {
        use scroll_db::DatabaseConnectionProvider;

        for pending in [false, true] {
            for observed_number in [4, 5] {
                let original = setup_test_db().await;
                let (genesis_block, genesis) = rpc_block(0, B256::ZERO, 0xf0);
                let mut blocks = vec![(genesis_block, genesis)];
                for number in 1..=5 {
                    blocks.push(rpc_block(number, blocks.last().unwrap().1.hash, number as u8));
                }
                let head = blocks[5].1;
                let safe = blocks[if pending { 1 } else { 3 }].1;
                seed_frontier(&original, genesis, safe).await;
                original.set_l2_head_block_number(head.number).await.unwrap();
                // Two rewinds, then a forward safe move. Every old acknowledged start matters.
                for (from, to) in [(5, 4), (4, 2), (2, 3)] {
                    let expected =
                        StoredForkchoiceState { head, safe: blocks[from].1, finalized: genesis };
                    let target = StoredForkchoiceState { safe: blocks[to].1, ..expected };
                    original
                        .set_pending_frontier_transition(PendingFrontierTransition {
                            expected,
                            target,
                        })
                        .await
                        .unwrap();
                    original.clear_pending_frontier_transition().await.unwrap();
                }
                if pending {
                    let expected =
                        StoredForkchoiceState { head, safe: blocks[3].1, finalized: genesis };
                    let target = StoredForkchoiceState { safe, ..expected };
                    original
                        .set_pending_frontier_transition(PendingFrontierTransition {
                            expected,
                            target,
                        })
                        .await
                        .unwrap();
                }
                let path = original.tmp_dir().unwrap().path().join("test.db");
                original.inner().get_connection().clone().close().await.unwrap();

                // Even the recovery FCU can lose its tag write. Reopen again and repeat recovery.
                for attempt in 0..2 {
                    let database = Database::new(path.to_str().unwrap()).await.unwrap();
                    let asserter = Asserter::new();
                    let push_observed = || {
                        push_tags(&asserter, &blocks[5].0, &blocks[observed_number].0, &blocks[0].0)
                    };
                    push_observed();
                    if pending && attempt == 0 {
                        asserter.push_success(&Some(blocks[5].0.clone()));
                        push_observed();
                    }
                    asserter.push_success(&Some(blocks[safe.number as usize].0.clone()));
                    asserter.push_success(&Some(blocks[5].0.clone()));
                    let (provider, client, mut engine) =
                        mock_engine(asserter, ForkchoiceState::from_block_info(genesis));
                    client.push_fork_choice_updated(ScriptedResponse::Ok(valid_fcu()));
                    recover_database_frontier(
                        &provider,
                        &mut engine,
                        &database,
                        HeadSelection::StoredHead,
                    )
                    .await
                    .unwrap();
                    assert_eq!(engine.fcs(), &ForkchoiceState::new(head, safe, genesis));
                    assert_eq!(client.fork_choice_updated_calls(), 1);
                    assert_eq!(database.get_pending_frontier_transition().await.unwrap(), None);
                    database.inner().get_connection().clone().close().await.unwrap();
                }
            }
        }
    }

    #[tokio::test]
    async fn safe_history_rejects_unexplained_forks_and_disconnected_moves() {
        // A history is not blanket permission for an ahead tag, a hash mismatch, or a target
        // disconnected from the acknowledged moves.
        for case in 0..4 {
            let database = setup_test_db().await;
            let (genesis_block, genesis) = rpc_block(0, B256::ZERO, 0xf0);
            let (_, safe) = rpc_block(if case < 2 { 2 } else { 1 }, genesis.hash, 0xa1);
            let history_safe = rpc_block(2, genesis.hash, 0xa1).1;
            let (_, intermediate) = rpc_block(4, safe.hash, 0xa4);
            let (old_block, old) = rpc_block(5, intermediate.hash, 0xa5);
            seed_frontier(&database, genesis, safe).await;
            for (from, to) in [(old, intermediate), (intermediate, history_safe)] {
                let expected = StoredForkchoiceState { head: old, safe: from, finalized: genesis };
                let target = StoredForkchoiceState { safe: to, ..expected };
                database
                    .set_pending_frontier_transition(PendingFrontierTransition { expected, target })
                    .await
                    .unwrap();
                database.clear_pending_frontier_transition().await.unwrap();
            }
            if case >= 2 {
                let (_, unrelated) = rpc_block(3, genesis.hash, 0xb3);
                // Case 2 has no shared safe endpoint. Case 3 matches the DB target's height,
                // but not its hash, so neither can borrow the old 5 -> 4 evidence.
                let target_safe = if case == 2 { safe } else { rpc_block(1, genesis.hash, 0xb1).1 };
                let expected =
                    StoredForkchoiceState { head: old, safe: unrelated, finalized: genesis };
                let target = StoredForkchoiceState { safe: target_safe, ..expected };
                database
                    .set_pending_frontier_transition(PendingFrontierTransition { expected, target })
                    .await
                    .unwrap();
                database.clear_pending_frontier_transition().await.unwrap();
            }
            let (observed_block, observed) = match case {
                0 => rpc_block(5, intermediate.hash, 0xb5),
                1 => rpc_block(6, old.hash, 0xa6),
                _ => (old_block, old),
            };
            let asserter = Asserter::new();
            push_tags(&asserter, &observed_block, &observed_block, &genesis_block);
            let (provider, client, mut engine) =
                mock_engine(asserter, ForkchoiceState::from_block_info(genesis));
            let error = recover_database_frontier(
                &provider,
                &mut engine,
                &database,
                HeadSelection::StoredHead,
            )
            .await
            .unwrap_err();
            assert!(
                matches!(error, ChainOrchestratorError::FrontierConflict { tag: "safe", observed: block, .. } if block == observed)
            );
            assert_eq!(client.fork_choice_updated_calls(), 0);
            assert_eq!(database.get_pending_frontier_transition().await.unwrap(), None);
            assert_eq!(database.get_l2_head_block_number().await.unwrap(), safe.number);
        }
    }

    #[tokio::test]
    async fn acknowledged_old_fork_survives_finalization_on_the_new_branch() {
        let database = setup_test_db().await;
        let (genesis_block, genesis) = rpc_block(0, B256::ZERO, 0xf0);
        let (_, common) = rpc_block(1, genesis.hash, 0xa1);
        let (old_block, old) = rpc_block(2, common.hash, 0xa2);
        let (_, old_head) = rpc_block(3, old.hash, 0xa3);
        let (_, new_parent) = rpc_block(2, common.hash, 0xb2);
        let (safe_block, safe) = rpc_block(3, new_parent.hash, 0xb3);
        seed_frontier(&database, genesis, safe).await;
        database
            .update_batch_status(B256::ZERO, rollup_node_primitives::BatchStatus::Finalized)
            .await
            .unwrap();
        for (from, to) in [(old_head, old), (old, common), (common, safe)] {
            let expected = StoredForkchoiceState { head: old_head, safe: from, finalized: genesis };
            let target = if to == safe {
                StoredForkchoiceState { head: safe, safe, finalized: safe }
            } else {
                StoredForkchoiceState { safe: to, ..expected }
            };
            database
                .set_pending_frontier_transition(PendingFrontierTransition { expected, target })
                .await
                .unwrap();
            database.clear_pending_frontier_transition().await.unwrap();
        }
        let confirmed = database.get_confirmed_frontier_transition().await.unwrap().unwrap();
        assert!(confirmed.safe_starts().any(|start| start == old));
        let asserter = Asserter::new();
        push_tags(&asserter, &safe_block, &old_block, &genesis_block);
        // The observed finalized tag is a canonical ancestor of the new finalized block.
        asserter.push_success(&Some(genesis_block));
        asserter.push_success(&Some(safe_block));
        let (provider, client, mut engine) =
            mock_engine(asserter, ForkchoiceState::from_block_info(genesis));
        client.push_fork_choice_updated(ScriptedResponse::Ok(valid_fcu()));
        recover_database_frontier(&provider, &mut engine, &database, HeadSelection::StoredHead)
            .await
            .unwrap();
        assert_eq!(engine.fcs(), &ForkchoiceState::from_block_info(safe));
        assert_eq!(client.fork_choice_updated_calls(), 1);
    }

    #[tokio::test]
    async fn startup_rejects_database_frontier_conflicting_with_finalized() {
        let database = setup_test_db().await;
        let (genesis_block, genesis) = rpc_block(0, B256::ZERO, 0xf0);
        let (safe_block, safe) = rpc_block(100, genesis.hash, 0xa1);
        seed_frontier(&database, genesis, safe).await;
        let asserter = Asserter::new();
        push_tags(&asserter, &safe_block, &safe_block, &safe_block);
        let (provider, client, mut engine) =
            mock_engine(asserter, ForkchoiceState::from_block_info(genesis));
        let error =
            recover_database_frontier(&provider, &mut engine, &database, HeadSelection::StoredHead)
                .await
                .unwrap_err();
        assert!(matches!(error, ChainOrchestratorError::FrontierConflict { tag: "finalized", .. }));
        assert_eq!(client.fork_choice_updated_calls(), 0);
        assert_eq!(database.get_pending_frontier_transition().await.unwrap(), None);
        drop(genesis_block);
    }

    #[tokio::test]
    async fn absent_tags_fall_back_to_the_provider_genesis() {
        // A fresh node, and one that optimistic sync moved past genesis without setting tags.
        let (genesis_block, genesis) = rpc_block(0, B256::ZERO, 0xf0);
        let (head_block, head) = rpc_block(1, genesis.hash, 0xf1);
        for (block, info) in [(genesis_block.clone(), genesis), (head_block, head)] {
            let asserter = Asserter::new();
            asserter.push_success(&Some(block));
            asserter.push_success(&Option::<RpcBlock<ScrollRpcTransaction>>::None);
            asserter.push_success(&Option::<RpcBlock<ScrollRpcTransaction>>::None);
            if info != genesis {
                asserter.push_success(&Some(genesis_block.clone()));
            }
            let provider =
                ProviderBuilder::<_, _, Scroll>::default().connect_mocked_client(asserter);
            let observed = ForkchoiceState::try_from_provider(&provider).await.unwrap();
            assert_eq!(observed, Some(ForkchoiceState::new(info, genesis, genesis)));
        }
        let database = setup_test_db().await;
        let (provider, _, mut engine) =
            mock_engine(Asserter::new(), ForkchoiceState::from_genesis(B256::ZERO));
        let error =
            recover_database_frontier(&provider, &mut engine, &database, HeadSelection::StoredHead)
                .await
                .unwrap_err();
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
        let (provider, client, mut engine) =
            mock_engine(Asserter::new(), ForkchoiceState::from_genesis(B256::ZERO));
        assert!(ensure_database_frontier(
            &provider,
            &mut engine,
            &database,
            HeadSelection::StoredHead
        )
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
        assert!(ensure_database_frontier(
            &provider,
            &mut engine,
            &database,
            HeadSelection::StoredHead
        )
        .await
        .unwrap_err()
        .is_frontier_fatal());
        assert_eq!(client.fork_choice_updated_calls(), 0);
    }
}
