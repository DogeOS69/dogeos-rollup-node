use super::ChainOrchestratorError;
use alloy_provider::Provider;
use dogeos_rpc_types::Scroll;
use futures::{stream::FuturesOrdered, TryStreamExt};
use rollup_node_primitives::{
    BatchConsolidationOutcome, BatchInfo, BatchStatus, L2BlockInfoWithL1Messages,
};
use scroll_derivation_pipeline::BatchDerivationResult;
use scroll_engine::{block_matches_attributes, ForkchoiceState};

/// Reconciles a batch of derived attributes with the L2 chain to produce a reconciliation result.
///
/// The batch is borrowed so reconciliation can be recomputed from fresh canonical L2 state on
/// every attempt. Reorg actions retain stable indices into the held batch instead of cloning the
/// derived attributes.
pub(crate) async fn reconcile_batch<L2P: Provider<Scroll>>(
    l2_provider: L2P,
    batch: &BatchDerivationResult,
    fcs: &ForkchoiceState,
) -> Result<BatchReconciliationResult, ChainOrchestratorError> {
    let mut futures = FuturesOrdered::new();
    for (index, attributes) in batch.attributes.iter().enumerate() {
        let l2_provider = &l2_provider;
        let fut = async move {
            // Fetch the block corresponding to the derived attributes from the L2 provider.
            let current_block = l2_provider
                .get_block(attributes.block_number.into())
                .full()
                .await?
                .map(|b| b.into_consensus().map_transactions(|tx| tx.inner.into_inner()));

            let current_block = if let Some(block) = current_block {
                block
            } else {
                // The block does not exist, a reorg is needed.
                return Ok::<_, ChainOrchestratorError>(BlockConsolidationAction::Reorg(index));
            };

            // Check if the block matches the derived attributes.
            if block_matches_attributes(&attributes.attributes, attributes.base_fee, &current_block)
            {
                // Extract the block info with L1 messages.
                let block_info: L2BlockInfoWithL1Messages = (&current_block).into();

                // The derived attributes match the L2 chain but are associated with a block
                // number less than or equal to the finalized block, so skip.
                if attributes.block_number <= fcs.finalized_block_info().number {
                    Ok::<_, ChainOrchestratorError>(BlockConsolidationAction::Skip(block_info))
                } else {
                    // The block matches the derived attributes but is above the finalized block,
                    // so we need to update the fcs.
                    Ok::<_, ChainOrchestratorError>(BlockConsolidationAction::UpdateFcs(block_info))
                }
            } else {
                // The block does not match the derived attributes, a reorg is needed.
                Ok::<_, ChainOrchestratorError>(BlockConsolidationAction::Reorg(index))
            }
        };
        futures.push_back(fut);
    }

    let mut actions: Vec<BlockConsolidationAction> = futures.try_collect().await?;

    // Every block was compared against the L2 chain as it stands now, before any of these
    // actions ran. Once one block is rebuilt, the blocks after it are children of a block that
    // no longer exists, so a later block that matched the old chain must not be kept: fork
    // choice would be moved back onto the discarded branch. From the first rebuild on,
    // everything is rebuilt, in order.
    if let Some(first_reorg) = actions.iter().position(BlockConsolidationAction::is_reorg) {
        for (index, action) in actions.iter_mut().enumerate().skip(first_reorg) {
            *action = BlockConsolidationAction::Reorg(index);
        }
    }

    Ok(BatchReconciliationResult {
        batch_info: batch.batch_info,
        actions,
        target_status: batch.target_status,
    })
}

/// The result of reconciling a batch with the L2 chain.
#[derive(Debug)]
pub(crate) struct BatchReconciliationResult {
    /// The batch info for the consolidated batch.
    pub batch_info: BatchInfo,
    /// The actions that must be performed on the L2 chain to consolidate the batch.
    pub actions: Vec<BlockConsolidationAction>,
    /// The target status of the batch after consolidation.
    pub target_status: BatchStatus,
}

impl BatchReconciliationResult {
    /// Aggregates the block consolidation actions into an aggregated set of actions required to
    /// consolidate the L2 chain with the batch.
    pub(crate) fn aggregate_actions(&self) -> AggregatedBatchConsolidationActions {
        let mut actions: Vec<BlockConsolidationAction> = vec![];
        for next in &self.actions {
            if let Some(last) = actions.last_mut() {
                match (last, next) {
                    (last, next) if last.is_update_fcs() && next.is_update_fcs() => {
                        *last = next.clone();
                    }
                    _ => {
                        actions.push(next.clone());
                    }
                }
            } else if !next.is_skip() {
                actions.push(next.clone());
            }
        }
        AggregatedBatchConsolidationActions { actions }
    }

    /// Consumes the reconciliation result and produces the consolidated chain by combining
    /// non-reorg block info with the reorg block results.
    pub(crate) async fn into_batch_consolidation_outcome(
        self,
        reorg_results: Vec<L2BlockInfoWithL1Messages>,
        l2_head_updated: bool,
    ) -> Result<BatchConsolidationOutcome, ChainOrchestratorError> {
        let mut consolidate_chain =
            BatchConsolidationOutcome::new(self.batch_info, self.target_status, l2_head_updated);

        // First append all non-reorg results to the consolidated chain.
        self.actions.into_iter().filter(|action| !action.is_reorg()).for_each(|action| {
            consolidate_chain.push_block(action.into_block_info().expect("must have block info"));
        });

        // Append the reorg results at the end of the consolidated chain.
        for block in reorg_results {
            consolidate_chain.push_block(block);
        }

        Ok(consolidate_chain)
    }
}

/// The aggregated actions that must be performed on the L2 chain to consolidate a batch.
#[derive(Debug, Clone)]
pub(crate) struct AggregatedBatchConsolidationActions {
    /// The aggregated actions that must be performed on the L2 chain to consolidate a batch.
    pub actions: Vec<BlockConsolidationAction>,
}

/// An action that must be performed on the L2 chain to consolidate a block.
#[derive(Debug, Clone)]
pub(crate) enum BlockConsolidationAction {
    /// Update the fcs to the given block.
    UpdateFcs(L2BlockInfoWithL1Messages),
    /// The derived attributes match the L2 chain and the safe head is already at or beyond the
    /// block, so no action is needed.
    Skip(L2BlockInfoWithL1Messages),
    /// Reorganize the chain with the derived attributes at this stable pending-batch index.
    Reorg(usize),
}

impl BlockConsolidationAction {
    /// Returns true if the action is to update the fcs.
    pub(crate) const fn is_update_fcs(&self) -> bool {
        matches!(self, Self::UpdateFcs(_))
    }

    /// Returns true if the action is to skip the block.
    pub(crate) const fn is_skip(&self) -> bool {
        matches!(self, Self::Skip(_))
    }

    /// Returns true if the action is to perform a reorg.
    pub(crate) const fn is_reorg(&self) -> bool {
        matches!(self, Self::Reorg(_))
    }

    /// Consumes the action and returns the block info if the action is to update the safe head or
    /// skip, returns None for reorg.
    pub(crate) fn into_block_info(self) -> Option<L2BlockInfoWithL1Messages> {
        match self {
            Self::UpdateFcs(info) | Self::Skip(info) => Some(info),
            Self::Reorg(_) => None,
        }
    }
}

impl std::fmt::Display for BlockConsolidationAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UpdateFcs(info) => {
                write!(f, "UpdateSafeHead to block {}", info.block_info.number)
            }
            Self::Skip(info) => write!(f, "Skip block {}", info.block_info.number),
            Self::Reorg(index) => write!(f, "Reorg with derived attributes at index {index}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_consensus::Header as ConsensusHeader;
    use alloy_primitives::{Sealable, B256, U256};
    use alloy_provider::ProviderBuilder;
    use alloy_rpc_types_eth::{Block as RpcBlock, Header as RpcHeader};
    use alloy_transport::mock::Asserter;
    use dogeos_reth_engine::ScrollPayloadAttributes;
    use dogeos_rpc_types::ScrollRpcTransaction;
    use rollup_node_primitives::{BatchStatus, BlockInfo};
    use scroll_derivation_pipeline::DerivedAttributes;

    const SAFE: u64 = 100;
    const DERIVED_BASE_FEE: u64 = 1_015_680_000;

    fn derived(block_number: u64, base_fee: u64) -> DerivedAttributes {
        DerivedAttributes {
            block_number,
            base_fee: U256::from(base_fee),
            attributes: ScrollPayloadAttributes {
                transactions: Some(vec![]),
                no_tx_pool: true,
                ..Default::default()
            },
        }
    }

    /// Answers the L2 provider's block-by-number request with a block that matches `attributes`
    /// except for its base fee, which is whatever the chain actually has.
    fn push_block(asserter: &Asserter, number: u64, base_fee: u64) {
        let header =
            ConsensusHeader { number, base_fee_per_gas: Some(base_fee), ..Default::default() };
        let rpc_header = RpcHeader::from_consensus(header.seal_slow(), None, None);
        asserter.push_success(&Some(RpcBlock::<ScrollRpcTransaction, _>::empty(rpc_header)));
    }

    fn batch(attributes: Vec<DerivedAttributes>) -> BatchDerivationResult {
        BatchDerivationResult {
            attributes,
            batch_info: BatchInfo::new(1, B256::repeat_byte(1)),
            skipped_l1_messages: vec![],
            target_status: BatchStatus::Consolidated,
        }
    }

    fn fcs_at(number: u64) -> ForkchoiceState {
        ForkchoiceState::from_block_info(BlockInfo { number, hash: B256::repeat_byte(0x11) })
    }

    /// Every block is compared against the chain as it stands, before any of the actions run.
    /// A block that differs only in its base fee is rebuilt, and so is every block after it:
    /// they are children of a block that no longer exists, and keeping one would move fork
    /// choice back onto the discarded branch.
    #[tokio::test]
    async fn a_base_fee_mismatch_rebuilds_the_rest_of_the_batch() {
        let asserter = Asserter::new();
        // Block n disagrees on the base fee; n + 1 and n + 2 match the old chain exactly.
        push_block(&asserter, SAFE + 1, DERIVED_BASE_FEE - 1);
        push_block(&asserter, SAFE + 2, DERIVED_BASE_FEE);
        push_block(&asserter, SAFE + 3, DERIVED_BASE_FEE);
        let provider = ProviderBuilder::<_, _, Scroll>::default().connect_mocked_client(asserter);

        let batch = batch(vec![
            derived(SAFE + 1, DERIVED_BASE_FEE),
            derived(SAFE + 2, DERIVED_BASE_FEE),
            derived(SAFE + 3, DERIVED_BASE_FEE),
        ]);
        let result = reconcile_batch(provider, &batch, &fcs_at(SAFE)).await.unwrap();

        assert!(
            matches!(
                result.actions.as_slice(),
                [
                    BlockConsolidationAction::Reorg(0),
                    BlockConsolidationAction::Reorg(1),
                    BlockConsolidationAction::Reorg(2),
                ]
            ),
            "the suffix was not rebuilt: {:?}",
            result.actions
        );
        // And nothing survives aggregation that would move the head back.
        assert_eq!(result.aggregate_actions().actions.len(), 3);
    }

    /// A chain that agrees with the batch is left alone, so the rebuild above is the mismatch's
    /// doing rather than the rule firing on everything.
    #[tokio::test]
    async fn a_matching_chain_is_not_rebuilt() {
        let asserter = Asserter::new();
        for number in [SAFE + 1, SAFE + 2] {
            push_block(&asserter, number, DERIVED_BASE_FEE);
        }
        let provider = ProviderBuilder::<_, _, Scroll>::default().connect_mocked_client(asserter);

        let batch =
            batch(vec![derived(SAFE + 1, DERIVED_BASE_FEE), derived(SAFE + 2, DERIVED_BASE_FEE)]);
        let result = reconcile_batch(provider, &batch, &fcs_at(SAFE)).await.unwrap();

        assert!(
            result.actions.iter().all(|action| action.is_update_fcs()),
            "a matching chain was rebuilt: {:?}",
            result.actions
        );
    }

    /// A base-fee divergence at or below the finalized head cannot be reorged away, so it stops
    /// consolidation instead of being skipped. A finalized block that DA does not describe is a
    /// divergence the node must not paper over.
    #[tokio::test]
    async fn a_divergence_below_the_finalized_head_is_not_skipped() {
        let asserter = Asserter::new();
        push_block(&asserter, SAFE + 1, DERIVED_BASE_FEE - 1);
        let provider = ProviderBuilder::<_, _, Scroll>::default().connect_mocked_client(asserter);

        let batch = batch(vec![derived(SAFE + 1, DERIVED_BASE_FEE)]);
        // The finalized head is already past the derived block.
        let result = reconcile_batch(provider, &batch, &fcs_at(SAFE + 1)).await.unwrap();

        assert!(
            matches!(result.actions.as_slice(), [BlockConsolidationAction::Reorg(0)]),
            "a finalized divergence was skipped: {:?}",
            result.actions
        );
    }
}
