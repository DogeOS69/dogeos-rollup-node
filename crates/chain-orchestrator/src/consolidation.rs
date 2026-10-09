use super::ChainOrchestratorError;
use alloy_provider::Provider;
use dogeos_rpc_types::Scroll;
use futures::{stream::FuturesOrdered, TryStreamExt};
use rollup_node_primitives::{
    BatchConsolidationOutcome, BatchInfo, BatchStatus, BlockInfo, L2BlockInfoWithL1Messages,
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
    frontier: BlockInfo,
) -> Result<BatchReconciliationResult, ChainOrchestratorError> {
    validate_block_sequence(batch, frontier.number)?;

    let mut futures = FuturesOrdered::new();
    for attributes in &batch.attributes {
        let l2_provider = &l2_provider;
        let fut = async move {
            // Fetch the block corresponding to the derived attributes from the L2 provider.
            let current_block = l2_provider
                .get_block(attributes.block_number.into())
                .full()
                .await?
                .map(|b| b.into_consensus().map_transactions(|tx| tx.inner.into_inner()));

            Ok::<_, ChainOrchestratorError>(current_block)
        };
        futures.push_back(fut);
    }

    let current_blocks: Vec<_> = futures.try_collect().await?;
    let mut actions = Vec::with_capacity(batch.attributes.len());
    let mut expected_parent = frontier;

    for (index, (attributes, current_block)) in
        batch.attributes.iter().zip(current_blocks).enumerate()
    {
        // A duplicate delivery may replay a batch whose blocks are already covered by the
        // database-backed safe frontier. Verify that prefix against the canonical provider and
        // treat it as idempotently complete; safe history must never be rebuilt.
        if attributes.block_number <= frontier.number {
            let Some(current_block) = current_block else {
                return Err(ChainOrchestratorError::SafeBatchReplayMismatch {
                    batch_info: batch.batch_info,
                    block_number: attributes.block_number,
                    frontier,
                })
            };
            if current_block.header.number != attributes.block_number ||
                !block_matches_attributes(&attributes.attributes, &current_block)
            {
                return Err(ChainOrchestratorError::SafeBatchReplayMismatch {
                    batch_info: batch.batch_info,
                    block_number: attributes.block_number,
                    frontier,
                })
            }

            let block_info: L2BlockInfoWithL1Messages = (&current_block).into();
            if attributes.block_number == frontier.number && block_info.block_info != frontier {
                return Err(ChainOrchestratorError::SafeBatchReplayMismatch {
                    batch_info: batch.batch_info,
                    block_number: attributes.block_number,
                    frontier,
                })
            }
            expected_parent = block_info.block_info;
            actions.push(BlockConsolidationAction::Skip(block_info));
            continue
        }

        let Some(current_block) = current_block else {
            actions.push(BlockConsolidationAction::ReorgSuffix {
                first_attribute_index: index,
                expected_parent,
            });
            break
        };

        let parent_matches = current_block.header.parent_hash == expected_parent.hash;
        let number_matches = current_block.header.number == attributes.block_number;
        if !parent_matches ||
            !number_matches ||
            !block_matches_attributes(&attributes.attributes, &current_block)
        {
            actions.push(BlockConsolidationAction::ReorgSuffix {
                first_attribute_index: index,
                expected_parent,
            });
            break
        }

        let block_info: L2BlockInfoWithL1Messages = (&current_block).into();
        expected_parent = block_info.block_info;
        if attributes.block_number <= fcs.finalized_block_info().number {
            actions.push(BlockConsolidationAction::Skip(block_info));
        } else {
            actions.push(BlockConsolidationAction::UpdateFcs(block_info));
        }
    }

    Ok(BatchReconciliationResult {
        batch_info: batch.batch_info,
        actions,
        target_status: batch.target_status,
    })
}

/// Validates a derived batch's block-number sequence before issuing any L2 RPCs.
///
/// A fresh suffix must start immediately after the database frontier. A replay may start at or
/// below the frontier, but every subsequent attribute must still be contiguous. This preserves
/// idempotent reboot/re-delivery while rejecting gaps and duplicates before external work begins.
fn validate_block_sequence(
    batch: &BatchDerivationResult,
    frontier_block_number: u64,
) -> Result<(), ChainOrchestratorError> {
    let mut previous_block_number = None;

    for (attribute_index, attributes) in batch.attributes.iter().enumerate() {
        let anchor = previous_block_number.unwrap_or(frontier_block_number);
        let invalid = match previous_block_number {
            Some(previous) => previous.checked_add(1) != Some(attributes.block_number),
            None if attributes.block_number > frontier_block_number => {
                frontier_block_number.checked_add(1) != Some(attributes.block_number)
            }
            None => false,
        };

        if invalid {
            return Err(ChainOrchestratorError::InvalidDerivedBlockSequence {
                batch_info: batch.batch_info,
                attribute_index,
                previous_block_number: anchor,
                actual_block_number: attributes.block_number,
            })
        }
        previous_block_number = Some(attributes.block_number);
    }

    Ok(())
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
    /// Rebuild the remaining derived suffix on the exact tip of the verified canonical prefix.
    ReorgSuffix {
        /// The first derived attribute that must be rebuilt.
        first_attribute_index: usize,
        /// The exact parent on which the suffix must be built.
        expected_parent: BlockInfo,
    },
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
        matches!(self, Self::ReorgSuffix { .. })
    }

    /// Consumes the action and returns the block info if the action is to update the safe head or
    /// skip, returns None for reorg.
    pub(crate) fn into_block_info(self) -> Option<L2BlockInfoWithL1Messages> {
        match self {
            Self::UpdateFcs(info) | Self::Skip(info) => Some(info),
            Self::ReorgSuffix { .. } => None,
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
            Self::ReorgSuffix { first_attribute_index, expected_parent } => write!(
                f,
                "Reorg derived suffix at index {first_attribute_index} on parent {expected_parent}"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_consensus::{Header as ConsensusHeader, Sealable};
    use alloy_primitives::B256;
    use alloy_provider::ProviderBuilder;
    use alloy_rpc_types_eth::{Block as RpcBlock, Header as RpcHeader};
    use alloy_transport::mock::Asserter;
    use dogeos_reth_engine::ScrollPayloadAttributes;
    use dogeos_rpc_types::ScrollRpcTransaction;
    use scroll_derivation_pipeline::DerivedAttributes;

    fn batch(numbers: &[u64]) -> BatchDerivationResult {
        BatchDerivationResult {
            attributes: numbers
                .iter()
                .copied()
                .map(|block_number| DerivedAttributes {
                    block_number,
                    attributes: ScrollPayloadAttributes::default(),
                })
                .collect(),
            batch_info: BatchInfo::new(7, B256::repeat_byte(7)),
            skipped_l1_messages: vec![],
            target_status: BatchStatus::Consolidated,
        }
    }

    #[test]
    fn block_sequence_accepts_fresh_and_replayed_ranges() {
        assert!(validate_block_sequence(&batch(&[101, 102, 103]), 100).is_ok());
        assert!(validate_block_sequence(&batch(&[101, 102]), 102).is_ok());
        assert!(validate_block_sequence(&batch(&[101, 102, 103]), 102).is_ok());
        assert!(validate_block_sequence(&batch(&[u64::MAX]), u64::MAX).is_ok());
        assert!(validate_block_sequence(&batch(&[]), 100).is_ok());
    }

    #[test]
    fn block_sequence_rejects_gaps_duplicates_and_overflow() {
        for (numbers, frontier, attribute_index, previous, actual) in [
            (vec![102], 100, 0, 100, 102),
            (vec![101, 103], 100, 1, 101, 103),
            (vec![101, 101], 100, 1, 101, 101),
            (vec![u64::MAX, u64::MAX], u64::MAX - 1, 1, u64::MAX, u64::MAX),
        ] {
            let error = validate_block_sequence(&batch(&numbers), frontier).unwrap_err();
            assert!(matches!(
                error,
                ChainOrchestratorError::InvalidDerivedBlockSequence {
                    attribute_index: actual_index,
                    previous_block_number,
                    actual_block_number,
                    ..
                } if actual_index == attribute_index &&
                    previous_block_number == previous &&
                    actual_block_number == actual
            ));
        }
    }

    #[tokio::test]
    async fn parent_mismatch_reorgs_the_remaining_suffix() {
        let frontier = BlockInfo::new(100, B256::repeat_byte(0x10));
        let mut batch = batch(&[101, 102, 103]);
        for attributes in &mut batch.attributes {
            attributes.attributes.transactions = Some(vec![]);
        }

        let first_header =
            ConsensusHeader { parent_hash: frontier.hash, number: 101, ..Default::default() };
        let first_sealed = first_header.seal_slow();
        let first_info = BlockInfo::new(101, first_sealed.hash());
        let first = RpcBlock::<ScrollRpcTransaction, _>::empty(RpcHeader::from_consensus(
            first_sealed,
            None,
            None,
        ));

        let wrong_parent_header =
            ConsensusHeader { parent_hash: frontier.hash, number: 102, ..Default::default() };
        let wrong_parent = RpcBlock::<ScrollRpcTransaction, _>::empty(RpcHeader::from_consensus(
            wrong_parent_header.seal_slow(),
            None,
            None,
        ));

        // This block would match the last verified parent if considered independently. Once the
        // previous block mismatches, however, it remains part of the suffix to rebuild.
        let suffix_header =
            ConsensusHeader { parent_hash: first_info.hash, number: 103, ..Default::default() };
        let suffix = RpcBlock::<ScrollRpcTransaction, _>::empty(RpcHeader::from_consensus(
            suffix_header.seal_slow(),
            None,
            None,
        ));

        let asserter = Asserter::new();
        asserter.push_success(&Some(first));
        asserter.push_success(&Some(wrong_parent));
        asserter.push_success(&Some(suffix));
        let provider = ProviderBuilder::<_, _, Scroll>::default().connect_mocked_client(asserter);
        let fcs = ForkchoiceState::new(frontier, frontier, BlockInfo::default());

        let result = reconcile_batch(provider, &batch, &fcs, frontier).await.unwrap();
        assert_eq!(result.actions.len(), 2);
        assert!(matches!(
            &result.actions[0],
            BlockConsolidationAction::UpdateFcs(block) if block.block_info == first_info
        ));
        assert!(matches!(
            result.actions[1],
            BlockConsolidationAction::ReorgSuffix {
                first_attribute_index: 1,
                expected_parent,
            } if expected_parent == first_info
        ));
    }
}
