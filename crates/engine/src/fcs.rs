use crate::FcsError;
use alloy_chains::NamedChain;
use alloy_eips::{BlockId, BlockNumberOrTag};
use alloy_primitives::B256;
use alloy_provider::Provider;
use alloy_rpc_types_engine::ForkchoiceState as AlloyForkchoiceState;
use dogeos_chainspec::{DOGEOS_CHIKYU_GENESIS_HASH, DOGEOS_MAINNET_GENESIS_HASH};
use dogeos_rpc_types::Scroll;
use reth_chainspec::EthChainSpec;
use reth_primitives_traits::BlockHeader;
use rollup_node_primitives::BlockInfo;

/// The fork choice state.
///
/// The state is composed of the [`BlockInfo`] for `head`, `safe` block, and the `finalized`
/// blocks.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ForkchoiceState {
    head: BlockInfo,
    safe: BlockInfo,
    finalized: BlockInfo,
}

impl ForkchoiceState {
    /// Creates a new [`ForkchoiceState`] instance from the given [`BlockInfo`] instance.
    pub const fn from_block_info(block_info: BlockInfo) -> Self {
        Self::new(block_info, block_info, block_info)
    }

    /// Creates a new [`ForkchoiceState`] instance.
    pub const fn new(head: BlockInfo, safe: BlockInfo, finalized: BlockInfo) -> Self {
        Self { head, safe, finalized }
    }

    /// Creates a new [`ForkchoiceState`] instance setting the `head`, `safe` and `finalized` block
    /// info to the provided `genesis` hash.
    pub const fn from_genesis(genesis: B256) -> Self {
        Self::new(
            BlockInfo { hash: genesis, number: 0 },
            BlockInfo { hash: genesis, number: 0 },
            BlockInfo { hash: genesis, number: 0 },
        )
    }

    /// Reads the `latest`, `safe` and `finalized` tags from the provider and returns them clamped
    /// into `finalized <= safe <= head` (see [`clamp_startup_markers`]).
    ///
    /// Returns `None` if ANY of the three reads fails or is absent. That `None` is load-bearing:
    /// the caller distinguishes it from a provider that answered while sitting at genesis, and
    /// refuses to start on either when the database already knows a higher head.
    pub async fn from_provider<P: Provider<Scroll>>(provider: &P) -> Option<Self> {
        let latest_block =
            provider.get_block(BlockId::Number(BlockNumberOrTag::Latest)).await.ok()??;
        let mut safe_block =
            provider.get_block(BlockId::Number(BlockNumberOrTag::Safe)).await.ok()??;
        let finalized_block =
            provider.get_block(BlockId::Number(BlockNumberOrTag::Finalized)).await.ok()??;

        let (safe_number, finalized_number) = clamp_startup_markers(
            latest_block.header.number,
            safe_block.header.number,
            finalized_block.header.number,
        );
        if safe_number != safe_block.header.number {
            safe_block = if safe_number == finalized_block.header.number {
                finalized_block.clone()
            } else {
                latest_block.clone()
            };
        }
        let finalized_block = if finalized_number == finalized_block.header.number {
            finalized_block
        } else {
            latest_block.clone()
        };

        Some(Self {
            head: BlockInfo { number: latest_block.header.number, hash: latest_block.header.hash },
            safe: BlockInfo { number: safe_block.header.number, hash: safe_block.header.hash },
            finalized: BlockInfo {
                number: finalized_block.header.number,
                hash: finalized_block.header.hash,
            },
        })
    }

    /// Creates a [`ForkchoiceState`] instance setting the `head`, `safe` and `finalized` hash to
    /// the appropriate genesis values depending on the named chain.
    pub fn head_from_chain_spec<CS: EthChainSpec<Header: BlockHeader>>(
        chain_spec: CS,
    ) -> Option<Self> {
        Some(Self::from_genesis(genesis_hash_from_chain_spec(chain_spec)?))
    }

    /// Update the forkchoice state with the given `head`, `safe` and `finalized` block info.
    pub fn update(
        &mut self,
        head: Option<BlockInfo>,
        safe: Option<BlockInfo>,
        finalized: Option<BlockInfo>,
    ) -> Result<(), FcsError> {
        tracing::debug!(target: "scroll::engine::fcs", ?head, ?safe, ?finalized, current = ?self, "Updating fork choice state");
        // Check that at least one of head, safe or finalized is Some.
        if head.is_none() && safe.is_none() && finalized.is_none() {
            return Err(FcsError::NoUpdateProvided);
        }

        // Build the candidate new state.
        let new_finalized = finalized.unwrap_or(self.finalized);
        let new_safe = safe.unwrap_or(self.safe);
        let new_head = head.unwrap_or(self.head);

        // Check that the finalized block number is increasing or stays the same with the same hash.
        if new_finalized.number <= self.finalized.number && new_finalized != self.finalized {
            return Err(FcsError::FinalizedBlockNumberNotIncreasing);
        }

        // Assert invariants: head >= safe >= finalized.
        if new_head.number < new_safe.number {
            return Err(FcsError::HeadBelowSafe);
        }

        if new_safe.number < new_finalized.number {
            return Err(FcsError::SafeBelowFinalized);
        }

        // Commit the state.
        self.head = new_head;
        self.safe = new_safe;
        self.finalized = new_finalized;

        Ok(())
    }

    /// Updates the `head` block info.
    pub fn update_head_block_info(&mut self, head: BlockInfo) -> Result<(), FcsError> {
        self.update(Some(head), None, None)
    }

    /// Updates the `safe` block info.
    pub fn update_safe_block_info(&mut self, safe: BlockInfo) -> Result<(), FcsError> {
        self.update(None, Some(safe), None)
    }

    /// Updates the `finalized` block info.
    pub fn update_finalized_block_info(&mut self, finalized: BlockInfo) -> Result<(), FcsError> {
        self.update(None, None, Some(finalized))
    }

    /// Returns the block info for the `head` block.
    pub const fn head_block_info(&self) -> &BlockInfo {
        &self.head
    }

    /// Returns the block info for the `safe` block.
    pub const fn safe_block_info(&self) -> &BlockInfo {
        &self.safe
    }

    /// Returns the block info for the `finalized` block.
    pub const fn finalized_block_info(&self) -> &BlockInfo {
        &self.finalized
    }

    /// Returns the [`AlloyForkchoiceState`] representation of the fork choice state.
    pub const fn get_alloy_fcs(&self) -> AlloyForkchoiceState {
        AlloyForkchoiceState {
            head_block_hash: self.head.hash,
            safe_block_hash: self.safe.hash,
            finalized_block_hash: self.finalized.hash,
        }
    }

    /// Returns the [`AlloyForkchoiceState`] representation of the fork choice state, with the safe
    /// and finalized hashes set to 0x0.
    pub fn get_alloy_optimistic_fcs(&self) -> AlloyForkchoiceState {
        AlloyForkchoiceState {
            head_block_hash: self.head.hash,
            safe_block_hash: B256::default(),
            finalized_block_hash: B256::default(),
        }
    }

    /// Returns `true` if the fork choice state is the genesis state.
    pub const fn is_genesis(&self) -> bool {
        self.head.number == 0
    }
}

/// Clamps the startup safe and finalized markers read from the execution node into
/// `finalized <= safe <= head`, returning `(safe, finalized)`.
///
/// [`ForkchoiceState`] documents that ordering, and every later `update()` refuses with
/// `HeadBelowSafe`/`SafeBelowFinalized` when it does not hold — after the database is already
/// unwound, with nothing on the restart path to lower either marker. So a violation here is not a
/// transient state to run through, it is a node that never launches again.
///
/// Two shapes reach this. A crash between an unwind's durable database commit and the FCU that
/// lowers the safe marker leaves the execution node holding safe ABOVE the head this node resumes
/// from. And a finalized marker above the head would, if only safe were clamped, produce
/// `safe < finalized` — trading the launch failure for a node that starts, reads healthy, and can
/// never issue another forkchoice update. Finalized is therefore clamped FIRST, and safe is
/// floored on the clamped value.
///
/// The runtime reorg and administrative-unwind paths already drag their safe target down to the
/// effective head; startup is the seam that never did.
pub(crate) const fn clamp_startup_markers(head: u64, safe: u64, finalized: u64) -> (u64, u64) {
    let finalized = if finalized > head { head } else { finalized };
    let safe = if safe < finalized {
        finalized
    } else if safe > head {
        head
    } else {
        safe
    };
    (safe, finalized)
}

/// Returns the genesis hash for the given chain spec.
pub fn genesis_hash_from_chain_spec<CS: EthChainSpec<Header: BlockHeader>>(
    chain_spec: CS,
) -> Option<B256> {
    match chain_spec.chain().named() {
        Some(NamedChain::Scroll) => Some(DOGEOS_MAINNET_GENESIS_HASH),
        Some(NamedChain::ScrollSepolia) => Some(DOGEOS_CHIKYU_GENESIS_HASH),
        // `genesis_hash()` returns the SEALED hash when the spec carries one and
        // recomputes only otherwise. `genesis_header().hash_slow()` always
        // recomputes, and the two differ: chikyu's genesis document is
        // byte-identical to mainnet's in every field the header is built from,
        // so recomputing yields MAINNET's genesis hash for chikyu. Using it
        // made the database, the forkchoice fallback and the EL disagree on
        // chikyu's block 0 — a fresh node diverged at the first finalized
        // notification and an existing one failed startup outright.
        Some(NamedChain::Dev) | None => Some(chain_spec.genesis_hash()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::clamp_startup_markers;

    /// Pins the startup clamp that keeps a node launchable after a crash between
    /// an unwind's database commit and its safe-lowering forkchoice update.
    /// Without it the execution node's stale markers propagate a `HeadBelowSafe`
    /// (or, clamping only safe, a `SafeBelowFinalized`) out of `build()` on every
    /// restart, with nothing left to lower them.
    #[test]
    fn clamp_startup_markers_table() {
        // (head, safe, finalized) -> (safe, finalized)
        let cases: &[(u64, u64, u64, u64, u64)] = &[
            // Safe stranded above the head by a lost unwind FCU: drag it down.
            (100, 150, 40, 100, 40),
            // Safe below finalized: raise it to the floor.
            (100, 30, 40, 40, 40),
            // Finalized above the head clamps FIRST, so safe cannot be floored
            // onto a value above the head and end up below finalized.
            (100, 150, 200, 100, 100),
            (100, 30, 200, 100, 100),
            // Boundaries are already ordered and must pass through untouched.
            (100, 100, 40, 100, 40),
            (100, 40, 40, 40, 40),
            (100, 100, 100, 100, 100),
            // The ordinary case.
            (100, 80, 40, 80, 40),
        ];
        for (head, safe, finalized, want_safe, want_finalized) in cases {
            let got = clamp_startup_markers(*head, *safe, *finalized);
            assert_eq!(
                got,
                (*want_safe, *want_finalized),
                "clamp_startup_markers({head}, {safe}, {finalized})"
            );
            let (safe, finalized) = got;
            assert!(finalized <= safe && safe <= *head, "ordering violated: {got:?}");
        }
    }
}
