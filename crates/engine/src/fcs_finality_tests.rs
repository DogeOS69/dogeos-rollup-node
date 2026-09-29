//! Verification tests for REORG_FINDINGS F9 (`REORG_RUST_TEST_CASES.md` case 17, engine level):
//! once L2 blocks are finalized, the forkchoice state cannot be moved below them. The L1 reorg
//! path (`ChainOrchestrator::handle_l1_reorg`) calls `update_fcs(head, safe, None)` after the
//! database unwind lowers the safe block; with the finalized block above that safe block the
//! update fails with `SafeBelowFinalized`, and passing the lower finalized block explicitly fails
//! with `FinalizedBlockNumberNotIncreasing`. Assertions document current behaviour.
//!
//! Tracker rows (Reorg Issue Tracker, Private Mainnet): RG-46 (L2 finalized at zero synthetic depth
//! cannot be undone). Each test name starts with the RG key of the row it pins. Every test passes
//! today and documents behaviour that is not fixed; when a row is fixed, change the matching
//! assertion instead of deleting the test.

use crate::{test_utils::PanicEngineClient, Engine, EngineError, FcsError, ForkchoiceState};
use alloy_primitives::B256;
use rollup_node_primitives::BlockInfo;
use std::{future::Future, sync::Arc};

fn info(number: u64, tag: u8) -> BlockInfo {
    BlockInfo { number, hash: B256::repeat_byte(tag) }
}

#[test]
fn rg46_finalized_blocks_cannot_be_unfinalized_by_the_reorg_path() {
    // L2 blocks up to 10 were finalized at zero L1 depth (their batch's FinalizeBatch sat in the
    // synthetic head block).
    let finalized = info(10, 0x0a);
    let mut fcs = ForkchoiceState::new(finalized, finalized, finalized);

    // The synthetic L1 reorg removes that batch: the database's latest safe block is now 5, and
    // the reorg path passes no finalized value.
    let surviving_safe = info(5, 0x05);
    let err = fcs.update(Some(surviving_safe), Some(surviving_safe), None).unwrap_err();
    assert!(matches!(err, FcsError::SafeBelowFinalized), "got {err:?}");
    let err = fcs.update(None, Some(surviving_safe), None).unwrap_err();
    assert!(matches!(err, FcsError::SafeBelowFinalized), "got {err:?}");

    // Moving the finalized block back explicitly is rejected as well.
    let err =
        fcs.update(Some(surviving_safe), Some(surviving_safe), Some(surviving_safe)).unwrap_err();
    assert!(matches!(err, FcsError::FinalizedBlockNumberNotIncreasing), "got {err:?}");

    // A replacement block at a finalized height is rejected too (same number, other hash).
    let replacement = info(10, 0xee);
    let err = fcs.update(Some(replacement), Some(replacement), Some(replacement)).unwrap_err();
    assert!(matches!(err, FcsError::FinalizedBlockNumberNotIncreasing), "got {err:?}");

    // Nothing changed.
    assert_eq!(fcs, ForkchoiceState::new(finalized, finalized, finalized));
}

/// Polls `fut` once; the rejection must not wait on anything.
fn poll_once<F: Future>(fut: F) -> F::Output {
    let mut fut = std::pin::pin!(fut);
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    match fut.as_mut().poll(&mut cx) {
        std::task::Poll::Ready(output) => output,
        std::task::Poll::Pending => panic!("the update awaited the execution layer"),
    }
}

#[test]
fn rg46_engine_rejects_the_reorg_update_before_calling_the_execution_layer() {
    let finalized = info(10, 0x0a);
    // PanicEngineClient panics on any Engine API call: the rejection happens locally.
    let mut engine = Engine::new(
        Arc::new(PanicEngineClient),
        ForkchoiceState::new(finalized, finalized, finalized),
    );

    let err = poll_once(engine.update_fcs(None, Some(info(5, 0x05)), None)).unwrap_err();
    assert!(matches!(err, EngineError::FcsError(FcsError::SafeBelowFinalized)), "got {err:?}");
    assert_eq!(engine.fcs(), &ForkchoiceState::new(finalized, finalized, finalized));
}
