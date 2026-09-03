# vmark-pr-review follow-ups — PR #45 (issue #38 CI stabilization)

Aggregated unresolved-findings ledger for the whole review cycle. Every finding
from every pass is either fixed in the PR or recorded here.

## Unresolved

- **Remote block source has no metrics** (`crates/node/src/add_ons/remote_block_source.rs`)
  - Impact/evidence: Claude pass 1 m6 and pass 5 m2 — the add-on exports no
    metrics and `ChainOrchestratorStatus` does not model it, so a node can
    report healthy while permanently importing nothing. Partial mitigations
    now in the PR: elapsed-time rate-limited error logs with
    url/initialized/consecutive_failures/builds_abandoned context, a
    `builds_abandoned` counter in those logs, and an orchestrator-side
    `payload_building_jobs_cancelled` metric counter. Still missing:
    `remote_source_reachable` / `remote_source_last_imported_block` gauges and
    an abandoned-builds metric (not just log field).
  - First/most-recent pass: Claude pass 1 (2026-08-31T20:58Z); Claude pass 5
    (2026-08-31T23:20Z).
  - Why unaddressed: this file has no metrics infrastructure; adding a
    `remote_source_reachable` gauge / `remote_source_sync_errors` counter means
    choosing where the metrics registry plumbs through the add-on — a small
    design decision beyond this review loop.
  - Suggested Linear title: "rollup-node: metrics + status surface for the remote block source add-on"

- **End-to-end test for the remote source's pending-build retry path**
  (`crates/node/src/add_ons/remote_block_source.rs`)
  - Impact/evidence: Claude pass 3 finding 5 — the retry branch (build owed
    after a cancellation/timeout) has no direct end-to-end test. Its mechanics
    are covered indirectly: `test_chain_import_cancels_inflight_payload_job`
    pins the cancellation + `PayloadBuildingJobCancelled` event the retry
    reacts to, the landed-detection and retry cap are simple synchronous
    logic, and the config mismatch that made the stall reachable is now
    rejected by `validate()`.
  - First/most-recent pass: Claude pass 3 (2026-08-31T22:20Z, finding 5ii);
    Claude pass 5 (2026-08-31T23:20Z, coverage gap 1 — ranked highest: "a test
    here would have caught C1", the settlement regression pass 5 found).
    Codex pass 4 and Claude pass 5 both reshaped this logic, raising the value
    of the test further.
  - Why unaddressed: driving a deterministic cancellation *while the remote
    source is mid-wait* needs L1-reorg injection into the remote-source
    fixture node at a precise moment — new fixture plumbing, and a real risk
    of adding a new flaky test to a stabilization PR. Indirect coverage now
    exists for the pieces: the four cancellation-event tests, the two
    coalescing tests, and the validate() rules.
  - Suggested Linear title: "rollup-node: end-to-end test for the remote source's owed-build retry path"

- **Unit coverage for the handle's closed-channel surface**
  (`crates/chain-orchestrator/src/handle/mod.rs` `is_closed`/`try_build_block`; `classify_recv_error` lives in `crates/node/src/add_ons/remote_block_source.rs`)
  - Impact/evidence: Claude pass 9 S5 — `is_closed`/`try_build_block` and the
    gone-vs-transient classification have no direct unit tests, and a
    misclassification changes node-lifecycle behavior.
  - First/most-recent pass: Claude pass 9 (2026-09-01T00:22Z).
  - Why unaddressed: `ChainOrchestratorHandle` is generic over `FullNetwork`;
    constructing a concrete network type in a unit test needs test plumbing
    that does not exist in the handle crate today.
  - Suggested Linear title: "chain-orchestrator: unit-test the handle's closed-channel classification surface"

- **Runtime enforcement of the single-build-requester assumption**
  (`crates/node/src/add_ons/rpc.rs`, `crates/chain-orchestrator/src/lib.rs` enable arm)
  - Impact/evidence: Claude pass 11 M3 — `rollupNodeAdmin_enableAutomaticSequencing`
    can start the build timer on a remote-source node at runtime, breaking the
    attribution assumption validate() enforces at config time. The shipped
    docker launch script legitimately enables the admin RPC, so rejecting the
    flag combination in validate() would break it; the correct fix is for the
    enable command to be refused when the remote block source owns building,
    which needs that bit plumbed into the orchestrator.
  - First/most-recent pass: Claude pass 5 (doc caveat added), pass 11 M3
    (2026-09-01T01:40Z).
  - Why unaddressed: requires threading remote-source ownership into
    ChainOrchestrator construction; the mitigation is the documented caveat
    plus config validation, and skip attribution is now identity-gated so a
    runtime violation degrades to reorged-out side blocks rather than silent
    misattribution.
  - Suggested Linear title: "chain-orchestrator: refuse enableAutomaticSequencing while the remote block source owns building"

- **Unit tests for the ancestor walk's terminal paths**
  (`crates/node/src/add_ons/remote_block_source.rs`, `init_last_imported_block`)
  - Impact/evidence: Claude pass 11 — the genesis-mismatch and
    lookback-exhausted paths fail-stop the node and have no direct tests
    (absence-vs-divergence classification landed in the same pass).
  - First/most-recent pass: Claude pass 11 (2026-09-01T01:40Z); Claude
    pass 37 (2026-09-01, T2) escalated it: the destructive rewind path
    (`update_fcs_head(resume)` guarded solely by `diverged`, set in one walk
    arm) is untested EVERYWHERE — setting `diverged` on the absent-block arm,
    or inverting the `local_head > resume + 1` comparison, silently rewinds a
    follower's canonical head to chase a lagging replica with no error and no
    event.
  - Why unaddressed: full-path coverage needs a mocked BlockReader + remote
    RPC pair; no such harness exists in the add-on today. Pass 37 sketched a
    cheap alternative that avoids the harness: mirror the
    `settlement_decision` pattern — extract a per-height verdict pure fn over
    (local hash?, remote hash?) -> Match | Diverged | Absent, and a second
    over (local_head, local_safe, resume, diverged) ->
    Rewind | WaitForRemote | RefuseBelowSafe | Proceed, then table-test both
    beside `settlement_decision_table`. Deferred as a refactor of freshly
    review-hardened code at the tail of the loop.
  - Suggested Linear title: "rollup-node: unit-test the remote source's common-ancestor terminal paths and rewind verdict"

- **Widen `UpdateFcsHead`/`RevertToL1Block` replies to carry persistence failures**
  (`crates/chain-orchestrator/src/lib.rs`, command enum + admin RPC)
  - Impact/evidence: Claude pass 13 M3, pass 17 item 1 — when the DB write
    after a successful engine-head move fails, the admin caller sees only an
    opaque dropped-channel error. The PR now compensates in-process: the
    engine head is rolled back to the pre-command value on a persistence
    failure, and a rollback that does not commit (transport error or an
    INVALID forkchoice status) fail-stops the orchestrator so a restart
    re-converges on the persisted head — divergence can no longer persist
    in-process or across restarts. What remains is the reply type: the
    caller still cannot distinguish "rolled back, retryable" from
    "fail-stopped" (both surface as a dropped reply channel).
  - First/most-recent pass: Claude pass 13 (2026-09-01T04:00Z); Codex pass 18
    staleness note (2026-09-01T09:30Z); Claude pass 39 C1-secondary
    (2026-09-02) — `RevertToL1Block` collapses four distinct refusals (DB
    read failure, below-finalized target, head-lookup failure, SYNCING FCU)
    into one indistinguishable `false`; return `Result<(), String>` as
    `UpdateFcsHead` now does so the operator learns which refusal they hit.
    (The C1 PRIMARY — delta-derived FCU targets making the documented retry
    a silent no-op — was FIXED in pass 39: targets are now derived from
    absolute persisted state vs the engine mirror, pinned by
    `admin_unwind_refusal_is_sticky_until_state_converges`.)
  - Why unaddressed: changing the reply types is an API change across the
    command enum, handle, and admin RPC — beyond a review-loop fix. The
    explicit error log is the in-scope mitigation.
  - Note: a pre-latch numeric validation for RevertToL1Block (pass 25 M3)
    was tried and REMOVED in pass 28 — an above-range target is a harmless
    no-op unwind, and the latest-L1 marker is not advanced by batch commits,
    so the check refused legitimate reverts (broke e2e
    can_revert_to_l1_block). The remaining M3 surface is the reply type.
  - Suggested Linear title: "chain-orchestrator: propagate head-persistence failures to UpdateFcsHead/RevertToL1Block callers"

- **Integration tests for the consensus-path cancellation sites**
  (`crates/chain-orchestrator/src/lib.rs` — batch reconciliation, L1 reorg
  head move, optimistic sync; `crates/node/tests/sync.rs`)
  - Impact/evidence: Claude pass 13 g2 + pass 15 item 6 — of the eight
    `cancel_payload_building_job` call sites, four have dedicated tests
    (chain import, UpdateFcsHead, disable sequencing, RevertToL1Block) and
    they are the administrative ones. Untested: batch reconciliation (the
    routine derivation path — a false positive silently degrades block
    production on every batch), the L1-reorg head move, the L1-reorg
    carries-L1-messages guard, and optimistic sync including the pass-25
    INVALID-is-a-no-op guard, which nothing would catch if inverted or
    dropped. These are the branches where a stale job
    finalizing reorgs a derived/synced chain back out.
  - First/most-recent pass: Claude pass 13 (2026-09-01T04:00Z); Claude
    pass 15 (2026-09-01T07:30Z).
  - Why unaddressed: each needs a deterministic in-flight build held across
    an event none of the existing fixtures drive concurrently (a batch
    commit, an L1 reorg rewinding the L2 head, a peer block beyond the
    optimistic-sync threshold); building that scaffolding risks adding new
    flaky tests to a stabilization PR. Pass 23 T2 sketch for the L1-reorg
    pair: the raw two-node reorg fixture in sync.rs could issue a
    long-duration build_block immediately before the Reorg notification and
    assert the cancellation — but that fixture's payload duration is shared
    by its ~70 sequential builds, so the duration/interleaving needs care. The sites share
    `cancel_payload_building_job` with the four tested ones.
  - Suggested Linear title: "rollup-node: integration tests for payload-job cancellation on the consensus paths (batch reconciliation, L1 reorg, optimistic sync)"

- **Ancestor-walk DB reads block the poll tick**
  (`crates/node/src/add_ons/remote_block_source.rs`, `init_last_imported_block`)
  - Impact/evidence: Claude pass 13 m6 — the synchronous `block_hash` reads
    moved from launch onto the poll tick when resume-point derivation became
    lazy; normally 1–2 iterations, but a deep rewind can hold the add-on's
    task for up to `MAX_ANCESTOR_LOOKBACK` reads.
  - First/most-recent pass: Claude pass 13 (2026-09-01T04:00Z).
  - Why unaddressed: the add-on runs on its own spawned task, not the
    orchestrator loop, so the stall is self-contained; moving the walk onto
    `spawn_blocking` touches the provider's Send/lifetime bounds for a rare
    path. Low severity per the review.
  - Suggested Linear title: "rollup-node: move the remote source's ancestor walk off the async poll tick"

- **Purely numeric catch-up check cannot see a frozen or equal-height-reorged remote**
  (`crates/node/src/add_ons/remote_block_source.rs`, `follow_and_build` head comparison)
  - Impact/evidence: Claude pass 15 (pre-existing flag) — the per-tick check
    is `remote_head <= last_imported` with hashes compared only during
    resume-point derivation. A frozen remote RPC or a remote reorg to the
    same height leaves the node importing nothing, logging at trace, and
    resetting `consecutive_failures = 0` every tick — indistinguishable from
    healthy. Same shape on `main`; the metrics/status follow-up above is the
    natural place for the reachable/last-imported gauges that would expose it.
  - First/most-recent pass: Claude pass 15 (2026-09-01T07:30Z).
  - Why unaddressed: pre-existing behavior outside this PR's diff; a per-tick
    hash comparison changes steady-state RPC traffic and belongs with the
    metrics/status design decision (see the remote-source metrics entry
    above).
  - Suggested Linear title: "rollup-node: detect a frozen or equal-height-reorged remote in the block source's catch-up check"

- **Blind sleeps in the optimistic-sync consolidation test run under the soak lane**
  (`crates/node/tests/sync.rs` `test_should_consolidate_after_optimistic_sync`, ~:282 and ~:308)
  - Impact/evidence: Claude pass 15 (pre-existing flag) — two 1-second
    sleeps ("let the unsynced node process the optimistic sync" / "…the L1
    messages", the latter after 200 L1 messages) are capacity assumptions,
    and the nightly soak lane now runs this test under four CPU spinners and
    auto-comments on issue #38 — a capacity failure would be reported as a
    race regression.
  - First/most-recent pass: Claude pass 15 (2026-09-01T07:30Z).
  - Why unaddressed: pre-existing test structure outside the PR's own edits;
    replacing the sleeps needs an observable "L1 messages drained" signal the
    fixture does not currently expose, and this test was already soaked
    60/60 as-is. Optional nicety from pass 23: disabling automatic
    sequencing after the final build phase would stop the 20ms timer for the
    test's tail (residual-risk shrink only — the tail assertions self-heal
    via the continuous stream, so this was deliberately not changed late in
    the cycle).
  - Suggested Linear title: "rollup-node: replace blind sleeps in the optimistic-sync consolidation test with observable preconditions"

- **`PayloadBuildingJobStarted` event to make the coalescing tests' precondition observable**
  (`crates/chain-orchestrator/src/lib.rs` start sites, `crates/node/tests/sync.rs` coalescing tests)
  - Impact/evidence: Claude pass 17 item 6 — both coalescing tests encode "a
    job is already in flight" as a wall-clock precondition. Mitigation
    history: pass 17 dropped them from the loaded soak lane; pass 25
    replaced the timer test's blind sleep with bounded build-request retries
    and RE-ADDED both tests to the loaded lane (the precondition is no
    longer a capacity assumption). A started-event notified at the two
    `start_payload_building` success sites remains the cleaner root-cause
    fix that would remove the retry loop entirely.
  - First/most-recent pass: Claude pass 17 (2026-09-01T08:40Z).
  - Why unaddressed: new production event surface in a stabilization PR; the
    reviewer left it as the orchestrator's call and the lane filter closes
    the false-report path.
  - Suggested Linear title: "chain-orchestrator: emit PayloadBuildingJobStarted so coalescing tests can await an in-flight job"

- **Non-panicking config validation at launch**
  (`crates/node/src/node.rs` `ScrollRollupNode::new`, `crates/node/src/args.rs`)
  - Impact/evidence: Claude pass 19 M4 — the three new hard validate() rules
    are a breaking config change (two further checks warn without erroring
    after pass 25 downgraded them), and `.expect("Configuration validation failed")`
    turns a stale deployment manifest into a launch panic/crash-loop under a
    supervisor. The rules themselves are correct and the PR description now
    calls out the breaking change; the panic-vs-clean-exit shape predates
    this PR.
  - First/most-recent pass: Claude pass 19 (2026-09-01T10:30Z).
  - Why unaddressed: returning `eyre::Result` from `ScrollRollupNode::new`
    changes the constructor's public signature and its callers — beyond a
    review-loop fix.
  - Suggested Linear title: "rollup-node: exit cleanly (not panic) on config validation failure"

- **`reason` discriminant on `PayloadBuildingJobCancelled`**
  (`crates/chain-orchestrator/src/event.rs`, `cancel_payload_building_job`)
  - Impact/evidence: Claude pass 19 L2 + coverage-gap note — the event is a
    unit variant emitted from 14 documented logical paths (6 direct notify
    sites plus the shared cancel helper's 8 callers), so tests can only assert "some
    cancellation happened" (the chain-import test is now time-bounded as a
    partial mitigation), and consumers cannot tell a head-move cancel from a
    start failure. `cancel_payload_building_job` already carries a
    `&'static str` reason; lifting it into the event
    (`PayloadBuildingJobCancelled { reason }`) would make every cancellation
    site assertable from existing fixtures and close most of the
    orchestrator-side coverage gaps.
  - First/most-recent pass: Claude pass 19 (2026-09-01T10:30Z).
  - Why unaddressed: changes the public event enum shape late in the review
    cycle; every downstream match arm and the settlement's event handling
    would need auditing in the same change.
  - Suggested Linear title: "chain-orchestrator: carry the cancellation reason on PayloadBuildingJobCancelled"

- **Table-test the head-update fatal paths as a pure function**
  (`crates/chain-orchestrator/src/lib.rs` UpdateFcsHead arm and rollback)
  - Impact/evidence: Claude pass 21 coverage note, expanded by pass 23 T1 —
    FatalStateDivergence has only definition/raise sites in the tree: the
    forward non-VALID refusal, the rollback three-way outcome (VALID commits
    / SYNCING and INVALID do not / transport error), the post-unwind
    reorg-FCU arms, the remaining post-finalization divergence sites, the
    pass-28 combined head+safe unwind FCU (assert safe == head in the
    recorded argument and exactly ONE fork_choice_updated call), and the
    four `handle_outcome(...)?` propagation points have no tests — dropping
    a single `?` leaves the suite green. Pass 29 recipe: script a SYNCING
    FCU via ScriptedEngineClient, drive update_fcs_head, assert the reply is
    Err, the task alive, and fork_choice_updated_calls() == 1. Extracting
    settle_owed_build's head fetch into a parameter would likewise make its
    130-line decision-to-mutation mapping a fixture-free table test. The PR's own settlement_decision pattern applies directly:
    extract the outcome classification into a pure function and table-test
    it.
  - First/most-recent pass: Claude pass 21 (2026-09-01T11:50Z).
  - Why unaddressed: the refactor touches the command handler late in the
    review cycle; the logic just landed across passes 19-21 and the shape
    should settle before extraction.
  - Suggested Linear title: "chain-orchestrator: extract and table-test the head-update/rollback outcome classification"

- **Harden the reboot fixture's teardown before soaking the resume test**
  (`crates/node/src/test_utils/reboot.rs`, `shutdown_node`/`start_node`;
  `.github/workflows/nightly-soak.yml`)
  - Impact/evidence: observed during the pass-21 verification battery — the
    restarted node failed with "failed to open the database: IO error: lock
    hold by current process ... rocksdb/LOCK" plus a reth persistence-service
    error, under suite contention. `shutdown_node` ends in a blind 1-second
    cleanup sleep (the exact anti-pattern issue #38 removes elsewhere), so
    DB-handle release can lose the race with `start_node`. The resume test
    was therefore deliberately left OUT of the nightly soak filters: a
    teardown flake there would auto-file false race-regression reports on
    the tracking issue.
  - First/most-recent pass: orchestrator observation during Claude pass 21
    fixes (2026-09-01T12:20Z); Claude pass 37 (2026-09-01, T1) added the
    coverage consequence and a second root cause. Consequence: with the test
    skipped in test.yaml AND absent from every soak filter, the reworked
    resume/rewind logic (`init_last_imported_block` past genesis, the
    `diverged` classification, the `update_fcs_head` rewind) runs in NO CI
    lane — the docker pair restarts only the sequencer and the in-process
    tests initialize at local_head == 0. Second root cause (the hang the skip
    papered over): `start_node` calls `get_event_listener()` only AFTER
    relaunching the node (fixture.rs) while the add-on polls at 100ms, so the
    `l1_synced` wait both misses early events (false-pass window: builds 2-4
    of a broken walk can land pre-subscription) and can hang.
  - Why unaddressed: the fix is fixture-level (subscribe before the add-on
    starts — or a builder knob holding the first poll until Synced — plus
    replacing the teardown sleep with an observable release: poll the rocksdb
    lock's acquirability or await the persistence task handle) and risks
    touching every reboot-based test in a stabilization PR. The pass-37
    interim option WAS APPLIED in pass 39 (M6): nightly-soak now has a
    `soak-resume-quarantine` job running only this test, with comment-only
    reporting (never reopens the tracking issue) — the resume/rewind path
    has nightly signal again. The fixture-level fixes (subscribe before the
    add-on starts; observable teardown) remain open, and the test remains
    out of the merge gate until they land.
  - Suggested Linear title: "rollup-node: make the reboot fixture's shutdown observable, then soak the resume test"

- **Remote-source liveness set: typed import errors, walk resumption, settlement budget, event-lag recovery, sync-aware walk messages**
  (`crates/node/src/add_ons/remote_block_source.rs`,
  `crates/chain-orchestrator/src/lib.rs` ImportBlock arm)
  - Impact/evidence: Claude pass 25 M6/M7/M8/M12 — four related liveness and
    observability gaps the reviewer scoped as follow-ups: (M6) `ImportBlock`
    stringifies every orchestrator error, so repeated `InvalidBlock` for the
    same height cannot be distinguished from transient faults and escalated
    (the pass-25 rejection bound re-derives but cannot terminate a truly
    divergent remote, and any FatalStateDivergence under import_chain is
    downgraded to a String); the ancestor walk restarts from the top on every
    tick, making terminal escalation effectively unreachable on a mature
    chain (~8193 iterations); (M7) the settlement budget can head-of-line
    block imports up to 5x the 60s wait cap when a build parks behind the
    derivation gate — a Wait should not consume a retry when status() reports
    not-synced; (M8) broadcast lag (5000-event channel) can silently drop the
    outcome events the settlement waits on — a monotonic build-generation
    counter in status() would make a lost `BlockBuildingSkipped` settleable;
    (M12) during pipeline sync the walk misattributes "block unavailable" to
    the remote — clamp the walk start with the provider's best block and say
    "still syncing". Also: deep divergence is terminal above the lookback
    window but resumes from genesis below it (two outcomes for one fault
    class), and "ticks since last import" would expose an alternating Ok/Err
    livelock that `consecutive_failures` hides.
  - First/most-recent pass: Claude pass 25 (2026-09-01T13:40Z).
  - Why unaddressed: all need either a typed error channel through
    `import_block`, new status surface, or walk-state persistence — design
    changes the reviewer explicitly recommended scoping as follow-ups rather
    than landing late in a stabilization PR. The in-PR mitigations (bounded
    rejection re-derive, local-head loop guard, strict outcome identity)
    close the consensus-facing rewind/livelock holes.
  - Suggested Linear title: "rollup-node: remote block source liveness — typed import errors, resumable walk, sync-aware settlement"

- **Not-synced import tick advances the resume pointer and drops that height's build**
  (re-raised: Codex pass 40 P3, 2026-09-02 — the book's per-import build
  promise was qualified in-loop to "while the node is synced"; actually
  retaining the not-synced build debt remains this entry's open behavior
  change.)
  (`crates/node/src/add_ons/remote_block_source.rs`, follow loop not-synced branch)
  - Impact/evidence: Claude pass 25 (production observation attached to M11)
    — when a tick imports a block while the node is not L1-synced, the
    branch `continue`s after the pointer already advanced, so that height's
    build is skipped permanently with no owed-build bookkeeping — the one
    spot in the rewritten loop outside the settlement machinery. Not a
    consensus fault (the block itself was imported).
  - First/most-recent pass: Claude pass 25 (2026-09-01T13:40Z).
  - Why unaddressed: folding the branch into the owed-build machinery needs
    a decision on whether a not-synced tick should defer the build (set
    pending) or skip it; the observable-precondition fix in the resume test
    removes the flake this caused in CI.
  - Suggested Linear title: "rollup-node: owed-build bookkeeping for imports that land while L1-unsynced"

- **[RESOLVED IN THIS PR — pass 51 verified: carried onto the fixture at `test_utils/fixture.rs` and `reboot.rs`]** Test-fixture ergonomics follow-ups**
  (`crates/node/src/test_utils/{event_utils,fixture}.rs`, `reboot.rs`)
  - Impact/evidence: Claude pass 25 minors — `EventWaiter::label` is
    `&'static str`, so `block_sequenced(target)` cannot carry the target
    number into its timeout message (Cow would fix it); the
    `remote_source_url` builder override is not carried onto the fixture, so
    `start_node()` silently reconnects a restarted node to the real
    sequencer (a future restart-under-gated-remote test would pass for the
    wrong reason); `where_n_events` applies its timeout per node (documented
    in-PR, not unified); the drain loop needs a live clock (comment added
    in-PR for future `start_paused` tests).
  - First/most-recent pass: Claude pass 25 (2026-09-01T13:40Z).
  - Why unaddressed: pure test-infrastructure ergonomics with no current
    false-pass; each touches shared fixture surface used by every suite.
  - Suggested Linear title: "rollup-node: test-fixture ergonomics — Cow labels, restart URL carry-over, unified waiter budgets"

- **[RESOLVED IN THIS PR — pass 51 verified: the arm now sends `Err(String)` before returning (`lib.rs`'s administrative head-update rejection)]** FcuRejected refusal is a bare RecvError to the admin caller**
  (`crates/chain-orchestrator/src/lib.rs` UpdateFcsHead arm, `handle/mod.rs`)
  - Impact/evidence: Claude pass 25 minor — the new refusal drops the reply
    sender, so "engine refused the head" and "orchestrator is gone" are the
    same RecvError to the caller — the ambiguity `is_closed()` was added to
    resolve for BuildBlock. Same reply-widening as the persistence entry
    above; the two should land together.
  - First/most-recent pass: Claude pass 25 (2026-09-01T13:40Z).
  - Why unaddressed: covered by the existing reply-widening entry; recorded
    here so the refusal path is not forgotten when that lands.
  - Suggested Linear title: "chain-orchestrator: reply Result to UpdateFcsHead so refusals are distinguishable"

- **Metric skew: parked BuildBlock jobs count park time as build latency**
  (`crates/chain-orchestrator/src/lib.rs` BuildBlock arm, metrics)
  - Impact/evidence: Claude pass 25 minor — `start_block_building_recording`
    runs when the command is handled, but a job started under a closed gate
    parks until the gate reopens, so park time lands in the build-duration
    histograms that alarm on build latency (the timer path could never
    park). Carrying the start Instant on PayloadBuildingJob fixes it.
  - First/most-recent pass: Claude pass 25 (2026-09-01T13:40Z).
  - Why unaddressed: touches the metric recording lifecycle; low urgency
    while the histograms are used qualitatively.
  - Suggested Linear title: "chain-orchestrator: measure build duration from job start, not command receipt"

- **FatalStateDivergence early return pre-empts held-batch fatal accounting**
  (`crates/chain-orchestrator/src/lib.rs` run loop command arm)
  - Impact/evidence: Claude pass 25 minor — the fatal-divergence check runs
    before the `held_unwind_context` branch, so a future fatal variant
    raised from the RevertToL1Block arm would skip
    `log_fatal_held_operation`/`record_fatal()`. Unreachable today.
  - First/most-recent pass: Claude pass 25 (2026-09-01T13:40Z).
  - Why unaddressed: dead path today; reordering the two checks is trivial
    but touches the fail-stop routing that just settled.
  - Suggested Linear title: "chain-orchestrator: order fatal-divergence vs held-batch accounting in the run loop"

- **Asynchronous signing failure must emit a SignerEvent, not just a log line**
  (`crates/signer/src/lib.rs` signer task, `crates/chain-orchestrator/src/lib.rs` signer arm)
  - Impact/evidence: Claude pass 31 M1 — the orchestrator's fatal contract
    covers only the ENQUEUE of a signing request (which essentially never
    fails); the actual signing failure (remote KMS) is handled in the signer
    task as a bare error log with no event. By then the head is committed and
    the L1 messages are marked consumed, so the node keeps sequencing on an
    unsigned, never-announced block — exactly the outcome the enqueue path
    declares fatal, reached by the likelier route.
  - First/most-recent pass: Claude pass 31 (2026-09-01T17:00Z).
  - Why unaddressed: needs a failure variant on SignerEvent and handling in
    the orchestrator — an API change in the signer crate, beyond the review
    loop's local-fix bar per the reviewer's own recommendation.
  - Suggested Linear title: "signer: surface signing failures as SignerEvents so the orchestrator can apply its fatal contract"

- **Behavioural tests for the finalized-floor refusal and the Reissue arm**
  (`crates/chain-orchestrator/src/lib.rs`, `crates/node/src/add_ons/remote_block_source.rs`)
  - Impact/evidence: Claude pass 31 T5 — the two cheapest untested fail-stop
    behaviors: the RevertToL1Block finalized-floor refusal (seed a finalized
    L1 block, assert the reply is false and no UnwoundToL1Block event), and
    settle_owed_build's Reissue arm ("exactly one BuildBlock per observed
    cancellation" — the property whose violation double-builds a height).
    Folded into the existing fail-stop/table-test entries above; recorded
    separately because the reviewer called these two out as cheap enough to
    land with existing fixtures.
  - First/most-recent pass: Claude pass 31 (2026-09-01T17:00Z).
  - Why unaddressed: the finalized-floor test needs a fixture path that
    seeds L1 finalization metadata (none of the sync.rs fixtures do); the
    Reissue test needs the extracted-head-fetch refactor already recorded in
    the table-test entry.
  - Suggested Linear title: "rollup-node: behavioural tests for the finalized-floor refusal and owed-build re-issue"

- **Docker lane hangs die without a nextest summary (per-test bound missing)**
  (`.github/workflows/test.yaml`, integration-docker-compose)
  - Impact/evidence: Claude pass flag — no `.config/nextest.toml`, so no
    slow-timeout/terminate-after. The lane now has a 75-minute STEP timeout
    (added in this PR), so a hang no longer rides to the job-level SIGKILL —
    but a single hanging docker test still eats the whole step budget and
    the run ends without a per-test summary.
  - First/most-recent pass: Claude (Opus, high thinking), 2026-08-31T20:58Z.
  - Why unaddressed: needs a decision between a CLI `--slow-timeout` (version
    -sensitive syntax) and introducing `.config/nextest.toml` (affects local
    runs too); period must comfortably exceed the ~4-minute recovery test.
    Pass 29 sketch: a four-line `[profile.default] slow-timeout = { period =
    "60s", terminate-after = 8 }` would also convert every remaining
    unbounded fixture await (builder().build(), start_node(),
    node.connect()) into a named test failure with a stack.
  - Suggested Linear title: "ci: bound docker-lane test hangs with a nextest slow-timeout"

- **Confirm `sync` is not a required status check** (repo settings)
  - Impact/evidence: Claude pass flag — `sync.yaml` no longer runs on push; if
    repo settings list `sync` as a required check, merges would block forever.
    `GET /branches/main/protection` returns 404 with this token (likely no
    protection rules, but unverifiable without admin).
  - First/most-recent pass: Claude (Opus, high thinking), 2026-08-31T20:58Z.
  - Why unaddressed: requires repo-admin visibility.
  - Suggested Linear title: "repo settings: verify no required status check references the sync workflow"

- **Post-batch-revert safe-marker FCU has the finalized-marker's crash window, stale-high direction**
  (`crates/chain-orchestrator/src/lib.rs`, batch-revert handler's safe FCU)
  - Impact/evidence: orchestrator observation while fixing the Codex pass 38
    P1 (2026-09-02). The finalized marker got a replay path (marker
    recomputed over already-`Finalized` batches on every finalized
    notification; FCU reissued when the mirror is behind). The batch-revert
    safe marker shares the ordering — durable database revert first, safe
    FCU second — so a crash between them restarts with the EL's safe marker
    stale HIGH relative to the database. Consequences differ from the
    finalized case: a too-high safe floor makes below-safe rewind refusals
    over-aggressive rather than under-protective, and consolidation replay
    naturally re-advances safe, so the window is self-limiting but not
    self-healing on its own.
  - First/most-recent pass: orchestrator observation after Codex pass 38
    (2026-09-02).
  - Why unaddressed: not reviewer-flagged; the recovery dynamics (stale-high,
    partially self-correcting via consolidation replay) need their own
    analysis before choosing between a startup reconcile and a
    recompute-on-replay mirror of the finalized fix.
  - Suggested Linear title: "rollup-node: reconcile the EL safe marker against the database after a crash between batch revert and its FCU"

- **Behavioral tests for the remaining new decision points (pass 39 M5 leftovers)**
  (`crates/sequencer/src/lib.rs`, `crates/chain-orchestrator/src/lib.rs`)
  - Impact/evidence: Claude pass 39 M5 (2026-09-02). Fixed in-loop: the
    below-finalized fail-stops at both sites, the admin-unwind refusal
    stickiness, the DB marker recompute, and `ScriptedEngineClient` now
    records every forkchoice state it is passed. Still untested: (a)
    `SequencerError::FcuNotValid` — the check that prevents signing and
    gossiping a block the EL never adopted; the sequencer crate has zero
    tests and no harness; (b) `import_chain`'s SYNCING -> re-enter-L2-sync
    transition on the hot gossip path (needs a peer-import harness with a
    scripted block client); (c) the `UpdateFcsHead` refusal + compensating
    rollback pair (needs DB failure injection).
  - First/most-recent pass: Claude pass 39 (2026-09-02).
  - Why unaddressed: each needs a new harness (sequencer test scaffold,
    peer-import scaffold, fault-injecting DB) — beyond a review-loop fix.
  - Suggested Linear title: "rollup-node: harness + tests for FcuNotValid, import-SYNCING re-entry, and the UpdateFcsHead rollback"

- **Pin the nextest install action by revision**
  (`.github/workflows/test.yaml`, `nightly-soak.yml` — `taiki-e/install-action@nextest`)
  - Impact/evidence: Claude pass 39 minor 13 (2026-09-02) — the action is
    referenced by a floating tag; a compromised or breaking release lands in
    every lane at once. The same pass's main concern (two exclusion
    mechanisms in the merge gate) was fixed in-loop by folding the libtest
    `--skip`s into the nextest filterset.
  - First/most-recent pass: Claude pass 39 (2026-09-02).
  - Why unaddressed: pinning needs a vetted commit SHA for every usage and a
    renovate/dependabot story so the pin does not rot; a repo-wide action
    audit is better done once, outside this PR.
  - Suggested Linear title: "ci: pin third-party actions by commit SHA across workflows"

- **[RESOLVED pass 61] Blocked-resume ancestor walk re-runs every tick with no backoff**
  (`crates/node/src/add_ons/remote_block_source.rs`, `follow_and_build` /
  `init_last_imported_block`)
  - RESOLUTION: pass 61 (Claude finding 7) added a geometric backoff
    (`FOLLOWER_BACKOFF_MAX_SHIFT`/`FOLLOWER_BACKOFF_CAP`) in `run_until_shutdown`:
    after a failed tick the next poll is delayed by
    `poll_interval_ms << min(consecutive_failures, 8)`, capped at 30s, reset on
    the next success. This caps the ancestor-walk storm without blunting the
    transient-fault retry. (Caching the walk result was the alternative; the
    backoff is simpler and does not add state to the walk itself.)
  - Impact/evidence: Claude pass 43 m3 (2026-09-02). When the resume point
    cannot be established (`resume < local_safe`, or the `!diverged`
    "remote trails" bail), the walk returns Err after completing and caches
    nothing, so `last_imported_block` stays `None` and the next tick redoes
    the full descent — up to `MAX_ANCESTOR_LOOKBACK` (8192) sequential
    `eth_getBlockByNumber` calls, continuously, at the 100ms default, against
    a condition that does not self-resolve until the remote itself moves.
  - First/most-recent pass: Claude pass 43 (2026-09-02).
  - Why unaddressed: a correct fix (cache `(resume, resume_hash, diverged)`
    and skip the re-walk while local head/safe are unchanged, or lengthen the
    effective interval while the pointer cannot be established) adds state to
    the hot tick loop and must not blunt the legitimate transient-fault retry
    at poll cadence; deferred rather than land a half-measure in an
    already-large review round on freshly-hardened code.
  - Suggested Linear title: "rollup-node: back off the remote-source ancestor walk while the resume point cannot be established"

- **Reviewer-personal follow-up ledger and the plan doc are tracked in the PR**
  (`.claude/vmark-pr-review-follow-ups.md`, `docs/superpowers/plans/2026-08-31-issue-38-ci-stabilization.md`)
  - Impact/evidence: Claude pass 43 m7 (2026-09-02). `.claude/` has no
    tracked files on `main`; this PR adds a 546-line reviewer ledger under a
    tool-config directory plus an 866-line plan doc. Production files
    (test.yaml, nightly-soak.yml, Makefile) previously pointed at the ledger
    as canonical — those pointers were made self-contained in pass 43, but
    whether either file should ship in the product repo is a merge decision
    for the author, not the review loop.
  - First/most-recent pass: Claude pass 43 (2026-09-02).
  - Why unaddressed: the ledger is the review loop's live working file (it
    tracks every deferred finding across all passes); removing it mid-loop
    would lose that tracking. Decide at handoff whether to `git rm` it (and
    the plan doc, or relocate under a neutral `docs/` name) before merge.
  - Suggested Linear title: "rollup-node: drop the reviewer ledger / relocate the plan doc before merging PR #45"

- **Remote-source imports are re-gossiped over scroll-wire with a forged
  all-zero signature** (`crates/node/src/add_ons/remote_block_source.rs`,
  `crates/network/src/manager.rs`)
  - Impact/evidence: Claude pass 45 P1 (2026-09-02). Pre-existing — identical
    on `main`. Every successful remote-source import produces a
    `block_import_outcome(valid_block(..))`, which reaches `announce_block`.
    The eth-wire branch is correctly gated on `verify_block_signature`, but
    the scroll-wire branch announces unconditionally, so a follower with
    scroll-wire peers under `--consensus.algorithm system-contract` gossips
    `NewBlock { signature: 0x00..00 }` on every import. Each peer's
    `recover_signer` fails, charges `BadBlock` reputation against this node,
    and eventually disconnects it — while this node's logs show only
    successful imports.
  - First/most-recent pass: Claude pass 45 (2026-09-02).
  - Why unaddressed: pre-existing, and the correct fix is a signature change —
    adding an `announce`/`source` flag to `ChainOrchestratorCommand::ImportBlock`
    so remote-source imports skip the announce entirely (those blocks reach
    peers from the real sequencer anyway). The local alternative (gate the
    scroll-wire branch on `should_announce_eth_wire`) changes announce
    behaviour for ALL blocks, not just remote-source ones, which is too broad
    to land unverified in this PR.
  - Suggested Linear title: "rollup-node: do not re-announce remote-source imports over scroll-wire with a forged signature"

- **`CanRetry` treats every `DbErr` as retryable with no retry bound**
  (`crates/database/db/src/../service/retry.rs`)
  - Impact/evidence: Claude pass 45, noted alongside C1. `CanRetry for
    DatabaseError` classifies every `DbErr` retryable and
    `Retry::new_with_default_config` sets `max_retries: None`, so any
    DETERMINISTIC SQL fault becomes an invisible livelock at 20 Hz behind a
    `debug!` rather than a crash. C1 was one route into it (fixed at the
    source); the retry policy itself is unchanged.
  - First/most-recent pass: Claude pass 45 (2026-09-02).
  - Why unaddressed: classifying which `DbErr` variants are genuinely
    transient, and choosing a retry bound and its failure behaviour, is a
    policy decision for the database layer well beyond this PR's diff.
  - Suggested Linear title: "rollup-node: bound database retries and stop classifying deterministic SQL faults as retryable"

- **[pass 61 F3] `--private-key` flag silently removed with no migration hint**
  (`crates/node/src/args.rs` `SignerArgs::private_key` `#[arg(skip)]`)
  - Impact/evidence: Claude pass 61 finding 3 (2026-09-02). The PR adds
    `#[arg(skip)]`, removing the previously-working `--private-key` flag. An
    operator still passing it dies at clap parse time ("unexpected argument")
    with no pointer to `--signer.key-file` / `--signer.aws-kms-key-id`. The
    security motive (raw key in `ps` / `/proc/<pid>/cmdline`) is sound.
  - Why unaddressed: product judgment. A hidden `--signer.private-key` rejected
    in `validate()` with a migration message gives the best UX (the key is on
    argv the moment it is typed regardless, so it is no worse security-wise) but
    contradicts the field's security comment and touches signer wiring; doc-only
    is the safe minimum. Also update the PR breaking-change callout (lists 3 of
    the 5 hard rules).
  - Suggested Linear title: "rollup-node: migration hint for the removed --private-key flag"

- **[pass 61 F8] Remote-source rewind depth is floored only by the local safe head**
  (`crates/node/src/add_ons/remote_block_source.rs` `decide_follow_action`)
  - Impact/evidence: Claude pass 61 finding 8 (2026-09-02). `Rewind` is returned
    whenever `local_head` exceeds `resume + 1` with divergence, floored only by
    `resume` at-or-above `local_safe`. On a fresh follower whose safe is still 0,
    a remote forking near genesis drives an administrative rewind of nearly the
    whole local chain (bounded at `MAX_ANCESTOR_LOOKBACK` = 8192: deep, not
    unbounded).
  - Why unaddressed: the fix is a new operator flag `--remote-source.max-rewind-depth`
    with a default — a config/product decision, not a local fix.
  - Suggested Linear title: "rollup-node: bound remote-source rewind depth with a configurable max"

- **[pass 61 F9] `delete_mismatched_genesis_blocks` public write op can leave the DB genesis-less**
  (`crates/database/db/src/operations.rs`, `db.rs` wrapper, `metrics.rs`)
  - Impact/evidence: Claude pass 61 finding 9 (2026-09-02). The standalone
    `Database` wrapper does the DELETE with no compensating insert; against a DB
    whose only height-0 row is the migration seed it commits a genesis-less
    `l2_block` and the next `get_latest_safe_l2_info()` panics. Only tests call
    it; production uses the safe `reconcile_genesis_block`.
  - Why unaddressed: the tx-level method is on the shared `DatabaseOperations`
    trait and used INTERNALLY by `reconcile_genesis_block`, so gating only the
    test-facing wrapper cleanly needs splitting it off the trait — structural
    churn for a footgun with no production caller.
  - Suggested Linear title: "scroll-db: gate delete_mismatched_genesis_blocks behind test/test-utils"

- **[pass 61] Coverage gaps flagged as merge-time decisions (not defects)**
  - Impact/evidence: Claude pass 61. (a) Consolidation fetch-retry
    (`ConsolidationFetchFailed` + `consolidate_chain_with_retry`) has no test;
    the scripted `l2_provider` makes it cheap to pin. (b) Remote-source rewind
    EXECUTION is untested (the decision tables are). (c) `ForkchoiceState::from_provider`
    refusal arms are untested and load-bearing (the caller hard-bails the node).
    (d) `handle_l1_finalized`'s marker-exceeds-head deferral arm is untested
    (deliberate, documented).
  - Why unaddressed: batched as a coverage decision, scoped out of an
    already-large round; a and c are worth adding.
  - Suggested Linear title: "rollup-node: close fetch-retry / rewind-execution / from_provider-refusal coverage gaps"

- **[pass 61] Minor comment/error-string accuracy nits (batched)**
  - Impact/evidence: Claude pass 61 minor batch, remaining after pass-61 fixes:
    `error.rs` GenesisMismatch summary vs raise-on-any-foreign-row;
    `finalize_consolidated_batches` trait doc omits the blockless-Consolidated
    handling; a purge-bound comment overstates the exclusive bound. All cosmetic
    (no behavior), reviewer-verified.
  - Why unaddressed: pure comment precision, batched to keep the pass bounded.
  - Suggested Linear title: "rollup-node: minor comment/error-string accuracy cleanup"

- **[pass 63 A2] L2-sync recheck re-collects the reverted-tx range on every gossip announcement**
  (`crates/chain-orchestrator/src/lib.rs` `recheck_l2_sync_target`, called from `handle_block_from_peer`)
  - Impact/evidence: Claude pass 63 A2 (2026-09-03). During a backward latch the
    recheck (run at the top of every announcement, before the already-known
    short-circuit) re-collects the reverted-tx range — up to
    MAX_REVERTED_TX_COLLECTION_BLOCKS (1024) serial full-block RPCs — on the
    single-task run loop, per announcement, even for already-known blocks.
  - Why unaddressed: the fix caches the collection on `L2SyncRecheck`, adding a
    second field + invalidation to a struct just churned by A1; performance, not
    correctness. Deferred to avoid stacking two struct changes in one pass.
  - Suggested Linear title: "chain-orchestrator: cache the recheck reverted-tx collection per latch"

- **[pass 63 A3/A5/A6] Test-precision cluster around the recheck rewind and finalized-marker replay**
  (`crates/chain-orchestrator/src/lib.rs` tests; `crates/node/tests/e2e.rs`)
  - Impact/evidence: Claude pass 63. A3: the BACKWARD-rewind recheck seam (fixed
    in b4273c4/ba75dd1) has no test — reverting that commit leaves the suite
    green; needs an integration scenario driving a ChainReorged import to a lower
    height that gets SYNCING then VALID. A5: `l1_finalization_replay_reissues_
    marker_fcu_once` is vacuous — the held batch also becomes eligible and its
    block dominates the marker, and both blocks share hash 0x22, so the hash
    assertion can't tell (the DB-layer `finalize_consolidated_batches_recomputes_
    over_finalized_rows` DOES pin the Finalized-inclusion non-vacuously). A6:
    three fail-stop tests assert only the `FatalStateDivergence` variant while an
    adjacent branch returns the same variant — assert the `&'static str` message
    to pin the intended branch.
  - Why unaddressed: A3 needs integration scaffolding; A5 is hard to make
    non-vacuous under the head>=safe constraint (the held batch's consolidation
    block must exceed the safe head) without risking the safe-reconciliation the
    test already guards; A6 needs each intended branch's exact message. Deferred
    as a focused test-hardening batch.
  - Suggested Linear title: "chain-orchestrator: pin the recheck backward-rewind, de-vacuum the replay test, and message-match the fail-stop tests"

- **[pass 63 A7/B8] Finalized-marker eligible set is unbounded and evaluated in the write lock**
  (`crates/database/db/src/operations.rs` `finalize_consolidated_batches`)
  - Impact/evidence: Claude pass 63 A7 (2026-09-03). `marker_filter` includes
    `Finalized`, so the eligible set is every batch ever finalized; it is
    materialized (`IN (subquery)`) and run on every finalized notification inside
    the caller's `tx_mut`, holding the single write mutex. B8: legacy blockless
    `Consolidated` batches are silently skipped, contradicting the neighbouring
    comment.
  - Why unaddressed: the fix is a correlated `Expr::exists` rewrite (the shape
    used ~10 lines below) — semantically identical, query-plan improvement;
    inferred not measured, and worth doing carefully with a scale check.
  - Suggested Linear title: "scroll-db: bound the finalized-marker query with a correlated EXISTS"

- **[pass 63 A8] Both cfg(test-utils)-gated startup refusals are unexercised**
  (`crates/node/src/args.rs` l1.url-required and --test/--blob.anvil_url rules)
  - Impact/evidence: Claude pass 63 A8 (2026-09-03). Every CI lane builds
    `--all-features`, so `test_validate_requires_l1_url` reduces to
    `assert!(is_ok())` with a dead message check, and the --test-without-anvil
    refusal has no test at all.
  - Why unaddressed: the fix extracts `const fn`s taking the flag as a parameter
    and table-tests both polarities (mirroring `startup_refusal`); a separate,
    self-contained test-coverage task.
  - Suggested Linear title: "rollup-node: table-test the cfg-gated startup refusals independent of --all-features"

- **[pass 63 minor batch] B5/B7/B10/B11 residual test/doc precision**
  - Impact/evidence: Claude pass 63. B5: `collect_reverted_txs_in_range` can
    never return `Err`, so four callers carry dead `Err` arms with misleading
    comments. B7: `GenesisMismatch` doc/`stored` labelling could be tightened.
    B10: a 2s wait races a 2s hold backoff in one test and can drain the scripted
    FCU queue into a panic rather than an assertion. B11: a `reconcile_genesis_block`
    test assertion holds unconditionally (it never writes l2_head_block).
  - Why unaddressed: cosmetic / test-precision, batched to keep the pass bounded.
  - Suggested Linear title: "rollup-node: residual test/doc precision (dead Err arms, race-y wait, unconditional assert)"

## Pass 35 findings — resolution (loop resumed 2026-09-01)

All five majors and eight minors from Claude pass 35 were FIXED in the
resume round (reorg-FCU clamping + fatal FcsError, best-effort tx
collection at all three sites, initialized_once moved before the guards,
flagship l1_synced wait removed, resume test out of every lane, cancel
before the import-path set_syncing, reporter timeout wording, catch-up-
aware stall budget, const docs, Landed doc, FcuRejected doc, remote-source
book section + PR-body rule list). Remaining from that pass:

- **P35-i1 (post-merge action)** nightly-soak.yml gets zero pre-merge
  validation (schedule/dispatch only) — manually dispatch it with
  iterations=1 immediately after merge, before the first scheduled run.
- **P35-i3** Pre-existing config gap: `--consensus.algorithm noop` with
  `--sequencer.enabled`, no signer, and no `--test` passes validate() and
  panics at the "signer must be present" expect once a build finalizes
  (timer path panics identically). The new rules do not close it.
  - Suggested Linear title: "rollup-node: validate that an enabled sequencer has a signer source under non-noop AND noop consensus"
- (P35-i2, the linear non-resumable ancestor walk, is already covered by
  the remote-source liveness entry above.)

## Pass 44 findings — resolution (Codex, 2026-09-02)

Codex pass 44 returned two P1s, both fallout from the pass-43 changes; both
are FIXED in this PR, so nothing from this pass is deferred.

- **P44-1 (FIXED)** A batch restored by an L1 reorg that removed its revert
  came back as `Consolidated` regardless of whether it had ever been derived.
  Pass 43 made `finalize_consolidated_batches` tolerate a batch with no L2
  block rows (instead of erroring and freezing finality), which turned that
  restore into a silent skip: the underived batch was marked `Finalized`,
  and only `Committed` rows are ever queued for derivation, so its blocks
  never entered the finalized chain. `delete_batch_revert_gt_block_number`
  now restores each batch to the status its own rows can prove —
  `Consolidated` with L2 blocks on record, `Committed` (the derivation
  queue) without. Pinned by
  `reverted_batches_restore_to_a_derivable_status`.
- **P44-2 (FIXED)** The pass-43 genesis reconciliation aborted startup on a
  populated custom-chain database written by `origin/main`. That startup
  inserted the real genesis without removing the migration-seeded row (the
  insert cannot overwrite — its conflict key is the block hash), so height 0
  holds TWO rows, and `get_l2_block_info_by_number(0)` returns either one.
  Upgrading such a node hit `GenesisMismatch` and panicked. The
  reconciliation is now a single `reconcile_genesis_block` operation that
  reads the whole height-0 set and decides on this chain's own genesis:
  present means the rows beside it are the legacy duplicate and are dropped;
  absent stays fatal. Pinned by
  `populated_database_reconciles_a_legacy_genesis_duplicate`.
- Also fixed in passing: the `GenesisMismatch` message carried a run of
  stray spaces before `{stored}`.

## Pass 45 findings — resolution (Claude, 2026-09-02)

Claude pass 45 returned 1 critical, 3 majors introduced by the PR, 2
pre-existing majors, 1 coverage gap and 10 minors. All are fixed except P1
(scroll-wire re-announce) and m10 (the ledger/plan-doc merge decision), both
recorded above.

- **C1 (FIXED)** `finalize_consolidated_batches` materialised one bind
  parameter per ever-finalized batch. The marker filter includes `Finalized`,
  so that set only grows; past SQLite's 32766-parameter ceiling the statement
  fails permanently, and the unbounded retry layer turns that into a silent
  finality freeze inside the caller's write transaction. The eligible set is
  now joined as a subquery.
- **M1 (FIXED)** The administrative unwind was the only head-rewind site that
  did not purge the L1-message-to-L2-block mappings. A message left stamped
  with a rewound block number is skipped by `get_n_messages(NotIncluded(..))`,
  so the next build takes the following queue index and the freed one lands a
  block later — an out-of-order queue every peer rejects. Also gated the head
  target on `<` so an anchor above the mirror can no longer make a "revert"
  issue a forward forkchoice move.
- **M2 (FIXED)** `validate()` accepted `remote-source.enabled` +
  `sequencer.auto-start` without `remote-source.build`, leaving the block
  timer running while the remote source imports the real sequencer's chain —
  a fork from a node configured as a read-only mirror. The rule no longer
  depends on `build`.
- **M3 (FIXED)** `test_should_consolidate_after_optimistic_sync` gated a
  consolidation that grows with runner slowness on the fixed 30s default
  waiter. Given an explicit 120s budget so it fails on behaviour, not
  capacity.
- **P2 (FIXED)** Pre-existing but one line, in code this PR reworks and beside
  the height check it added: a remote that ignores `fullTransactions` yields a
  body-stripped block that the EL rejects forever as "invalid block". Now
  checked with `BlockTransactions::is_full`.
- **G1 (FIXED)** Added `l1_reorg_drags_the_safe_target_down_to_a_lower_head`.
  Every other reorg test runs head == safe, where the whole safe-target match
  is a no-op; the new test runs head BELOW safe and fails with
  `FatalStateDivergence` when the pass-43 clamp arm is removed (verified).
- **NEW P1 found while fixing m9 (FIXED)** The dev migration seeds upstream
  Scroll's dev genesis (`0x14844a4f…`) while the DogeOS dev spec computes
  `0x31ad4874…`. Pass 43's unconditional reconciliation therefore rejected
  every EXISTING `--chain dev` database at startup as another chain's data.
  (Pass 45 had checked this and concluded "not a brick" on the premise that
  `dev.json` is byte-identical to upstream Scroll's; the new test disproves
  that premise.) `reconcile_genesis_block` now takes the migration's seeded
  genesis and treats a height-0 row matching it as a row this node wrote,
  replacing it in place. Pinned by
  `populated_database_reconciles_a_migration_seeded_genesis`.
- **Minors** m1 (comment corrected — the guard is defensive; the seeded genesis
  batch makes the marker a floor, never `None`), m2 (`GenesisMissing` instead
  of a silent `Ok(0)`), m3 (metered wrapper), m4 (each soak pattern asserted
  to match a test), m5 (soak docker lane logs at the merge gate's verbosity),
  m6 (CLAUDE.md exclusions), m7 (`BatchReverted.safe_head` documents both
  clamps), m8 (book documents the head rewind and the two fail-stops), m9
  (`genesis_seed_pairing_holds_for_shipped_chain_specs`) — all FIXED.

## Pass 46 findings — resolution (Codex, 2026-09-02)

Codex pass 46 returned three findings, all on the pass-44/45 changes
themselves. All FIXED; nothing deferred from this pass.

- **P46-1 (FIXED)** The pass-45 M2 rule over-rejected: it aborted startup on
  `remote-source.enabled` + `sequencer.auto-start` even with
  `sequencer.enabled` unset, where `build()` constructs no Sequencer at all, so
  `auto-start` starts no timer and there is no second producer. That broke the
  templated fleet layout the adjacent warn arm explicitly blesses — one flag
  set across roles, toggled per role. The rule is now gated on
  `sequencer_enabled`, which is exactly the condition the hazard needs. Pinned
  from both sides by `test_validate_remote_source_with_auto_start_fails` and
  the new `test_validate_remote_source_with_inert_auto_start_is_accepted`.
- **P46-2 (FIXED)** The pass-44 restore classified blockless batches with a
  linear `derived_hashes.contains()` per reverted hash — quadratic byte
  comparisons inside the reorg transaction, and a `BatchRevertRange` can span
  thousands of batches. Now a `HashSet` lookup.
- **P46-3 (FIXED)** The pass-45 book section (m8) overstated the add-on's
  fail-stop: a genesis mismatch and an exhausted lookback are terminal only
  before the FIRST successful initialization. After one success
  `init_last_imported_block` converts both to ordinary retryable errors and
  the node stays up, re-walking at poll cadence. The section now says so, and
  points operators at the repeated sync-error logs, since a follower stuck in
  that loop imports nothing while looking healthy.

## Pass 47 findings — resolution (Claude, 2026-09-02)

Claude pass 47 returned 1 critical, 5 majors and 12 minors. All FIXED;
nothing deferred from this pass beyond the standing ledger entries.

- **C1 (FIXED)** `--chain dogeos-chikyu` was bricked at startup. Pass 43
  switched the reconciliation's genesis source from `chain_spec.genesis_hash()`
  to `genesis_hash_from_chain_spec()`, and the two differ: the former returns
  the SEALED hash a spec carries, the latter RECOMPUTES the header hash.
  Chikyu's genesis document is byte-identical to mainnet's in every field the
  header is built from, so recomputing yields MAINNET's genesis hash for
  chikyu. An existing chikyu database then failed `GenesisMismatch` on every
  start; a fresh one recorded mainnet's hash and diverged at the first
  finalized notification. `fcs.rs`'s `Dev | None` arm now returns
  `chain_spec.genesis_hash()` — a no-op for dev, custom chains and mainnet,
  where sealed and recomputed agree.
  - The pass-45 guard test passed for the WRONG reason: its only chikyu
    assertion was `assert_ne!(recomputed, seed)`, true because the recomputed
    value was mainnet's hash. It now asserts
    `genesis_hash_from_chain_spec(spec) == Some(spec.genesis_hash())`, verified
    to fail on the pre-fix code.
- **M1 (FIXED)** A crash between an unwind's durable database commit and the
  FCU that lowers the engine's safe marker left the EL holding safe above the
  resumed head. `from_provider` clamped `safe >= finalized` but never
  `safe <= head`, and the startup repair propagated the resulting
  `HeadBelowSafe` — so the node failed to launch on every subsequent restart,
  with nothing to lower the marker. Added the clamp, and made the repair loop
  drag safe down rather than propagate.
- **M2 (FIXED)** `from_provider` swallows three RPC reads and the genesis
  fallback set head = safe = finalized = 0 with no log line, leaving every
  safe/finalized guard vacuous until the next finalized notification. The
  fallback now logs, and a genesis mirror on a populated database is refused
  outright.
- **M3 (FIXED)** `?e` rendered the full eyre chain, and alloy's transport
  appends `for url ({url})` with basic-auth credentials and query-string API
  keys intact — defeating the host/port redaction on the same line. Both log
  sites now scrub the message against the configured URL before logging it,
  and the scrub happens before the string is stored in the rate limiter.
- **M4 (FIXED)** Imported remote blocks were never checked for parent linkage,
  though the comment claimed the fork case was covered. A forked backend
  answers the right height with a wrong parent; the engine returns SYNCING for
  an unknown parent, which drops a healthy follower out of synced mode, and the
  next tick commits the mirror on SYNCING to a head the local EL never adopts —
  after which every ancestor probe returns immediately, forever. Now compared
  against the local block hash before import.
- **M5 (FIXED)** Commit 8555993's message claimed the timer-coalescing test's
  pinger interval had been raised above the payload duration; `sync.rs` was not
  in that commit and the tree still had a 200ms pinger against a 3000ms
  payload. Ping N started a manual job and ping N+1 coalesced with THAT, so the
  test could pass without a timer job ever being involved — while being one of
  five patterns soaked nightly as recurrence protection. Interval raised to
  3500ms, above the payload duration.
- **Minors (all FIXED)** `reconcile_genesis_block`'s foreign-row check hoisted
  above the fresh/populated split (that split reads the `l2_head_block`
  METADATA counter, which `unwind()` can drive to 0 with another chain's rows
  still present, so the fresh branch would have grafted this chain's genesis
  over foreign data); the stall classifier now requires a NUMERIC advance
  (`Some(_)` sorts above `None`, so the tick that merely establishes the
  pointer counted as healthy deep catch-up); the admin-unwind purge refuses
  instead of fail-stopping, matching the two database reads above it; the
  stale pre-pass-43 clamp comment deleted; the `L1Reorg` and `BatchReverted`
  event docs corrected; the `reconcile_genesis_block` doc now names
  `GenesisMissing`; the book's auto-start rule and terminal-error section
  corrected (the lookback bound is 8192 blocks and does NOT imply a
  misconfigured URL); the soak rename guard is now COUNT-granular, not
  pattern-granular (`coalesces_with` covers 2 tests and
  `cancels_inflight_payload_job` covers 4, so a non-empty match was not
  proof); "both in-process soak jobs" corrected to three.
- **Test gaps (FIXED)** `decide_follow_action_table` gained the
  `resume == local_safe` boundary rows (widening the guard to `<=` passed the
  whole table while turning the steady state into a permanent refusal), and
  `populated_database_without_a_genesis_row_is_reported_missing` covers the
  fourth `reconcile_genesis_block` outcome, including that the check runs
  before any write.

## Pass 48 findings — resolution (Codex, 2026-09-02)

Codex pass 48 returned one P1, on the pass-47 M4 fix itself. FIXED; nothing
deferred from this pass.

- **P48-1 (FIXED)** The new parent-linkage check returned an error without
  clearing `last_imported_block`. A changed parent is the ORDINARY shape of a
  remote reorg at or below the pointer, not only the misrouted-backend case the
  check was written for, so failing while keeping the pointer re-fetched the
  same block on every poll and wedged the follower on the old fork — the
  permanent no-import state the check exists to prevent, reached by a different
  route. The mismatch now clears the pointer and resets
  `consecutive_import_rejections` before returning, matching every other
  resync path in the add-on (local head advanced, local head rewound, import
  rejected `MAX_IMPORT_REJECTIONS` times), so the next tick re-walks to the new
  common ancestor.

## Pass 49 findings — resolution (Claude, 2026-09-02)

Claude pass 49 returned 1 major, 8 production-impacting minors and 5
documentation-staleness items. All FIXED; nothing deferred from this pass.

- **M1 (FIXED)** The `Rewind` arm re-read only NUMBERS after the ancestor walk;
  `resume_hash` still came from the walk, up to `MAX_ANCESTOR_LOOKBACK`
  sequential remote round-trips earlier. The CAS could not close that gap — its
  anchor is read after the walk, so a local reorg during the walk is already
  baked in and the CAS passes. In the shipped docker topology (remote source
  plus gossip peers) a gossip import replacing that height makes the node rewind
  onto an abandoned branch, cancel the in-flight job and purge L1-message
  mappings, on a consensus path. The hash is now re-checked against the local
  provider immediately before the FCU.
- **Minors 2 + 3 (FIXED)** The pass-47 safe clamp composed badly with the
  finalized floor: with `finalized > head`, clamp one raised safe to finalized
  and clamp two lowered it to head, yielding `safe < finalized` — a state whose
  every later `update()` returns `SafeBelowFinalized` while the repair loop
  (gated on `l2_head > finalized`) never runs. That is strictly worse than the
  launch failure the clamp removed. Neither reviewer could construct a reth path
  reporting finalized above latest, so it was latent. Extracted
  `clamp_startup_markers`, which clamps finalized FIRST, and table-tested it —
  both halves of the pass-47 M1 fix had zero coverage, and deleting the clamp
  left the whole workspace suite green.
- **Minor 4 (FIXED)** The soak rename guard ran `grep -c` under `set -e`, which
  exits 1 on zero matches, so the step aborted BEFORE printing the diagnostic it
  exists to produce — in exactly the renamed-test case. Added `|| true`.
- **Minor 5 (FIXED)** The stall classifier required a numeric pointer advance,
  which a fresh follower's first tick can never satisfy (it both establishes the
  pointer and imports the backlog), so every bring-up with a real backlog that
  exceeded the 600s budget was logged as a black-holed connection. Now keyed on
  an `imported_this_tick` flag: what the tick DID, not where the pointer
  started. (Two passes running: the original bug was `Option` ordering, the
  pass-47 fix over-corrected.)
- **Minor 6 (FIXED)** The parent-linkage guard silently no-opped when the local
  hash was absent, importing unverified on the one iteration it is most needed.
  An absent local hash is now treated as a stale pointer, like every other
  unknown-state path in the file.
- **Minor 7 (FIXED)** `UpdateFcsHead` accepted a FORWARD head move while its
  sibling explicitly refuses one; a forward move purges mappings only above the
  target and then advances the anchor over blocks whose message stamping never
  ran. Not reachable from any in-tree caller, but the handle is public API.
- **Minor 8 (FIXED, doc-only)** `FatalStateDivergence`'s type doc promised
  restart convergence that three of its sites cannot deliver — the repair loop
  is gated on the very condition two of them report as violated, and the
  finalized-marker mismatch re-raises on the first notification after boot
  (crash-loop). Qualified.
- **Minor 9 (FIXED)** The startup bail keyed on `fcs.is_genesis()`, which a
  provider answering all three reads from a wiped or resynced execution-node
  datadir also satisfies — sending the operator after reachability for no
  reason. The fallback is now bound explicitly and the two cases have separate
  messages.
- **Docs (FIXED)** `CLAUDE.md` named a test target that no longer exists;
  `Makefile`'s `test-docker` still passed `--no-tests=pass` while every other
  lane was hardened to `fail`; the pointer-reset list omitted the parent-linkage
  resets; `from_provider`'s doc described genesis values it does not set and
  omitted that its `None` is load-bearing; `reconcile_genesis_block`'s doc
  presented the foreign-row check as populated-only when it deliberately runs on
  both paths.
- **Also added** tests for `redact_remote`/`safe_remote_host`, which had none
  and are the only thing between a URL carrying basic-auth credentials or an
  API key and an `error!` line.

## Pass 50 findings — resolution (Codex, 2026-09-02)

Codex pass 50 returned one P1, on the pass-49 clamp itself. FIXED; nothing
deferred from this pass.

- **P50-1 (FIXED)** The pass-49 clamp lowered `finalized` down to `latest` when
  a provider snapshot reported finalized above latest. That silently REGRESSES
  the finality floor: an L1 reorg or administrative unwind could then commit
  database state below the execution node's actual finalized block before the
  engine rejected the forkchoice update, leaving persistent divergence that
  needs manual repair — worse than not starting. `from_provider` now refuses an
  inconsistent snapshot (logging the three heights and returning `None`, which
  the caller already handles), and `clamp_startup_markers` became
  `clamp_startup_safe`, which moves only `safe`. Finality is never lowered.
  - Worth noting how this arrived: pass-49 finding 2 correctly identified that
    clamping safe alone could produce `safe < finalized`, and the fix chose the
    wrong repair — clamping finalized rather than rejecting the snapshot. Both
    reviewers had called the shape latent (no reth path reporting finalized
    above latest was found), which is why it survived a pass.

## Pass 51 findings — resolution (Claude, 2026-09-02)

Claude pass 51 returned 8 critical/major, 9 minors and 5 test gaps. All the
code findings are FIXED; the test gaps are recorded below.

- **F1 (FIXED)** Restart silently accepted the exact snapshot the run loop
  calls irreconcilable. `unwind()` commits the rewound head durably BEFORE the
  below-finalized fail-stops fire, so the process died with the database anchor
  below the engine's finalized block — and on restart both new bails passed
  while the repair loop, gated `while l2_head > finalized`, never ran. The node
  came up healthy-looking with mappings above the anchor already purged, and the
  next build re-selected consumed L1 messages into a queue gap every peer
  rejects. Startup now refuses that shape explicitly.
- **F2 (FIXED)** `collect_reverted_txs_in_range` was all-or-nothing AND
  unbounded. One transport blip discarded every transaction already collected —
  and this PR changed all three call sites from propagating to `warn!` + empty,
  so a 500-block unwind silently lost 500 blocks of user transactions under a
  log line reading like "there were none". It is also one serial full-block RPC
  per block on the single-task run loop, and this PR made it reachable over up
  to `MAX_ANCESTOR_LOOKBACK` blocks on any remote reorg. Now per-block
  resilient (failures counted and reported at `error!`) and capped at
  `MAX_REVERTED_TX_COLLECTION_BLOCKS`, logging truncation.
- **F3 (FIXED)** The whole config was `Debug`-dumped at INFO on every launch,
  and `url::Url`'s `Debug` prints userinfo, path and query verbatim — leaking
  the L1 API key, the remote-source credentials, the blob provider URL and the
  KMS key id, defeating the redaction added elsewhere in this PR. Hand-written
  `Debug` for the four secret-carrying argument groups.
- **F4 (FIXED)** `redact_remote` no-opped precisely when the URL carried
  credentials: reqwest strips userinfo into an `Authorization` header before
  building its error, so the error text carries the STRIPPED URL, which never
  matched the configured string — and the path/query API key reached the
  rate-limited log. The pass-49 test passed because it built its message from
  the unstripped URL. Both forms are now scrubbed, and the test covers the
  stripped one (verified to fail without the fix).
- **F5 (FIXED)** `SignerArgs.private_key` had no `#[arg]` attribute inside a
  `clap::Args` derive, making it a POSITIONAL that accepts a raw hex signing key
  on argv — landing in `ps`, `/proc/<pid>/cmdline` and shell history. Now
  `#[arg(skip)]`.
- **F6 (FIXED)** The docker soak lane lacked the per-pattern count guard the
  in-process lanes carry; its pattern covers two tests, so renaming the recovery
  one left the nightly green — and that test is the surviving signal for a path
  already out of the merge gate.
- **F7 (FIXED)** Omitting `--l1.url` panics a release build (the test-utils
  fallback is not compiled in), while `validate()` permitted it for exactly the
  configuration the book advertises. Added as the LAST rule so more specific
  diagnostics still fire first; eight test configs updated.
- **F8 (FIXED)** The genesis comment still said "the computed header hash …
  NOT `chain_spec.genesis_hash()`", both clauses false since the chikyu fix, and
  acting on it would restore that P1.
- **Minors (FIXED)** `.expect()` replaced with `map_err` so the genesis errors'
  actionable Display text survives; the rewind guard now also refuses an
  equal-height different-hash swap (the purge is a no-op for it); two live-path
  messages regained their `\` continuations; `consolidate_chain`'s doc moved
  back off the retry wrapper; the fourth `None` cause documented; the two
  startup hypotheses merged into one message; a dead duplicate `#[arg]`
  removed; the L1-reorg finalized floor now logs like its twin;
  `--chain.chain-buffer-size` documented as inert in both the code and the book
  (threading it through is a feature change, and deleting a shipped CLI flag
  breaks deployments); the book's "arbitrarily far" rewind claim reconciled with
  the 8192-block bound it states two paragraphs later.

### Test gaps recorded (not closed this pass)

Ranked by the reviewer; none are regressions, all are pre-existing coverage
holes in code this PR wrote:

1. `handle_signer_event`'s canonicality classification and monotone anchor
   write — ~90 lines, four branches, zero tests. Widening `<` to `<=` demotes a
   canonical signed block to the signature-only path and the sequencer silently
   stops gossiping.
2. `handle_batch_revert`'s safe clamps and fatal arms — the L1-reorg twin got a
   test in pass 45, this site has none, and `BatchReverted.safe_head` changed
   meaning with nothing pinning it.
3. `ConsolidationFetchFailed` classification and in-place retry — the mechanism
   separating one RPC timeout from killing the node, entirely uncovered.
4. `reconcile_genesis_block`'s foreign-genesis check on a fresh-LOOKING
   database — all four tests seed a head first, so none exercises the ordering
   the doc calls load-bearing.
5. `from_provider`'s inconsistent-snapshot refusal and the startup repair clamp
   — the newest code in the PR; `clamp_startup_safe_table` reaches neither.

## Pass 52 findings — resolution (Codex, 2026-09-02)

Codex pass 52 returned two findings, both on code this PR wrote. Both FIXED.

- **P52-1 (FIXED)** The peer-import site latches L2 sync mode when the engine
  answers SYNCING for a head a previously-synced node imported, and the only
  exit was a LATER import returning VALID. On a quiescent chain no later import
  arrives — peers re-announce blocks this node already knows, and the remote
  source short-circuits once its head is not behind — so nothing re-issued the
  forkchoice update and the node stayed internally syncing forever with L1
  notifications and sequencing gated, while the execution node had long since
  caught up. The latched head is now remembered and re-checked at the top of
  every peer announcement, before any short-circuit: an already-known block is
  the only signal a quiescent chain still produces. VALID leaves sync mode (and
  consolidates, like the original exit); INVALID drops the re-check rather than
  re-issuing a head the engine rejected; a transport error is left for the next
  announcement.
- **P52-2 (FIXED)** The pass-51 `--test` exemption from the new `l1.url` rule
  was ungated, but the fallback watcher only exists under
  `cfg(feature = "test-utils")` — so in the shipped no-default-features binary
  the documented `--test` invocation without `--l1.url` passed validation and
  then hit the very same unwrap. The exemption is now gated on the feature, so
  it only applies to builds that actually carry the fallback.

### Test gap recorded (pass 52)

`recheck_l2_sync_target` is UNTESTED, and it is the largest structural change
made during this review loop — a new orchestrator field plus a helper on the
consensus path. The chain-orchestrator test module has no harness for driving
`handle_block_from_peer` (no test constructs a `NewBlockWithPeer`), so covering
it means building one: signed block construction, consensus checks and a network
event path. That is more than a review-loop fix should take on mid-pass, and a
hastily built harness risks adding a flaky test to the PR that exists to remove
flakiness. It joins the five gaps recorded in pass 51 and should be closed
before merge, ranked alongside `handle_signer_event`.

Specifically worth pinning: SYNCING latches and records the target; a later
announcement of an ALREADY-KNOWN block re-issues and, on VALID, leaves sync mode
and consolidates; INVALID drops the re-check instead of re-issuing forever; a
transport error leaves the target in place for the next announcement.

## Pass 53 findings — resolution (Claude, 2026-09-02)

Claude pass 53 returned 2 critical, 6 majors and 11 minors. All FIXED.

**Four of them — C1, M1, M2, M3 — were defects in the ~50 lines added by pass
52, which the ledger had already recorded as untested. That gap is now closed.**

- **C1 (FIXED)** `recheck_l2_sync_target` re-issued the latched head with no
  comparison against the current mirror, and `ForkchoiceState::update` enforces
  monotonicity only on `finalized` — so a BACKWARD head move is legal and
  commits on VALID. Any other site that moved the mirror first (an optimistic
  sync jumping ahead and purging all mappings, an administrative unwind, an L1
  reorg) left the target stale, and the next announcement silently rewound the
  engine with none of the machinery a real rewind pairs with. The latch now
  records the mirror it was taken against and is dropped as soon as the mirror
  moves off it.
  - Deliberately NOT the `mirror != target` variant one reviewer proposed: at
    latch time the checked FCU never committed, so the mirror is never equal to
    the target and that test drops on every latch, reinstating the wedge pass 52
    existed to close. Both behaviours are now pinned by tests.
- **M1 (FIXED)** The latch exit was the only route out of L2 sync that did not
  fail-stop on a consolidation failure — a plain `?` that `handle_outcome`
  swallows, while both siblings escalate. A validation failure purges every
  L1-to-L2 mapping before returning, so the node would run on marked synced with
  the pool never opening and nothing re-running consolidation.
- **M2 (FIXED)** The latch exit never persisted the L2 anchor. The latch is
  taken because `import_chain` returned early, BEFORE its own persistence block,
  so nothing had written the head the recheck commits. On the quiescent chain
  this feature exists for, the startup repair would rewind the engine by the size
  of the latched import on every restart, and once derivation's finalized marker
  passed that height, startup would refuse outright.
- **M3 (FIXED)** The recheck was unreachable from the `ImportBlock` command
  path, so a remote-block-source node — which imports through that command and
  has no gossip — kept the wedge intact, for precisely the topology this PR
  exists to stabilize.
- **C2 (FIXED)** Seven orchestrator tests used
  `loop { if let Some(X) = events.next().await { break } }`. `EventStream`
  returns `Ready(None)` permanently once the run loop exits, so the loop spins
  on a ready future, never yields `Pending`, and the enclosing timeout is never
  re-polled: the test hangs at 100% CPU instead of failing, in exactly the
  regression it pins. All seven now use the `while let` shape and assert the
  stream did not close.
- **M4 (FIXED)** `RollupNodeNetworkArgs` was missed by the pass-51 redaction
  pass and holds `sequencer_url` as a plain `String`, printed verbatim by the
  startup config dump.
- **M5 (FIXED)** The `l1.url` rule steered operators into a second panic: the
  watcher is skipped whenever `--test` is set without an anvil URL, so supplying
  the URL satisfied validation and then hit the same unwrap.
- **M6 (FIXED)** The quarantine soak lane's build step had no count guard, and
  its soak step carries `continue-on-error` — so a rename would end green with
  the ordinary quarantine comment, silently losing the only automated signal for
  the resume/rewind path.
- **Minors (FIXED)** A test for the `l1.url` rule (the only `validate()` rule
  with none — deleting it left the suite green); book wording for the
  now-unconditional L1 requirement; the signer error no longer offers a private
  key that has no CLI flag; three log literals regained their continuations; the
  finalize tolerance no longer cites a shape pass 44 removed; `GenesisMismatch`
  documented as fresh-and-populated; the rate-limiter comment reconciled with
  `ERROR_LOG_MIN_INTERVAL`.

### Still open from earlier passes

The five test gaps recorded in pass 51 remain, minus none: `handle_signer_event`,
`handle_batch_revert`'s clamps, `ConsolidationFetchFailed`, the foreign-genesis
check on a fresh-looking database, and `from_provider`'s inconsistent-snapshot
refusal. Pass 53 also suggests a `.config/nextest.toml` `terminate-after` and a
step-level `timeout-minutes` on the `unit` job, neither of which exists.

## Pass 54 findings — resolution (Codex, 2026-09-02)

Codex pass 54 returned one P1, on the pass-53 staleness guard. FIXED.

- **P54-1 (FIXED)** The pass-53 C1 guard dropped a stale latch by CLEARING it,
  leaving L2 marked syncing with no recovery target at all. After a far-ahead
  optimistic sync or a successful administrative head move, a peer re-announcing
  the now-current block takes the already-known path and imports nothing, so on
  a quiescent chain nothing reopens the gate and L1 processing and sequencing
  stay disabled indefinitely — the same wedge, entered from the other side. A
  stale latch is now REPLACED by one onto the current mirror, which is always a
  safe target (re-issuing the head the engine already holds is not a move), so
  the next announcement probes whether the execution node has adopted it.
- **Also closed the related hole** pass 53 noted but left open: the
  optimistic-sync entry called `set_syncing()` without latching at all, so that
  route into sync mode had no recovery target either. It now latches onto the
  head it just committed. Every route into L2 sync mode now carries one.
- The stale-latch test's assertion had encoded the bug (`is_none()`); it now
  pins the re-latch instead.

### Loop observation, for the handoff

Three consecutive passes have now found defects in this one mechanism:
pass 52 introduced it, pass 53 found a critical backward-forkchoice hole in it,
pass 54 found that pass 53's guard reintroduced the original wedge. The
mechanism is small but sits on the consensus path and has proven easy to get
wrong in both directions. It deserves human review before merge regardless of
what the next automated pass says.

## Pass 55 findings — resolution (Claude, 2026-09-02)

Claude pass 55 returned 6 majors and 13 minors. Majors FIXED; the remaining
minors are recorded below.

- **M1 (FIXED)** The INVALID arm of `recheck_l2_sync_target` had the SAME defect
  pass 54 fixed twelve lines above it: it cleared the latch while leaving L2
  marked syncing. Fixing one arm and not its sibling is exactly the failure mode
  this mechanism keeps producing. It now re-latches onto the current mirror.
- **M2 (FIXED — the root cause)** The re-check ran only from the two import
  paths, so the cure was gated on precisely the traffic whose absence creates
  the problem, and this PR makes entry into `Syncing` far more common (any
  SYNCING forkchoice response on an ordinary import now latches). Added a
  `L2_SYNC_RECHECK_INTERVAL` arm to the run loop, last in the biased order and
  gated on a live latch, so recovery no longer depends on inbound traffic. This
  is the structural fix the previous three passes were each patching around.
- **M3 (FIXED)** The `l1.url` exemption was `cfg!(test-utils) && test_args.test`,
  but the mock-watcher fallback it guards is keyed ONLY on the cfg.
  `scroll-debug --bootnodes` / `--valid-signer` set `test = false` without a URL
  and so panicked at startup — worked on `main`, and `debug_toolkit` has no tests
  so CI stayed green. The exemption now matches the fallback exactly.
- **M5 (FIXED)** `from_provider`'s three `.ok()??` destroyed the transport error
  at the point this loop made it load-bearing: the caller now hard-bails and
  offers the operator two hypotheses with nothing logged to discriminate them.
  Each tag read now logs which one failed and why.
- **Minors (FIXED)** Five malformed literals from lost `\` continuations (the
  third time this class has appeared); `--blob.anvil-url` corrected to
  `--blob.anvil_url`; the latch doc updated to describe the re-latch rather than
  the behaviour pass 54 replaced; the book's `--l1.url` contradiction resolved.

### Recorded, not fixed

- **M4** `remote_block_source.rs`'s `remote_head <= last_imported` conflates
  caught-up with remote-rewound-below-the-pointer; the `<` case returns Ok,
  resets `consecutive_failures` and can log "Recovered" on the tick the source
  stopped following. Every sibling stale-pointer guard clears the pointer and
  errors. Left because changing the follow-loop's control flow at this point in
  the loop carries more risk than the diagnostic defect it fixes; it should be
  taken with the other remote-source work.
- **M6** `test_remote_block_source_resumes_from_correct_head` is out of both the
  merge gate and `make test`, leaving only the comment-only quarantine lane. Its
  exclusion is justified by a live defect already tracked at ledger:348; the new
  part is that its only regression test is gone. A merge decision, not a code
  fix.
- Pass 55's own remaining minors: three dead `Err` arms on
  `collect_reverted_txs_in_range` (it can no longer fail after pass 51 made it
  per-block resilient), and `GenesisMissing` not hoisted above the
  fresh/populated split the way `GenesisMismatch` was.

## Pass 56 findings — resolution (Codex, 2026-09-02)

Codex pass 56 returned two findings, both on the database layer. Both FIXED and
pinned by tests verified to fail without the fix.

Notably NEITHER is on the L2-sync latch: the pass-55 M2 structural change
(interval-driven recheck) appears to have settled the area that the previous
four passes each found a defect in.

- **P56-1 (FIXED)** The UPGRADE path of the pass-44 defect. Pass 44 stopped the
  revert restore from producing blockless `Consolidated` rows, but a database
  written by the OLD logic already holds them — and `finalize_consolidated_batches`
  swept such a row to `Finalized` while the derivation query selects only
  `Committed`, so that batch's payload was omitted from the finalized chain
  permanently. The transition now splits on whether the batch actually
  contributed block rows: only those may finalize.

  Codex offered two remedies — normalise legacy rows to `Committed`, or restrict
  the transition. The first was tried FIRST and is wrong: a batch whose
  derivation legitimately yields zero blocks is indistinguishable here from a
  legacy underived one, so re-queueing re-derives it on every finalized
  notification forever. The existing test
  `l1_finalization_joint_safe_raise_after_marker_regression` caught that
  immediately (its held batch consolidates with no attributes). The shipped fix
  is the conservative one: blockless rows stay `Consolidated` — not falsely
  marked final, and still visible.

  KNOWN LIMIT, recorded rather than hidden: this stops a legacy underived batch
  being recorded as finalized, but does NOT get its payload derived. The
  derivation query selects only `Committed`, and nothing can safely tell that row
  apart from an empty batch. Repairing genuinely underived legacy rows needs a
  migration, which is out of scope here. Pinned by
  `legacy_blockless_consolidated_batches_are_not_finalized`.
- **P56-2 (FIXED)** `reconcile_genesis_block` decided fresh-vs-populated from the
  `l2_head_block` METADATA anchor alone, and `unwind()` can leave that at 0 with
  real history still stored. With no height-0 row the fresh path then inserted
  this chain's genesis BENEATH the retained history, masking exactly the
  truncated-or-corrupt state `GenesisMissing` exists to report. Populated is now
  decided from stored rows as well as the counter. Pinned by
  `a_zero_anchor_with_stored_history_is_still_populated`.

## Pass 57 findings — resolution (Claude, 2026-09-02)

Claude pass 57 returned 4 majors and 7 minors. All FIXED.

- **M1 (FIXED)** `CLAUDE.md`'s documented dev command panics at startup: the
  binary builds without `test-utils` (no default feature), so both new `l1.url`
  rules reject it and `validate()` is `.expect()`ed. The PR had updated the book
  for these rules but missed CLAUDE.md, which it also edits.
- **M2 (FIXED)** The transport-error arm of the re-check wedged the node behind a
  single `debug!` every 10s: while the latch is stuck the node polls no L1 and
  sequences nothing, reporting healthy. That was tolerable when the probe was
  opportunistic; pass 55 made it the PRIMARY recovery path. It now counts
  consecutive failures and escalates to `warn!`.
- **M3 (FIXED)** Three of the re-check's five arms were untested — reverting the
  INVALID re-latch to `= None`, exactly what it said before pass 55, left the
  suite green while wedging the node. Added tests for the INVALID arm and the
  transport arm.
- **M4 (FIXED)** The interval arm had ZERO coverage: `L2_SYNC_RECHECK_INTERVAL`
  appeared at exactly two sites, its definition and its `interval()`. The pass-55
  claim that recovery no longer depends on inbound traffic was entirely
  unverified. Now pinned by a paused-clock test that drives the run loop with NO
  inputs at all and asserts the latched head is probed — verified to fail when
  the arm is removed.
- **Minors (FIXED)** Six more malformed literals from lost `\` continuations
  (fourth appearance); the "Last in the biased order" comment was wrong (two arms
  follow it); the field doc predated the interval arm.

### Note on the test work itself

The first version of the interval test used a `yield_now` loop under a paused
clock, which keeps the runtime busy so the clock never advances — it spun for
599s before being killed. That is the SAME hazard pass 53 fixed across seven
event-wait loops, reintroduced while writing a test for something else. The
shipped version advances the clock explicitly. Worth remembering: a paused clock
plus any busy-wait is a hang, not a failure.

`tokio`'s `test-util` feature was added as a dev-dependency of
`rollup-node-chain-orchestrator` for the paused clock, so the interval is
provable without a real 10s wait in the unit lane.

## Pass 58 findings — resolution (Codex, 2026-09-02)

Codex pass 58 returned one P1, again on the L2-sync latch. FIXED.

- **P58-1 (FIXED)** The pass-57 INVALID re-latch had a hole precisely where the
  latch comes from the optimistic-sync entry. That entry latches with
  `target == latched_from == mirror` (it commits its target even on SYNCING, so
  the target already IS the mirror), so re-latching onto the mirror re-latched
  the very block just rejected: every later probe answered INVALID and the node
  stayed L2-syncing forever, polling no L1 and sequencing nothing. When the
  rejected target is the mirror, the engine is rejecting the head it itself
  holds — irreconcilable in place — so that now fail-stops with
  `FatalStateDivergence` rather than looping. Pinned by
  `invalid_recheck_of_the_mirror_itself_fail_stops`.

### The pass-57 interval test was flaky, and this pass caught it

`the_interval_arm_recovers_sync_mode_without_any_inbound_traffic` failed once in
a combined run and passed alone; three re-runs then passed. `tokio::spawn` does
not poll the task, so the entire advance budget could elapse before the run loop
created its interval — roughly a one-in-six failure. It now yields before each
advance and is bounded by a REAL-time deadline (a paused clock cannot bound
itself). Six consecutive combined runs clean.

That is a test I added, to a PR whose purpose is removing flaky tests, two passes
after fixing seven hanging waits of a related kind.

### Standing recommendation, restated

This is the SIXTH consecutive pass to find a defect in this ~60-line mechanism
(52 introduced it, 53/54/55/57/58 each found another hole). Every fix has been
locally correct and has then exposed the next case. It now has five arms, an
interval driver, six tests and a fail-stop. The wedge it closes is real, but the
mechanism does not belong in a CI-stabilization PR on this evidence — it should
be reviewed by a human, and probably split into its own change.

## Pass 59 findings — resolution (Claude, 2026-09-02)

Claude pass 59 returned 7 majors plus minors. Five majors FIXED; two recorded.

- **F1 (FIXED)** The L2-sync recheck moved the engine head WITHOUT cancelling an
  in-flight payload job — the only head-moving seam missing from a list
  `event.rs` documents as exhaustive. `BuildBlock` is deliberately not gated on
  sync state, so a job parked on the pre-recheck parent would finalize a SIBLING
  at the new height and, if the engine adopted it, reorg the imported head back
  out with no purge, no refill and no event.
- **F3 (FIXED)** `l2_sync_recheck_failures` was a LIFETIME total. The pass-57
  reset landed in the `RevertToL1Block` handler instead of the recheck — a
  `replace(..., 1)` that hit the first textual match — so a normal node never
  reset it, and after ~6 lifetime blips every later single failure warned that
  the node was gated when it was not. That destroyed the exact signal pass 57
  added. Reset now sits on the `Ok` arm: reaching the engine ends the run.
- **F4 (FIXED)** Another event loop with no `None` arm, so a closed stream spins
  on a ready future and its timeout is never re-polled. Same class as the seven
  fixed in pass 53, in a PR whose purpose is removing CI hangs.
- **F5 (FIXED)** The three startup refusals had ZERO coverage despite the
  predicate having been corrected five times across this loop. Extracted
  `startup_refusal` and table-tested it, including the `head == finalized`
  boundary that a `<`/`<=` slip would break silently.
- **F7 (FIXED)** The four cancellation tests allowed 2s probe + action + 2s wait
  against a 3s job; a self-completing job early-returns with no event and reads
  as a behavioural regression. Raised to 8000ms.
  - NOTE: the blanket edit initially caught a FIFTH site — the coalescing test,
    whose documented invariant is that its 3500ms pinger must EXCEED the payload
    duration. Raising it to 8000 silently reinstated the exact bug pass 47 M5
    fixed. Reverted to 3000ms; only the four cancellation tests were raised.

- **Minors (FIXED)** Eight more malformed literals (fifth recurrence of this
  class); the sequencer's only `error!` moved to the documented target; the probe
  arm's biased-order comment corrected (network and L1 arms are below it); three
  book/doc contradictions.

### Recorded for a HUMAN decision, not fixed

- **F2 — the admin-unwind "retryable" refusals.** Four arms reply `false`, reset
  the watcher and return `Ok` AFTER `unwind_and_revalidate_held_batch` has
  already committed: messages deleted, mappings purged, persisted head rewound.
  The watcher then re-emits `Synced`, reopening the sequencer gate while the
  engine head is still pre-unwind, and the purged messages read
  `l2_block_number IS NULL` so they are re-selected into an out-of-order queue
  every peer rejects. The identical failure in `handle_l1_reorg` is
  `FatalStateDivergence` on the stated grounds that the unwind is already
  committed.

  This is NOT mechanical: refuse-rather-than-fail-stop was a deliberate pass-47
  decision, and reversing it changes operator-facing behaviour on an admin
  command. The reviewer verified a partial mitigation (the sequencer arm is also
  gated on `!has_pending_derivation_work()`, so a reverted range containing
  batches re-stamps before the gate opens; the hole is a range with none).
  Options: make the arms fatal like their twins, or keep the refusal but skip
  `l1_watcher.revert_to_l1_block(..)` so L1 stays latched Syncing.

  The comment at that site claiming "Nothing durable has changed yet" is
  factually wrong and is the reasoning the design rests on — it should be
  corrected whichever way the decision goes.

- **F6 — the finalized-marker query plan.** The pass-45 C1 subquery may force
  `USE TEMP B-TREE FOR ORDER BY` on the finality hot path, inside
  `handle_l1_finalized`'s write transaction, over a set that is never pruned.
  Conditional on an `EXPLAIN QUERY PLAN` this loop has not run; if a temp B-tree
  appears the fix is a correlated `EXISTS`, otherwise it is a comment fix.

### Seventh consecutive pass on the L2-sync latch

F1 and F3 are both in it. The standing recommendation is unchanged and
strengthening: human review, and probably its own change.
