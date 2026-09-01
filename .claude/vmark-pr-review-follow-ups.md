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
  (`crates/chain-orchestrator/src/handle/mod.rs`, `classify_recv_error`)
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
  - First/most-recent pass: Claude pass 11 (2026-09-01T01:40Z).
  - Why unaddressed: needs a mocked BlockReader + remote RPC pair; no such
    harness exists in the add-on today. The settlement decision logic, by
    contrast, was extracted into a pure function and table-tested in-PR.
  - Suggested Linear title: "rollup-node: unit-test the remote source's common-ancestor terminal paths"

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
    staleness note (2026-09-01T09:30Z).
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
    fixes (2026-09-01T12:20Z).
  - Why unaddressed: the fix is fixture-level (replace the sleep with an
    observable release — poll the rocksdb lock's acquirability or await the
    persistence task handle) and risks touching every reboot-based test in a
    stabilization PR.
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

- **Test-fixture ergonomics follow-ups**
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

- **FcuRejected refusal is a bare RecvError to the admin caller**
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
