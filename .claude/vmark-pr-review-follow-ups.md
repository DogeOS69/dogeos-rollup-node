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
    failure (with a loud error if the rollback itself fails), so divergence
    across restarts occurs only when BOTH writes fail. What remains is the
    reply type: the caller still cannot distinguish "reverted cleanly
    refused" from any internal failure.
  - First/most-recent pass: Claude pass 13 (2026-09-01T04:00Z); Codex pass 18
    staleness note (2026-09-01T09:30Z).
  - Why unaddressed: changing the reply types is an API change across the
    command enum, handle, and admin RPC — beyond a review-loop fix. The
    explicit error log is the in-scope mitigation.
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
    carries-L1-messages guard, and optimistic sync including the pass-14
    `!result.is_invalid()` guard, which nothing would catch if inverted or
    dropped. These are the branches where a stale job
    finalizing reorgs a derived/synced chain back out.
  - First/most-recent pass: Claude pass 13 (2026-09-01T04:00Z); Claude
    pass 15 (2026-09-01T07:30Z).
  - Why unaddressed: each needs a deterministic in-flight build held across
    an event none of the existing fixtures drive concurrently (a batch
    commit, an L1 reorg rewinding the L2 head, a peer block beyond the
    optimistic-sync threshold); building that scaffolding risks adding new
    flaky tests to a stabilization PR. The sites share
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
    60/60 as-is.
  - Suggested Linear title: "rollup-node: replace blind sleeps in the optimistic-sync consolidation test with observable preconditions"

- **`PayloadBuildingJobStarted` event to make the coalescing tests' precondition observable**
  (`crates/chain-orchestrator/src/lib.rs` start sites, `crates/node/tests/sync.rs` coalescing tests)
  - Impact/evidence: Claude pass 17 item 6 — both coalescing tests encode "a
    job is already in flight" as a wall-clock sleep. The in-scope mitigation
    was to drop them from the loaded soak lane (contention can legitimately
    defeat the precondition and auto-file a false race regression); a
    started-event notified at the two `start_payload_building` success sites
    would remove the sleeps and let both tests run under load.
  - First/most-recent pass: Claude pass 17 (2026-09-01T08:40Z).
  - Why unaddressed: new production event surface in a stabilization PR; the
    reviewer left it as the orchestrator's call and the lane filter closes
    the false-report path.
  - Suggested Linear title: "chain-orchestrator: emit PayloadBuildingJobStarted so coalescing tests can await an in-flight job"

- **Docker lane can hang to the 90-minute SIGKILL with no nextest summary**
  (`.github/workflows/test.yaml`, integration-docker-compose)
  - Impact/evidence: Claude pass flag — no `.config/nextest.toml`, so no
    slow-timeout/terminate-after; with `--no-fail-fast` and
    `--test-threads=1`, a hanging docker test burns the full job cap and dies
    without a summary.
  - First/most-recent pass: Claude (Opus, high thinking), 2026-08-31T20:58Z.
  - Why unaddressed: needs a decision between a CLI `--slow-timeout` (version
    -sensitive syntax) and introducing `.config/nextest.toml` (affects local
    runs too); period must comfortably exceed the ~4-minute recovery test.
  - Suggested Linear title: "ci: bound docker-lane test hangs with a nextest slow-timeout"

- **Confirm `sync` is not a required status check** (repo settings)
  - Impact/evidence: Claude pass flag — `sync.yaml` no longer runs on push; if
    repo settings list `sync` as a required check, merges would block forever.
    `GET /branches/main/protection` returns 404 with this token (likely no
    protection rules, but unverifiable without admin).
  - First/most-recent pass: Claude (Opus, high thinking), 2026-08-31T20:58Z.
  - Why unaddressed: requires repo-admin visibility.
  - Suggested Linear title: "repo settings: verify no required status check references the sync workflow"
