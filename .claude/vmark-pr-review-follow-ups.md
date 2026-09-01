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
    exists for the pieces: both cancellation-event tests, the coalescing
    tests, and the validate() rules.
  - Suggested Linear title: "rollup-node: end-to-end test for the remote source's owed-build retry path"

- **Outcome-event identity (numbers on skip/cancel events)**
  (`crates/chain-orchestrator/src/event.rs`, remote block source waits)
  - Impact/evidence: Claude pass 9 S1 — `BlockBuildingSkipped` and
    `PayloadBuildingJobCancelled` carry no height, so attribution of
    numberless outcomes rests on the single-requester assumption plus
    import-time cancellation. The PR ships a cheap mitigation (one stale
    numberless outcome is ignored after an abandonment); giving the events an
    identity (a block_number field, gated at-or-above expected like
    `BlockSequenced`) removes the assumption entirely.
  - First/most-recent pass: Claude pass 9 (2026-09-01T00:22Z).
  - Why unaddressed: additive event-payload change rippling through every
    match on the enum; the mitigation covers the realistic window.
  - Suggested Linear title: "chain-orchestrator: carry the target height on skip/cancel build outcome events"

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
