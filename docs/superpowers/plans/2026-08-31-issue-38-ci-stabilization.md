# Issue #38 CI Stabilization Implementation Plan

> **Checkbox note (2026-09-01):** the `- [ ]` boxes below reflect plan-authoring
> time and were deliberately left unticked — execution is tracked by PR #45's
> commits, not this file. Workstream B3 (compose ordering) was skipped: it was
> optional and became unnecessary once B2 landed.


> **Historical plan-of-record — superseded by the shipped code.** This document
> guided the initial implementation; the shipped design then evolved through a
> multi-pass adversarial review cycle on PR #45 (commits `31f2742`, `bd7f303`,
> `98ff292`, `e86177f`, `61e927d`, `64d38d2` and successors), which added
> mechanisms this plan never describes: `PayloadBuildingJobCancelled` and the
> shared `cancel_payload_building_job` helper at every head-moving and
> input-invalidating site, `BuildBlockCoalesced` and its contract tests, the
> remote source's owed-build settlement (`pending_build`, the
> observed-cancellation re-issue gate, `settle_owed_build`, the shared retry
> budget), terminal-error classification that fail-stops the add-on, config
> validation tying `remote-source.build` to the sequencer flags, cancellation
> metrics, the `--no-fail-fast` lanes, and the L1 fee-estimation fix in the
> docker test utils. Some code snippets below are stale (e.g.
> `init_last_imported_block` now takes `&self`, the struct has more fields,
> the soak workflow shipped permanently). Where this document and the code
> disagree, the code and the PR #45 description are authoritative. Checkboxes
> were deliberately left unticked; execution was tracked in the PR itself.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the three CI defects from https://github.com/DogeOS69/dogeos-rollup-node/issues/38 at their root causes: the flaky `test_should_consolidate_after_optimistic_sync`, the remote-source/sequencer startup race in the Docker tests, and the sync workflow's missing-configuration preflight.

**Architecture:** Three independent workstreams that can land as one PR or three small PRs (recommended: three). Workstream A removes a real race between the sequencer's automatic block-building timer and the test's manual build commands, plus hardens the shared event-waiter utility. Workstream B moves the remote-block-source's first contact with the remote node out of the node launch path so startup never depends on the remote being up. Workstream C adds a cheap preflight to the sync workflow and gates the workflow until the (separately discovered) chainspec breakage is resolved.

**Tech Stack:** Rust (tokio, alloy, reth-based workspace), cargo-nextest, GitHub Actions, Docker Compose.

**Spec:** https://github.com/DogeOS69/dogeos-rollup-node/issues/38 (acceptance criteria quoted below). Root-cause investigation notes: session of 2026-08-31, summarized in the "Root causes" section.

## Global Constraints

- Repository: `DogeOS69/dogeos-rollup-node`, base branch `main` (investigated at tree `bad1d9e`).
- From the issue, verbatim: "Optimistic-sync test is made deterministic without weakening its assertion or skipping coverage."
- From the issue, verbatim: "Remote-source Docker tests wait/retry at the correct dependency boundary and no longer depend on launch timing."
- From the issue, verbatim: "Sync workflow validates required configuration before the expensive Rust build."
- From the issue, verbatim: "No timeout-only workaround, blanket retry of failed jobs, or additional hardware escalation is used as the fix."
- Repeated CI evidence on the existing Blacksmith 8-vCPU runners is required before closing the issue (Task D1).
- macOS local builds: export `SDKROOT=$(xcrun --show-sdk-path)` first, or `reth-mdbx-sys` bindgen fails with "'assert.h' file not found".
- Run integration tests with: `cargo nextest run --all-features --locked -E '<filter>'`.

## Root causes (evidence, read before implementing)

**Defect 1 — flaky `test_should_consolidate_after_optimistic_sync`** (`crates/node/tests/sync.rs:196`).
The test configures the sequencer fixture with `auto_start(true)` and `block_time(20)` (ms), i.e. a live 20 ms automatic block-building timer with `allow_empty_blocks(true)` and noop consensus (always allowed to sequence). It then drives 200+ *manual* `build_block()` commands asserting **exact** block numbers (`expect_block_number(i + 1)`).

Why it usually passes: payload building takes 40 ms (fixture default). `ChainOrchestratorCommand::BuildBlock` calls `Sequencer::start_payload_building` directly (`crates/chain-orchestrator/src/lib.rs:440-446`), which **silently replaces** any in-flight payload job. The auto timer's job almost never survives the ~10-20 ms gap between loop iterations, so the manual command replaces it before it completes and numbering stays consistent.

Why it fails under load: when the runner is slow, an auto-triggered job *completes* inside the inter-iteration gap. Its `BlockSequenced(N)` event is consumed and **discarded** by whichever waiter is active at that moment (`wait_for_event_on_all` in `crates/node/src/test_utils/event_utils.rs` drops non-matching events from the shared per-node `EventStream`). The next manual build then produces `N+1`, and `block_sequenced(N)` waits for an event that already went by. Block numbers only grow, so the wait can never succeed → the observed `Timeout (30.0s) waiting for event 'alloy_consensus::block::Block<...>' on 1 nodes (completed 0/1)`.

Secondary hazards found in the same utility (worth fixing while here): the waiter consumes at most one event per 10 ms sleep (~100 events/s cap), and reth's `EventStream` **silently drops** events when the broadcast channel (capacity 5000) lags — both convert backlog into lost events under load.

**Defect 2 — remote-source startup crash** (`crates/node/src/add_ons/remote_block_source.rs`).
`RemoteBlockSourceAddOn::new()` performs remote I/O during node launch: `remote.get_block_number()` plus a common-ancestor walk. The `RetryBackoffLayer::new(10, 100, 330)` on the client is no protection: alloy's default retry policy retries only rate-limit/temporarily-unavailable errors — **connection-refused is not retried** (`TransportErrorKind::is_retry_err`). The `?` at `crates/node/src/add_ons/mod.rs:220` then aborts the whole node launch, so the node's own RPC on 8546 never comes up — exactly the CI failure. Runtime outages are already tolerated (errors in `follow_and_build` are logged and retried each poll tick); only startup is fragile. The compose file (`tests/docker-compose.remote-source.yml`) also has no ordering between `rollup-node-remote-source` and `rollup-node-sequencer` (both depend only on `l1-node`), so this races on every run.

**Defect 3 — sync workflow** (`.github/workflows/sync.yaml`, `crates/node/tests/sync.rs:27`).
Two layers. (a) GitHub Actions passes a missing secret as an **empty string**, so the test's `std::env::var("ALCHEMY_KEY")` skip-guard (which only catches *unset*) does not fire, and the failure surfaces only after a ~20-minute release build. (b) Deeper: since the Tsuki migration (#5) the test uses `DOGEOS_CHIKYU` (chain id `0x5fdaf3`, not a `NamedChain`), so `NodeConfig::from_chainspec` falls through to genesis extra-field parsing and fails at `crates/primitives/src/node/config.rs:123` — the chikyu genesis in the pinned dogeos-reth has **no** `scroll.l1Config.startL1Block` (nor `systemContractAddress` under `l1Config`). Its `l1ChainId` is `111111`, not Sepolia, so the Sepolia-Alchemy URL and Scroll S3 blob bucket in the test are Scroll-heritage leftovers: **the test cannot pass on the current tree even with a valid key**. Fixing that needs a product decision (DogeOS L1 endpoint + blob source + new golden hash, plus genesis fields in dogeos-reth) and is out of scope here; the workflow is gated until then (Task C3).

---

## Workstream A — deterministic optimistic-sync test

### Task A1: Reproduce and pin down the failure locally

**Files:**
- No code changes. Evidence-gathering only.

**Interfaces:**
- Produces: a reproduction command and observed failure rate, pasted into the PR description and issue #38.

- [ ] **Step 1: Build the test binary once**

```bash
cd ~/work/DogeOS69/dogeos-rollup-node
export SDKROOT=$(xcrun --show-sdk-path)   # macOS only
cargo nextest run --all-features --locked -E 'test(test_should_consolidate_after_optimistic_sync)' --no-run
```

- [ ] **Step 2: Run the test 30 times unloaded, then 30 times under CPU load**

```bash
for i in $(seq 30); do
  cargo nextest run --all-features --locked -E 'test(test_should_consolidate_after_optimistic_sync)' || echo "FAILED iteration $i" >> /tmp/issue38-repro.log
done
# Load: saturate cores to mimic a busy runner, then repeat the loop.
for c in $(seq $(sysctl -n hw.ncpu 2>/dev/null || nproc)); do yes > /dev/null & done
for i in $(seq 30); do
  cargo nextest run --all-features --locked -E 'test(test_should_consolidate_after_optimistic_sync)' || echo "FAILED-loaded iteration $i" >> /tmp/issue38-repro.log
done
kill %1 %2 %3 %4 %5 %6 %7 %8 2>/dev/null || true; jobs -p | xargs kill 2>/dev/null || true
cat /tmp/issue38-repro.log
```

Expected: at least one failure under load with the same `Timeout ... Block<...>` error. If no failure in 30 loaded runs, still proceed — the mechanism is confirmed from the CI log and code reading; note the non-reproduction in the PR.

- [ ] **Step 3: (Optional corroboration) confirm the auto/manual interplay in one passing run**

```bash
RUST_LOG=rollup_node::sequencer=trace,scroll::chain_orchestrator=info \
  cargo nextest run --all-features --locked -E 'test(test_should_consolidate_after_optimistic_sync)' --no-capture 2>&1 \
  | grep -cE "Payload building job already in progress, skipping slot"
```

Expected: a nonzero count — the 20 ms timer is live and colliding with manual builds even in passing runs.

> **Execution finding (2026-08-31):** removing the timer alone (the original
> Task A2) exposed a second, hidden role it played: its continuous block stream
> also acted as delivery retries for two one-shot gossips — triggering the
> follower's optimistic sync right after `connect()`, and landing the
> consolidation-triggering block while the follower's sync pipeline is busy.
> With the timer gone those became single-shot and the test timed out on a
> unit-typed wait. A2 as finally shipped therefore *defers* the timer instead
> of removing it: the exact-numbered loop runs with automatic sequencing
> disabled, and `enable_automatic_sequencing()` turns the timer back on for
> the sync/consolidation phase (an intermediate retry-loop design was tried
> and replaced — no such loops exist in the tree). Additionally,
> `wait_n_events` (used by the chain-orchestrator tests in the same file) got a
> 60 s timeout so an unmet expectation fails with a diagnosis instead of
> hanging the binary; `test_chain_orchestrator_l1_reorg` was observed hanging
> exactly that way under parallel-run contention on an unrelated pre-existing
> race (tracked separately from #38).

### Task A2: Remove the auto-sequencer from the exact-numbering test

**Files:**
- Modify: `crates/node/tests/sync.rs:199-209` (the fixture builder of `test_should_consolidate_after_optimistic_sync`)

**Interfaces:**
- Consumes: `TestFixture::builder()` API as-is.
- Produces: nothing new; test behavior only.

- [ ] **Step 1: Edit the fixture: drop `auto_start(true)` (KEEP `block_time(20)`)**

> **Superseded detail:** the shipped test keeps `.block_time(20)` and re-enables
> the timer after the loop via `enable_automatic_sequencing()` — the
> sync/consolidation phase depends on its continuous block stream. Only
> `.auto_start(true)` is dropped. Following the original instruction below
> literally re-breaks the test.

Every block the exact-number loop asserts on is triggered manually via `build_block()`; during that loop the automatic timer only injects nondeterminism. Replace the builder chain:

```rust
let mut sequencer = TestFixture::builder()
    .sequencer()
    .with_memory_db()
    .with_eth_scroll_bridge(true)
    .with_scroll_wire(true)
    .with_l1_message_delay(0)
    .allow_empty_blocks(true)
    .build()
    .await?;
```

(`.auto_start(true)` deleted — the `sequencer()` preset already sets `auto_start = false`; as shipped, `.block_time(20)` stays and the timer is re-enabled after the loop.)

Do NOT touch the assertions: 200 exact-numbered blocks, `optimistic_sync`, `chain_extended(202)`, `L1MessageNotFoundInDatabase` all stay. Coverage is unchanged — the follower still optimistically syncs (201 blocks ahead > the 100-block trigger) and still consolidates.

- [ ] **Step 2: Run the test and verify it passes**

```bash
cargo nextest run --all-features --locked -E 'test(test_should_consolidate_after_optimistic_sync)'
```

Expected: PASS.

- [ ] **Step 3: Re-run the loaded 30-iteration loop from Task A1 Step 2**

Expected: 30/30 pass under load (this is the determinism evidence for the acceptance criterion).

- [ ] **Step 4: Commit**

```bash
git add crates/node/tests/sync.rs
git commit -m "test(sync): drive all blocks manually in optimistic-sync consolidation test

The 20ms auto-build timer raced the test's manual build_block() calls
with exact block-number expectations; on slow runners an auto job
completed in the inter-iteration gap and shifted numbering, hanging
the exact-number waiter for 30s (issue #38, defect 1). All asserted
blocks were already manually triggered; the timer added nothing."
```

### Task A3 (optional hardening): Guard manual BuildBlock against clobbering an in-flight payload job

**Files:**
- Modify: `crates/chain-orchestrator/src/lib.rs:440-446` (`handle_command`, `BuildBlock` arm)
- Test: `crates/node/tests/sync.rs` (new test appended)

**Interfaces:**
- Consumes: `Sequencer::payload_building_job()` (already public, `crates/sequencer/src/lib.rs:74`).
- Produces: coalescing semantics for `ChainOrchestratorHandle::build_block()` — a manual request while a job is in flight rides on that job's `BlockSequenced`/`BlockBuildingSkipped` instead of restarting it. All existing callers (`build_and_await_block`, `RemoteBlockSourceAddOn::follow_and_build`) already wait on the event stream, so coalescing is compatible.

**Honesty note on TDD:** a black-box failing test for this guard is not achievable in isolation — the clobbered job emits no event, so "replace" and "coalesce" produce the same observable sequence in a manual-only scenario; the difference only becomes visible in combination with the auto timer, which is exactly the A2 race. The guard closes the underlying mechanism (silently discarded engine work, timing-shifted numbering) rather than a currently failing case; no in-tree test besides the A2 test mixes `auto_start(true)` with manual exact-number builds today. Skip this task if the team prefers strict no-fix-without-red-test; keep it if they want the recurrence class closed. The new test below is a regression net for the *coalescing contract*, not a pre-fix red test.

- [ ] **Step 1: Write the contract test (passes pre- and post-fix; guards the coalescing semantics going forward)**

Append to `crates/node/tests/sync.rs`:

```rust
/// Contract: a manual build request while a payload building job is in
/// flight coalesces with it — two rapid build_block() commands yield
/// exactly one new block and numbering stays contiguous.
#[allow(clippy::large_stack_frames)]
#[tokio::test]
async fn test_manual_build_block_coalesces_with_inflight_job() -> eyre::Result<()> {
    reth_tracing::init_test_tracing();

    let mut fixture = TestFixture::builder()
        .sequencer()
        .with_memory_db()
        .payload_building_duration(500) // long job so the second command lands mid-flight
        .allow_empty_blocks(true)
        .build()
        .await?;

    fixture.l1().sync().await?;

    // Fire two build commands back-to-back; the second arrives while the
    // 500ms job from the first is still in flight.
    fixture.sequencer().rollup_manager_handle.build_block();
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    fixture.sequencer().rollup_manager_handle.build_block();

    // Exactly one block results: number 1.
    fixture.expect_event().block_sequenced(1).await?;

    // A follow-up build produces number 2 — numbering is contiguous.
    fixture.build_block().expect_block_number(2).build_and_await_block().await?;

    Ok(())
}
```

Note: verify the exact accessor for the handle on `NodeHandle` (`fixture.sequencer().rollup_manager_handle`); `block_builder.rs:99` uses `sequencer_node.rollup_manager_handle`, so it is a public field.

- [ ] **Step 2: Implement the guard**

In `crates/chain-orchestrator/src/lib.rs`, `handle_command`, replace the `BuildBlock` arm:

```rust
ChainOrchestratorCommand::BuildBlock => {
    if let Some(sequencer) = self.sequencer.as_mut() {
        if sequencer.payload_building_job().is_some() {
            tracing::debug!(target: "scroll::chain_orchestrator", "BuildBlock requested while a payload building job is in flight; coalescing with the in-flight job");
        } else {
            sequencer.start_payload_building(&mut self.engine).await?;
        }
    } else {
        tracing::error!(target: "scroll::chain_orchestrator", "Received BuildBlock command but sequencer is not configured");
    }
}
```

- [ ] **Step 3: Run the new test and the neighboring suites**

```bash
cargo nextest run --all-features --locked -E 'test(test_manual_build_block_coalesces_with_inflight_job)'
cargo nextest run --all-features --locked -E 'binary(sync) or binary(remote_block_source)'
```

Expected: all PASS (remote_block_source tests exercise build_block() + event-wait and must stay green under coalescing).

- [ ] **Step 4: Commit**

```bash
git add crates/chain-orchestrator/src/lib.rs crates/node/tests/sync.rs
git commit -m "fix(chain-orchestrator): coalesce manual BuildBlock with in-flight payload job

A manual BuildBlock command silently replaced an in-flight (typically
timer-triggered) payload job, discarding its engine work and making
block numbering timing-dependent (issue #38, defect 1)."
```

### Task A4: Make the event waiter drain all ready events per pass

**Files:**
- Modify: `crates/node/src/test_utils/event_utils.rs:288-333` (`wait_for_event_on_all` inner loop)

**Interfaces:**
- Consumes/Produces: same public API; behavior change is throughput only.

- [ ] **Step 1: Replace the one-event-per-10ms loop with a drain loop**

Current shape consumes at most one event per node per 10 ms sleep (~100 events/s), which lets the broadcast channel (capacity 5000, silently lossy on lag) back up under load. Replace the body of the `timeout(...)` async block:

```rust
loop {
    let mut drained_any = false;

    for (idx, &node_index) in node_indices.iter().enumerate() {
        if results[idx].is_some() {
            continue;
        }

        let node_handle = self.fixture.nodes[node_index].as_mut().ok_or_else(|| {
            eyre::eyre!("Node at index {} has been shutdown", node_index)
        })?;
        let events = &mut node_handle.chain_orchestrator_rx;

        // Drain every event that is already available on this node.
        while let Some(event) = events.next().now_or_never() {
            drained_any = true;
            match event {
                Some(event) => {
                    if let Some(value) = extractor(&event) {
                        results[idx] = Some(value);
                        completed += 1;

                        if completed == node_count {
                            return Ok(results
                                .into_iter()
                                .map(|r| r.unwrap())
                                .collect::<Vec<T>>());
                        }
                        break; // this node is done; move to the next node
                    }
                }
                None => {
                    return Err(eyre::eyre!(
                        "Event stream ended without matching event on node {}",
                        node_index
                    ));
                }
            }
        }
    }

    // Only sleep when nothing was immediately available.
    if !drained_any {
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    }
}
```

- [ ] **Step 2: Run the full non-docker integration suite as the regression net**

```bash
cargo nextest run --all-features --locked --no-tests=pass <!-- superseded: shipped as --no-tests=fail --> -E 'kind(test) and not test(docker)' -- --skip test_should_consolidate_to_block_15k
```

Expected: same set of passes as before the change (61 tests in the CI lane).

- [ ] **Step 3: Commit**

```bash
git add crates/node/src/test_utils/event_utils.rs
git commit -m "test(utils): drain all ready events per pass in event waiter

The waiter consumed at most one event per 10ms sleep (~100/s), letting
the lossy broadcast event channel back up and drop events under load
(issue #38, defect 1 hardening)."
```

### Task A5 (optional hardening): fail fast on unreachable exact-number waits

**Files:**
- Modify: `crates/node/src/test_utils/event_utils.rs:36-46` (`block_sequenced`)

**Interfaces:**
- Produces: `block_sequenced(target)` errors immediately with a diagnostic when it observes a `BlockSequenced` with `number > target` (numbers are monotone per node, so the target can never arrive). Converts any future recurrence of this class of bug from a 30 s silent timeout into an instant, self-explaining failure.

- [ ] **Step 1: Implement via a pre-check wrapper**

```rust
/// Wait for block sequenced event on all specified nodes.
pub async fn block_sequenced(self, target: u64) -> eyre::Result<DogeosBlock> {
    let overshoot = std::sync::atomic::AtomicU64::new(0);
    let result = self
        .wait_for_event_on_all(|e| {
            if let ChainOrchestratorEvent::BlockSequenced(block) = e {
                if block.header.number > target {
                    overshoot.store(block.header.number, std::sync::atomic::Ordering::Relaxed);
                }
                (block.header.number == target).then(|| block.clone())
            } else {
                None
            }
        })
        .await;
    let seen = overshoot.load(std::sync::atomic::Ordering::Relaxed);
    if result.is_err() && seen > target {
        return Err(eyre::eyre!(
            "Waited for BlockSequenced({target}) but observed BlockSequenced({seen}); \
             block numbers are monotone so the target can no longer arrive"
        ));
    }
    result.map(|v| v.first().expect("should have block sequenced").clone())
}
```

(Behavior on success is identical; only the timeout error message improves. If the borrow rules around the `Fn` closure fight back, capture via `Arc<AtomicU64>`.)

- [ ] **Step 2: Run the sync + remote_block_source suites; commit**

```bash
cargo nextest run --all-features --locked -E 'binary(sync) or binary(remote_block_source)'
git add crates/node/src/test_utils/event_utils.rs
git commit -m "test(utils): fail fast when an exact-number block wait is unreachable"
```

---

## Workstream B — remote-source startup at the correct dependency boundary

### Task B1: Failing regression test — remote-source node must launch with its remote down

**Files:**
- Modify: `crates/node/src/test_utils/fixture.rs` (builder: URL override knob)
- Test: `crates/node/tests/remote_block_source.rs` (new test appended)

**Interfaces:**
- Consumes: `TestFixture::builder().sequencer().remote_source_node()` (existing).
- Produces: builder method `remote_source_url(url: reqwest::Url)` overriding the derived sequencer URL; used only by tests.

- [ ] **Step 1: Add the builder override**

In `TestFixtureBuilder` (fixture.rs, next to `has_remote_source_node`): add field

```rust
    /// Overrides the URL the remote source node connects to (tests only;
    /// defaults to the local sequencer's RPC URL).
    remote_source_url_override: Option<reqwest::Url>,
```

(initialize to `None` in the builder's `Default`/constructor), add method

```rust
    /// Override the URL the remote source node connects to.
    pub fn remote_source_url(mut self, url: reqwest::Url) -> Self {
        self.remote_source_url_override = Some(url);
        self
    }
```

and in `build()` (fixture.rs:768-770) replace the URL derivation:

```rust
    let sequencer_url: reqwest::Url = match self.remote_source_url_override.clone() {
        Some(url) => url,
        None => format!("http://localhost:{}", nodes[0].rpc_url().port().unwrap()).parse()?,
    };
```

- [ ] **Step 2: Write the failing test**

Append to `crates/node/tests/remote_block_source.rs`:

```rust
/// Node launch must not depend on the remote block source being reachable.
/// Before the fix, RemoteBlockSourceAddOn::new() probed the remote during
/// launch_add_ons and a connection-refused error aborted the entire node
/// (issue #38, defect 2). The node must come up with the remote down, then
/// import and build once the remote appears.
#[allow(clippy::large_stack_frames)]
#[tokio::test]
async fn test_remote_source_node_launches_when_remote_unreachable() -> eyre::Result<()> {
    reth_tracing::init_test_tracing();

    // Reserve a port and free it again: connections to it are now refused.
    let placeholder = std::net::TcpListener::bind("127.0.0.1:0")?;
    let proxy_port = placeholder.local_addr()?.port();
    drop(placeholder);

    // Build sequencer + remote-source fixture with the remote URL pointed at
    // the dead port. Pre-fix this call fails inside launch_add_ons.
    let mut fixture = rollup_node::test_utils::TestFixture::builder()
        .sequencer()
        .remote_source_node()
        .remote_source_url(format!("http://127.0.0.1:{proxy_port}").parse()?)
        .build()
        .await?;

    fixture.l1().sync().await?;

    // Sequencer produces blocks 1-2 while the remote-source add-on can only
    // log connection errors.
    for i in 1..=2 {
        fixture.build_block().expect_block_number(i).build_and_await_block().await?;
    }

    // Bring the "remote" up: forward the reserved port to the sequencer RPC.
    let sequencer_port = fixture.sequencer().node.rpc_url().port().unwrap();
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", proxy_port)).await?;
    tokio::spawn(async move {
        loop {
            let Ok((mut inbound, _)) = listener.accept().await else { break };
            let Ok(mut outbound) =
                tokio::net::TcpStream::connect(("127.0.0.1", sequencer_port)).await
            else {
                continue;
            };
            tokio::spawn(async move {
                let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
            });
        }
    });

    // Remote source recovers: imports blocks 1-2 and builds block 3 on top
    // (same event pattern as test_remote_block_source).
    fixture.expect_event_on(1).block_sequenced(3).await?;

    Ok(())
}
```

(Verify the exact `rpc_url()` accessor on the sequencer node handle; `build()` at fixture.rs:770 calls `nodes[0].rpc_url()` on the same underlying type. Known small race: another process could grab `proxy_port` between drop and rebind — acceptable for a test; note it in a comment if the reviewer asks.)

- [ ] **Step 3: Run it, expect launch failure**

```bash
cargo nextest run --all-features --locked -E 'test(test_remote_source_node_launches_when_remote_unreachable)'
```

Expected: FAIL at `.build().await?` with a connection-refused error propagated from `launch_add_ons`.

- [ ] **Step 4: Commit the red test (skipped if repo policy forbids committing failing tests; then fold into B2's commit)**

### Task B2: Move remote probing out of the launch path (lazy init)

**Files:**
- Modify: `crates/node/src/add_ons/remote_block_source.rs`
- Modify: `crates/node/src/add_ons/mod.rs:213-228` (call-site generics only, if needed)

**Interfaces:**
- Consumes: existing `ChainOrchestratorHandle`, `EventStream`, `BlockReader` provider.
- Produces: `RemoteBlockSourceAddOn::new()` performs **no remote I/O** (cannot fail on remote availability); `last_imported_block: Option<u64>` initialized on first successful remote contact inside the poll loop, where errors are already logged-and-retried each tick.

- [ ] **Step 1: Restructure the add-on**

In `remote_block_source.rs`:

1. Make the struct generic over the provider and make the resume point lazy:

```rust
pub struct RemoteBlockSourceAddOn<N, P>
where
    N: FullNetwork<Primitives = DogeosNetworkPrimitives>,
{
    config: RemoteBlockSourceArgs,
    orchestrator_handle: ChainOrchestratorHandle<N>,
    events: EventStream<ChainOrchestratorEvent>,
    remote: RootProvider<Scroll>,
    /// Local block reader used to find the highest common block with the
    /// remote on first successful contact.
    provider: P,
    /// Last block imported from the remote. `None` until the remote has been
    /// reached once and the highest common block determined.
    last_imported_block: Option<u64>,
}
```

2. `new()` keeps only local work (URL check, retry-layer client build, `get_event_listener`) and stores `provider`; delete the `get_block_number` call and the ancestor walk from it; return `last_imported_block: None`.

3. Add the moved logic as a method (code moved verbatim from the old `new()`, bounds `P: BlockReader`):

```rust
    /// Determines the highest common block between the local chain and the
    /// remote node. Called on the first successful contact with the remote;
    /// a failure here is retried on the next poll tick.
    async fn init_last_imported_block(&mut self) -> eyre::Result<u64> {
        let local_head = self.orchestrator_handle.status().await?.l2.fcs.head_block_info().number;
        let remote_head = self.remote.get_block_number().await?;

        let mut search = local_head.min(remote_head);
        let last_imported_block = loop {
            if search == 0 {
                break 0;
            }
            let local_hash = self.provider.block_hash(search)?;
            let remote_block = self.remote.get_block_by_number(search.into()).await?;
            match (local_hash, remote_block) {
                (Some(lh), Some(rb)) if lh == rb.header.hash => break search,
                _ => search = search.saturating_sub(1),
            }
        };
        tracing::info!(
            target: "scroll::remote_source",
            last_imported_block,
            local_head,
            remote_head,
            "Determined highest common block with remote"
        );
        Ok(last_imported_block)
    }
```

4. At the top of `follow_and_build()`:

```rust
    async fn follow_and_build(&mut self) -> eyre::Result<()> {
        if self.last_imported_block.is_none() {
            let resume = self.init_last_imported_block().await?;
            self.last_imported_block = Some(resume);
        }
        let mut last_imported = self.last_imported_block.expect("initialized above");
        // ... existing loop, using `last_imported` and writing back
        // `self.last_imported_block = Some(next_block_num)` where it currently
        // assigns `self.last_imported_block = next_block_num`.
```

(Adjust the remaining reads/writes of `self.last_imported_block` in the loop accordingly — mechanical.)

5. Update `mod.rs` if the extra generic parameter needs annotation at the call site (`RemoteBlockSourceAddOn::new(config, handle, rpc_handle.provider().clone())` — inference should cover it). The `?` at mod.rs:220 stays: it now only guards local failures (event-listener plumbing).

- [ ] **Step 2: Run the regression test, expect pass**

```bash
cargo nextest run --all-features --locked -E 'test(test_remote_source_node_launches_when_remote_unreachable)'
```

Expected: PASS — node launches with the remote down, catches up when it appears.

- [ ] **Step 3: Run the existing remote-source suite (resume-point semantics must be unchanged)**

```bash
cargo nextest run --all-features --locked -E 'binary(remote_block_source)'
```

Expected: PASS — `test_remote_block_source_resumes_from_correct_head` exercises the ancestor walk at restart, now on first poll instead of at launch, with identical results.

- [ ] **Step 4: Commit**

```bash
git add crates/node/src/add_ons/remote_block_source.rs crates/node/src/add_ons/mod.rs crates/node/src/test_utils/fixture.rs crates/node/tests/remote_block_source.rs
git commit -m "fix(node): remote block source no longer probes the remote during launch

RemoteBlockSourceAddOn::new() called get_block_number + ancestor walk
inside launch_add_ons; alloy's retry layer does not retry
connection-refused, so a not-yet-ready remote aborted the whole node
and its RPC never came up (issue #38, defect 2). The resume point is
now determined on the first successful poll, where errors are already
logged and retried at poll cadence."
```

### Task B3 (optional, belt-and-braces): deterministic compose ordering

**Files:**
- Modify: `tests/docker-compose.remote-source.yml`

**Interfaces:**
- Produces: `rollup-node-remote-source` starts only after the sequencer's RPC answers. With B2 landed this is not needed for correctness — include it only if the team wants container startup ordering to be self-documenting. Check first whether the runtime image has `curl`/`wget` (`docker run --rm --entrypoint sh <image> -c 'command -v curl wget'`); if it has neither, use the bash `/dev/tcp` probe below (bash is present — the entrypoints use it).

- [ ] **Step 1: Add a healthcheck to `rollup-node-sequencer` and a dependency to `rollup-node-remote-source`**

```yaml
  rollup-node-sequencer:
    # ... existing config ...
    healthcheck:
      test: ["CMD", "bash", "-c", "exec 3<>/dev/tcp/127.0.0.1/8545"]
      interval: 2s
      timeout: 5s
      retries: 60
      start_period: 5s

  rollup-node-remote-source:
    # ... existing config ...
    depends_on:
      l1-node:
        condition: service_healthy
      rollup-node-sequencer:
        condition: service_healthy
```

- [ ] **Step 2: Run both docker tests locally; commit**

```bash
cargo nextest run --all-features --locked --no-tests=pass <!-- superseded: shipped as --no-tests=fail --> -E 'test(docker_test_remote_block_source)' --test-threads=1
git add tests/docker-compose.remote-source.yml
git commit -m "test(docker): gate remote-source container on sequencer RPC readiness"
```

---

## Workstream C — sync workflow preflight

### Task C1: Cheap preflight job before any toolchain/build work

**Files:**
- Modify: `.github/workflows/sync.yaml`

**Interfaces:**
- Produces: a `preflight` job; the build job runs only after it succeeds.

- [ ] **Step 1: Add the preflight and wire `needs`**

```yaml
jobs:
  preflight:
    runs-on: ubuntu-latest
    timeout-minutes: 5
    steps:
      - name: Validate required configuration
        env:
          ALCHEMY_KEY: ${{ secrets.ALCHEMY_KEY }}
        run: |
          set -euo pipefail
          if [ -z "${ALCHEMY_KEY:-}" ]; then
            echo "::error::ALCHEMY_KEY secret is missing or empty. The sync workflow needs it to reach the L1 RPC; provision the repository secret (or keep this workflow disabled) instead of paying for a ~20 minute release build that cannot succeed." 
            exit 1
          fi

  sync:
    needs: preflight
    runs-on: ubuntu-latest
    timeout-minutes: 25
    # ... existing steps unchanged ...
```

- [ ] **Step 2: Validate the workflow file**

```bash
actionlint .github/workflows/sync.yaml 2>/dev/null || python3 -c "import yaml,sys; yaml.safe_load(open('.github/workflows/sync.yaml')); print('yaml ok')"
```

Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/sync.yaml
git commit -m "ci(sync): fail on missing/empty ALCHEMY_KEY before the release build (issue #38, defect 3)"
```

### Task C2: Treat an empty ALCHEMY_KEY as unset in the test guard

**Files:**
- Modify: `crates/node/tests/sync.rs:31-36`

**Interfaces:**
- Produces: local `cargo test` runs skip cleanly with an empty env var too (GitHub passes missing secrets as empty strings).

- [ ] **Step 1: Tighten the guard**

```rust
    // Prepare the config for a L1 consolidation. GitHub Actions passes a
    // missing secret as an EMPTY string, so treat empty as unset.
    let alchemy_key = match std::env::var("ALCHEMY_KEY") {
        Ok(key) if !key.trim().is_empty() => key,
        _ => {
            eprintln!("ALCHEMY_KEY environment variable is not set or empty. Skipping test.");
            return Ok(());
        }
    };
```

- [ ] **Step 2: Verify both paths compile and skip**

```bash
ALCHEMY_KEY= cargo nextest run --all-features --locked -E 'test(test_should_consolidate_to_block_15k)' --no-tests=pass <!-- superseded: shipped as --no-tests=fail -->
```

Expected: test runs and returns Ok (skip message on stderr), no failure.

- [ ] **Step 3: Commit**

```bash
git add crates/node/tests/sync.rs
git commit -m "test(sync): skip 15k consolidation test on empty ALCHEMY_KEY, not just unset"
```

### Task C3: Gate the workflow and file the follow-up for the real breakage

**Files:**
- Modify: `.github/workflows/sync.yaml` (trigger)
- Create: follow-up GitHub issue (not a file in this repo)

**Interfaces:**
- Produces: the sync workflow no longer burns red runs on every main push while the 15k test is unrunnable; a follow-up issue owns the re-pointing decision.

Context (from the investigation, verifiable in seconds): `test_should_consolidate_to_block_15k` now uses `DOGEOS_CHIKYU`, whose genesis (pinned dogeos-reth `8f0b98b`, `crates/dogeos-chainspec/res/genesis/chikyu_dogeos.json`) has no `scroll.l1Config.startL1Block`, so `NodeConfig::from_chainspec` errors at `crates/primitives/src/node/config.rs:123` before any networking. Its `l1ChainId` is `111111` (not Sepolia), so the test's Sepolia-Alchemy URL and Scroll S3 blob bucket are stale Scroll-heritage values. Even with the secret provisioned the workflow fails. This cannot be fixed inside issue #38: it needs chikyu genesis fields in dogeos-reth, a DogeOS L1 RPC + blob source, and a new golden block hash.

- [ ] **Step 1: Switch the trigger to manual until the follow-up lands**

```yaml
name: sync

on:
  # Gated to manual runs: test_should_consolidate_to_block_15k cannot pass on
  # the current tree (DOGEOS_CHIKYU genesis lacks scroll.l1Config.startL1Block;
  # its L1 is chain 111111, not Sepolia). See issue #38 and the follow-up issue
  # <link> before re-enabling on push.
  workflow_dispatch:
```

- [ ] **Step 2: File the follow-up issue**

```bash
gh issue create --repo DogeOS69/dogeos-rollup-node \
  --title "Re-point test_should_consolidate_to_block_15k at DogeOS chikyu infrastructure" \
  --body "$(cat <<'EOF'
Split out of #38 (defect 3). Since the Tsuki migration (#5) the sync workflow's
test uses DOGEOS_CHIKYU but keeps Scroll-Sepolia-era inputs, so it fails at
NodeConfig::from_chainspec (crates/primitives/src/node/config.rs:123) before any
networking — with or without ALCHEMY_KEY:

- chikyu genesis (dogeos-reth crates/dogeos-chainspec/res/genesis/chikyu_dogeos.json)
  has no scroll.l1Config.startL1Block and no l1Config.systemContractAddress;
  chain id 0x5fdaf3 is not a NamedChain so the extra-fields parser is mandatory.
- chikyu l1ChainId is 111111 — the test's https://eth-sepolia.g.alchemy.com URL and
  scroll-sepolia-blob-data S3 bucket cannot serve it.

Needed to make the workflow meaningful again:
1. Add startL1Block + systemContractAddress to the chikyu genesis l1Config in
   dogeos-reth (and bump the pin here).
2. Choose the L1 RPC endpoint + blob source for chikyu's L1 (chain 111111) and
   thread them through the test config (ALCHEMY_KEY becomes a generic L1_RPC_URL
   secret, or is retired).
3. Record a new golden block hash at a chosen consolidation height and re-enable
   the sync workflow's push trigger (gated to workflow_dispatch in #38).
EOF
)"
```

- [ ] **Step 3: Commit the workflow gate**

```bash
git add .github/workflows/sync.yaml
git commit -m "ci(sync): gate to workflow_dispatch until the 15k test is re-pointed at chikyu infra"
```

---

## Workstream D — CI evidence (issue acceptance)

### Task D1: Repeated green evidence on the existing 8-vCPU runners

**Files:**
- No permanent changes; a temporary branch/workflow tweak is allowed for evidence collection.

- [ ] **Step 1: Push the fix branch(es) and let the normal PR lanes run; re-run each affected lane 5×**

```bash
gh workflow run test.yaml --ref <fix-branch> 2>/dev/null || true   # or: gh run rerun <run-id>
for i in 1 2 3 4 5; do gh run rerun <latest-run-id> --repo DogeOS69/dogeos-rollup-node; done
```

(Serial reruns of the same run id; wait for completion between reruns — `gh run watch`.)

- [ ] **Step 2: Targeted soak of the two fixed tests in one temporary CI step (on the fix branch only)**

```yaml
      - name: Soak the previously flaky tests
        run: |
          for i in $(seq 25); do
            cargo nextest run --all-features --locked \
              -E 'test(test_should_consolidate_after_optimistic_sync)' || exit 1
          done
```

and 10 serial iterations of the two `docker_test_remote_block_source_*` tests in the docker lane. Remove the soak step before merge.

- [ ] **Step 3: Post the evidence table (run links, pass counts) on issue #38 and tick its acceptance boxes**

---

## Self-review notes

- Spec coverage: defect 1 → A1-A5; defect 2 → B1-B3; defect 3 → C1-C3; acceptance-evidence criterion → D1; "no timeout-only workaround" → no existing timeout was raised or loosened anywhere. (The implementation did *add* bounded waits where waits could previously hang forever — `wait_n_events` and, after review, the remote block source's build-outcome wait with a pending-build retry — these convert silent hangs into diagnosed failures/retries rather than masking a root cause.) Assertions untouched (A2 removes an incidental config, not an assertion).
- The one acceptance criterion this plan reads narrowly is "Sync workflow validates required configuration before the expensive Rust build": C1 satisfies it literally, and C3 goes further because the investigation showed the workflow still cannot pass with the secret present; if the team prefers to keep the push trigger red instead of gating, drop C3 Step 1 and keep Steps 2-3.
- Type consistency: `last_imported_block: Option<u64>` is introduced in B2 and only read there; `remote_source_url` builder method is introduced in B1 Step 1 and consumed in B1 Step 2.
