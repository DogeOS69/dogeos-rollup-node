//! Fixture-only control, outside materialization windows. No RPC method added.
use super::*;
use dogeos_reth_rpc::MultiProofObserver;
use reth_rpc_eth_api::helpers::SpawnBlocking;
use std::{path::Path, time::Instant};

// CLOCK_THREAD_CPUTIME_ID on this Linux 64-bit host. No filesystem I/O or lock
// in the optional synchronous-stage clock callback.
#[cfg(all(target_os = "linux", target_pointer_width = "64"))]
pub(super) fn thread_cpu() -> Option<Duration> {
    #[repr(C)]
    struct Timespec {
        seconds: std::ffi::c_long,
        nanos: std::ffi::c_long,
    }
    unsafe extern "C" {
        fn clock_gettime(clock: std::ffi::c_int, value: *mut Timespec) -> std::ffi::c_int;
    }
    let mut value = Timespec { seconds: 0, nanos: 0 };
    // SAFETY: valid writable timespec for Linux's C ABI; constant is a thread clock.
    if unsafe { clock_gettime(3, &raw mut value) } != 0 ||
        value.seconds < 0 ||
        !(0..1_000_000_000).contains(&value.nanos)
    {
        return None;
    }
    Some(Duration::new(value.seconds as u64, value.nanos as u32))
}
#[cfg(not(all(target_os = "linux", target_pointer_width = "64")))]
pub(super) fn thread_cpu() -> Option<Duration> {
    None
}

fn export(path: &Path, value: &Value) -> eyre::Result<()> {
    let temporary = path.with_extension("next");
    std::fs::write(&temporary, serde_json::to_vec_pretty(value)?)?;
    std::fs::rename(temporary, path)?;
    Ok(())
}

pub(super) async fn service(
    output: &Path,
    fixture: &mut TestFixture,
    observer: &MultiProofObserver,
    permits: u32,
    poisoned: &mut bool,
) -> eyre::Result<()> {
    let request_path = output.join("control-request.json");
    if !request_path.exists() {
        return Ok(());
    }
    let request: Value = serde_json::from_slice(&std::fs::read(&request_path)?)?;
    let id = request["id"].as_str().ok_or_else(|| eyre::eyre!("control id required"))?;
    eyre::ensure!(
        !id.is_empty() && id.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'-'),
        "invalid control id"
    );
    let response_path = output.join(format!("control-response-{id}.json"));
    std::fs::remove_file(&request_path)?;
    *poisoned |= request["uncertainUntrackedWork"].as_bool().unwrap_or(true);
    let start = Instant::now();
    let eth = fixture.sequencer().node.inner.add_ons_handle.rpc_registry.eth_api().clone();
    let outcome = tokio::time::timeout(Duration::from_secs(30), async {
        if *poisoned {
            export(&response_path, &json!({"id":id,"drained":false,"reason":"uncertain untracked ordinary/ancillary worker cancellation; reuse stopped","snapshot":observer.snapshot(),"drainWallMs":start.elapsed().as_secs_f64()*1000.}))?;
            return Ok::<_, eyre::Report>(());
        }
        // Stop ingress is the caller's protocol: all materializer futures ended;
        // sequencing is frozen. Shared workers must finish BEFORE acquiring every
        // ordinary permit or the barrier itself would deadlock waiting for them.
        observer.await_idle().await;
        let all = eth.acquire_many_owned_tracing(permits).await?;
        let baseline = if request["action"] == "providerBaseline" {
            let proof_request = serde_json::from_value(request["request"].clone())?;
            let window = request["proofWindow"].as_u64().ok_or_else(|| eyre::eyre!("proof window required"))?;
            let provider = fixture.sequencer().node.inner.provider.clone();
            // One synchronous worker owns both arms and the single frozen provider.
            // A failed join/timeout never publishes a drained=true acknowledgment.
            Some(tokio::task::spawn_blocking(move || super::baseline::compare(&provider, proof_request, window)).await??)
        } else { None };
        let snapshot = observer.snapshot();
        eyre::ensure!(snapshot.iter().all(|stage| stage.active == 0), "active observer after barrier");
        // Ingress waits for the response: release every permit before publishing
        // that acknowledgment so the barrier cannot queue the next sample.
        drop(all);
        export(&response_path, &json!({"id":id,"drained":true,"snapshot":snapshot,
            "providerBaseline":baseline,"allTracingPermitsHeldAtSnapshot":permits,"permitsReleasedBeforeAck":true,"drainWallMs":start.elapsed().as_secs_f64()*1000.,
            "scope":"no uncertain untracked cancellation; observer idle then all tracing permits held during snapshot, released before ack; caller ingress stopped"}))?;
        Ok(())
    }).await;
    if !matches!(outcome, Ok(Ok(()))) {
        *poisoned = true;
        export(
            &response_path,
            &json!({"id":id,"drained":false,"reason":format!("barrier failed: {outcome:?}"),"drainWallMs":start.elapsed().as_secs_f64()*1000.}),
        )?;
    }
    Ok(())
}

#[tokio::test]
async fn observer_registration_and_poisoned_barrier_are_explicit() -> eyre::Result<()> {
    let observer = Arc::new(MultiProofObserver::default());
    let mut fixture = rollup_node::test_utils::RANGE_MULTIPROOF_OBSERVER
        .scope(observer.clone(), fixture_with_tsuki(false, rpc_args(true, false), false))
        .await?;
    // Successful launch rejects duplicate registration; actual shared proof must
    // reach the observer-bearing instance rather than silently fall back.
    let client = fixture.sequencer().node.rpc_client().unwrap();
    assert_equivalent(&client, fixture.chain_spec.genesis_hash(), targets(0)).await?;
    observer.await_idle().await;
    let snapshot = serde_json::to_value(observer.snapshot())?;
    let request = snapshot.as_array().unwrap().iter().find(|s| s["stage"] == "request").unwrap();
    assert_eq!(request["success"]["samples"], 1);
    let output = std::env::temp_dir().join(format!(
        "range-control-{}-{}",
        std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_nanos()
    ));
    std::fs::create_dir(&output)?;
    let mut poisoned = false;
    for (id, uncertain, expected) in
        [("healthy", false, true), ("cancelled", true, false), ("reuse", false, false)]
    {
        export(
            &output.join("control-request.json"),
            &json!({"id":id,"uncertainUntrackedWork":uncertain}),
        )?;
        service(
            &output,
            &mut fixture,
            &observer,
            rpc_args(true, false).rpc_proof_permits as u32,
            &mut poisoned,
        )
        .await?;
        let response: Value = serde_json::from_slice(&std::fs::read(
            output.join(format!("control-response-{id}.json")),
        )?)?;
        assert_eq!(response["drained"], expected);
        if expected {
            assert_eq!(response["permitsReleasedBeforeAck"], true);
        }
    }
    std::fs::remove_dir_all(output)?;
    fixture.shutdown_node(0).await?;
    Ok(())
}
