//! Paired provider operations on one frozen state view. This is not ordinary RPC timing.
use super::*;
use dogeos_reth_rpc::{
    build_proofs_observed, verify_account_proof, GetProofsRequest, MultiProofObserver,
    MultiProofProvider, ProofStage,
};
use std::time::Instant;

pub(super) fn compare(
    provider: &impl MultiProofProvider,
    request: GetProofsRequest,
    proof_window: u64,
) -> eyre::Result<Value> {
    eyre::ensure!(
        request.targets.len() == 4 &&
            request.targets.iter().map(|t| t.storage_keys.len()).sum::<usize>() == 8,
        "paired fixture requires its fixed four-account/eight-key shape"
    );
    let mut addresses = std::collections::HashSet::new();
    for target in &request.targets {
        let keys: std::collections::HashSet<_> = target.storage_keys.iter().collect();
        eyre::ensure!(
            addresses.insert(target.address) &&
                target.storage_keys.len() <= 4 &&
                keys.len() == target.storage_keys.len(),
            "invalid paired target shape"
        );
    }
    let (root, state) = provider.proof_snapshot(request.block_hash, proof_window)?;
    let mut pairs = Vec::new();
    let mut seed = 1066u64;
    for repetition in 0..6 {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        let order = if seed & 1 == 0 { [false, true] } else { [true, false] };
        let mut arms = Vec::new();
        let mut bytes = Vec::new();
        for shared in order {
            let observer = MultiProofObserver::with_thread_cpu_clock(super::control::thread_cpu);
            let before = super::control::thread_cpu();
            let start = Instant::now();
            let responses = if shared {
                build_proofs_observed(state.as_ref(), &request, root, Some(&observer))?
            } else {
                let mut responses = Vec::new();
                for target in &request.targets {
                    let proof = observer
                        .measure(ProofStage::OrdinaryProofReconstruction, || {
                            state.proof(Default::default(), target.address, &target.storage_keys)
                        })?;
                    observer.measure(ProofStage::AccountVerification, || {
                        verify_account_proof(&proof, root)
                    })?;
                    let response = observer.measure(ProofStage::AccountConversion, || {
                        Ok::<_, eyre::Report>(proof.into_eip1186_response(
                            target.storage_keys.iter().copied().map(Into::into).collect(),
                        ))
                    })?;
                    responses.push(response);
                }
                responses
            };
            let encoded =
                observer.measure(ProofStage::Serialization, || serde_json::to_vec(&responses))?;
            let wall_ms = start.elapsed().as_secs_f64() * 1000.;
            let cpu_ns = before
                .zip(super::control::thread_cpu())
                .and_then(|(a, b)| b.checked_sub(a))
                .map(|d| d.as_nanos());
            arms.push(json!({"mode":if shared {"shared"} else {"ordinary"},"wallMs":wall_ms,"threadCpuNs":cpu_ns,
                "snapshot":observer.snapshot(),"responseBytes":encoded.len(),"responseKeccak256":alloy_primitives::keccak256(&encoded)}));
            bytes.push(encoded);
        }
        eyre::ensure!(bytes[0] == bytes[1], "paired provider bytes differ");
        pairs.push(
            json!({"repetition":repetition,"warmup":repetition==0,"exactBytes":true,"arms":arms}),
        );
    }
    Ok(
        json!({"scope":"same frozen state provider/root/targets; N state.proof versus one state.multiproof plus strict verification, conversion and serialization; excludes RPC/dispatch/snapshot selection; not ordinary RPC CPU",
        "request":request,"root":root,"sameSnapshotBothArms":true,"joinedWorker":true,"pairs":pairs}),
    )
}
