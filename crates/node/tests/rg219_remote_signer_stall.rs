//! RG-219: block signing through a remote signer when one signing call never answers.
//!
//! The sequencer signs every block through the configured signer, here the AWS KMS
//! signer, and announces a block to peers only once it is signed. Signed blocks are
//! handed out in order, so one call that never returns holds back every later block.
//!
//! The test runs the real AWS KMS signer (`--signer.aws-kms-key-id`) against a local
//! KMS-compatible HTTP endpoint. The endpoint signs with an in-memory key and leaves
//! exactly one `Sign` request unanswered; every other request, including any later
//! request for the same block, is answered at once.
//!
//! The follower must catch up with the sequencer soon after the unanswered call: the
//! signer gives up on the stuck call and asks again (#90). The test lives in its own
//! binary because it points the AWS SDK at the local endpoint through process
//! environment variables.

use alloy_primitives::{hex, B256};
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use rollup_node::test_utils::TestFixture;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

/// The `Sign` request (1-based) that is never answered.
const UNANSWERED_SIGN_REQUEST: usize = 3;

/// How long the follower gets to catch up, counted from 5 s after the unanswered call.
const CATCH_UP_BUDGET: Duration = Duration::from_secs(60);

/// DER prefix of a secp256k1 `SubjectPublicKeyInfo` with an uncompressed point.
const SECP256K1_SPKI_PREFIX: &str = "3056301006072a8648ce3d020106052b8104000a034200";

/// A local endpoint that speaks the two AWS KMS calls the signer uses.
struct KmsEndpoint {
    url: String,
    sign_requests: Arc<AtomicUsize>,
}

impl KmsEndpoint {
    async fn spawn(key: PrivateKeySigner) -> eyre::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", listener.local_addr()?);
        let sign_requests = Arc::new(AtomicUsize::new(0));
        let counter = sign_requests.clone();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else { return };
                tokio::spawn(serve(stream, key.clone(), counter.clone()));
            }
        });
        Ok(Self { url, sign_requests })
    }

    fn sign_requests(&self) -> usize {
        self.sign_requests.load(Ordering::SeqCst)
    }
}

/// Serves one HTTP/1.1 request and closes the connection.
async fn serve(mut stream: TcpStream, key: PrivateKeySigner, sign_requests: Arc<AtomicUsize>) {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    let header_end = loop {
        let Ok(n) = stream.read(&mut chunk).await else { return };
        if n == 0 {
            return;
        }
        buf.extend_from_slice(&chunk[..n]);
        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break pos + 4;
        }
    };
    let head = String::from_utf8_lossy(&buf[..header_end]).to_ascii_lowercase();
    let header = |name: &str| {
        head.lines().find_map(|l| l.strip_prefix(name)?.strip_prefix(':').map(|v| v.trim()))
    };
    let length: usize = header("content-length").and_then(|v| v.parse().ok()).unwrap_or(0);
    let target = header("x-amz-target").unwrap_or_default().to_string();
    while buf.len() < header_end + length {
        let Ok(n) = stream.read(&mut chunk).await else { return };
        if n == 0 {
            return;
        }
        buf.extend_from_slice(&chunk[..n]);
    }
    let request: serde_json::Value =
        serde_json::from_slice(&buf[header_end..header_end + length]).unwrap_or_default();
    let key_id = request["KeyId"].as_str().unwrap_or_default();

    let body = match target.as_str() {
        "trentservice.getpublickey" => {
            let point = key.credential().verifying_key().to_encoded_point(false);
            let mut spki = hex::decode(SECP256K1_SPKI_PREFIX).expect("valid hex");
            spki.extend_from_slice(point.as_bytes());
            serde_json::json!({
                "KeyId": key_id,
                "PublicKey": base64(&spki),
                "KeySpec": "ECC_SECG_P256K1",
                "KeyUsage": "SIGN_VERIFY",
                "SigningAlgorithms": ["ECDSA_SHA_256"],
            })
        }
        "trentservice.sign" => {
            if sign_requests.fetch_add(1, Ordering::SeqCst) + 1 == UNANSWERED_SIGN_REQUEST {
                // Keep the connection open and never answer.
                std::future::pending::<()>().await;
            }
            let digest = unbase64(request["Message"].as_str().unwrap_or_default());
            let signature = key.sign_hash_sync(&B256::from_slice(&digest)).expect("sign digest");
            let der = der_signature(
                &signature.r().to_be_bytes::<32>(),
                &signature.s().to_be_bytes::<32>(),
            );
            serde_json::json!({
                "KeyId": key_id,
                "Signature": base64(&der),
                "SigningAlgorithm": "ECDSA_SHA_256",
            })
        }
        _ => return,
    };
    let body = body.to_string();
    let response = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: application/x-amz-json-1.1\r\n\
         x-amzn-requestid: rg219\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = stream.write_all(response.as_bytes()).await;
}

/// DER `ECDSA-Sig-Value` from big-endian `r` and `s`.
fn der_signature(r: &[u8], s: &[u8]) -> Vec<u8> {
    let integer = |bytes: &[u8]| {
        let start = bytes.iter().position(|b| *b != 0).unwrap_or(bytes.len() - 1);
        let mut value = bytes[start..].to_vec();
        if value[0] & 0x80 != 0 {
            value.insert(0, 0);
        }
        let mut out = vec![0x02, value.len() as u8];
        out.extend(value);
        out
    };
    let mut body = integer(r);
    body.extend(integer(s));
    let mut out = vec![0x30, body.len() as u8];
    out.extend(body);
    out
}

const BASE64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

fn base64(bytes: &[u8]) -> String {
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let n =
            chunk.iter().enumerate().fold(0u32, |acc, (i, b)| acc | (*b as u32) << (16 - 8 * i));
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(BASE64[(n >> (18 - 6 * i) & 63) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

fn unbase64(text: &str) -> Vec<u8> {
    let mut out = Vec::new();
    let (mut acc, mut bits) = (0u32, 0);
    for c in text.bytes().filter(|c| *c != b'=') {
        let Some(v) = BASE64.iter().position(|b| *b == c) else { continue };
        acc = acc << 6 | v as u32;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
        }
    }
    out
}

/// One block-signing call to the remote signer never returns. The sequencer keeps
/// building blocks; the follower must still catch up shortly after.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rg219_follower_catches_up_after_one_signing_call_never_returns() -> eyre::Result<()> {
    reth_tracing::init_test_tracing();

    let key = PrivateKeySigner::random();
    let kms = KmsEndpoint::spawn(key.clone()).await?;
    // The AWS SDK reads its endpoint, region and credentials from the environment.
    // Nothing else in this test binary reads the environment concurrently.
    for (name, value) in [
        ("AWS_ENDPOINT_URL_KMS", kms.url.as_str()),
        ("AWS_REGION", "us-east-1"),
        ("AWS_ACCESS_KEY_ID", "rg219-test"),
        ("AWS_SECRET_ACCESS_KEY", "rg219-test"),
        ("AWS_EC2_METADATA_DISABLED", "true"),
        ("AWS_CONFIG_FILE", "/nonexistent/rg219-aws-config"),
        ("AWS_SHARED_CREDENTIALS_FILE", "/nonexistent/rg219-aws-credentials"),
    ] {
        std::env::set_var(name, value);
    }
    std::env::remove_var("AWS_PROFILE");

    let mut fixture = TestFixture::builder()
        .sequencer()
        .followers(1)
        .with_sequencer_auto_start(true)
        .block_time(250)
        .allow_empty_blocks(true)
        .with_eth_scroll_bridge(true)
        .with_scroll_wire(true)
        .config(|config| config.signer_args.aws_kms_key_id = Some("rg219-test-key".to_string()))
        .build()
        .await?;
    fixture.l1().for_node(0).sync().await?;

    // Wait for the unanswered call.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    while kms.sign_requests() < UNANSWERED_SIGN_REQUEST {
        assert!(
            tokio::time::Instant::now() < deadline,
            "only {} of {UNANSWERED_SIGN_REQUEST} Sign requests arrived within 60 s",
            kms.sign_requests()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let follower_before = fixture.get_block(1).await?.header.number;

    // The sequencer keeps building while the call is outstanding.
    // Must stay below the signer's per-attempt timeout (10 s today), or the hold check fails.
    tokio::time::sleep(Duration::from_secs(5)).await;
    let sequencer_head = fixture.get_sequencer_block().await?.header.number;
    let follower_head = fixture.get_block(1).await?.header.number;
    println!(
        "RG-219 5 s after the unanswered call: sequencer={sequencer_head} \
         follower={follower_head} (was {follower_before}) sign_requests={}",
        kms.sign_requests()
    );
    assert!(
        sequencer_head >= UNANSWERED_SIGN_REQUEST as u64 + 5,
        "the sequencer stopped building at {sequencer_head}"
    );
    // The unanswered call holds back every later block, so the follower has not moved past it.
    assert!(
        follower_head <= UNANSWERED_SIGN_REQUEST as u64,
        "follower at {follower_head} was not held back by the unanswered call"
    );

    // The follower reaches the head the sequencer had at that point.
    let start = tokio::time::Instant::now();
    loop {
        let follower = fixture.get_block(1).await?.header.number;
        let sequencer = fixture.get_sequencer_block().await?.header.number;
        println!(
            "RG-219 t={}s sequencer={sequencer} follower={follower} sign_requests={}",
            start.elapsed().as_secs(),
            kms.sign_requests()
        );
        if follower >= sequencer_head {
            break;
        }
        assert!(
            start.elapsed() < CATCH_UP_BUDGET,
            "follower stuck at {follower} while the sequencer is at {sequencer}, {}s after one \
             signing call stopped answering",
            CATCH_UP_BUDGET.as_secs() + 5
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    Ok(())
}
