//! Blob providers must authenticate downloaded contents before returning or caching them.

use alloy_eips::eip4844::{
    builder::{SidecarBuilder, SimpleCoder},
    BlobTransactionSidecarItem, BlobTransactionValidationError,
};
use alloy_primitives::B256;
use alloy_rpc_types_beacon::sidecar::{BeaconBlobBundle, BlobData};
use rollup_node_providers::{BeaconClientProvider, BlobProvider, L1ProviderError, S3BlobProvider};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    task::JoinHandle,
};

fn sidecar(data: &[u8]) -> BlobTransactionSidecarItem {
    SidecarBuilder::<SimpleCoder>::from_slice(data)
        .build_4844()
        .unwrap()
        .into_iter()
        .next()
        .unwrap()
}

/// Serve exactly these requests, then close the listener so cache misses cannot pass unnoticed.
async fn serve(responses: Vec<(String, Vec<u8>)>) -> (reqwest::Url, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap()).parse().unwrap();
    let task = tokio::spawn(async move {
        for (path, body) in responses {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            while !request.ends_with(b"\r\n\r\n") {
                request.push(stream.read_u8().await.unwrap());
            }
            assert!(request.starts_with(format!("GET {path} HTTP/1.1\r\n").as_bytes()));
            let header = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(header.as_bytes()).await.unwrap();
            stream.write_all(&body).await.unwrap();
            stream.shutdown().await.unwrap();
        }
    });
    (url, task)
}

async fn beacon(
    sidecars: Vec<BlobTransactionSidecarItem>,
) -> (BeaconClientProvider, JoinHandle<()>) {
    let bundle = BeaconBlobBundle::new(
        sidecars
            .into_iter()
            .enumerate()
            .map(|(index, item)| BlobData {
                index: index as u64,
                blob: item.blob,
                kzg_commitment: item.kzg_commitment,
                kzg_proof: item.kzg_proof,
                signed_block_header: Default::default(),
                kzg_commitment_inclusion_proof: Default::default(),
            })
            .collect(),
    );
    let (url, task) = serve(vec![
        ("/eth/v1/config/spec".into(), br#"{"data":{"SECONDS_PER_SLOT":"12"}}"#.to_vec()),
        ("/eth/v1/beacon/genesis".into(), br#"{"data":{"genesis_time":"0"}}"#.to_vec()),
        ("/eth/v1/beacon/blob_sidecars/1".into(), serde_json::to_vec(&bundle).unwrap()),
    ])
    .await;
    (BeaconClientProvider::new_http(url).await, task)
}

#[tokio::test]
async fn beacon_returns_verified_blob_and_caches_verified_sibling() {
    let first = sidecar(b"first");
    let second = sidecar(b"second");
    let first_hash = B256::from(first.to_kzg_versioned_hash());
    let second_hash = B256::from(second.to_kzg_versioned_hash());
    let (provider, server) = beacon(vec![first.clone(), second.clone()]).await;

    let fetched = provider.blob(12, first_hash).await.unwrap().unwrap();
    assert_eq!(fetched.as_ref(), first.blob.as_ref());
    server.await.unwrap();

    let cached = provider.blob(12, second_hash).await.unwrap().unwrap();
    assert_eq!(cached.as_ref(), second.blob.as_ref());
}

#[tokio::test]
async fn beacon_rejects_tampered_blob_with_original_commitment_and_proof() {
    let mut item = sidecar(b"original");
    let hash = item.to_kzg_versioned_hash().into();
    item.blob[63] ^= 1;
    let (provider, server) = beacon(vec![item]).await;

    assert!(matches!(provider.blob(12, hash).await, Err(L1ProviderError::BlobValidation(_))));
    server.await.unwrap();
}

#[tokio::test]
async fn beacon_rejects_wrong_proof() {
    let mut item = sidecar(b"original");
    let hash = item.to_kzg_versioned_hash().into();
    item.kzg_proof = sidecar(b"different").kzg_proof;
    let (provider, server) = beacon(vec![item]).await;

    assert!(matches!(provider.blob(12, hash).await, Err(L1ProviderError::BlobValidation(_))));
    server.await.unwrap();
}

#[tokio::test]
async fn beacon_rejects_invalid_sibling_before_populating_cache() {
    let requested = sidecar(b"requested");
    let sibling = sidecar(b"valid sibling");
    let mut poisoned = sidecar(b"poisoned sibling");
    let requested_hash = requested.to_kzg_versioned_hash().into();
    let sibling_hash = sibling.to_kzg_versioned_hash().into();
    let poisoned_hash = poisoned.to_kzg_versioned_hash().into();
    poisoned.blob[63] ^= 1;
    let (provider, server) = beacon(vec![requested, sibling, poisoned]).await;

    assert!(matches!(
        provider.blob(12, requested_hash).await,
        Err(L1ProviderError::BlobValidation(_))
    ));
    server.await.unwrap();

    // Neither an earlier valid sibling nor the poisoned blob may survive the rejected response.
    assert!(provider.blob(12, sibling_hash).await.is_err());
    assert!(provider.blob(12, poisoned_hash).await.is_err());
}

#[tokio::test]
async fn s3_returns_blob_matching_requested_hash() {
    let item = sidecar(b"original");
    let hash = B256::from(item.to_kzg_versioned_hash());
    let (url, server) = serve(vec![(format!("/{hash}"), item.blob.to_vec())]).await;
    let provider = S3BlobProvider::new_http(url);

    let fetched = provider.blob(0, hash).await.unwrap().unwrap();
    assert_eq!(fetched.as_ref(), item.blob.as_ref());
    server.await.unwrap();
}

#[tokio::test]
async fn s3_rejects_tampered_blob_under_original_hash() {
    let mut item = sidecar(b"original");
    let expected = B256::from(item.to_kzg_versioned_hash());
    item.blob[63] ^= 1;
    let (url, server) = serve(vec![(format!("/{expected}"), item.blob.to_vec())]).await;
    let provider = S3BlobProvider::new_http(url);

    assert!(matches!(
        provider.blob(0, expected).await,
        Err(L1ProviderError::BlobValidation(
            BlobTransactionValidationError::WrongVersionedHash { expected: hash, .. }
        )) if hash == expected
    ));
    server.await.unwrap();
}

#[tokio::test]
async fn s3_rejects_invalid_field_elements() {
    let mut item = sidecar(b"original");
    let hash = B256::from(item.to_kzg_versioned_hash());
    item.blob[..32].fill(0xff);
    let (url, server) = serve(vec![(format!("/{hash}"), item.blob.to_vec())]).await;
    let provider = S3BlobProvider::new_http(url);

    assert!(matches!(
        provider.blob(0, hash).await,
        Err(L1ProviderError::BlobValidation(BlobTransactionValidationError::KZGError(_)))
    ));
    server.await.unwrap();
}
