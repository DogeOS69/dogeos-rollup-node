use crate::{BlobProvider, L1ProviderError};
use reqwest::Client;
use std::sync::Arc;

use alloy_eips::eip4844::{
    c_kzg, env_settings::EnvKzgSettings, kzg_to_versioned_hash, Blob,
    BlobTransactionValidationError,
};
use alloy_primitives::B256;

/// An implementation of a blob provider client using S3.
#[derive(Debug, Clone)]
pub struct S3BlobProvider {
    /// The base URL for the S3 service.
    pub base_url: String,
    /// HTTP client for making requests.
    pub client: Client,
}

impl S3BlobProvider {
    /// Creates a new [`S3BlobProvider`] from the provided url.
    pub fn new_http(base: reqwest::Url) -> Self {
        // If base ends with a slash, remove it
        let mut base = base.to_string();
        if base.ends_with('/') {
            base.remove(base.len() - 1);
        }
        Self { base_url: base, client: reqwest::Client::new() }
    }
}

#[async_trait::async_trait]
impl BlobProvider for S3BlobProvider {
    #[allow(clippy::large_stack_frames)]
    async fn blob(
        &self,
        _block_timestamp: u64,
        hash: B256,
    ) -> Result<Option<Arc<Blob>>, L1ProviderError> {
        let url = format!("{}/{}", self.base_url, hash);
        let response = self.client.get(&url).send().await.map_err(L1ProviderError::S3Provider)?;

        if response.status().is_success() {
            let blob_data = response.bytes().await.map_err(L1ProviderError::S3Provider)?;

            let blob = Blob::try_from(blob_data.as_ref())
                .map_err(|_| L1ProviderError::Other("Invalid blob data"))?;
            // S3 serves raw blobs without proofs; derive the commitment from the contents.
            let kzg_blob = c_kzg::Blob::from_bytes(blob.as_slice())
                .map_err(BlobTransactionValidationError::KZGError)?;
            let commitment = EnvKzgSettings::Default
                .get()
                .blob_to_kzg_commitment(&kzg_blob)
                .map_err(BlobTransactionValidationError::KZGError)?;
            let have = kzg_to_versioned_hash(commitment.to_bytes().as_ref());
            if have != hash {
                return Err(BlobTransactionValidationError::WrongVersionedHash {
                    have,
                    expected: hash,
                }
                .into());
            }
            Ok(Some(Arc::new(blob)))
        } else if response.status() == 404 {
            Ok(None)
        } else {
            Err(L1ProviderError::Other("S3 client HTTP error"))
        }
    }
}
