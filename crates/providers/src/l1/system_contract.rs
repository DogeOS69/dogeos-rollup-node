use crate::L1ProviderError;

use alloy_eips::BlockId;
use alloy_primitives::{Address, U256};
use alloy_provider::Provider;

/// The storage slot of the authorized signer.
pub const AUTHORIZED_SIGNER_STORAGE_SLOT: U256 = U256::from_limbs([0x67, 0x0, 0x0, 0x0]);

/// Provides access to the L1 system contract.
#[async_trait::async_trait]
pub trait SystemContractProvider {
    /// Returns the authorized signer from the system contract on the L1, reading storage at the
    /// given [`BlockId`].
    ///
    /// The block is always explicit: runtime refreshes pin the read to the observed L1 head hash so
    /// that a head advance or reorg between observing the head and reading the signer cannot cache
    /// a different fork's value; the startup read may use [`alloy_eips::BlockNumberOrTag::Latest`]
    /// explicitly. There is deliberately no unqualified variant that silently defaults to `latest`.
    async fn authorized_signer_at(
        &self,
        address: Address,
        block_id: BlockId,
    ) -> Result<Address, L1ProviderError>;
}

#[async_trait::async_trait]
impl<P: Provider> SystemContractProvider for P {
    async fn authorized_signer_at(
        &self,
        address: Address,
        block_id: BlockId,
    ) -> Result<Address, L1ProviderError> {
        let signer =
            self.get_storage_at(address, AUTHORIZED_SIGNER_STORAGE_SLOT).block_id(block_id).await?;
        Ok(Address::from_slice(&signer.to_be_bytes::<32>()[12..]))
    }
}
