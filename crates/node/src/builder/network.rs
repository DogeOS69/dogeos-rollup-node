use alloy_chains::NamedChain;
use alloy_consensus::BlockHeader as _;
use alloy_primitives::{Address, Signature};
use dogeos_chainspec::DogeosChainSpec;
use dogeos_reth_primitives::DogeosPrimitives;
use reth_chainspec::EthChainSpec;
use reth_network::{
    config::NetworkMode,
    primitives::BasicNetworkPrimitives,
    protocol::{RlpxSubProtocol, RlpxSubProtocols},
    transform::header::HeaderTransform,
    NetworkConfig, NetworkHandle, NetworkManager, PeersInfo,
};
use reth_node_api::{NodeTypes, TxTy};
use reth_node_builder::{components::NetworkBuilder, BuilderContext, FullNodeTypes};
use reth_primitives_traits::{BlockHeader, Header};
use reth_transaction_pool::{PoolTransaction, TransactionPool};
use rollup_node_primitives::sig_encode_hash;
use scroll_network::{EthWireBlockImport, EthWireBlockWithPeer};
use std::{fmt, fmt::Debug, sync::Arc};
use tokio::sync::mpsc::Sender;
use tracing::{debug, info};

use crate::args::RollupNodeNetworkArgs;

/// The network builder for Scroll.
#[derive(Debug)]
pub struct ScrollNetworkBuilder {
    /// Additional `RLPx` sub-protocols to be added to the network.
    scroll_sub_protocols: RlpxSubProtocols,
    /// The address expected to have signed downloaded legacy headers.
    signer: Option<Address>,
    /// Sender used to bridge Reth's `eth` block-import callback into the rollup network manager.
    eth_wire_block_tx: Sender<EthWireBlockWithPeer>,
    /// Temporary: enable the legacy geth-to-Reth downloaded-header transform for the one-way
    /// Testnet crossover. Prohibited on `DogeOS` Mainnet. Scheduled for removal with the rest of
    /// the geth compatibility code.
    legacy_geth_header_transform: bool,
}

impl ScrollNetworkBuilder {
    /// Create a new [`ScrollNetworkBuilder`].
    pub fn new(eth_wire_block_tx: Sender<EthWireBlockWithPeer>) -> Self {
        Self {
            scroll_sub_protocols: RlpxSubProtocols::default(),
            signer: None,
            eth_wire_block_tx,
            legacy_geth_header_transform: false,
        }
    }

    /// Add a scroll sub-protocol to the network builder.
    pub fn with_sub_protocol(mut self, protocol: RlpxSubProtocol) -> Self {
        self.scroll_sub_protocols.push(protocol);
        self
    }

    /// Set the signer expected to have signed downloaded legacy headers.
    pub const fn with_signer(mut self, signer: Option<Address>) -> Self {
        self.signer = signer;
        self
    }

    /// Enable or disable the temporary legacy geth-to-Reth downloaded-header transform.
    pub const fn with_legacy_geth_header_transform(mut self, enabled: bool) -> Self {
        self.legacy_geth_header_transform = enabled;
        self
    }
}

impl<Node, Pool> NetworkBuilder<Node, Pool> for ScrollNetworkBuilder
where
    Node:
        FullNodeTypes<Types: NodeTypes<ChainSpec = DogeosChainSpec, Primitives = DogeosPrimitives>>,
    Pool: TransactionPool<
            Transaction: PoolTransaction<
                Consensus = TxTy<Node::Types>,
                Pooled = dogeos_protocol_types::ScrollPooledTransaction,
            >,
        > + Unpin
        + 'static,
{
    type Network = NetworkHandle<DogeosNetworkPrimitives>;

    async fn build_network(
        self,
        ctx: &BuilderContext<Node>,
        pool: Pool,
    ) -> eyre::Result<Self::Network> {
        let chain_spec = ctx.chain_spec();
        let named_chain = chain_spec.chain().named();
        let authorized_signer = if self.signer.is_none() {
            RollupNodeNetworkArgs::default_authorized_signer(named_chain)
        } else {
            self.signer
        };

        // The downloaded-header transform is `None` unless the temporary crossover control is
        // explicitly enabled, and is prohibited on DogeOS Mainnet.
        let header_transform = resolve_header_transform(
            self.legacy_geth_header_transform,
            named_chain,
            authorized_signer,
        )?;

        // set the network mode to work.
        let config = ctx.network_config()?;
        let config = NetworkConfig {
            network_mode: NetworkMode::Work,
            block_import: Box::new(EthWireBlockImport::new(self.eth_wire_block_tx)),
            header_transform,
            extra_protocols: self.scroll_sub_protocols,
            ..config
        };

        let network = NetworkManager::builder(config).await?;
        let handle = ctx.start_network(network, pool);
        info!(target: "reth::cli", enode=%handle.local_node_record(), "P2P networking initialized");
        Ok(handle)
    }
}

/// Network primitive types used by Scroll networks.
type DogeosNetworkPrimitives =
    BasicNetworkPrimitives<DogeosPrimitives, dogeos_protocol_types::ScrollPooledTransaction>;

/// Resolves the optional downloaded-header transform for the given crossover setting and chain.
///
/// The legacy transform exists only for the temporary one-way Testnet geth-to-Reth crossover. It
/// defaults off (`None`), and enabling it on `DogeOS` Mainnet (`NamedChain::Scroll`) is rejected so
/// Mainnet always runs with the ordinary Reth download path. The decision is intentionally gated on
/// the chain identity rather than the configured signer, because Mainnet and Chikyu both configure
/// an authorized signer.
fn resolve_header_transform(
    legacy_enabled: bool,
    chain: Option<NamedChain>,
    authorized_signer: Option<Address>,
) -> eyre::Result<Option<Arc<dyn HeaderTransform<Header>>>> {
    if !legacy_enabled {
        return Ok(None);
    }
    if chain == Some(NamedChain::Scroll) {
        eyre::bail!("network.legacy-geth-header-transform must not be enabled on DogeOS Mainnet");
    }
    Ok(Some(Arc::new(ScrollHeaderTransform::new(authorized_signer))))
}

/// Errors that can occur during signature validation
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum HeaderTransformError {
    /// Invalid signature length (expected 65 bytes)
    InvalidSignature,
    /// Invalid signer (not authorized)
    InvalidSigner(Address),
    /// Signature recovery failed
    RecoveryFailed,
}

impl fmt::Display for HeaderTransformError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidSignature => write!(f, "Invalid signature length, expected 65 bytes"),
            Self::InvalidSigner(signer) => write!(f, "Invalid signer, not authorized: {}", signer),
            Self::RecoveryFailed => write!(f, "Failed to recover signer from signature"),
        }
    }
}

impl std::error::Error for HeaderTransformError {}

/// A downloaded-header [`HeaderTransform`] for the temporary legacy geth-to-Reth crossover.
///
/// Legacy l2geth headers carry the block signature in `extra_data`, but the canonical `DogeOS`
/// header hashes over empty `extra_data`. This transform canonicalizes downloaded headers by
/// stripping `extra_data` so they match the sequenced form. Signer verification is performed for
/// observability only: it logs but never drops or rejects a header. Header acceptance is decided by
/// the downstream hash/linkage/consensus checks, exactly as before — the prior implementation also
/// returned the canonicalized header regardless of signer verification (it only gated an
/// out-of-band signature persistence that no longer exists).
#[derive(Debug, Clone)]
#[non_exhaustive]
pub(crate) struct ScrollHeaderTransform {
    signer: Option<Address>,
}

impl ScrollHeaderTransform {
    /// Returns a new [`ScrollHeaderTransform`].
    pub(crate) const fn new(signer: Option<Address>) -> Self {
        Self { signer }
    }
}

#[async_trait::async_trait]
impl HeaderTransform<Header> for ScrollHeaderTransform {
    async fn map(&self, mut headers: Vec<Header>) -> Vec<Header> {
        // TODO: remove this once we deprecate l2geth.
        let signer = self.signer;
        // Recover signers on a blocking task to keep secp256k1 work off the async runtime. The
        // transform must return exactly one header per input and preserve ordering.
        tokio::task::spawn_blocking(move || {
            use rayon::prelude::*;

            headers.par_iter_mut().for_each(|header| {
                // Canonicalize: strip the legacy signature carried in extra_data.
                let signature_bytes = std::mem::take(&mut header.extra_data);
                let Ok(signature) = parse_65b_signature(&signature_bytes) else {
                    debug!(
                        target: "scroll::network::header_transform",
                        number = header.number(),
                        "downloaded header carried no parseable legacy signature; canonicalized without signer verification",
                    );
                    return;
                };
                // Observability-only signer check against the canonicalized header.
                if let Err(err) = recover_and_verify_signer(&signature, header, signer) {
                    debug!(
                        target: "scroll::network::header_transform",
                        number = header.number(),
                        %err,
                        "downloaded header failed legacy signer verification (observability only)",
                    );
                }
            });
            headers
        })
        .await
        .expect("header transform task panicked")
    }
}

/// Recover signer from signature and verify authorization.
fn recover_and_verify_signer<H: BlockHeader>(
    signature: &Signature,
    header: &H,
    authorized_signer: Option<Address>,
) -> Result<Address, HeaderTransformError> {
    let hash = sig_encode_hash(&header_to_alloy(header));

    // Recover signer from signature
    let signer = reth_primitives_traits::crypto::secp256k1::recover_signer(signature, hash)
        .map_err(|_| HeaderTransformError::RecoveryFailed)?;

    // Verify signer is authorized
    if Some(signer) != authorized_signer {
        return Err(HeaderTransformError::InvalidSigner(signer));
    }

    Ok(signer)
}

/// Parse a canonical 65-byte secp256k1 signature: r (32) | s (32) | v (1).
fn parse_65b_signature(bytes: &[u8]) -> Result<Signature, HeaderTransformError> {
    if bytes.len() != 65 {
        return Err(HeaderTransformError::InvalidSignature);
    }

    let signature =
        Signature::from_raw(bytes).map_err(|_| HeaderTransformError::InvalidSignature)?;

    Ok(signature)
}

/// Convert a generic `BlockHeader` to `alloy_consensus::Header`
fn header_to_alloy<H: BlockHeader>(header: &H) -> Header {
    Header {
        parent_hash: header.parent_hash(),
        ommers_hash: header.ommers_hash(),
        beneficiary: header.beneficiary(),
        state_root: header.state_root(),
        transactions_root: header.transactions_root(),
        receipts_root: header.receipts_root(),
        logs_bloom: header.logs_bloom(),
        difficulty: header.difficulty(),
        number: header.number(),
        gas_limit: header.gas_limit(),
        gas_used: header.gas_used(),
        timestamp: header.timestamp(),
        extra_data: header.extra_data().clone(),
        mix_hash: header.mix_hash().unwrap_or_default(),
        nonce: header.nonce().unwrap_or_default(),
        base_fee_per_gas: header.base_fee_per_gas(),
        withdrawals_root: header.withdrawals_root(),
        blob_gas_used: header.blob_gas_used(),
        excess_blob_gas: header.excess_blob_gas(),
        parent_beacon_block_root: header.parent_beacon_block_root(),
        requests_hash: header.requests_hash(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::Bytes;

    fn header_with_extra(number: u64, extra: Vec<u8>) -> Header {
        Header { number, extra_data: Bytes::from(extra), ..Default::default() }
    }

    #[test]
    fn legacy_transform_disabled_yields_no_hook() {
        // Default-off on every chain, including a configured-signer chain.
        for chain in [None, Some(NamedChain::Scroll), Some(NamedChain::ScrollSepolia)] {
            let hook = resolve_header_transform(false, chain, Some(Address::repeat_byte(1)))
                .expect("disabled transform must never error");
            assert!(hook.is_none(), "disabled crossover must produce no header transform");
        }
    }

    #[test]
    fn legacy_transform_rejected_on_mainnet() {
        let result =
            resolve_header_transform(true, Some(NamedChain::Scroll), Some(Address::repeat_byte(1)));
        assert!(result.is_err(), "enabling the legacy transform on Mainnet must be rejected");
    }

    #[test]
    fn legacy_transform_enabled_on_testnet_and_custom_chains() {
        // Chikyu/Testnet (ScrollSepolia) and a custom chain (None) may enable it explicitly.
        for chain in [Some(NamedChain::ScrollSepolia), None] {
            let hook = resolve_header_transform(true, chain, Some(Address::repeat_byte(1)))
                .expect("enabling the legacy transform off Mainnet must succeed");
            assert!(hook.is_some(), "enabled crossover must install a header transform");
        }
    }

    #[tokio::test]
    async fn map_canonicalizes_preserving_order_and_cardinality() {
        // Distinct, non-signature extra_data payloads across three ordered headers.
        let transform = ScrollHeaderTransform::new(Some(Address::repeat_byte(9)));
        let input = vec![
            header_with_extra(1, vec![0xaa; 65]),
            header_with_extra(2, vec![0xbb; 10]),
            header_with_extra(3, vec![]),
        ];

        let out = transform.map(input).await;

        // Cardinality and ordering are preserved.
        assert_eq!(out.len(), 3);
        assert_eq!(out.iter().map(|h| h.number).collect::<Vec<_>>(), vec![1, 2, 3]);
        // Canonicalization strips extra_data from every header.
        assert!(out.iter().all(|h| h.extra_data.is_empty()));
    }

    #[tokio::test]
    async fn map_keeps_headers_with_invalid_or_unauthorized_signatures() {
        // With an authorized signer configured, an unverifiable signature must not drop the
        // header: signer checking is observability only.
        let transform = ScrollHeaderTransform::new(Some(Address::repeat_byte(7)));
        let input = vec![
            header_with_extra(1, vec![0x01; 65]), // valid length, unauthorized signer
            header_with_extra(2, vec![0x02; 3]),  // wrong length, cannot recover a signature
        ];

        let out = transform.map(input).await;

        assert_eq!(out.len(), 2, "headers must survive failed signer verification");
        assert!(out.iter().all(|h| h.extra_data.is_empty()), "extra_data is always canonicalized");
    }
}
