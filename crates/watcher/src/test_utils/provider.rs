use crate::Block;
use std::{
    collections::{HashMap, VecDeque},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

use alloy_eips::BlockNumberOrTag;
use alloy_json_rpc::RpcError;
use alloy_network::Ethereum;
use alloy_primitives::{Address, BlockNumber, StorageValue, TxHash, B256, U256, U64};
use alloy_provider::{EthGetBlock, Provider, ProviderCall, RootProvider, RpcWithBlock};
use alloy_rpc_types_eth::{BlockId, Filter, Log, Transaction};
use alloy_transport::{TransportErrorKind, TransportResult};

/// A mock implementation of the [`Provider`] trait.
#[derive(Debug)]
pub struct MockProvider {
    blocks: Arc<Mutex<HashMap<BlockNumber, Vec<Block>>>>,
    transactions: HashMap<B256, Transaction>,
    logs: Arc<Mutex<VecDeque<Log>>>,
    finalized_blocks: Arc<Mutex<Vec<Block>>>,
    latest_blocks: Arc<Mutex<Vec<Block>>>,
    storage_responses: Arc<Mutex<VecDeque<TransportResult<StorageValue>>>>,
    storage_read_count: Arc<AtomicUsize>,
    /// The block ids requested through [`Provider::get_storage_at`], in order.
    storage_block_ids: Arc<Mutex<Vec<BlockId>>>,
    /// The value returned when no scripted storage response remains. `None` (the default) is
    /// strict and returns an error for unscripted reads; integration fixtures that
    /// intentionally rely on a stable value must opt in via
    /// [`MockProvider::with_default_storage_value`].
    default_storage_response: Option<StorageValue>,
}

impl MockProvider {
    /// Returns a new [`MockProvider`] from the iterator over blocks, the finalized and the latest
    /// block.
    pub fn new(
        blocks: impl Iterator<Item = Block>,
        transactions: impl Iterator<Item = Transaction>,
        logs: impl Iterator<Item = Log>,
        finalized_blocks: Vec<Block>,
        latest_blocks: Vec<Block>,
    ) -> Self {
        let mut b = HashMap::new();
        for block in blocks {
            b.entry(block.header.number).or_insert(Vec::new()).push(block);
        }
        Self {
            blocks: Arc::new(Mutex::new(b)),
            transactions: transactions.map(|tx| (*tx.inner.tx_hash(), tx)).collect(),
            logs: Arc::new(Mutex::new(logs.collect())),
            finalized_blocks: Arc::new(Mutex::new(finalized_blocks)),
            latest_blocks: Arc::new(Mutex::new(latest_blocks)),
            storage_responses: Arc::new(Mutex::new(VecDeque::new())),
            storage_read_count: Arc::new(AtomicUsize::new(0)),
            storage_block_ids: Arc::new(Mutex::new(Vec::new())),
            default_storage_response: None,
        }
    }

    /// Sets the scripted storage responses returned by [`Provider::get_storage_at`].
    pub fn with_storage_responses(
        mut self,
        responses: impl IntoIterator<Item = TransportResult<StorageValue>>,
    ) -> Self {
        self.storage_responses = Arc::new(Mutex::new(responses.into_iter().collect()));
        self
    }

    /// Opt in to a lenient default storage value returned once the scripted responses are
    /// exhausted. Without this, unscripted [`Provider::get_storage_at`] reads return an error so a
    /// test cannot silently depend on an implicit zero value.
    pub const fn with_default_storage_value(mut self, value: StorageValue) -> Self {
        self.default_storage_response = Some(value);
        self
    }

    /// Returns the number of storage requests made through this provider.
    pub fn storage_read_count(&self) -> usize {
        self.storage_read_count.load(Ordering::Relaxed)
    }

    /// Returns the block ids requested through [`Provider::get_storage_at`], in order.
    pub fn storage_block_ids(&self) -> Vec<BlockId> {
        self.storage_block_ids.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl Provider for MockProvider {
    fn root(&self) -> &RootProvider<Ethereum> {
        unreachable!("unused calls")
    }

    fn get_chain_id(&self) -> ProviderCall<alloy_rpc_client::NoParams, U64, u64> {
        ProviderCall::Ready(Some(Ok(0)))
    }

    fn get_block(&self, block_id: BlockId) -> EthGetBlock<Block> {
        let val = match block_id {
            BlockId::Hash(_) => unimplemented!("hash query is not supported"),
            BlockId::Number(number_or_tag) => match number_or_tag {
                BlockNumberOrTag::Latest => {
                    let mut blocks = self.latest_blocks.lock().unwrap();
                    if blocks.is_empty() {
                        None
                    } else {
                        blocks.drain(..1).next()
                    }
                }
                BlockNumberOrTag::Finalized => {
                    let mut blocks = self.finalized_blocks.lock().unwrap();
                    if blocks.is_empty() {
                        None
                    } else {
                        blocks.drain(..1).next()
                    }
                }
                BlockNumberOrTag::Number(number) => {
                    let mut blocks = self.blocks.lock().unwrap();
                    blocks.get_mut(&number).and_then(|blocks| {
                        if blocks.len() > 1 {
                            blocks.drain(..1).next()
                        } else {
                            blocks.first().cloned()
                        }
                    })
                }
                _ => unimplemented!("can only query by number, latest or finalized"),
            },
        };
        EthGetBlock::new_provider(
            block_id,
            Box::new(move |_kind| {
                let val = val.clone().ok_or(RpcError::NullResp).map(Some);
                ProviderCall::Ready(Some(val))
            }),
        )
    }

    async fn get_logs(&self, _filter: &Filter) -> TransportResult<Vec<Log>> {
        let logs = self.logs.lock().unwrap().pop_front().map(|l| vec![l]).unwrap_or_default();
        Ok(logs)
    }

    fn get_storage_at(
        &self,
        _address: Address,
        _key: U256,
    ) -> RpcWithBlock<(Address, U256), StorageValue> {
        let storage_responses = Arc::clone(&self.storage_responses);
        let storage_read_count = Arc::clone(&self.storage_read_count);
        let storage_block_ids = Arc::clone(&self.storage_block_ids);
        let default_storage_response = self.default_storage_response;
        RpcWithBlock::new_provider(move |block_id| {
            storage_read_count.fetch_add(1, Ordering::Relaxed);
            storage_block_ids.lock().unwrap().push(block_id);
            let response = storage_responses.lock().unwrap().pop_front().unwrap_or_else(|| {
                default_storage_response.map(Ok).unwrap_or_else(|| {
                    Err(TransportErrorKind::custom_str(
                        "unscripted MockProvider::get_storage_at read (strict mode); \
                         script a response or opt in with `with_default_storage_value`",
                    ))
                })
            });
            ProviderCall::Ready(Some(response))
        })
    }

    fn get_transaction_by_hash(
        &self,
        hash: TxHash,
    ) -> ProviderCall<(TxHash,), Option<Transaction>> {
        ProviderCall::Ready(Some(Ok(self.transactions.get(&hash).cloned())))
    }
}
