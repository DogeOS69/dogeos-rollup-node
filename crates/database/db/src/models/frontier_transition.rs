use crate::{FrontierTransitionKind, PendingFrontierTransition, StoredForkchoiceState};
use alloy_primitives::B256;
use rollup_node_primitives::BlockInfo;
use sea_orm::{entity::prelude::*, ActiveValue};

const SINGLETON_ID: i32 = 1;

/// The single durable Engine forkchoice transition owned by the chain orchestrator.
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "frontier_transition")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    id: i32,
    kind: String,
    expected_head_number: i64,
    expected_head_hash: Vec<u8>,
    expected_safe_number: i64,
    expected_safe_hash: Vec<u8>,
    expected_finalized_number: i64,
    expected_finalized_hash: Vec<u8>,
    target_head_number: i64,
    target_head_hash: Vec<u8>,
    target_safe_number: i64,
    target_safe_hash: Vec<u8>,
    target_finalized_number: i64,
    target_finalized_hash: Vec<u8>,
    batch_hash: Option<Vec<u8>>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}

fn number(value: u64) -> i64 {
    value.try_into().expect("block number should fit in i64")
}

fn block(number: i64, hash: &[u8]) -> BlockInfo {
    BlockInfo { number: number as u64, hash: B256::from_slice(hash) }
}

impl From<PendingFrontierTransition> for ActiveModel {
    fn from(value: PendingFrontierTransition) -> Self {
        Self {
            id: ActiveValue::Set(SINGLETON_ID),
            kind: ActiveValue::Set(value.kind.as_str().to_owned()),
            expected_head_number: ActiveValue::Set(number(value.expected.head.number)),
            expected_head_hash: ActiveValue::Set(value.expected.head.hash.to_vec()),
            expected_safe_number: ActiveValue::Set(number(value.expected.safe.number)),
            expected_safe_hash: ActiveValue::Set(value.expected.safe.hash.to_vec()),
            expected_finalized_number: ActiveValue::Set(number(value.expected.finalized.number)),
            expected_finalized_hash: ActiveValue::Set(value.expected.finalized.hash.to_vec()),
            target_head_number: ActiveValue::Set(number(value.target.head.number)),
            target_head_hash: ActiveValue::Set(value.target.head.hash.to_vec()),
            target_safe_number: ActiveValue::Set(number(value.target.safe.number)),
            target_safe_hash: ActiveValue::Set(value.target.safe.hash.to_vec()),
            target_finalized_number: ActiveValue::Set(number(value.target.finalized.number)),
            target_finalized_hash: ActiveValue::Set(value.target.finalized.hash.to_vec()),
            batch_hash: ActiveValue::Set(value.batch_hash.map(|hash| hash.to_vec())),
        }
    }
}

impl TryFrom<Model> for PendingFrontierTransition {
    type Error = crate::DatabaseError;

    fn try_from(value: Model) -> Result<Self, Self::Error> {
        Ok(Self {
            kind: FrontierTransitionKind::try_from(value.kind.as_str())?,
            expected: StoredForkchoiceState {
                head: block(value.expected_head_number, &value.expected_head_hash),
                safe: block(value.expected_safe_number, &value.expected_safe_hash),
                finalized: block(value.expected_finalized_number, &value.expected_finalized_hash),
            },
            target: StoredForkchoiceState {
                head: block(value.target_head_number, &value.target_head_hash),
                safe: block(value.target_safe_number, &value.target_safe_hash),
                finalized: block(value.target_finalized_number, &value.target_finalized_hash),
            },
            batch_hash: value.batch_hash.map(|hash| B256::from_slice(&hash)),
        })
    }
}
