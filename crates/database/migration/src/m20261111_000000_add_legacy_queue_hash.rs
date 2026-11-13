//! Adds the `legacy_queue_hash` column to `l1_message`.
//!
//! Testnet-only: `Chikyu` batches below codec 12 name their L1 messages by Scroll's masked queue
//! hash; codec 12 (`TsukiV2`) and later use the full-width hash in `queue_hash`. This column holds
//! the masked chain and stays empty on a chain whose `TsukiV2` activates at genesis (mainnet). The
//! node recomputes both columns at startup (`recompute_l1_message_queue_hashes`), because a
//! static migration cannot see the chain spec. Remove with the legacy rule when no supported
//! network has batches below codec 12.

use super::m20250304_125946_add_l1_msg_table::L1Message;
use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(L1Message::Table)
                    .add_column(binary_len_null(LegacyQueueHash::LegacyQueueHash, 32))
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .name("idx_l1_message_legacy_queue_hash")
                    .table(L1Message::Table)
                    .col(LegacyQueueHash::LegacyQueueHash)
                    .unique()
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_index(
                Index::drop()
                    .name("idx_l1_message_legacy_queue_hash")
                    .table(L1Message::Table)
                    .to_owned(),
            )
            .await?;
        manager
            .alter_table(
                Table::alter()
                    .table(L1Message::Table)
                    .drop_column(LegacyQueueHash::LegacyQueueHash)
                    .to_owned(),
            )
            .await
    }
}

#[derive(DeriveIden)]
enum LegacyQueueHash {
    LegacyQueueHash,
}
