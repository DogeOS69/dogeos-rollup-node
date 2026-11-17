//! Adds the `legacy_queue_hash` column to `l1_message`.
//!
//! Testnet-only: `Chikyu` batches below codec 12 name their L1 messages by Scroll's masked queue
//! hash; codec 12 (`TsukiV2`) and later use the full-width hash in `queue_hash`. This column holds
//! the masked chain and stays empty on a chain whose `TsukiV2` activates at genesis (mainnet). The
//! node recomputes both columns at startup (`recompute_l1_message_queue_hashes`), because a
//! static migration cannot see the chain spec. Added by dogeos-rollup-node #107. Remove with the
//! legacy rule when no supported network has batches below codec 12.

use super::m20250304_125946_add_l1_msg_table::L1Message;
use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // SQLite DDL can persist before SeaORM records this migration as applied.
        if !manager
            .has_column(L1Message::Table.to_string(), LegacyQueueHash::LegacyQueueHash.to_string())
            .await?
        {
            // An old/interrupted downgrade may have left a marker for the dropped chain.
            // Invalidate before ADD: a crash must never pair it with a newly empty column.
            invalidate_queue_hash_rule(manager).await?;
            manager
                .alter_table(
                    Table::alter()
                        .table(L1Message::Table)
                        .add_column(binary_len_null(LegacyQueueHash::LegacyQueueHash, 32))
                        .to_owned(),
                )
                .await?;
        }
        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name(LEGACY_QUEUE_HASH_INDEX)
                    .table(L1Message::Table)
                    .col(LegacyQueueHash::LegacyQueueHash)
                    .unique()
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Forget completion before any destructive DDL, including on a resumed downgrade.
        invalidate_queue_hash_rule(manager).await?;
        if manager.has_index(L1Message::Table.to_string(), LEGACY_QUEUE_HASH_INDEX).await? {
            manager
                .drop_index(
                    Index::drop().name(LEGACY_QUEUE_HASH_INDEX).table(L1Message::Table).to_owned(),
                )
                .await?;
        }
        if manager
            .has_column(L1Message::Table.to_string(), LegacyQueueHash::LegacyQueueHash.to_string())
            .await?
        {
            manager
                .alter_table(
                    Table::alter()
                        .table(L1Message::Table)
                        .drop_column(LegacyQueueHash::LegacyQueueHash)
                        .to_owned(),
                )
                .await?;
        }
        Ok(())
    }
}

const LEGACY_QUEUE_HASH_INDEX: &str = "idx_l1_message_legacy_queue_hash";

async fn invalidate_queue_hash_rule(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .exec_stmt(
            Query::delete()
                .from_table(Alias::new("metadata"))
                .and_where(Expr::col(Alias::new("key")).eq("l1_message_queue_hash_rule"))
                .to_owned(),
        )
        .await
}

#[derive(DeriveIden)]
enum LegacyQueueHash {
    LegacyQueueHash,
}
