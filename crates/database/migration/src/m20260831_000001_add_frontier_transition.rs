use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(FrontierTransition::Table)
                    .if_not_exists()
                    .col(integer(FrontierTransition::Id).primary_key())
                    .col(string(FrontierTransition::Kind))
                    .col(big_unsigned(FrontierTransition::ExpectedHeadNumber))
                    .col(binary_len(FrontierTransition::ExpectedHeadHash, 32))
                    .col(big_unsigned(FrontierTransition::ExpectedSafeNumber))
                    .col(binary_len(FrontierTransition::ExpectedSafeHash, 32))
                    .col(big_unsigned(FrontierTransition::ExpectedFinalizedNumber))
                    .col(binary_len(FrontierTransition::ExpectedFinalizedHash, 32))
                    .col(big_unsigned(FrontierTransition::TargetHeadNumber))
                    .col(binary_len(FrontierTransition::TargetHeadHash, 32))
                    .col(big_unsigned(FrontierTransition::TargetSafeNumber))
                    .col(binary_len(FrontierTransition::TargetSafeHash, 32))
                    .col(big_unsigned(FrontierTransition::TargetFinalizedNumber))
                    .col(binary_len(FrontierTransition::TargetFinalizedHash, 32))
                    .col(binary_len_null(FrontierTransition::BatchHash, 32))
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager.drop_table(Table::drop().table(FrontierTransition::Table).to_owned()).await
    }
}

#[derive(DeriveIden)]
enum FrontierTransition {
    Table,
    Id,
    Kind,
    ExpectedHeadNumber,
    ExpectedHeadHash,
    ExpectedSafeNumber,
    ExpectedSafeHash,
    ExpectedFinalizedNumber,
    ExpectedFinalizedHash,
    TargetHeadNumber,
    TargetHeadHash,
    TargetSafeNumber,
    TargetSafeHash,
    TargetFinalizedNumber,
    TargetFinalizedHash,
    BatchHash,
}
