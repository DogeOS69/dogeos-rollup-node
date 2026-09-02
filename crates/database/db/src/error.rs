use super::L1MessageKey;
use alloy_primitives::B256;
use sea_orm::sqlx::Error as SqlxError;

/// The error type for database operations.
#[derive(Debug, thiserror::Error)]
pub enum DatabaseError {
    /// A database error occurred.
    #[error("database error: {0}")]
    DatabaseError(#[from] sea_orm::DbErr),
    /// An error occurred at the sqlx level.
    #[error("A sqlx error occurred: {0}")]
    SqlxError(#[from] SqlxError),
    /// A generic error occurred.
    #[error("parse signature error: {0}")]
    ParseSignatureError(String),
    /// Failed to serde the metadata value.
    #[error("failed to serde metadata value: {0}")]
    MetadataSerdeError(#[from] serde_json::Error),
    /// The L1 message was not found in database.
    #[error("L1 message at key [{0}] not found in database")]
    L1MessageNotFound(L1MessageKey),
    /// The configured chain's genesis does not match a populated database.
    #[error(
        "configured chain genesis {configured} does not match the existing database genesis {stored}; is the database path pointed at another chain's data?"
    )]
    GenesisMismatch {
        /// The genesis hash the node was configured with.
        configured: B256,
        /// The genesis hash already recorded in the database.
        stored: B256,
    },
    /// A populated database carries no genesis (height-0) row.
    #[error(
        "database has an L2 head above genesis but no block 0 row; the database is truncated or \
         corrupt and cannot be reconciled against configured genesis {configured}"
    )]
    GenesisMissing {
        /// The genesis hash the node was configured with.
        configured: B256,
    },
    /// Failed to commit the transaction to database.
    #[error("TXMut commit failed")]
    CommitFailed,
    /// Failed to rollback the transaction.
    #[error("TXMut rollback failed")]
    RollbackFailed,
}
