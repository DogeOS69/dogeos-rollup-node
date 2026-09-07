use super::L1MessageKey;
use alloy_primitives::{Bytes, B256};
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
    /// A height-0 row was found that is neither the configured chain's genesis nor the genesis
    /// the static migration seeds — the database belongs to another chain.
    ///
    /// Raised on fresh and populated databases alike: the check deliberately runs before
    /// the fresh/populated split, because that split reads a metadata counter an unwind can
    /// drive to zero while another chain's rows remain.
    #[error(
        "configured chain genesis {configured} does not match the existing database genesis {stored}; is the database path pointed at another chain's data?"
    )]
    GenesisMismatch {
        /// The genesis hash the node was configured with.
        configured: B256,
        /// The block-0 hash already recorded in the database, as the raw stored bytes rather
        /// than a parsed hash: a corrupt row can hold other than 32 of them, and the diagnostic
        /// must show what is actually there instead of a zero hash standing in for it.
        stored: Bytes,
    },
    /// A populated database carries no genesis (height-0) row.
    #[error(
        "database has an L2 head or L2 block rows above genesis but no block 0 row; the database is \
         truncated or corrupt and cannot be reconciled against configured genesis {configured}"
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
