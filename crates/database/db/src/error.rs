use super::L1MessageKey;
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
    /// A frontier transition row contains an unknown kind.
    #[error("invalid frontier transition kind: {0}")]
    InvalidFrontierTransitionKind(String),
    /// The database has no non-reverted batch-indexed L2 block to use as its safe frontier.
    #[error("database contains no safe L2 frontier")]
    MissingSafeL2Frontier,
    /// The L1 message was not found in database.
    #[error("L1 message at key [{0}] not found in database")]
    L1MessageNotFound(L1MessageKey),
    /// Failed to commit the transaction to database.
    #[error("TXMut commit failed")]
    CommitFailed,
    /// Failed to rollback the transaction.
    #[error("TXMut rollback failed")]
    RollbackFailed,
}
