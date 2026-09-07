//! Test utilities for the database crate.

use crate::DatabaseConnectionProvider;

use super::Database;
use alloy_primitives::B256;
use scroll_migration::{MigrationInfo, Migrator, MigratorTrait, ScrollDevMigrationInfo};

/// Instantiates a new in-memory database and runs the migrations
/// to set up the schema.
pub async fn setup_test_db() -> Database {
    let dir = tempfile::Builder::new()
        .prefix("scroll-test-")
        .rand_bytes(8)
        .tempdir()
        .expect("failed to create temp dir");
    let db = Database::test(dir).await.unwrap();
    Migrator::<ScrollDevMigrationInfo>::up(db.inner().get_connection(), None).await.unwrap();
    db
}

/// The genesis hash the migration behind [`setup_test_db`] seeds at height 0.
///
/// This is upstream Scroll's dev genesis and is NOT the genesis of any chain spec shipped here —
/// the same divergence a real node reconciles at startup.
pub fn seeded_test_genesis() -> B256 {
    ScrollDevMigrationInfo::genesis_hash()
}
