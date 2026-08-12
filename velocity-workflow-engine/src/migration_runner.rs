//! Database migration runner for VELOCITY-WorkFlow.
//!
//! Provides versioned schema migration management with support for applying
//! and rolling back migrations. SQL files are embedded at compile time via
//! `include_str!`.
//!
//! # Architecture
//!
//! The [`MigrationRunner`] operates on any adapter implementing [`MigrationAdapter`],
//! which extends [`DatabaseAdapter`] with migration-specific operations (execute SQL,
//! check version, record/rollback migrations). The [`InMemoryAdapter`] provides a
//! built-in implementation for testing.
//!
//! # Example
//!
//! ```rust
//! use velocity_workflow_engine::db_adapter::InMemoryAdapter;
//! use velocity_workflow_engine::migration_runner::MigrationRunner;
//!
//! let adapter = InMemoryAdapter::new();
//! let mut runner = MigrationRunner::new(Box::new(adapter));
//! let result = runner.run_all().unwrap();
//! assert_eq!(result.versions_applied.len(), 6);
//! ```

use std::time::Instant;

use crate::db_adapter::{DatabaseAdapter, DatabaseError, DatabaseResult, InMemoryAdapter};

// ─── Embedded Migration SQL ──────────────────────────────────────────────────

const MIGRATION_001_UP: &str = include_str!("../../migrations/001_initial_schema.sql");
const MIGRATION_002_UP: &str = include_str!("../../migrations/002_add_workflow_metadata.sql");
const MIGRATION_003_UP: &str = include_str!("../../migrations/003_add_audit_tables.sql");
const MIGRATION_004_UP: &str = include_str!("../../migrations/004_add_scheduling_tables.sql");
const MIGRATION_005_UP: &str = include_str!("../../migrations/005_add_multi_region_tables.sql");
const MIGRATION_006_UP: &str = include_str!("../../migrations/006_add_search_attribute_schema.sql");

const MIGRATION_001_DOWN: &str = include_str!("../../migrations/rollback/001_rollback.sql");
const MIGRATION_002_DOWN: &str = include_str!("../../migrations/rollback/002_rollback.sql");
const MIGRATION_003_DOWN: &str = include_str!("../../migrations/rollback/003_rollback.sql");
const MIGRATION_004_DOWN: &str = include_str!("../../migrations/rollback/004_rollback.sql");
const MIGRATION_005_DOWN: &str = include_str!("../../migrations/rollback/005_rollback.sql");
const MIGRATION_006_DOWN: &str = include_str!("../../migrations/rollback/006_rollback.sql");

// ─── Migration Types ─────────────────────────────────────────────────────────

/// A single database migration with up and down SQL.
#[derive(Debug, Clone)]
pub struct Migration {
    /// The version number (1-based, sequential).
    pub version: u32,
    /// Human-readable migration name.
    pub name: &'static str,
    /// SQL to apply the migration.
    pub up_sql: &'static str,
    /// SQL to roll back the migration.
    pub down_sql: &'static str,
}

/// Status of a single migration (applied or pending).
#[derive(Debug, Clone)]
pub struct MigrationStatus {
    pub version: u32,
    pub name: &'static str,
    pub applied: bool,
    pub applied_at_ms: Option<u64>,
}

/// Result of running all pending migrations.
#[derive(Debug, Clone)]
pub struct MigrationResult {
    /// Versions that were applied during this run.
    pub versions_applied: Vec<u32>,
    /// Total wall-clock duration in milliseconds.
    pub total_duration_ms: u64,
}

/// Errors specific to the migration runner.
#[derive(Debug, Clone)]
pub enum MigrationError {
    /// A database error occurred.
    Database(DatabaseError),
    /// Attempted to roll back to a version higher than current.
    InvalidTargetVersion { current: u32, target: u32 },
    /// A migration SQL execution failed.
    SqlError { version: u32, message: String },
}

impl std::fmt::Display for MigrationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Database(e) => write!(f, "database error: {}", e),
            Self::InvalidTargetVersion { current, target } => {
                write!(f, "invalid target version {} (current: {})", target, current)
            }
            Self::SqlError { version, message } => {
                write!(f, "migration {} failed: {}", version, message)
            }
        }
    }
}

impl std::error::Error for MigrationError {}

impl From<DatabaseError> for MigrationError {
    fn from(e: DatabaseError) -> Self {
        Self::Database(e)
    }
}

// ─── Migration Adapter Trait ─────────────────────────────────────────────────

/// Extension trait for adapters that support migration operations.
///
/// Adds SQL execution and migration tracking on top of [`DatabaseAdapter`].
pub trait MigrationAdapter: DatabaseAdapter {
    /// Execute arbitrary SQL (e.g., a migration script).
    fn execute_sql(&self, sql: &str) -> DatabaseResult<()>;

    /// Get the current schema version (0 if no migrations applied).
    fn get_schema_version(&self) -> DatabaseResult<u32>;

    /// Record a migration as applied.
    fn record_migration(&self, version: u32, name: &str, duration_ms: u64) -> DatabaseResult<()>;

    /// Remove a migration record (for rollback).
    fn remove_migration_record(&self, version: u32) -> DatabaseResult<()>;
}

// ─── InMemoryAdapter Migration Support ───────────────────────────────────────

impl MigrationAdapter for InMemoryAdapter {
    fn execute_sql(&self, _sql: &str) -> DatabaseResult<()> {
        // In-memory adapter accepts all SQL without executing it.
        // This simulates successful migration application.
        Ok(())
    }

    fn get_schema_version(&self) -> DatabaseResult<u32> {
        Ok(self.migration_version())
    }

    fn record_migration(&self, version: u32, _name: &str, _duration_ms: u64) -> DatabaseResult<()> {
        self.set_migration_version(version);
        Ok(())
    }

    fn remove_migration_record(&self, version: u32) -> DatabaseResult<()> {
        // Roll back version to version - 1
        if version > 0 {
            self.set_migration_version(version - 1);
        }
        Ok(())
    }
}

// ─── Migration Registry ──────────────────────────────────────────────────────

/// Returns all known migrations in version order.
fn all_migrations() -> Vec<Migration> {
    vec![
        Migration {
            version: 1,
            name: "initial_schema",
            up_sql: MIGRATION_001_UP,
            down_sql: MIGRATION_001_DOWN,
        },
        Migration {
            version: 2,
            name: "add_workflow_metadata",
            up_sql: MIGRATION_002_UP,
            down_sql: MIGRATION_002_DOWN,
        },
        Migration {
            version: 3,
            name: "add_audit_tables",
            up_sql: MIGRATION_003_UP,
            down_sql: MIGRATION_003_DOWN,
        },
        Migration {
            version: 4,
            name: "add_scheduling_tables",
            up_sql: MIGRATION_004_UP,
            down_sql: MIGRATION_004_DOWN,
        },
        Migration {
            version: 5,
            name: "add_multi_region_tables",
            up_sql: MIGRATION_005_UP,
            down_sql: MIGRATION_005_DOWN,
        },
        Migration {
            version: 6,
            name: "add_search_attribute_schema",
            up_sql: MIGRATION_006_UP,
            down_sql: MIGRATION_006_DOWN,
        },
    ]
}

// ─── Migration Runner ────────────────────────────────────────────────────────

/// Manages versioned database migrations.
///
/// The runner checks which migrations are pending, applies them in order,
/// and supports rollback to a previous version.
pub struct MigrationRunner {
    adapter: Box<dyn MigrationAdapter>,
}

impl MigrationRunner {
    /// Create a new runner with the given migration-capable adapter.
    pub fn new(adapter: Box<dyn MigrationAdapter>) -> Self {
        Self { adapter }
    }

    /// Get a reference to the underlying database adapter.
    pub fn adapter(&self) -> &dyn DatabaseAdapter {
        self.adapter.as_ref()
    }

    /// Get the current schema version.
    pub fn current_version(&self) -> u32 {
        self.adapter.get_schema_version().unwrap_or(0)
    }

    /// Return the list of migrations that have not yet been applied.
    pub fn pending_migrations(&self) -> Vec<Migration> {
        let current = self.current_version();
        all_migrations()
            .into_iter()
            .filter(|m| m.version > current)
            .collect()
    }

    /// Apply all pending migrations in order.
    pub fn run_all(&mut self) -> Result<MigrationResult, MigrationError> {
        let start = Instant::now();
        let pending = self.pending_migrations();
        let mut versions_applied = Vec::new();

        for migration in &pending {
            let m_start = Instant::now();

            self.adapter
                .execute_sql(migration.up_sql)
                .map_err(|e| MigrationError::SqlError {
                    version: migration.version,
                    message: e.to_string(),
                })?;

            let duration_ms = m_start.elapsed().as_millis() as u64;

            self.adapter
                .record_migration(migration.version, migration.name, duration_ms)
                .map_err(MigrationError::Database)?;

            versions_applied.push(migration.version);
        }

        let total_duration_ms = start.elapsed().as_millis() as u64;

        Ok(MigrationResult {
            versions_applied,
            total_duration_ms,
        })
    }

    /// Roll back the last applied migration.
    pub fn rollback_last(&mut self) -> Result<(), MigrationError> {
        let current = self.current_version();
        if current == 0 {
            return Ok(());
        }
        self.rollback_to(current - 1)
    }

    /// Roll back migrations until the schema is at the given target version.
    pub fn rollback_to(&mut self, target: u32) -> Result<(), MigrationError> {
        let current = self.current_version();
        if target > current {
            return Err(MigrationError::InvalidTargetVersion { current, target });
        }
        if target == current {
            return Ok(());
        }

        let migrations = all_migrations();
        // Roll back in reverse order: current, current-1, ..., target+1
        for version in (target + 1..=current).rev() {
            let migration = migrations
                .iter()
                .find(|m| m.version == version)
                .expect("migration must exist for version");

            self.adapter
                .execute_sql(migration.down_sql)
                .map_err(|e| MigrationError::SqlError {
                    version: migration.version,
                    message: e.to_string(),
                })?;

            self.adapter
                .remove_migration_record(version)
                .map_err(MigrationError::Database)?;
        }

        Ok(())
    }

    /// Show the status of all known migrations.
    pub fn status(&self) -> Vec<MigrationStatus> {
        let current = self.current_version();
        all_migrations()
            .into_iter()
            .map(|m| MigrationStatus {
                version: m.version,
                name: m.name,
                applied: m.version <= current,
                applied_at_ms: if m.version <= current { Some(0) } else { None },
            })
            .collect()
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_runner() -> MigrationRunner {
        MigrationRunner::new(Box::new(InMemoryAdapter::new()))
    }

    #[test]
    fn test_initial_version_is_zero() {
        let runner = make_runner();
        assert_eq!(runner.current_version(), 0);
    }

    #[test]
    fn test_pending_migrations_returns_all_when_empty() {
        let runner = make_runner();
        let pending = runner.pending_migrations();
        assert_eq!(pending.len(), 6);
        assert_eq!(pending[0].version, 1);
        assert_eq!(pending[5].version, 6);
    }

    #[test]
    fn test_run_all_applies_all_migrations() {
        let mut runner = make_runner();
        let result = runner.run_all().unwrap();
        assert_eq!(result.versions_applied, vec![1, 2, 3, 4, 5, 6]);
        assert_eq!(runner.current_version(), 6);
    }

    #[test]
    fn test_run_all_idempotent() {
        let mut runner = make_runner();
        let r1 = runner.run_all().unwrap();
        assert_eq!(r1.versions_applied.len(), 6);

        let r2 = runner.run_all().unwrap();
        assert!(r2.versions_applied.is_empty());
        assert_eq!(runner.current_version(), 6);
    }

    #[test]
    fn test_rollback_last() {
        let mut runner = make_runner();
        runner.run_all().unwrap();
        assert_eq!(runner.current_version(), 6);

        runner.rollback_last().unwrap();
        assert_eq!(runner.current_version(), 5);
    }

    #[test]
    fn test_rollback_to_specific_version() {
        let mut runner = make_runner();
        runner.run_all().unwrap();

        runner.rollback_to(3).unwrap();
        assert_eq!(runner.current_version(), 3);

        // Pending should be 4, 5, 6
        let pending = runner.pending_migrations();
        assert_eq!(pending.len(), 3);
        assert_eq!(pending[0].version, 4);
    }

    #[test]
    fn test_rollback_to_same_version_is_noop() {
        let mut runner = make_runner();
        runner.run_all().unwrap();
        runner.rollback_to(6).unwrap();
        assert_eq!(runner.current_version(), 6);
    }

    #[test]
    fn test_rollback_to_higher_version_errors() {
        let mut runner = make_runner();
        runner.run_all().unwrap();
        let err = runner.rollback_to(10).unwrap_err();
        assert!(matches!(err, MigrationError::InvalidTargetVersion { .. }));
    }

    #[test]
    fn test_status_shows_applied_and_pending() {
        let mut runner = make_runner();
        runner.run_all().unwrap();
        runner.rollback_to(3).unwrap();

        let status = runner.status();
        assert_eq!(status.len(), 6);
        assert!(status[0].applied);  // v1
        assert!(status[1].applied);  // v2
        assert!(status[2].applied);  // v3
        assert!(!status[3].applied); // v4
        assert!(!status[4].applied); // v5
        assert!(!status[5].applied); // v6
    }

    #[test]
    fn test_migration_sql_is_embedded() {
        let migrations = all_migrations();
        for m in &migrations {
            assert!(!m.up_sql.is_empty(), "migration {} up_sql is empty", m.version);
            assert!(!m.down_sql.is_empty(), "migration {} down_sql is empty", m.version);
            assert!(m.up_sql.contains("BEGIN;"), "migration {} up_sql missing BEGIN", m.version);
            assert!(m.up_sql.contains("COMMIT;"), "migration {} up_sql missing COMMIT", m.version);
            assert!(m.down_sql.contains("BEGIN;"), "migration {} down_sql missing BEGIN", m.version);
        }
    }

    #[test]
    fn test_reapply_after_rollback() {
        let mut runner = make_runner();
        runner.run_all().unwrap();
        assert_eq!(runner.current_version(), 6);

        runner.rollback_to(2).unwrap();
        assert_eq!(runner.current_version(), 2);

        let result = runner.run_all().unwrap();
        assert_eq!(result.versions_applied, vec![3, 4, 5, 6]);
        assert_eq!(runner.current_version(), 6);
    }
}
