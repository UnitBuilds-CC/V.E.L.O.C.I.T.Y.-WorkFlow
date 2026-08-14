//! Postgres storage adapter for the embedded engine.
//!
//! Provides a `StorageBackend` implementation backed by PostgreSQL.
//! The adapter creates and manages the following tables:
//!
//! - `velocity_workflows`: Workflow execution records
//! - `velocity_journal`: Durable step journal entries
//! - `velocity_state`: Key-value durable state
//!
//! # Schema
//! ```sql
//! CREATE TABLE IF NOT EXISTS velocity_workflows (
//!     workflow_id TEXT PRIMARY KEY,
//!     function_name TEXT NOT NULL,
//!     status TEXT NOT NULL DEFAULT 'running',
//!     input JSONB,
//!     output JSONB,
//!     error TEXT,
//!     created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
//!     updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
//! );
//!
//! CREATE TABLE IF NOT EXISTS velocity_journal (
//!     id BIGSERIAL PRIMARY KEY,
//!     workflow_id TEXT NOT NULL REFERENCES velocity_workflows(workflow_id),
//!     sequence BIGINT NOT NULL,
//!     function_name TEXT NOT NULL,
//!     input JSONB,
//!     output JSONB,
//!     error TEXT,
//!     completed BOOLEAN NOT NULL DEFAULT false,
//!     created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
//!     UNIQUE(workflow_id, sequence)
//! );
//!
//! CREATE TABLE IF NOT EXISTS velocity_state (
//!     workflow_id TEXT NOT NULL REFERENCES velocity_workflows(workflow_id),
//!     key TEXT NOT NULL,
//!     value JSONB NOT NULL,
//!     updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
//!     PRIMARY KEY (workflow_id, key)
//! );
//! ```

use crate::storage::{StorageBackend, StorageError};
use deadpool_postgres::{Config, Pool, Runtime, ManagerConfig, RecyclingMethod};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio_postgres::NoTls;
use tokio_postgres::config::Host;

// ─── Postgres Config ─────────────────────────────────────────────────────────

/// Configuration for the Postgres adapter.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresConfig {
    /// PostgreSQL connection URL
    pub url: String,
    /// Maximum number of connections in the pool
    pub max_connections: u32,
    /// Connection timeout in seconds
    pub connect_timeout_secs: u64,
    /// Schema name (default: public)
    pub schema: String,
    /// Whether to run migrations on init
    pub auto_migrate: bool,
}

impl Default for PostgresConfig {
    fn default() -> Self {
        Self {
            url: "postgres://localhost:5432/velocity".to_string(),
            max_connections: 10,
            connect_timeout_secs: 5,
            schema: "public".to_string(),
            auto_migrate: true,
        }
    }
}

// ─── SQL Schema ──────────────────────────────────────────────────────────────

/// SQL statements for schema creation.
pub const SCHEMA_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS velocity_workflows (
    workflow_id TEXT PRIMARY KEY,
    function_name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'running',
    input JSONB,
    output JSONB,
    error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS velocity_journal (
    id BIGSERIAL PRIMARY KEY,
    workflow_id TEXT NOT NULL REFERENCES velocity_workflows(workflow_id) ON DELETE CASCADE,
    sequence BIGINT NOT NULL,
    function_name TEXT NOT NULL,
    input JSONB,
    output JSONB,
    error TEXT,
    completed BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(workflow_id, sequence)
);

CREATE TABLE IF NOT EXISTS velocity_state (
    workflow_id TEXT NOT NULL REFERENCES velocity_workflows(workflow_id) ON DELETE CASCADE,
    key TEXT NOT NULL,
    value JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (workflow_id, key)
);

CREATE INDEX IF NOT EXISTS idx_velocity_workflows_status ON velocity_workflows(status);
CREATE INDEX IF NOT EXISTS idx_velocity_workflows_created ON velocity_workflows(created_at);
CREATE INDEX IF NOT EXISTS idx_velocity_journal_workflow ON velocity_journal(workflow_id);
CREATE INDEX IF NOT EXISTS idx_velocity_state_workflow ON velocity_state(workflow_id);
"#;

/// SQL statements for runtime operations.
#[allow(dead_code)]
pub mod queries {
    pub const UPSERT_WORKFLOW: &str = r#"
        INSERT INTO velocity_workflows (workflow_id, function_name, status, input, output, error, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, NOW())
        ON CONFLICT (workflow_id) DO UPDATE SET
            status = EXCLUDED.status,
            output = EXCLUDED.output,
            error = EXCLUDED.error,
            updated_at = NOW()
    "#;

    pub const LOAD_WORKFLOW: &str = r#"
        SELECT output FROM velocity_workflows WHERE workflow_id = $1
    "#;

    pub const INSERT_JOURNAL: &str = r#"
        INSERT INTO velocity_journal (workflow_id, sequence, function_name, input, output, error, completed)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (workflow_id, sequence) DO UPDATE SET
            output = EXCLUDED.output,
            error = EXCLUDED.error,
            completed = EXCLUDED.completed
    "#;

    pub const LOAD_JOURNAL: &str = r#"
        SELECT sequence, function_name, input, output, error, completed
        FROM velocity_journal
        WHERE workflow_id = $1
        ORDER BY sequence ASC
    "#;

    pub const UPSERT_STATE: &str = r#"
        INSERT INTO velocity_state (workflow_id, key, value, updated_at)
        VALUES ($1, $2, $3, NOW())
        ON CONFLICT (workflow_id, key) DO UPDATE SET
            value = EXCLUDED.value,
            updated_at = NOW()
    "#;

    pub const LOAD_STATE: &str = r#"
        SELECT value FROM velocity_state WHERE workflow_id = $1 AND key = $2
    "#;

    pub const DELETE_STATE: &str = r#"
        DELETE FROM velocity_state WHERE workflow_id = $1 AND key = $2
    "#;

    pub const LIST_WORKFLOWS: &str = r#"
        SELECT workflow_id FROM velocity_workflows ORDER BY created_at DESC
    "#;

    pub const DELETE_WORKFLOW: &str = r#"
        DELETE FROM velocity_workflows WHERE workflow_id = $1
    "#;
}

// ─── Postgres Adapter ────────────────────────────────────────────────────────

/// Postgres storage adapter.
///
/// This adapter stores all durable state in PostgreSQL. It uses a connection
/// pool (deadpool-postgres) for efficient connection management.
pub struct PostgresAdapter {
    config: PostgresConfig,
    pool: Arc<Pool>,
    /// Schema initialization status
    initialized: Arc<tokio::sync::Mutex<bool>>,
    /// Pending operations buffer (for batch writes)
    buffer: Arc<tokio::sync::Mutex<Vec<PendingOp>>>,
    /// Whether to use batch mode
    batch_mode: bool,
}

/// A pending storage operation (for batch mode).
#[derive(Debug, Clone)]
#[allow(dead_code, clippy::enum_variant_names)]
enum PendingOp {
    SaveWorkflow {
        workflow_id: String,
        function_name: String,
        output: serde_json::Value,
    },
    SaveJournal {
        workflow_id: String,
        entry: serde_json::Value,
    },
    SaveState {
        workflow_id: String,
        key: String,
        value: serde_json::Value,
    },
}

impl PostgresAdapter {
    /// Create a new Postgres adapter with the given configuration.
    pub async fn new(config: PostgresConfig) -> Result<Self, StorageError> {
        // Parse the connection URL
        let pg_config: tokio_postgres::Config = config.url.parse()
            .map_err(|e| StorageError::Connection(format!("Invalid connection URL: {}", e)))?;
        
        // Create deadpool config
        let mut deadpool_config = Config::new();
        deadpool_config.user = pg_config.get_user().map(|s| s.to_string());
        deadpool_config.password = pg_config.get_password().map(|p| String::from_utf8_lossy(p).to_string());
        deadpool_config.dbname = pg_config.get_dbname().map(|s| s.to_string());
        
        // Extract host and port from pg_config
        if let Some(host) = pg_config.get_hosts().first() {
            match host {
                Host::Tcp(h) => deadpool_config.host = Some(h.clone()),
                _ => {}
            }
        }
        if let Some(port) = pg_config.get_ports().first() {
            deadpool_config.port = Some(*port);
        }
        
        deadpool_config.manager = Some(ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        });
        
        let pool = deadpool_config.create_pool(Some(Runtime::Tokio1), NoTls)
            .map_err(|e| StorageError::Connection(format!("Failed to create connection pool: {}", e)))?;
        
        Ok(Self {
            config,
            pool: Arc::new(pool),
            initialized: Arc::new(tokio::sync::Mutex::new(false)),
            buffer: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            batch_mode: false,
        })
    }

    /// Enable batch mode for improved write performance.
    ///
    /// In batch mode, writes are buffered and flushed periodically
    /// or when the buffer reaches a threshold.
    pub fn with_batch_mode(mut self, enabled: bool) -> Self {
        self.batch_mode = enabled;
        self
    }

    /// Get the SQL schema creation statements.
    pub fn schema_sql(&self) -> &str {
        SCHEMA_SQL
    }

    /// Get the configuration.
    pub fn config(&self) -> &PostgresConfig {
        &self.config
    }

    /// Flush any buffered operations.
    pub async fn flush(&self) -> Result<(), StorageError> {
        let mut buffer = self.buffer.lock().await;
        // Execute all pending ops in a transaction
        if buffer.is_empty() {
            return Ok(());
        }
        
        let mut client = self.pool.get().await
            .map_err(|e| StorageError::Connection(format!("Failed to get connection: {}", e)))?;
        
        let transaction = client.transaction().await
            .map_err(|e| StorageError::Query(format!("Failed to start transaction: {}", e)))?;
        
        for op in buffer.iter() {
            match op {
                PendingOp::SaveWorkflow { workflow_id, function_name, output } => {
                    transaction.execute(queries::UPSERT_WORKFLOW, &[
                        &workflow_id.as_str(),
                        &function_name.as_str(),
                        &"completed",
                        &serde_json::Value::Null,
                        output,
                        &None::<String>,
                    ]).await.map_err(|e| StorageError::Query(format!("Failed to save workflow: {}", e)))?;
                }
                PendingOp::SaveJournal { workflow_id, entry } => {
                    let seq = entry.get("sequence").and_then(|v| v.as_i64()).unwrap_or(0);
                    let func_name = entry.get("function_name").and_then(|v| v.as_str()).unwrap_or("");
                    let input = entry.get("input").cloned();
                    let output = entry.get("output").cloned();
                    let error = entry.get("error").and_then(|v| v.as_str());
                    let completed = entry.get("completed").and_then(|v| v.as_bool()).unwrap_or(false);
                    
                    transaction.execute(queries::INSERT_JOURNAL, &[
                        &workflow_id.as_str(),
                        &seq,
                        &func_name,
                        &input,
                        &output,
                        &error,
                        &completed,
                    ]).await.map_err(|e| StorageError::Query(format!("Failed to save journal: {}", e)))?;
                }
                PendingOp::SaveState { workflow_id, key, value } => {
                    transaction.execute(queries::UPSERT_STATE, &[
                        &workflow_id.as_str(),
                        &key.as_str(),
                        value,
                    ]).await.map_err(|e| StorageError::Query(format!("Failed to save state: {}", e)))?;
                }
            }
        }
        
        transaction.commit().await
            .map_err(|e| StorageError::Query(format!("Failed to commit transaction: {}", e)))?;
        
        buffer.clear();
        Ok(())
    }

    /// Get the number of pending buffered operations.
    pub async fn pending_count(&self) -> usize {
        self.buffer.lock().await.len()
    }
}

impl StorageBackend for PostgresAdapter {
    fn init_schema(&self) -> Result<(), StorageError> {
        tracing::info!("init_schema called - starting schema initialization");
        
        let pool = self.pool.clone();
        let initialized = self.initialized.clone();
        
        // Use spawn_blocking to run async code on a separate thread
        let handle = tokio::runtime::Handle::current();
        std::thread::spawn(move || {
            handle.block_on(async {
                let client = pool.get().await
                    .map_err(|e| StorageError::Connection(format!("Failed to get connection: {}", e)))?;
                
                tracing::info!("Got database connection, executing schema SQL");
                
                // Execute each statement separately
                for statement in SCHEMA_SQL.split(';') {
                    let trimmed = statement.trim();
                    if !trimmed.is_empty() {
                        tracing::info!("Executing: {}", trimmed);
                        client.execute(trimmed, &[]).await
                            .map_err(|e| StorageError::Query(format!("Failed to execute schema SQL '{}': {}", trimmed, e)))?;
                    }
                }
                
                tracing::info!("Schema initialization completed successfully");
                
                let mut init = initialized.lock().await;
                *init = true;
                Ok(())
            })
        }).join().unwrap()
    }

    fn save_workflow(
        &self,
        workflow_id: &str,
        function_name: &str,
        output: &serde_json::Value,
    ) -> Result<(), StorageError> {
        if self.batch_mode {
            let mut buffer = self.buffer.blocking_lock();
            buffer.push(PendingOp::SaveWorkflow {
                workflow_id: workflow_id.to_string(),
                function_name: function_name.to_string(),
                output: output.clone(),
            });
            return Ok(());
        }

        if workflow_id.is_empty() {
            return Err(StorageError::Query(
                "workflow_id cannot be empty".to_string(),
            ));
        }

        // Execute the upsert synchronously
        let pool = self.pool.clone();
        let wf_id = workflow_id.to_string();
        let fn_name = function_name.to_string();
        let out = output.clone();
        
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let client = pool.get().await
                    .map_err(|e| StorageError::Connection(format!("Failed to get connection: {}", e)))?;
                
                client.execute(queries::UPSERT_WORKFLOW, &[
                    &wf_id.as_str(),
                    &fn_name.as_str(),
                    &"completed",
                    &serde_json::Value::Null,
                    &out,
                    &None::<String>,
                ]).await.map_err(|e| StorageError::Query(format!("Failed to save workflow: {}", e)))?;
                
                Ok(())
            })
        })
    }

    fn load_workflow(&self, workflow_id: &str) -> Result<Option<serde_json::Value>, StorageError> {
        if workflow_id.is_empty() {
            return Err(StorageError::Query(
                "workflow_id cannot be empty".to_string(),
            ));
        }

        let pool = self.pool.clone();
        let wf_id = workflow_id.to_string();
        
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let client = pool.get().await
                    .map_err(|e| StorageError::Connection(format!("Failed to get connection: {}", e)))?;
                
                let row = client.query_opt(queries::LOAD_WORKFLOW, &[&wf_id.as_str()]).await
                    .map_err(|e| StorageError::Query(format!("Failed to load workflow: {}", e)))?;
                
                Ok(row.and_then(|r| r.get::<_, Option<serde_json::Value>>("output")))
            })
        })
    }

    fn save_journal_entry(
        &self,
        workflow_id: &str,
        entry: &serde_json::Value,
    ) -> Result<(), StorageError> {
        if self.batch_mode {
            let mut buffer = self.buffer.blocking_lock();
            buffer.push(PendingOp::SaveJournal {
                workflow_id: workflow_id.to_string(),
                entry: entry.clone(),
            });
            return Ok(());
        }

        let pool = self.pool.clone();
        let wf_id = workflow_id.to_string();
        let ent = entry.clone();
        
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let client = pool.get().await
                    .map_err(|e| StorageError::Connection(format!("Failed to get connection: {}", e)))?;
                
                let seq = ent.get("sequence").and_then(|v| v.as_i64()).unwrap_or(0);
                let func_name = ent.get("function_name").and_then(|v| v.as_str()).unwrap_or("");
                let input = ent.get("input").cloned();
                let output = ent.get("output").cloned();
                let error = ent.get("error").and_then(|v| v.as_str());
                let completed = ent.get("completed").and_then(|v| v.as_bool()).unwrap_or(false);
                
                client.execute(queries::INSERT_JOURNAL, &[
                    &wf_id.as_str(),
                    &seq,
                    &func_name,
                    &input,
                    &output,
                    &error,
                    &completed,
                ]).await.map_err(|e| StorageError::Query(format!("Failed to save journal: {}", e)))?;
                
                Ok(())
            })
        })
    }

    fn load_journal(&self, workflow_id: &str) -> Result<Vec<serde_json::Value>, StorageError> {
        let pool = self.pool.clone();
        let wf_id = workflow_id.to_string();
        
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let client = pool.get().await
                    .map_err(|e| StorageError::Connection(format!("Failed to get connection: {}", e)))?;
                
                let rows = client.query(queries::LOAD_JOURNAL, &[&wf_id.as_str()]).await
                    .map_err(|e| StorageError::Query(format!("Failed to load journal: {}", e)))?;
                
                let mut entries = Vec::new();
                for row in rows {
                    let entry = serde_json::json!({
                        "sequence": row.get::<_, i64>("sequence"),
                        "function_name": row.get::<_, String>("function_name"),
                        "input": row.get::<_, Option<serde_json::Value>>("input"),
                        "output": row.get::<_, Option<serde_json::Value>>("output"),
                        "error": row.get::<_, Option<String>>("error"),
                        "completed": row.get::<_, bool>("completed"),
                    });
                    entries.push(entry);
                }
                
                Ok(entries)
            })
        })
    }

    fn save_state(
        &self,
        workflow_id: &str,
        key: &str,
        value: &serde_json::Value,
    ) -> Result<(), StorageError> {
        if self.batch_mode {
            let mut buffer = self.buffer.blocking_lock();
            buffer.push(PendingOp::SaveState {
                workflow_id: workflow_id.to_string(),
                key: key.to_string(),
                value: value.clone(),
            });
            return Ok(());
        }

        let pool = self.pool.clone();
        let wf_id = workflow_id.to_string();
        let k = key.to_string();
        let v = value.clone();
        
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let client = pool.get().await
                    .map_err(|e| StorageError::Connection(format!("Failed to get connection: {}", e)))?;
                
                client.execute(queries::UPSERT_STATE, &[
                    &wf_id.as_str(),
                    &k.as_str(),
                    &v,
                ]).await.map_err(|e| StorageError::Query(format!("Failed to save state: {}", e)))?;
                
                Ok(())
            })
        })
    }

    fn load_state(
        &self,
        workflow_id: &str,
        key: &str,
    ) -> Result<Option<serde_json::Value>, StorageError> {
        let pool = self.pool.clone();
        let wf_id = workflow_id.to_string();
        let k = key.to_string();
        
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let client = pool.get().await
                    .map_err(|e| StorageError::Connection(format!("Failed to get connection: {}", e)))?;
                
                let row = client.query_opt(queries::LOAD_STATE, &[&wf_id.as_str(), &k.as_str()]).await
                    .map_err(|e| StorageError::Query(format!("Failed to load state: {}", e)))?;
                
                Ok(row.and_then(|r| r.get::<_, Option<serde_json::Value>>("value")))
            })
        })
    }

    fn delete_state(&self, workflow_id: &str, key: &str) -> Result<bool, StorageError> {
        let pool = self.pool.clone();
        let wf_id = workflow_id.to_string();
        let k = key.to_string();
        
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let client = pool.get().await
                    .map_err(|e| StorageError::Connection(format!("Failed to get connection: {}", e)))?;
                
                let rows_affected = client.execute(queries::DELETE_STATE, &[&wf_id.as_str(), &k.as_str()]).await
                    .map_err(|e| StorageError::Query(format!("Failed to delete state: {}", e)))?;
                
                Ok(rows_affected > 0)
            })
        })
    }

    fn list_workflows(&self) -> Result<Vec<String>, StorageError> {
        let pool = self.pool.clone();
        
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let client = pool.get().await
                    .map_err(|e| StorageError::Connection(format!("Failed to get connection: {}", e)))?;
                
                let rows = client.query(queries::LIST_WORKFLOWS, &[]).await
                    .map_err(|e| StorageError::Query(format!("Failed to list workflows: {}", e)))?;
                
                let workflow_ids: Vec<String> = rows.iter()
                    .map(|r| r.get::<_, String>("workflow_id"))
                    .collect();
                
                Ok(workflow_ids)
            })
        })
    }

    fn delete_workflow(&self, workflow_id: &str) -> Result<(), StorageError> {
        let pool = self.pool.clone();
        let wf_id = workflow_id.to_string();
        
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let client = pool.get().await
                    .map_err(|e| StorageError::Connection(format!("Failed to get connection: {}", e)))?;
                
                client.execute(queries::DELETE_WORKFLOW, &[&wf_id.as_str()]).await
                    .map_err(|e| StorageError::Query(format!("Failed to delete workflow: {}", e)))?;
                
                Ok(())
            })
        })
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> PostgresConfig {
        PostgresConfig {
            url: "postgres://test:test@localhost:5432/velocity_test".to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn test_adapter_creation() {
        let adapter = PostgresAdapter::new(test_config());
        assert_eq!(
            adapter.config().url,
            "postgres://test:test@localhost:5432/velocity_test"
        );
        assert_eq!(adapter.pending_count(), 0);
    }

    #[test]
    fn test_schema_sql() {
        let adapter = PostgresAdapter::new(test_config());
        let sql = adapter.schema_sql();
        assert!(sql.contains("velocity_workflows"));
        assert!(sql.contains("velocity_journal"));
        assert!(sql.contains("velocity_state"));
    }

    #[test]
    fn test_init_schema() {
        let adapter = PostgresAdapter::new(test_config());
        assert!(adapter.init_schema().is_ok());
    }

    #[test]
    fn test_save_workflow_validation() {
        let adapter = PostgresAdapter::new(test_config());
        let result = adapter.save_workflow("", "fn", &serde_json::json!("out"));
        assert!(result.is_err());
    }

    #[test]
    fn test_save_workflow_ok() {
        let adapter = PostgresAdapter::new(test_config());
        let result = adapter.save_workflow("wf-1", "fn", &serde_json::json!("out"));
        assert!(result.is_ok());
    }

    #[test]
    fn test_batch_mode() {
        let adapter = PostgresAdapter::new(test_config()).with_batch_mode(true);

        // These should be buffered, not executed
        adapter
            .save_workflow("wf-1", "fn", &serde_json::json!("out"))
            .unwrap();
        adapter
            .save_journal_entry("wf-1", &serde_json::json!({"seq": 0}))
            .unwrap();
        adapter
            .save_state("wf-1", "key", &serde_json::json!("val"))
            .unwrap();

        assert_eq!(adapter.pending_count(), 3);

        // Flush should clear the buffer
        adapter.flush().unwrap();
        assert_eq!(adapter.pending_count(), 0);
    }

    #[test]
    fn test_config_defaults() {
        let config = PostgresConfig::default();
        assert_eq!(config.max_connections, 10);
        assert_eq!(config.connect_timeout_secs, 5);
        assert_eq!(config.schema, "public");
        assert!(config.auto_migrate);
    }

    #[test]
    fn test_query_constants() {
        assert!(queries::UPSERT_WORKFLOW.contains("velocity_workflows"));
        assert!(queries::LOAD_WORKFLOW.contains("workflow_id"));
        assert!(queries::INSERT_JOURNAL.contains("velocity_journal"));
        assert!(queries::LOAD_JOURNAL.contains("ORDER BY sequence"));
        assert!(queries::UPSERT_STATE.contains("ON CONFLICT"));
        assert!(queries::DELETE_WORKFLOW.contains("DELETE FROM"));
    }

    #[test]
    fn test_schema_has_indexes() {
        let sql = SCHEMA_SQL;
        assert!(sql.contains("CREATE INDEX"));
        assert!(sql.contains("idx_velocity_workflows_status"));
        assert!(sql.contains("idx_velocity_journal_workflow"));
    }
}
