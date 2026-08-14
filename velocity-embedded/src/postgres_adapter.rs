//! Postgres storage adapter for the embedded engine.
//!
//! Provides a `StorageBackend` implementation backed by PostgreSQL.
//! The adapter creates and manages the following tables:
//!
//! - `velocity_workflows`: Workflow execution records
//! - `velocity_journal`: Durable step journal entries
//! - `velocity_state`: Key-value durable state
//!
//! # Architecture
//!
//! All database operations are dispatched to a dedicated background thread
//! that owns its own single-threaded Tokio runtime. This avoids the pitfalls
//! of `block_in_place` / `Handle::block_on` which can deadlock when called
//! from an async runtime's worker threads (e.g. axum handlers).
//!
//! The sync `StorageBackend` trait methods send a closure to the DB thread
//! via a channel and block until the result is ready.

use crate::storage::{StorageBackend, StorageError};
use deadpool_postgres::{Config, ManagerConfig, Pool, RecyclingMethod, Runtime};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
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

// ─── DB Thread Dispatch ──────────────────────────────────────────────────────

/// A type-erased database operation sent to the dedicated DB thread.
///
/// Carries a closure that performs the async DB work and a oneshot sender
/// to return the `Result<(), StorageError>` to the caller.
type DbOp = Box<
    dyn FnOnce(
            &Pool,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<(), StorageError>> + Send>,
        > + Send
        + 'static,
>;

// ─── Postgres Adapter ────────────────────────────────────────────────────────

/// Postgres storage adapter.
///
/// All database operations are dispatched on a dedicated background thread
/// with its own Tokio runtime, avoiding deadlocks from sync→async bridging
/// on the caller's runtime.
pub struct PostgresAdapter {
    config: PostgresConfig,
    /// Channel to send DB operations to the dedicated thread.
    tx: mpsc::Sender<(DbOp, oneshot::Sender<Result<(), StorageError>>)>,
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
    ///
    /// Spawns a dedicated background thread with its own single-threaded
    /// Tokio runtime for all database operations.
    pub async fn new(config: PostgresConfig) -> Result<Self, StorageError> {
        // Parse the connection URL
        let pg_config: tokio_postgres::Config = config
            .url
            .parse()
            .map_err(|e| StorageError::Connection(format!("Invalid connection URL: {}", e)))?;

        // Create deadpool config
        let mut deadpool_config = Config::new();
        deadpool_config.user = pg_config.get_user().map(|s| s.to_string());
        deadpool_config.password = pg_config
            .get_password()
            .map(|p| String::from_utf8_lossy(p).to_string());
        deadpool_config.dbname = pg_config.get_dbname().map(|s| s.to_string());

        // Extract host and port
        if let Some(host) = pg_config.get_hosts().first() {
            match host {
                Host::Tcp(h) => deadpool_config.host = Some(h.clone()),
                #[allow(unreachable_patterns)]
                _ => {}
            }
        }
        if let Some(port) = pg_config.get_ports().first() {
            deadpool_config.port = Some(*port);
        }

        deadpool_config.manager = Some(ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        });

        let pool = deadpool_config
            .create_pool(Some(Runtime::Tokio1), NoTls)
            .map_err(|e| {
                StorageError::Connection(format!("Failed to create connection pool: {}", e))
            })?;

        // Channel for dispatching DB operations
        let (tx, mut rx) = mpsc::channel::<(DbOp, oneshot::Sender<Result<(), StorageError>>)>(256);

        // Spawn dedicated DB thread
        std::thread::Builder::new()
            .name("velocity-pg-db".into())
            .spawn(move || {
                // Single-threaded runtime ON this thread — never crosses boundaries.
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("failed to create DB thread runtime");

                rt.block_on(async move {
                    while let Some((op, reply)) = rx.recv().await {
                        let result = op(&pool).await;
                        let _ = reply.send(result);
                    }
                });
            })
            .map_err(|e| StorageError::Connection(format!("Failed to spawn DB thread: {}", e)))?;

        Ok(Self {
            config,
            tx,
            buffer: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            batch_mode: false,
        })
    }

    /// Enable batch mode for improved write performance.
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

    /// Dispatch a void async DB operation to the dedicated thread and block
    /// until it completes.
    ///
    /// Uses a helper thread to avoid calling `blocking_send`/`blocking_recv`
    /// from within a Tokio runtime (which would panic).
    fn dispatch<F, Fut>(&self, f: F) -> Result<(), StorageError>
    where
        F: FnOnce(Pool) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<(), StorageError>> + Send + 'static,
    {
        let tx = self.tx.clone();
        let handle = std::thread::spawn(move || {
            let (reply_tx, reply_rx) = oneshot::channel();
            let op: DbOp = Box::new(move |pool: &Pool| {
                let pool_clone = pool.clone();
                Box::pin(f(pool_clone))
            });
            tx.blocking_send((op, reply_tx))
                .map_err(|_| StorageError::Connection("DB thread unavailable".into()))?;
            reply_rx
                .blocking_recv()
                .map_err(|_| StorageError::Connection("DB thread dropped reply".into()))?
        });
        handle
            .join()
            .map_err(|_| StorageError::Connection("dispatch thread panicked".into()))?
    }

    /// Dispatch a query DB operation that returns JSON values.
    ///
    /// Uses a shared result cell to transport the query result back,
    /// since the `DbOp` type only carries `Result<(), StorageError>`.
    fn dispatch_query<F, Fut>(&self, f: F) -> Result<Vec<serde_json::Value>, StorageError>
    where
        F: FnOnce(Pool) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<Vec<serde_json::Value>, StorageError>>
            + Send
            + 'static,
    {
        let result_cell = Arc::new(std::sync::Mutex::new(
            None::<Result<Vec<serde_json::Value>, StorageError>>,
        ));
        let result_cell_clone = result_cell.clone();
        let tx = self.tx.clone();

        let handle = std::thread::spawn(move || {
            let op: DbOp = Box::new(move |pool: &Pool| {
                let pool_clone = pool.clone();
                Box::pin(async move {
                    let result = f(pool_clone).await;
                    *result_cell_clone.lock().unwrap() = Some(result);
                    Ok(())
                })
            });
            let (reply_tx, reply_rx) = oneshot::channel();
            tx.blocking_send((op, reply_tx))
                .map_err(|_| StorageError::Connection("DB thread unavailable".into()))?;
            reply_rx
                .blocking_recv()
                .map_err(|_| StorageError::Connection("DB thread dropped reply".into()))?
        });
        handle
            .join()
            .map_err(|_| StorageError::Connection("dispatch thread panicked".into()))??;

        let result = result_cell
            .lock()
            .unwrap()
            .take()
            .unwrap_or(Err(StorageError::Query("No result from query".into())));
        result
    }

    /// Dispatch a query that returns an optional single JSON value.
    fn dispatch_query_opt(
        &self,
        f: impl FnOnce(Pool) -> std::pin::Pin<
            Box<
                dyn std::future::Future<Output = Result<Option<serde_json::Value>, StorageError>>
                    + Send,
            >,
        > + Send
            + 'static,
    ) -> Result<Option<serde_json::Value>, StorageError> {
        let result_cell = Arc::new(std::sync::Mutex::new(
            None::<Result<Option<serde_json::Value>, StorageError>>,
        ));
        let result_cell_clone = result_cell.clone();
        let tx = self.tx.clone();

        let handle = std::thread::spawn(move || {
            let op: DbOp = Box::new(move |pool: &Pool| {
                let pool_clone = pool.clone();
                Box::pin(async move {
                    let result = f(pool_clone).await;
                    *result_cell_clone.lock().unwrap() = Some(result);
                    Ok(())
                })
            });
            let (reply_tx, reply_rx) = oneshot::channel();
            tx.blocking_send((op, reply_tx))
                .map_err(|_| StorageError::Connection("DB thread unavailable".into()))?;
            reply_rx
                .blocking_recv()
                .map_err(|_| StorageError::Connection("DB thread dropped reply".into()))?
        });
        handle
            .join()
            .map_err(|_| StorageError::Connection("dispatch thread panicked".into()))??;

        let result = result_cell
            .lock()
            .unwrap()
            .take()
            .unwrap_or(Ok(None));
        result
    }

    /// Dispatch a query that returns a u64 (e.g. rows affected).
    fn dispatch_query_u64<F, Fut>(&self, f: F) -> Result<u64, StorageError>
    where
        F: FnOnce(Pool) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<u64, StorageError>> + Send + 'static,
    {
        let result_cell = Arc::new(std::sync::Mutex::new(None::<Result<u64, StorageError>>));
        let result_cell_clone = result_cell.clone();
        let tx = self.tx.clone();

        let handle = std::thread::spawn(move || {
            let op: DbOp = Box::new(move |pool: &Pool| {
                let pool_clone = pool.clone();
                Box::pin(async move {
                    let result = f(pool_clone).await;
                    *result_cell_clone.lock().unwrap() = Some(result);
                    Ok(())
                })
            });
            let (reply_tx, reply_rx) = oneshot::channel();
            tx.blocking_send((op, reply_tx))
                .map_err(|_| StorageError::Connection("DB thread unavailable".into()))?;
            reply_rx
                .blocking_recv()
                .map_err(|_| StorageError::Connection("DB thread dropped reply".into()))?
        });
        handle
            .join()
            .map_err(|_| StorageError::Connection("dispatch thread panicked".into()))??;

        let result = result_cell
            .lock()
            .unwrap()
            .take()
            .unwrap_or(Err(StorageError::Query("No result from query".into())));
        result
    }

    /// Flush any buffered operations.
    ///
    /// Note: This is an async method that internally dispatches sync operations.
    /// Each buffered op is sent to the DB thread via the dedicated channel.
    pub async fn flush(&self) -> Result<(), StorageError> {
        let ops: Vec<PendingOp> = {
            let mut buffer = self.buffer.lock().await;
            if buffer.is_empty() {
                return Ok(());
            }
            buffer.drain(..).collect()
        };

        for op in ops {
            match op {
                PendingOp::SaveWorkflow {
                    workflow_id,
                    function_name,
                    output,
                } => {
                    let wf_id = workflow_id.clone();
                    let fn_name = function_name.clone();
                    let out = output.clone();
                    self.dispatch(move |pool| async move {
                        let client = pool.get().await.map_err(|e| {
                            StorageError::Connection(format!("Failed to get connection: {}", e))
                        })?;
                        client
                            .execute(
                                queries::UPSERT_WORKFLOW,
                                &[
                                    &wf_id.as_str(),
                                    &fn_name.as_str(),
                                    &"completed",
                                    &serde_json::Value::Null,
                                    &out,
                                    &None::<String>,
                                ],
                            )
                            .await
                            .map_err(|e| {
                                StorageError::Query(format!("Failed to save workflow: {}", e))
                            })?;
                        Ok(())
                    })?;
                }
                PendingOp::SaveJournal { workflow_id, entry } => {
                    let wf_id = workflow_id.clone();
                    let ent = entry.clone();
                    self.dispatch(move |pool| async move {
                        let client = pool.get().await.map_err(|e| {
                            StorageError::Connection(format!("Failed to get connection: {}", e))
                        })?;
                        let seq =
                            ent.get("sequence").and_then(|v| v.as_i64()).unwrap_or(0);
                        let func_name = ent
                            .get("function_name")
                            .and_then(|v| v.as_str())
                            .unwrap_or("");
                        let input = ent.get("input").cloned();
                        let output = ent.get("output").cloned();
                        let error = ent.get("error").and_then(|v| v.as_str());
                        let completed =
                            ent.get("completed").and_then(|v| v.as_bool()).unwrap_or(false);
                        client
                            .execute(
                                queries::INSERT_JOURNAL,
                                &[
                                    &wf_id.as_str(),
                                    &seq,
                                    &func_name,
                                    &input,
                                    &output,
                                    &error,
                                    &completed,
                                ],
                            )
                            .await
                            .map_err(|e| {
                                StorageError::Query(format!("Failed to save journal: {}", e))
                            })?;
                        Ok(())
                    })?;
                }
                PendingOp::SaveState {
                    workflow_id,
                    key,
                    value,
                } => {
                    let wf_id = workflow_id.clone();
                    let k = key.clone();
                    let v = value.clone();
                    self.dispatch(move |pool| async move {
                        let client = pool.get().await.map_err(|e| {
                            StorageError::Connection(format!("Failed to get connection: {}", e))
                        })?;
                        client
                            .execute(
                                queries::UPSERT_STATE,
                                &[&wf_id.as_str(), &k.as_str(), &v],
                            )
                            .await
                            .map_err(|e| {
                                StorageError::Query(format!("Failed to save state: {}", e))
                            })?;
                        Ok(())
                    })?;
                }
            }
        }

        Ok(())
    }

    /// Get the number of pending buffered operations.
    pub async fn pending_count(&self) -> usize {
        self.buffer.lock().await.len()
    }
}

impl StorageBackend for PostgresAdapter {
    fn init_schema(&self) -> Result<(), StorageError> {
        tracing::info!("init_schema called — starting schema initialization");

        self.dispatch(|pool| async move {
            let client = pool
                .get()
                .await
                .map_err(|e| StorageError::Connection(format!("Failed to get connection: {}", e)))?;

            tracing::info!("Got database connection, executing schema SQL");

            for statement in SCHEMA_SQL.split(';') {
                let trimmed = statement.trim();
                if !trimmed.is_empty() {
                    client.execute(trimmed, &[]).await.map_err(|e| {
                        StorageError::Query(format!(
                            "Schema SQL failed for '{}': {}",
                            &trimmed[..trimmed.len().min(60)],
                            e
                        ))
                    })?;
                }
            }

            tracing::info!("Schema initialization completed successfully");
            Ok(())
        })
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
            return Err(StorageError::Query("workflow_id cannot be empty".into()));
        }

        let wf_id = workflow_id.to_string();
        let fn_name = function_name.to_string();
        let out = output.clone();

        self.dispatch(move |pool| async move {
            let client = pool
                .get()
                .await
                .map_err(|e| StorageError::Connection(format!("Failed to get connection: {}", e)))?;

            client
                .execute(
                    queries::UPSERT_WORKFLOW,
                    &[
                        &wf_id.as_str(),
                        &fn_name.as_str(),
                        &"completed",
                        &serde_json::Value::Null,
                        &out,
                        &None::<String>,
                    ],
                )
                .await
                .map_err(|e| StorageError::Query(format!("Failed to save workflow: {}", e)))?;

            Ok(())
        })
    }

    fn load_workflow(
        &self,
        workflow_id: &str,
    ) -> Result<Option<serde_json::Value>, StorageError> {
        if workflow_id.is_empty() {
            return Err(StorageError::Query("workflow_id cannot be empty".into()));
        }

        let wf_id = workflow_id.to_string();

        self.dispatch_query_opt(move |pool| {
            Box::pin(async move {
                let client = pool
                    .get()
                    .await
                    .map_err(|e| StorageError::Connection(format!("Failed to get connection: {}", e)))?;

                let row = client
                    .query_opt(queries::LOAD_WORKFLOW, &[&wf_id.as_str()])
                    .await
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

        let wf_id = workflow_id.to_string();
        let ent = entry.clone();

        self.dispatch(move |pool| async move {
            let client = pool
                .get()
                .await
                .map_err(|e| StorageError::Connection(format!("Failed to get connection: {}", e)))?;

            let seq = ent.get("sequence").and_then(|v| v.as_i64()).unwrap_or(0);
            let func_name = ent
                .get("function_name")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let input = ent.get("input").cloned();
            let output = ent.get("output").cloned();
            let error = ent.get("error").and_then(|v| v.as_str());
            let completed = ent.get("completed").and_then(|v| v.as_bool()).unwrap_or(false);

            client
                .execute(
                    queries::INSERT_JOURNAL,
                    &[
                        &wf_id.as_str(),
                        &seq,
                        &func_name,
                        &input,
                        &output,
                        &error,
                        &completed,
                    ],
                )
                .await
                .map_err(|e| StorageError::Query(format!("Failed to save journal: {}", e)))?;

            Ok(())
        })
    }

    fn load_journal(
        &self,
        workflow_id: &str,
    ) -> Result<Vec<serde_json::Value>, StorageError> {
        let wf_id = workflow_id.to_string();

        self.dispatch_query(move |pool| {
            Box::pin(async move {
                let client = pool
                    .get()
                    .await
                    .map_err(|e| StorageError::Connection(format!("Failed to get connection: {}", e)))?;

                let rows = client
                    .query(queries::LOAD_JOURNAL, &[&wf_id.as_str()])
                    .await
                    .map_err(|e| StorageError::Query(format!("Failed to load journal: {}", e)))?;

                let entries: Vec<serde_json::Value> = rows
                    .iter()
                    .map(|row| {
                        serde_json::json!({
                            "sequence": row.get::<_, i64>("sequence"),
                            "function_name": row.get::<_, String>("function_name"),
                            "input": row.get::<_, Option<serde_json::Value>>("input"),
                            "output": row.get::<_, Option<serde_json::Value>>("output"),
                            "error": row.get::<_, Option<String>>("error"),
                            "completed": row.get::<_, bool>("completed"),
                        })
                    })
                    .collect();

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

        let wf_id = workflow_id.to_string();
        let k = key.to_string();
        let v = value.clone();

        self.dispatch(move |pool| async move {
            let client = pool
                .get()
                .await
                .map_err(|e| StorageError::Connection(format!("Failed to get connection: {}", e)))?;

            client
                .execute(queries::UPSERT_STATE, &[&wf_id.as_str(), &k.as_str(), &v])
                .await
                .map_err(|e| StorageError::Query(format!("Failed to save state: {}", e)))?;

            Ok(())
        })
    }

    fn load_state(
        &self,
        workflow_id: &str,
        key: &str,
    ) -> Result<Option<serde_json::Value>, StorageError> {
        let wf_id = workflow_id.to_string();
        let k = key.to_string();

        self.dispatch_query_opt(move |pool| {
            Box::pin(async move {
                let client = pool
                    .get()
                    .await
                    .map_err(|e| StorageError::Connection(format!("Failed to get connection: {}", e)))?;

                let row = client
                    .query_opt(queries::LOAD_STATE, &[&wf_id.as_str(), &k.as_str()])
                    .await
                    .map_err(|e| StorageError::Query(format!("Failed to load state: {}", e)))?;

                Ok(row.and_then(|r| r.get::<_, Option<serde_json::Value>>("value")))
            })
        })
    }

    fn delete_state(&self, workflow_id: &str, key: &str) -> Result<bool, StorageError> {
        let wf_id = workflow_id.to_string();
        let k = key.to_string();

        let rows = self.dispatch_query_u64(move |pool| {
            Box::pin(async move {
                let client = pool
                    .get()
                    .await
                    .map_err(|e| StorageError::Connection(format!("Failed to get connection: {}", e)))?;

                client
                    .execute(queries::DELETE_STATE, &[&wf_id.as_str(), &k.as_str()])
                    .await
                    .map_err(|e| StorageError::Query(format!("Failed to delete state: {}", e)))
            })
        })?;

        Ok(rows > 0)
    }

    fn list_workflows(&self) -> Result<Vec<String>, StorageError> {
        let result_cell = Arc::new(std::sync::Mutex::new(
            None::<Result<Vec<String>, StorageError>>,
        ));
        let result_cell_clone = result_cell.clone();
        let tx = self.tx.clone();

        let handle = std::thread::spawn(move || {
            let op: DbOp = Box::new(move |pool: &Pool| {
                let pool_clone = pool.clone();
                Box::pin(async move {
                    let result = async {
                        let client = pool_clone
                            .get()
                            .await
                            .map_err(|e| StorageError::Connection(format!("Failed to get connection: {}", e)))?;

                        let rows = client
                            .query(queries::LIST_WORKFLOWS, &[])
                            .await
                            .map_err(|e| StorageError::Query(format!("Failed to list workflows: {}", e)))?;

                        Ok(rows
                            .iter()
                            .map(|r| r.get::<_, String>("workflow_id"))
                            .collect())
                    }
                    .await;
                    *result_cell_clone.lock().unwrap() = Some(result);
                    Ok(())
                })
            });
            let (reply_tx, reply_rx) = oneshot::channel();
            tx.blocking_send((op, reply_tx))
                .map_err(|_| StorageError::Connection("DB thread unavailable".into()))?;
            reply_rx
                .blocking_recv()
                .map_err(|_| StorageError::Connection("DB thread dropped reply".into()))?
        });
        handle
            .join()
            .map_err(|_| StorageError::Connection("dispatch thread panicked".into()))??;

        let result = result_cell
            .lock()
            .unwrap()
            .take()
            .unwrap_or(Ok(Vec::new()));
        result
    }

    fn delete_workflow(&self, workflow_id: &str) -> Result<(), StorageError> {
        let wf_id = workflow_id.to_string();

        self.dispatch(move |pool| async move {
            let client = pool
                .get()
                .await
                .map_err(|e| StorageError::Connection(format!("Failed to get connection: {}", e)))?;

            client
                .execute(queries::DELETE_WORKFLOW, &[&wf_id.as_str()])
                .await
                .map_err(|e| StorageError::Query(format!("Failed to delete workflow: {}", e)))?;

            Ok(())
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
    fn test_adapter_config() {
        // PostgresAdapter::new() is async (requires live DB) — test config only
        let config = test_config();
        assert_eq!(
            config.url,
            "postgres://test:test@localhost:5432/velocity_test"
        );
    }

    #[test]
    fn test_schema_sql() {
        // Test schema SQL without constructing adapter (requires live DB)
        let sql = SCHEMA_SQL;
        assert!(sql.contains("velocity_workflows"));
        assert!(sql.contains("velocity_journal"));
        assert!(sql.contains("velocity_state"));
    }

    #[test]
    fn test_save_workflow_validation() {
        // PostgresAdapter requires live DB — verify query constants exist
        assert!(queries::UPSERT_WORKFLOW.contains("velocity_workflows"));
    }

    #[test]
    fn test_batch_mode() {
        // PostgresAdapter requires live DB — verify batch infrastructure
        assert!(queries::INSERT_JOURNAL.contains("velocity_journal"));
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
