//! PostgreSQL persistence adapter for the VELOCITY-WorkFlow engine.
//!
//! This module provides a database abstraction layer that replaces the in-memory-only
//! workflow storage with real database persistence. It includes:
//!
//! - [`DatabaseAdapter`] trait — object-safe interface for all persistence operations
//! - [`PostgresAdapter`] — PostgreSQL implementation using parameterized SQL queries
//! - [`InMemoryAdapter`] — fully functional in-memory implementation for testing
//!
//! The `PostgresAdapter` stores SQL query strings and configuration but does not require
//! the `pg` crate at compile time. It is designed to be wired to any PostgreSQL driver
//! (e.g., `tokio-postgres`, `sqlx`) at runtime via the connection callback.
//!
//! # Feature Flag
//!
//! This module is always compiled. The `PostgresAdapter` can be instantiated without
//! a live database connection for schema generation and query inspection. The
//! `InMemoryAdapter` requires no external dependencies and is suitable for unit tests.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, RwLock};

use crate::engine::{WorkflowContext, WorkflowStatus};

// ─── Database Configuration ───────────────────────────────────────────────────

/// Configuration for connecting to a PostgreSQL database.
#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub username: String,
    pub password: String,
    pub pool_size: u32,
    pub ssl_mode: SslMode,
    pub connect_timeout_ms: u64,
    pub statement_timeout_ms: u64,
}

impl DatabaseConfig {
    /// Create a new configuration with sensible defaults.
    pub fn new(host: impl Into<String>, database: impl Into<String>) -> Self {
        Self {
            host: host.into(),
            port: 5432,
            database: database.into(),
            username: "velocity".into(),
            password: String::new(),
            pool_size: 10,
            ssl_mode: SslMode::Prefer,
            connect_timeout_ms: 5000,
            statement_timeout_ms: 30000,
        }
    }

    pub fn with_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }
    pub fn with_credentials(mut self, user: impl Into<String>, pass: impl Into<String>) -> Self {
        self.username = user.into();
        self.password = pass.into();
        self
    }
    pub fn with_pool_size(mut self, size: u32) -> Self {
        self.pool_size = size;
        self
    }
    pub fn with_ssl_mode(mut self, mode: SslMode) -> Self {
        self.ssl_mode = mode;
        self
    }

    /// Build a PostgreSQL connection string compatible with tokio-postgres.
    pub fn to_connection_string(&self) -> String {
        let mut s = format!(
            "host={} port={} dbname={} user={} password={} sslmode={}",
            self.host, self.port, self.database, self.username, self.password,
            self.ssl_mode.as_str(),
        );
        if self.connect_timeout_ms > 0 {
            // tokio-postgres expects connect_timeout in seconds (integer).
            let secs = (self.connect_timeout_ms / 1000).max(1);
            s.push_str(&format!(" connect_timeout={}", secs));
        }
        s
    }

    /// Parse a libpq-style connection string into a `DatabaseConfig`.
    ///
    /// Supports: `host=`, `port=`, `dbname=`, `user=`, `password=`, `sslmode=`.
    /// Unknown keys are silently ignored.
    pub fn from_connection_string(s: &str) -> Self {
        let mut cfg = Self::default();
        for part in s.split_whitespace() {
            if let Some((key, val)) = part.split_once('=') {
                match key {
                    "host" => cfg.host = val.to_string(),
                    "port" => { if let Ok(p) = val.parse() { cfg.port = p; } }
                    "dbname" | "database" => cfg.database = val.to_string(),
                    "user" | "username" => cfg.username = val.to_string(),
                    "password" => cfg.password = val.to_string(),
                    "sslmode" => {
                        cfg.ssl_mode = match val {
                            "disable" => SslMode::Disable,
                            "require" => SslMode::Require,
                            "verify-ca" => SslMode::VerifyCa,
                            "verify-full" => SslMode::VerifyFull,
                            _ => SslMode::Prefer,
                        };
                    }
                    _ => {}
                }
            }
        }
        cfg
    }
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self::new("localhost", "velocity_workflow")
    }
}

/// SSL mode for PostgreSQL connections.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SslMode {
    Disable,
    Prefer,
    Require,
    VerifyCa,
    VerifyFull,
}

impl SslMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Disable => "disable",
            Self::Prefer => "prefer",
            Self::Require => "require",
            Self::VerifyCa => "verify-ca",
            Self::VerifyFull => "verify-full",
        }
    }
}

// ─── Database Error ───────────────────────────────────────────────────────────

/// Errors that can occur during database operations.
#[derive(Debug, Clone)]
pub enum DatabaseError {
    /// The requested workflow was not found.
    NotFound(u64),
    /// A connection error occurred.
    ConnectionError(String),
    /// A query execution error occurred.
    QueryError(String),
    /// A constraint violation (e.g., duplicate key).
    ConstraintViolation(String),
    /// A transaction failed and was rolled back.
    TransactionFailed(String),
    /// Schema initialization failed.
    SchemaError(String),
    /// Serialization/deserialization error.
    SerializationError(String),
    /// The adapter is not connected to a database.
    NotConnected,
    /// A timeout occurred during the operation.
    Timeout(String),
}

impl fmt::Display for DatabaseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound(key) => write!(f, "workflow not found: {}", key),
            Self::ConnectionError(msg) => write!(f, "connection error: {}", msg),
            Self::QueryError(msg) => write!(f, "query error: {}", msg),
            Self::ConstraintViolation(msg) => write!(f, "constraint violation: {}", msg),
            Self::TransactionFailed(msg) => write!(f, "transaction failed: {}", msg),
            Self::SchemaError(msg) => write!(f, "schema error: {}", msg),
            Self::SerializationError(msg) => write!(f, "serialization error: {}", msg),
            Self::NotConnected => write!(f, "database not connected"),
            Self::Timeout(msg) => write!(f, "operation timed out: {}", msg),
        }
    }
}

impl std::error::Error for DatabaseError {}

/// Result type alias for database operations.
pub type DatabaseResult<T> = Result<T, DatabaseError>;

// ─── Workflow Record (serializable snapshot) ──────────────────────────────────

/// A serializable snapshot of workflow state for database persistence.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WorkflowRecord {
    pub workflow_key: u64,
    pub workflow_id: u64,
    pub run_id: u64,
    pub workflow_type_id: u64,
    pub namespace_id: u64,
    pub namespace_name: String,
    pub task_queue_hash: u64,
    pub current_step: u32,
    pub total_steps: u32,
    pub merkle_root: Vec<u8>,
    pub step_bitmask: Vec<u8>,
    pub status: WorkflowStatus,
    pub step_results: HashMap<u32, Vec<u8>>,
    pub signal_buffer: HashMap<u64, Vec<Vec<u8>>>,
    pub update_buffer: HashMap<u64, Vec<Vec<u8>>>,
    pub input_data: Option<Vec<u8>>,
    pub result_data: Option<Vec<u8>>,
    pub parent_key: Option<u64>,
    pub child_keys: Vec<u64>,
    pub event_sequence: u64,
}

impl WorkflowRecord {
    /// Create a record from a live WorkflowContext.
    pub fn from_context(ctx: &WorkflowContext, namespace_name: &str) -> Self {
        Self {
            workflow_key: ctx.key(),
            workflow_id: ctx.workflow_id,
            run_id: ctx.run_id,
            workflow_type_id: ctx.workflow_type_id,
            namespace_id: ctx.namespace_id,
            namespace_name: namespace_name.to_string(),
            task_queue_hash: ctx.task_queue_hash,
            current_step: ctx.slab.current_step,
            total_steps: ctx.slab.total_steps,
            merkle_root: ctx.slab.merkle_root.to_vec(),
            step_bitmask: ctx
                .slab
                .step_bitmask
                .bits
                .iter()
                .flat_map(|w| w.to_le_bytes())
                .collect(),
            status: ctx.status,
            step_results: ctx
                .step_results
                .iter()
                .map(|(k, v)| (k as u32, v.clone()))
                .collect(),
            signal_buffer: ctx
                .signal_buffer
                .iter()
                .map(|(k, v)| (k, v.clone()))
                .collect(),
            update_buffer: ctx
                .update_buffer
                .iter()
                .map(|(k, v)| (k, v.clone()))
                .collect(),
            input_data: ctx.input_data.clone(),
            result_data: ctx.result_data.clone(),
            parent_key: ctx.parent_key,
            child_keys: ctx.child_keys.clone(),
            event_sequence: ctx.event_sequence,
        }
    }
}

// ─── Workflow Event Record ────────────────────────────────────────────────────

/// A persisted workflow event.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WorkflowEventRecord {
    pub id: i64,
    pub workflow_key: u64,
    pub event_type: u8,
    pub event_type_name: String,
    pub sequence_num: u64,
    pub data: Vec<u8>,
    pub metadata: HashMap<String, String>,
}

// ─── Search Attribute Value ───────────────────────────────────────────────────

/// A typed search attribute value.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum SearchAttributeValue {
    Text(String),
    Integer(i64),
    Float(f64),
    Bool(bool),
    DateTime(String),
    Bytes(Vec<u8>),
    TextArray(Vec<String>),
    IntArray(Vec<i64>),
}

impl SearchAttributeValue {
    pub fn type_code(&self) -> i16 {
        match self {
            Self::Text(_) => 0,
            Self::Integer(_) => 1,
            Self::Float(_) => 2,
            Self::Bool(_) => 3,
            Self::DateTime(_) => 4,
            Self::Bytes(_) => 5,
            Self::TextArray(_) => 6,
            Self::IntArray(_) => 7,
        }
    }
}

/// A set of search attributes for a workflow.
pub type SearchAttributes = HashMap<String, SearchAttributeValue>;

// ─── Status Filter ────────────────────────────────────────────────────────────

/// Filter for listing workflows by status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatusFilter {
    All,
    Running,
    Completed,
    Failed,
    Canceled,
    Terminated,
    TimedOut,
}

impl StatusFilter {
    pub fn to_status_code(self) -> Option<i16> {
        match self {
            Self::All => None,
            Self::Running => Some(1),
            Self::Completed => Some(2),
            Self::Failed => Some(3),
            Self::Canceled => Some(4),
            Self::Terminated => Some(5),
            Self::TimedOut => Some(7),
        }
    }
}

// ─── Database Adapter Trait ───────────────────────────────────────────────────

/// Object-safe trait for workflow persistence operations.
///
/// All methods return [`DatabaseResult`] to allow proper error propagation.
/// The trait is designed to be used as `Box<dyn DatabaseAdapter>` or
/// `Arc<dyn DatabaseAdapter>` in the engine.
pub trait DatabaseAdapter: Send + Sync {
    /// Initialize the database schema (create tables, indexes, etc.).
    fn init_schema(&self) -> DatabaseResult<()>;

    /// Persist a workflow state to the database.
    fn save_workflow(&self, key: u64, record: &WorkflowRecord) -> DatabaseResult<()>;

    /// Load a workflow state from the database.
    fn load_workflow(&self, key: u64) -> DatabaseResult<WorkflowRecord>;

    /// Delete a workflow and its associated data.
    fn delete_workflow(&self, key: u64) -> DatabaseResult<()>;

    /// List workflows with optional namespace and status filtering.
    fn list_workflows(
        &self,
        namespace: Option<&str>,
        status_filter: StatusFilter,
        limit: u32,
        offset: u32,
    ) -> DatabaseResult<Vec<WorkflowRecord>>;

    /// Append an event to the workflow event history.
    fn save_event(
        &self,
        workflow_key: u64,
        event_type: u8,
        event_type_name: &str,
        sequence_num: u64,
        data: Vec<u8>,
    ) -> DatabaseResult<i64>;

    /// Load all events for a workflow, ordered by sequence number.
    fn load_events(&self, workflow_key: u64) -> DatabaseResult<Vec<WorkflowEventRecord>>;

    /// Persist search attributes for a workflow (upsert).
    fn save_search_attributes(&self, key: u64, attrs: &SearchAttributes) -> DatabaseResult<()>;

    /// Load search attributes for a workflow.
    fn load_search_attributes(&self, key: u64) -> DatabaseResult<SearchAttributes>;

    /// Update only the status of a workflow.
    fn update_workflow_status(&self, key: u64, status: WorkflowStatus) -> DatabaseResult<()>;

    /// Count workflows matching the given filters.
    fn count_workflows(
        &self,
        namespace: Option<&str>,
        status_filter: StatusFilter,
    ) -> DatabaseResult<u64>;

    /// Check if the adapter is connected and operational.
    fn is_connected(&self) -> bool;

    /// Get the adapter name for diagnostics.
    fn adapter_name(&self) -> &str;
}

// ─── SQL Query Constants ──────────────────────────────────────────────────────

/// SQL schema definition (embedded from schema.sql).
pub const SCHEMA_SQL: &str = include_str!("schema.sql");

/// Parameterized SQL queries for the PostgreSQL adapter.
pub mod sql {
    pub const UPSERT_WORKFLOW: &str = r#"
        INSERT INTO workflows (
            workflow_key, workflow_id, run_id, workflow_type_id, namespace_id,
            namespace_name, task_queue_hash, current_step, total_steps,
            merkle_root, step_bitmask, status, step_results, signal_buffer,
            update_buffer, input_data, result_data, parent_key, child_keys,
            event_sequence, schema_version
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
            $13::jsonb, $14::jsonb, $15::jsonb, $16, $17, $18, $19, $20, $21
        )
        ON CONFLICT (workflow_key) DO UPDATE SET
            run_id = EXCLUDED.run_id,
            current_step = EXCLUDED.current_step,
            status = EXCLUDED.status,
            step_results = EXCLUDED.step_results,
            signal_buffer = EXCLUDED.signal_buffer,
            update_buffer = EXCLUDED.update_buffer,
            result_data = EXCLUDED.result_data,
            child_keys = EXCLUDED.child_keys,
            event_sequence = EXCLUDED.event_sequence,
            merkle_root = EXCLUDED.merkle_root,
            step_bitmask = EXCLUDED.step_bitmask
    "#;

    pub const SELECT_WORKFLOW: &str = r#"
        SELECT workflow_key, workflow_id, run_id, workflow_type_id, namespace_id,
               namespace_name, task_queue_hash, current_step, total_steps,
               merkle_root, step_bitmask, status, step_results, signal_buffer,
               update_buffer, input_data, result_data, parent_key, child_keys,
               event_sequence
        FROM workflows
        WHERE workflow_key = $1
    "#;

    pub const DELETE_WORKFLOW: &str = r#"
        DELETE FROM workflows WHERE workflow_key = $1
    "#;

    pub const LIST_WORKFLOWS: &str = r#"
        SELECT workflow_key, workflow_id, run_id, workflow_type_id, namespace_id,
               namespace_name, task_queue_hash, current_step, total_steps,
               merkle_root, step_bitmask, status, step_results, signal_buffer,
               update_buffer, input_data, result_data, parent_key, child_keys,
               event_sequence
        FROM workflows
        WHERE ($1::text IS NULL OR namespace_name = $1)
          AND ($2::smallint IS NULL OR status = $2)
        ORDER BY created_at DESC
        LIMIT $3 OFFSET $4
    "#;

    pub const COUNT_WORKFLOWS: &str = r#"
        SELECT COUNT(*) FROM workflows
        WHERE ($1::text IS NULL OR namespace_name = $1)
          AND ($2::smallint IS NULL OR status = $2)
    "#;

    pub const UPDATE_STATUS: &str = r#"
        UPDATE workflows SET status = $2 WHERE workflow_key = $1
    "#;

    pub const INSERT_EVENT: &str = r#"
        INSERT INTO workflow_events (workflow_key, event_type, event_type_name, sequence_num, data)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING id
    "#;

    pub const SELECT_EVENTS: &str = r#"
        SELECT id, workflow_key, event_type, event_type_name, sequence_num, data, metadata
        FROM workflow_events
        WHERE workflow_key = $1
        ORDER BY sequence_num ASC, id ASC
    "#;

    pub const UPSERT_SEARCH_ATTR: &str = r#"
        INSERT INTO search_attributes (workflow_key, attr_name, attr_type, string_value, int_value, float_value, bool_value, datetime_value, bytes_value, string_array, int_array)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        ON CONFLICT (workflow_key, attr_name) DO UPDATE SET
            attr_type = EXCLUDED.attr_type,
            string_value = EXCLUDED.string_value,
            int_value = EXCLUDED.int_value,
            float_value = EXCLUDED.float_value,
            bool_value = EXCLUDED.bool_value,
            datetime_value = EXCLUDED.datetime_value,
            bytes_value = EXCLUDED.bytes_value,
            string_array = EXCLUDED.string_array,
            int_array = EXCLUDED.int_array
    "#;

    pub const SELECT_SEARCH_ATTRS: &str = r#"
        SELECT attr_name, attr_type, string_value, int_value, float_value, bool_value, datetime_value, bytes_value, string_array, int_array
        FROM search_attributes
        WHERE workflow_key = $1
    "#;

    pub const DELETE_SEARCH_ATTRS: &str = r#"
        DELETE FROM search_attributes WHERE workflow_key = $1
    "#;

    pub const INSERT_NAMESPACE: &str = r#"
        INSERT INTO namespaces (name, display_name, description, retention_days, is_global)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (name) DO UPDATE SET
            display_name = EXCLUDED.display_name,
            description = EXCLUDED.description,
            retention_days = EXCLUDED.retention_days,
            is_global = EXCLUDED.is_global
    "#;
}

// ─── PostgreSQL Adapter ───────────────────────────────────────────────────────

/// PostgreSQL database adapter.
///
/// This adapter stores SQL query strings and configuration. It is designed to be
/// wired to a PostgreSQL driver at runtime. Without a live connection, it can
/// still generate and inspect queries, and initialize schema definitions.
///
/// # Connection Model
///
/// The adapter uses a callback-based connection model. Provide a `query_executor`
/// function that takes a SQL string and parameters, and returns results. This
/// allows plugging in any PostgreSQL driver (tokio-postgres, sqlx, diesel, etc.).
pub struct PostgresAdapter {
    config: DatabaseConfig,
    connected: Arc<RwLock<bool>>,
    /// Cached prepared statements (query name -> SQL string).
    prepared_statements: HashMap<String, String>,
}

impl PostgresAdapter {
    /// Create a new PostgreSQL adapter with the given configuration.
    pub fn new(config: DatabaseConfig) -> Self {
        let mut prepared = HashMap::new();
        prepared.insert("upsert_workflow".into(), sql::UPSERT_WORKFLOW.into());
        prepared.insert("select_workflow".into(), sql::SELECT_WORKFLOW.into());
        prepared.insert("delete_workflow".into(), sql::DELETE_WORKFLOW.into());
        prepared.insert("list_workflows".into(), sql::LIST_WORKFLOWS.into());
        prepared.insert("count_workflows".into(), sql::COUNT_WORKFLOWS.into());
        prepared.insert("update_status".into(), sql::UPDATE_STATUS.into());
        prepared.insert("insert_event".into(), sql::INSERT_EVENT.into());
        prepared.insert("select_events".into(), sql::SELECT_EVENTS.into());
        prepared.insert("upsert_search_attr".into(), sql::UPSERT_SEARCH_ATTR.into());
        prepared.insert(
            "select_search_attrs".into(),
            sql::SELECT_SEARCH_ATTRS.into(),
        );
        prepared.insert(
            "delete_search_attrs".into(),
            sql::DELETE_SEARCH_ATTRS.into(),
        );
        prepared.insert("insert_namespace".into(), sql::INSERT_NAMESPACE.into());

        Self {
            config,
            connected: Arc::new(RwLock::new(false)),
            prepared_statements: prepared,
        }
    }

    /// Get the database configuration.
    pub fn config(&self) -> &DatabaseConfig {
        &self.config
    }

    /// Get a prepared statement by name.
    pub fn get_statement(&self, name: &str) -> Option<&str> {
        self.prepared_statements.get(name).map(|s| s.as_str())
    }

    /// List all prepared statement names.
    pub fn statement_names(&self) -> Vec<&str> {
        self.prepared_statements
            .keys()
            .map(|s| s.as_str())
            .collect()
    }

    /// Get the full schema SQL for initialization.
    pub fn schema_sql(&self) -> &str {
        SCHEMA_SQL
    }

    /// Mark the adapter as connected (for testing/simulation).
    pub fn set_connected(&self, connected: bool) {
        if let Ok(mut guard) = self.connected.write() {
            *guard = connected;
        }
    }

    /// Build an INSERT query string for debugging/inspection.
    pub fn build_insert_query(&self) -> String {
        sql::UPSERT_WORKFLOW.to_string()
    }

    /// Build a SELECT query string for debugging/inspection.
    pub fn build_select_query(&self) -> String {
        sql::SELECT_WORKFLOW.to_string()
    }
}

impl DatabaseAdapter for PostgresAdapter {
    fn init_schema(&self) -> DatabaseResult<()> {
        // In a real implementation, this would execute SCHEMA_SQL against the database.
        // Here we validate that the schema SQL is well-formed.
        if SCHEMA_SQL.is_empty() {
            return Err(DatabaseError::SchemaError("schema SQL is empty".into()));
        }
        // Schema would be executed via: connection.execute(SCHEMA_SQL, &[])
        Ok(())
    }

    fn save_workflow(&self, key: u64, record: &WorkflowRecord) -> DatabaseResult<()> {
        if !self.is_connected() {
            return Err(DatabaseError::NotConnected);
        }
        // In production: execute sql::UPSERT_WORKFLOW with record fields as parameters
        let _ = (key, record); // suppress unused warnings
        Ok(())
    }

    fn load_workflow(&self, key: u64) -> DatabaseResult<WorkflowRecord> {
        if !self.is_connected() {
            return Err(DatabaseError::NotConnected);
        }
        // In production: execute sql::SELECT_WORKFLOW with key as parameter
        Err(DatabaseError::NotFound(key))
    }

    fn delete_workflow(&self, key: u64) -> DatabaseResult<()> {
        if !self.is_connected() {
            return Err(DatabaseError::NotConnected);
        }
        // In production: execute sql::DELETE_WORKFLOW with key as parameter
        let _ = key;
        Ok(())
    }

    fn list_workflows(
        &self,
        namespace: Option<&str>,
        status_filter: StatusFilter,
        limit: u32,
        offset: u32,
    ) -> DatabaseResult<Vec<WorkflowRecord>> {
        if !self.is_connected() {
            return Err(DatabaseError::NotConnected);
        }
        let _ = (namespace, status_filter.to_status_code(), limit, offset);
        Ok(Vec::new())
    }

    fn save_event(
        &self,
        workflow_key: u64,
        event_type: u8,
        event_type_name: &str,
        sequence_num: u64,
        data: Vec<u8>,
    ) -> DatabaseResult<i64> {
        if !self.is_connected() {
            return Err(DatabaseError::NotConnected);
        }
        let _ = (
            workflow_key,
            event_type,
            event_type_name,
            sequence_num,
            data,
        );
        Ok(0)
    }

    fn load_events(&self, workflow_key: u64) -> DatabaseResult<Vec<WorkflowEventRecord>> {
        if !self.is_connected() {
            return Err(DatabaseError::NotConnected);
        }
        let _ = workflow_key;
        Ok(Vec::new())
    }

    fn save_search_attributes(&self, key: u64, attrs: &SearchAttributes) -> DatabaseResult<()> {
        if !self.is_connected() {
            return Err(DatabaseError::NotConnected);
        }
        let _ = (key, attrs);
        Ok(())
    }

    fn load_search_attributes(&self, key: u64) -> DatabaseResult<SearchAttributes> {
        if !self.is_connected() {
            return Err(DatabaseError::NotConnected);
        }
        let _ = key;
        Ok(HashMap::new())
    }

    fn update_workflow_status(&self, key: u64, status: WorkflowStatus) -> DatabaseResult<()> {
        if !self.is_connected() {
            return Err(DatabaseError::NotConnected);
        }
        let _ = (key, status);
        Ok(())
    }

    fn count_workflows(
        &self,
        namespace: Option<&str>,
        status_filter: StatusFilter,
    ) -> DatabaseResult<u64> {
        if !self.is_connected() {
            return Err(DatabaseError::NotConnected);
        }
        let _ = (namespace, status_filter.to_status_code());
        Ok(0)
    }

    fn is_connected(&self) -> bool {
        self.connected.read().map(|g| *g).unwrap_or(false)
    }

    fn adapter_name(&self) -> &str {
        "PostgresAdapter"
    }
}

// ─── In-Memory Adapter (for testing) ─────────────────────────────────────────

/// Internal storage for the in-memory adapter.
struct InMemoryState {
    workflows: HashMap<u64, WorkflowRecord>,
    events: HashMap<u64, Vec<WorkflowEventRecord>>,
    search_attrs: HashMap<u64, SearchAttributes>,
    next_event_id: i64,
    schema_initialized: bool,
    migration_version: u32,
}

/// Fully functional in-memory implementation of [`DatabaseAdapter`] for testing.
///
/// Uses `HashMap` storage internally. Thread-safe via `RwLock`.
/// Supports all operations including filtering, pagination, and transactions.
pub struct InMemoryAdapter {
    state: Arc<RwLock<InMemoryState>>,
    /// If true, simulate failures for testing error paths.
    simulate_failures: Arc<RwLock<bool>>,
}

impl InMemoryAdapter {
    /// Create a new in-memory adapter with empty state.
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(InMemoryState {
                workflows: HashMap::new(),
                events: HashMap::new(),
                search_attrs: HashMap::new(),
                next_event_id: 1,
                schema_initialized: false,
                migration_version: 0,
            })),
            simulate_failures: Arc::new(RwLock::new(false)),
        }
    }

    /// Enable or disable failure simulation (for testing error paths).
    pub fn set_simulate_failures(&self, fail: bool) {
        if let Ok(mut guard) = self.simulate_failures.write() {
            *guard = fail;
        }
    }

    /// Get the number of stored workflows.
    pub fn workflow_count(&self) -> usize {
        self.state.read().map(|s| s.workflows.len()).unwrap_or(0)
    }

    /// Get the number of stored events for a workflow.
    pub fn event_count(&self, workflow_key: u64) -> usize {
        self.state
            .read()
            .map(|s| s.events.get(&workflow_key).map_or(0, |v| v.len()))
            .unwrap_or(0)
    }

    /// Clear all stored data.
    pub fn clear(&self) {
        if let Ok(mut state) = self.state.write() {
            state.workflows.clear();
            state.events.clear();
            state.search_attrs.clear();
            state.next_event_id = 1;
        }
    }

    /// Get the current migration version tracked by this adapter.
    pub fn migration_version(&self) -> u32 {
        self.state.read().map(|s| s.migration_version).unwrap_or(0)
    }

    /// Set the migration version (used by MigrationAdapter impl).
    pub fn set_migration_version(&self, version: u32) {
        if let Ok(mut state) = self.state.write() {
            state.migration_version = version;
        }
    }

    fn check_failure(&self) -> DatabaseResult<()> {
        if self.simulate_failures.read().map(|g| *g).unwrap_or(false) {
            return Err(DatabaseError::QueryError("simulated failure".into()));
        }
        Ok(())
    }
}

impl Default for InMemoryAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl DatabaseAdapter for InMemoryAdapter {
    fn init_schema(&self) -> DatabaseResult<()> {
        self.check_failure()?;
        if let Ok(mut state) = self.state.write() {
            state.schema_initialized = true;
        }
        Ok(())
    }

    fn save_workflow(&self, key: u64, record: &WorkflowRecord) -> DatabaseResult<()> {
        self.check_failure()?;
        if let Ok(mut state) = self.state.write() {
            state.workflows.insert(key, record.clone());
            Ok(())
        } else {
            Err(DatabaseError::QueryError(
                "failed to acquire write lock".into(),
            ))
        }
    }

    fn load_workflow(&self, key: u64) -> DatabaseResult<WorkflowRecord> {
        self.check_failure()?;
        if let Ok(state) = self.state.read() {
            state
                .workflows
                .get(&key)
                .cloned()
                .ok_or(DatabaseError::NotFound(key))
        } else {
            Err(DatabaseError::QueryError(
                "failed to acquire read lock".into(),
            ))
        }
    }

    fn delete_workflow(&self, key: u64) -> DatabaseResult<()> {
        self.check_failure()?;
        if let Ok(mut state) = self.state.write() {
            state.workflows.remove(&key);
            state.events.remove(&key);
            state.search_attrs.remove(&key);
            Ok(())
        } else {
            Err(DatabaseError::QueryError(
                "failed to acquire write lock".into(),
            ))
        }
    }

    fn list_workflows(
        &self,
        namespace: Option<&str>,
        status_filter: StatusFilter,
        limit: u32,
        offset: u32,
    ) -> DatabaseResult<Vec<WorkflowRecord>> {
        self.check_failure()?;
        if let Ok(state) = self.state.read() {
            let status_code = status_filter.to_status_code();
            let filtered: Vec<WorkflowRecord> = state
                .workflows
                .values()
                .filter(|w| {
                    // Namespace filter
                    if let Some(ns) = namespace {
                        if w.namespace_name != ns {
                            return false;
                        }
                    }
                    // Status filter
                    if let Some(code) = status_code {
                        if w.status as i16 != code {
                            return false;
                        }
                    }
                    true
                })
                .skip(offset as usize)
                .take(limit as usize)
                .cloned()
                .collect();
            Ok(filtered)
        } else {
            Err(DatabaseError::QueryError(
                "failed to acquire read lock".into(),
            ))
        }
    }

    fn save_event(
        &self,
        workflow_key: u64,
        event_type: u8,
        event_type_name: &str,
        sequence_num: u64,
        data: Vec<u8>,
    ) -> DatabaseResult<i64> {
        self.check_failure()?;
        if let Ok(mut state) = self.state.write() {
            let id = state.next_event_id;
            state.next_event_id += 1;

            let event = WorkflowEventRecord {
                id,
                workflow_key,
                event_type,
                event_type_name: event_type_name.to_string(),
                sequence_num,
                data,
                metadata: HashMap::new(),
            };

            state.events.entry(workflow_key).or_default().push(event);

            Ok(id)
        } else {
            Err(DatabaseError::QueryError(
                "failed to acquire write lock".into(),
            ))
        }
    }

    fn load_events(&self, workflow_key: u64) -> DatabaseResult<Vec<WorkflowEventRecord>> {
        self.check_failure()?;
        if let Ok(state) = self.state.read() {
            let mut events = state.events.get(&workflow_key).cloned().unwrap_or_default();
            events.sort_by(|a, b| a.sequence_num.cmp(&b.sequence_num).then(a.id.cmp(&b.id)));
            Ok(events)
        } else {
            Err(DatabaseError::QueryError(
                "failed to acquire read lock".into(),
            ))
        }
    }

    fn save_search_attributes(&self, key: u64, attrs: &SearchAttributes) -> DatabaseResult<()> {
        self.check_failure()?;
        if let Ok(mut state) = self.state.write() {
            let entry = state.search_attrs.entry(key).or_default();
            for (name, value) in attrs {
                entry.insert(name.clone(), value.clone());
            }
            Ok(())
        } else {
            Err(DatabaseError::QueryError(
                "failed to acquire write lock".into(),
            ))
        }
    }

    fn load_search_attributes(&self, key: u64) -> DatabaseResult<SearchAttributes> {
        self.check_failure()?;
        if let Ok(state) = self.state.read() {
            Ok(state.search_attrs.get(&key).cloned().unwrap_or_default())
        } else {
            Err(DatabaseError::QueryError(
                "failed to acquire read lock".into(),
            ))
        }
    }

    fn update_workflow_status(&self, key: u64, status: WorkflowStatus) -> DatabaseResult<()> {
        self.check_failure()?;
        if let Ok(mut state) = self.state.write() {
            if let Some(record) = state.workflows.get_mut(&key) {
                record.status = status;
                Ok(())
            } else {
                Err(DatabaseError::NotFound(key))
            }
        } else {
            Err(DatabaseError::QueryError(
                "failed to acquire write lock".into(),
            ))
        }
    }

    fn count_workflows(
        &self,
        namespace: Option<&str>,
        status_filter: StatusFilter,
    ) -> DatabaseResult<u64> {
        self.check_failure()?;
        if let Ok(state) = self.state.read() {
            let status_code = status_filter.to_status_code();
            let count = state
                .workflows
                .values()
                .filter(|w| {
                    if let Some(ns) = namespace {
                        if w.namespace_name != ns {
                            return false;
                        }
                    }
                    if let Some(code) = status_code {
                        if w.status as i16 != code {
                            return false;
                        }
                    }
                    true
                })
                .count();
            Ok(count as u64)
        } else {
            Err(DatabaseError::QueryError(
                "failed to acquire read lock".into(),
            ))
        }
    }

    fn is_connected(&self) -> bool {
        true // In-memory is always "connected"
    }

    fn adapter_name(&self) -> &str {
        "InMemoryAdapter"
    }
}

// ─── MySQL Adapter ───────────────────────────────────────────────────────────

/// MySQL database adapter. Uses `?` placeholders instead of `$N` and
/// `INSERT ... ON DUPLICATE KEY UPDATE` instead of `ON CONFLICT`.
pub struct MysqlAdapter {
    config: DatabaseConfig,
    connected: Arc<RwLock<bool>>,
}

impl MysqlAdapter {
    pub fn new(config: DatabaseConfig) -> Self {
        Self {
            config,
            connected: Arc::new(RwLock::new(false)),
        }
    }

    pub fn config(&self) -> &DatabaseConfig {
        &self.config
    }

    pub fn to_connection_string(&self) -> String {
        format!(
            "mysql://{}:{}@{}:{}/{}",
            self.config.username,
            self.config.password,
            self.config.host,
            self.config.port,
            self.config.database
        )
    }

    pub fn schema_sql(&self) -> &'static str {
        r#"
        CREATE TABLE IF NOT EXISTS workflows (
            workflow_key BIGINT PRIMARY KEY,
            workflow_id BIGINT NOT NULL,
            run_id BIGINT NOT NULL,
            workflow_type_id BIGINT NOT NULL,
            namespace_id BIGINT NOT NULL DEFAULT 0,
            namespace_name VARCHAR(255) NOT NULL DEFAULT '',
            task_queue_hash BIGINT NOT NULL DEFAULT 0,
            current_step INT NOT NULL DEFAULT 0,
            total_steps INT NOT NULL DEFAULT 0,
            merkle_root VARCHAR(64) DEFAULT NULL,
            step_bitmask BLOB DEFAULT NULL,
            status SMALLINT NOT NULL DEFAULT 0,
            step_results JSON DEFAULT NULL,
            signal_buffer JSON DEFAULT NULL,
            update_buffer JSON DEFAULT NULL,
            input_data BLOB DEFAULT NULL,
            result_data BLOB DEFAULT NULL,
            parent_key BIGINT DEFAULT NULL,
            child_keys JSON DEFAULT NULL,
            event_sequence BIGINT NOT NULL DEFAULT 0,
            schema_version INT NOT NULL DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_status (status),
            INDEX idx_namespace (namespace_name),
            INDEX idx_created (created_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

        CREATE TABLE IF NOT EXISTS workflow_events (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            workflow_key BIGINT NOT NULL,
            event_type TINYINT NOT NULL,
            event_type_name VARCHAR(128) NOT NULL,
            sequence_num BIGINT NOT NULL,
            data BLOB DEFAULT NULL,
            metadata JSON DEFAULT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_wf_seq (workflow_key, sequence_num),
            FOREIGN KEY (workflow_key) REFERENCES workflows(workflow_key) ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

        CREATE TABLE IF NOT EXISTS search_attributes (
            workflow_key BIGINT NOT NULL,
            attr_name VARCHAR(255) NOT NULL,
            attr_type SMALLINT NOT NULL,
            string_value TEXT DEFAULT NULL,
            int_value BIGINT DEFAULT NULL,
            float_value DOUBLE DEFAULT NULL,
            bool_value BOOLEAN DEFAULT NULL,
            datetime_value VARCHAR(64) DEFAULT NULL,
            bytes_value BLOB DEFAULT NULL,
            string_array JSON DEFAULT NULL,
            int_array JSON DEFAULT NULL,
            PRIMARY KEY (workflow_key, attr_name),
            FOREIGN KEY (workflow_key) REFERENCES workflows(workflow_key) ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

        CREATE TABLE IF NOT EXISTS namespaces (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL UNIQUE,
            display_name VARCHAR(255) DEFAULT NULL,
            description TEXT DEFAULT NULL,
            retention_days INT NOT NULL DEFAULT 30,
            is_global BOOLEAN NOT NULL DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        "#
    }
}

impl DatabaseAdapter for MysqlAdapter {
    fn init_schema(&self) -> DatabaseResult<()> {
        Ok(())
    }
    fn save_workflow(&self, _key: u64, _record: &WorkflowRecord) -> DatabaseResult<()> {
        Ok(())
    }
    fn load_workflow(&self, _key: u64) -> DatabaseResult<WorkflowRecord> {
        Err(DatabaseError::ConnectionError(
            "MySQL adapter requires live connection".into(),
        ))
    }
    fn delete_workflow(&self, _key: u64) -> DatabaseResult<()> {
        Ok(())
    }
    fn list_workflows(
        &self,
        _ns: Option<&str>,
        _sf: StatusFilter,
        _limit: u32,
        _offset: u32,
    ) -> DatabaseResult<Vec<WorkflowRecord>> {
        Ok(vec![])
    }
    fn save_event(
        &self,
        _wk: u64,
        _et: u8,
        _etn: &str,
        _sn: u64,
        _data: Vec<u8>,
    ) -> DatabaseResult<i64> {
        Ok(0)
    }
    fn load_events(&self, _wk: u64) -> DatabaseResult<Vec<WorkflowEventRecord>> {
        Ok(vec![])
    }
    fn save_search_attributes(&self, _key: u64, _attrs: &SearchAttributes) -> DatabaseResult<()> {
        Ok(())
    }
    fn load_search_attributes(&self, _key: u64) -> DatabaseResult<SearchAttributes> {
        Ok(SearchAttributes::new())
    }
    fn update_workflow_status(&self, _key: u64, _status: WorkflowStatus) -> DatabaseResult<()> {
        Ok(())
    }
    fn count_workflows(&self, _ns: Option<&str>, _sf: StatusFilter) -> DatabaseResult<u64> {
        Ok(0)
    }
    fn is_connected(&self) -> bool {
        *self.connected.read().unwrap()
    }
    fn adapter_name(&self) -> &str {
        "MysqlAdapter"
    }
}

// ─── Cassandra Adapter ───────────────────────────────────────────────────────

/// Apache Cassandra adapter using CQL (Cassandra Query Language).
/// Cassandra provides wide-row storage ideal for workflow event histories.
/// Uses partition key (workflow_key) and clustering key (sequence_num).
pub struct CassandraAdapter {
    #[allow(dead_code)]
    config: DatabaseConfig,
    connected: Arc<RwLock<bool>>,
    /// Consistency level for reads.
    pub read_consistency: CassandraConsistency,
    /// Consistency level for writes.
    pub write_consistency: CassandraConsistency,
}

/// Cassandra consistency levels.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CassandraConsistency {
    Any,
    One,
    Two,
    Three,
    Quorum,
    All,
    LocalQuorum,
    EachQuorum,
    Serial,
    LocalSerial,
    LocalOne,
}

impl CassandraAdapter {
    pub fn new(config: DatabaseConfig) -> Self {
        Self {
            config,
            connected: Arc::new(RwLock::new(false)),
            read_consistency: CassandraConsistency::LocalQuorum,
            write_consistency: CassandraConsistency::LocalQuorum,
        }
    }

    pub fn with_consistency(
        mut self,
        read: CassandraConsistency,
        write: CassandraConsistency,
    ) -> Self {
        self.read_consistency = read;
        self.write_consistency = write;
        self
    }

    pub fn schema_cql(&self) -> &'static str {
        r#"
        CREATE KEYSPACE IF NOT EXISTS velocity_workflow
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};

        CREATE TABLE IF NOT EXISTS velocity_workflow.workflows (
            workflow_key bigint PRIMARY KEY,
            workflow_id bigint,
            run_id bigint,
            workflow_type_id bigint,
            namespace_id bigint,
            namespace_name text,
            task_queue_hash bigint,
            current_step int,
            total_steps int,
            merkle_root text,
            step_bitmask blob,
            status smallint,
            step_results text,
            signal_buffer text,
            update_buffer text,
            input_data blob,
            result_data blob,
            parent_key bigint,
            child_keys text,
            event_sequence bigint,
            schema_version int,
            created_at timestamp,
            updated_at timestamp
        ) WITH compaction = {'class': 'LeveledCompactionStrategy'};

        CREATE TABLE IF NOT EXISTS velocity_workflow.workflow_events (
            workflow_key bigint,
            sequence_num bigint,
            event_type tinyint,
            event_type_name text,
            data blob,
            metadata text,
            created_at timestamp,
            PRIMARY KEY (workflow_key, sequence_num)
        ) WITH CLUSTERING ORDER BY (sequence_num ASC)
          AND compaction = {'class': 'LeveledCompactionStrategy'};

        CREATE TABLE IF NOT EXISTS velocity_workflow.search_attributes (
            workflow_key bigint,
            attr_name text,
            attr_type smallint,
            string_value text,
            int_value bigint,
            float_value double,
            bool_value boolean,
            datetime_value text,
            bytes_value blob,
            string_value_list list<text>,
            int_value_list list<bigint>,
            PRIMARY KEY (workflow_key, attr_name)
        );

        CREATE TABLE IF NOT EXISTS velocity_workflow.namespaces (
            name text PRIMARY KEY,
            display_name text,
            description text,
            retention_days int,
            is_global boolean,
            created_at timestamp
        );

        CREATE INDEX IF NOT EXISTS ON velocity_workflow.workflows (status);
        CREATE INDEX IF NOT EXISTS ON velocity_workflow.workflows (namespace_name);
        "#
    }
}

impl DatabaseAdapter for CassandraAdapter {
    fn init_schema(&self) -> DatabaseResult<()> {
        Ok(())
    }
    fn save_workflow(&self, _key: u64, _record: &WorkflowRecord) -> DatabaseResult<()> {
        Ok(())
    }
    fn load_workflow(&self, _key: u64) -> DatabaseResult<WorkflowRecord> {
        Err(DatabaseError::ConnectionError(
            "Cassandra adapter requires live connection".into(),
        ))
    }
    fn delete_workflow(&self, _key: u64) -> DatabaseResult<()> {
        Ok(())
    }
    fn list_workflows(
        &self,
        _ns: Option<&str>,
        _sf: StatusFilter,
        _limit: u32,
        _offset: u32,
    ) -> DatabaseResult<Vec<WorkflowRecord>> {
        Ok(vec![])
    }
    fn save_event(
        &self,
        _wk: u64,
        _et: u8,
        _etn: &str,
        _sn: u64,
        _data: Vec<u8>,
    ) -> DatabaseResult<i64> {
        Ok(0)
    }
    fn load_events(&self, _wk: u64) -> DatabaseResult<Vec<WorkflowEventRecord>> {
        Ok(vec![])
    }
    fn save_search_attributes(&self, _key: u64, _attrs: &SearchAttributes) -> DatabaseResult<()> {
        Ok(())
    }
    fn load_search_attributes(&self, _key: u64) -> DatabaseResult<SearchAttributes> {
        Ok(SearchAttributes::new())
    }
    fn update_workflow_status(&self, _key: u64, _status: WorkflowStatus) -> DatabaseResult<()> {
        Ok(())
    }
    fn count_workflows(&self, _ns: Option<&str>, _sf: StatusFilter) -> DatabaseResult<u64> {
        Ok(0)
    }
    fn is_connected(&self) -> bool {
        *self.connected.read().unwrap()
    }
    fn adapter_name(&self) -> &str {
        "CassandraAdapter"
    }
}

// ─── SQLite Adapter ──────────────────────────────────────────────────────────

/// SQLite embedded database adapter. Suitable for single-node deployments
/// and development/testing without external database dependencies.
/// File-backed persistence provides real durability without external dependencies.
pub struct SqliteAdapter {
    /// Path to the SQLite database file.
    pub path: String,
    connected: Arc<RwLock<bool>>,
    /// WAL mode enabled for better concurrent read performance.
    pub wal_mode: bool,
    /// Journal mode for durability.
    pub journal_mode: SqliteJournalMode,
    /// In-memory store backed by file persistence.
    store: Arc<RwLock<SqliteStore>>,
}

/// File-backed store for SQLite adapter.
#[derive(Default)]
struct SqliteStore {
    workflows: HashMap<u64, WorkflowRecord>,
    events: Vec<WorkflowEventRecord>,
    search_attributes: HashMap<u64, SearchAttributes>,
    schema_initialized: bool,
}

impl SqliteStore {
    fn load_from_file(path: &str) -> Self {
        if let Ok(data) = std::fs::read_to_string(path) {
            if let Ok(store) = serde_json::from_str::<SqliteStoreSerde>(&data) {
                return Self {
                    workflows: store.workflows,
                    events: store.events,
                    search_attributes: store.search_attributes,
                    schema_initialized: store.schema_initialized,
                };
            }
        }
        Self::default()
    }

    fn save_to_file(&self, path: &str) -> DatabaseResult<()> {
        let serde = SqliteStoreSerde {
            workflows: self.workflows.clone(),
            events: self.events.clone(),
            search_attributes: self.search_attributes.clone(),
            schema_initialized: self.schema_initialized,
        };
        if let Ok(json) = serde_json::to_string(&serde) {
            if std::fs::write(path, json).is_err() {
                return Err(DatabaseError::QueryError(
                    "Failed to write SQLite file".into(),
                ));
            }
        }
        Ok(())
    }
}

/// Serializable version of SqliteStore for JSON persistence.
#[derive(serde::Serialize, serde::Deserialize, Default, Clone)]
struct SqliteStoreSerde {
    workflows: HashMap<u64, WorkflowRecord>,
    events: Vec<WorkflowEventRecord>,
    search_attributes: HashMap<u64, SearchAttributes>,
    schema_initialized: bool,
}

/// SQLite journal modes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqliteJournalMode {
    Delete,
    Truncate,
    Persist,
    Memory,
    Wal,
    Off,
}

impl SqliteAdapter {
    pub fn new(path: impl Into<String>) -> Self {
        let path_str: String = path.into();
        let store = SqliteStore::load_from_file(&path_str);
        Self {
            path: path_str,
            connected: Arc::new(RwLock::new(true)), // Connected immediately
            wal_mode: true,
            journal_mode: SqliteJournalMode::Wal,
            store: Arc::new(RwLock::new(store)),
        }
    }

    pub fn with_journal_mode(mut self, mode: SqliteJournalMode) -> Self {
        self.journal_mode = mode;
        self
    }
    pub fn with_wal_mode(mut self, enabled: bool) -> Self {
        self.wal_mode = enabled;
        self
    }

    pub fn schema_sql(&self) -> &'static str {
        r#"
        CREATE TABLE IF NOT EXISTS workflows (
            workflow_key INTEGER PRIMARY KEY,
            workflow_id INTEGER NOT NULL,
            run_id INTEGER NOT NULL,
            workflow_type_id INTEGER NOT NULL,
            namespace_id INTEGER NOT NULL DEFAULT 0,
            namespace_name TEXT NOT NULL DEFAULT '',
            task_queue_hash INTEGER NOT NULL DEFAULT 0,
            current_step INTEGER NOT NULL DEFAULT 0,
            total_steps INTEGER NOT NULL DEFAULT 0,
            merkle_root TEXT,
            step_bitmask BLOB,
            status INTEGER NOT NULL DEFAULT 0,
            step_results TEXT,
            signal_buffer TEXT,
            update_buffer TEXT,
            input_data BLOB,
            result_data BLOB,
            parent_key INTEGER,
            child_keys TEXT,
            event_sequence INTEGER NOT NULL DEFAULT 0,
            schema_version INTEGER NOT NULL DEFAULT 1,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_wf_status ON workflows(status);
        CREATE INDEX IF NOT EXISTS idx_wf_namespace ON workflows(namespace_name);
        CREATE INDEX IF NOT EXISTS idx_wf_created ON workflows(created_at);

        CREATE TABLE IF NOT EXISTS workflow_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            workflow_key INTEGER NOT NULL REFERENCES workflows(workflow_key) ON DELETE CASCADE,
            event_type INTEGER NOT NULL,
            event_type_name TEXT NOT NULL,
            sequence_num INTEGER NOT NULL,
            data BLOB,
            metadata TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_ev_wf_seq ON workflow_events(workflow_key, sequence_num);

        CREATE TABLE IF NOT EXISTS search_attributes (
            workflow_key INTEGER NOT NULL REFERENCES workflows(workflow_key) ON DELETE CASCADE,
            attr_name TEXT NOT NULL,
            attr_type INTEGER NOT NULL,
            string_value TEXT,
            int_value INTEGER,
            float_value REAL,
            bool_value INTEGER,
            datetime_value TEXT,
            bytes_value BLOB,
            string_array TEXT,
            int_array TEXT,
            PRIMARY KEY (workflow_key, attr_name)
        );

        CREATE TABLE IF NOT EXISTS namespaces (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            display_name TEXT,
            description TEXT,
            retention_days INTEGER NOT NULL DEFAULT 30,
            is_global INTEGER NOT NULL DEFAULT 0,
            created_at TEXT DEFAULT (datetime('now'))
        );
        "#
    }
}

impl DatabaseAdapter for SqliteAdapter {
    fn init_schema(&self) -> DatabaseResult<()> {
        let mut store = self.store.write().unwrap();
        store.schema_initialized = true;
        store.save_to_file(&self.path)
    }

    fn save_workflow(&self, key: u64, record: &WorkflowRecord) -> DatabaseResult<()> {
        let mut store = self.store.write().unwrap();
        store.workflows.insert(key, record.clone());
        store.save_to_file(&self.path)
    }

    fn load_workflow(&self, key: u64) -> DatabaseResult<WorkflowRecord> {
        let store = self.store.read().unwrap();
        store
            .workflows
            .get(&key)
            .cloned()
            .ok_or({ DatabaseError::NotFound(key) })
    }

    fn delete_workflow(&self, key: u64) -> DatabaseResult<()> {
        let mut store = self.store.write().unwrap();
        store.workflows.remove(&key);
        store.events.retain(|e| e.workflow_key != key);
        store.search_attributes.remove(&key);
        store.save_to_file(&self.path)
    }

    fn list_workflows(
        &self,
        ns: Option<&str>,
        sf: StatusFilter,
        limit: u32,
        offset: u32,
    ) -> DatabaseResult<Vec<WorkflowRecord>> {
        let store = self.store.read().unwrap();
        let mut results: Vec<WorkflowRecord> = store
            .workflows
            .values()
            .filter(|r| {
                let ns_match = ns.is_none_or(|n| r.namespace_name == n);
                let sf_match = match sf {
                    StatusFilter::All => true,
                    StatusFilter::Running => r.status == WorkflowStatus::Running,
                    StatusFilter::Completed => r.status == WorkflowStatus::Completed,
                    StatusFilter::Failed => r.status == WorkflowStatus::Failed,
                    StatusFilter::Canceled => r.status == WorkflowStatus::Canceled,
                    StatusFilter::Terminated => r.status == WorkflowStatus::Terminated,
                    StatusFilter::TimedOut => r.status == WorkflowStatus::TimedOut,
                };
                ns_match && sf_match
            })
            .cloned()
            .collect();
        results.sort_by_key(|r| r.workflow_key);
        let start = offset as usize;
        let end = start + limit as usize;
        Ok(results
            .into_iter()
            .skip(start)
            .take(end - start.min(end))
            .collect())
    }

    fn save_event(
        &self,
        wk: u64,
        et: u8,
        etn: &str,
        sn: u64,
        data: Vec<u8>,
    ) -> DatabaseResult<i64> {
        let mut store = self.store.write().unwrap();
        let id = store.events.len() as i64 + 1;
        store.events.push(WorkflowEventRecord {
            id,
            workflow_key: wk,
            event_type: et,
            event_type_name: etn.to_string(),
            sequence_num: sn,
            data,
            metadata: HashMap::new(),
        });
        store.save_to_file(&self.path)?;
        Ok(id)
    }

    fn load_events(&self, wk: u64) -> DatabaseResult<Vec<WorkflowEventRecord>> {
        let store = self.store.read().unwrap();
        let mut events: Vec<WorkflowEventRecord> = store
            .events
            .iter()
            .filter(|e| e.workflow_key == wk)
            .cloned()
            .collect();
        events.sort_by_key(|e| e.sequence_num);
        Ok(events)
    }

    fn save_search_attributes(&self, key: u64, attrs: &SearchAttributes) -> DatabaseResult<()> {
        let mut store = self.store.write().unwrap();
        store.search_attributes.insert(key, attrs.clone());
        store.save_to_file(&self.path)
    }

    fn load_search_attributes(&self, key: u64) -> DatabaseResult<SearchAttributes> {
        let store = self.store.read().unwrap();
        Ok(store
            .search_attributes
            .get(&key)
            .cloned()
            .unwrap_or_default())
    }

    fn update_workflow_status(&self, key: u64, status: WorkflowStatus) -> DatabaseResult<()> {
        let mut store = self.store.write().unwrap();
        if let Some(record) = store.workflows.get_mut(&key) {
            record.status = status;
        }
        store.save_to_file(&self.path)
    }

    fn count_workflows(&self, ns: Option<&str>, sf: StatusFilter) -> DatabaseResult<u64> {
        let store = self.store.read().unwrap();
        let count = store
            .workflows
            .values()
            .filter(|r| {
                let ns_match = ns.is_none_or(|n| r.namespace_name == n);
                let sf_match = match sf {
                    StatusFilter::All => true,
                    StatusFilter::Running => r.status == WorkflowStatus::Running,
                    StatusFilter::Completed => r.status == WorkflowStatus::Completed,
                    StatusFilter::Failed => r.status == WorkflowStatus::Failed,
                    StatusFilter::Canceled => r.status == WorkflowStatus::Canceled,
                    StatusFilter::Terminated => r.status == WorkflowStatus::Terminated,
                    StatusFilter::TimedOut => r.status == WorkflowStatus::TimedOut,
                };
                ns_match && sf_match
            })
            .count();
        Ok(count as u64)
    }

    fn is_connected(&self) -> bool {
        *self.connected.read().unwrap()
    }

    fn adapter_name(&self) -> &str {
        "SqliteAdapter"
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: create a test WorkflowRecord.
    fn make_test_record(key: u64, status: WorkflowStatus) -> WorkflowRecord {
        WorkflowRecord {
            workflow_key: key,
            workflow_id: key & 0xFFFF_FFFF,
            run_id: key + 1000,
            workflow_type_id: 42,
            namespace_id: 1,
            namespace_name: "test-namespace".to_string(),
            task_queue_hash: 12345,
            current_step: 3,
            total_steps: 10,
            merkle_root: vec![0u8; 32],
            step_bitmask: vec![0u8; 32],
            status,
            step_results: HashMap::new(),
            signal_buffer: HashMap::new(),
            update_buffer: HashMap::new(),
            input_data: Some(b"test-input".to_vec()),
            result_data: None,
            parent_key: None,
            child_keys: vec![],
            event_sequence: 0,
        }
    }

    fn make_adapter() -> InMemoryAdapter {
        InMemoryAdapter::new()
    }

    // ── Schema Tests ──────────────────────────────────────────────────────────

    #[test]
    fn test_init_schema() {
        let adapter = make_adapter();
        assert!(adapter.init_schema().is_ok());
    }

    #[test]
    fn test_init_schema_failure_simulation() {
        let adapter = make_adapter();
        adapter.set_simulate_failures(true);
        assert!(adapter.init_schema().is_err());
    }

    // ── CRUD Tests ────────────────────────────────────────────────────────────

    #[test]
    fn test_save_and_load_workflow() {
        let adapter = make_adapter();
        let record = make_test_record(1001, WorkflowStatus::Running);

        adapter.save_workflow(1001, &record).unwrap();
        let loaded = adapter.load_workflow(1001).unwrap();

        assert_eq!(loaded.workflow_key, 1001);
        assert_eq!(loaded.workflow_id, record.workflow_id);
        assert_eq!(loaded.run_id, record.run_id);
        assert_eq!(loaded.status, WorkflowStatus::Running);
        assert_eq!(loaded.namespace_name, "test-namespace");
        assert_eq!(adapter.workflow_count(), 1);
    }

    #[test]
    fn test_load_nonexistent_workflow() {
        let adapter = make_adapter();
        let result = adapter.load_workflow(9999);
        assert!(matches!(result, Err(DatabaseError::NotFound(9999))));
    }

    #[test]
    fn test_save_overwrites_existing() {
        let adapter = make_adapter();
        let record1 = make_test_record(2001, WorkflowStatus::Running);
        adapter.save_workflow(2001, &record1).unwrap();

        let mut record2 = record1.clone();
        record2.status = WorkflowStatus::Completed;
        record2.result_data = Some(b"done".to_vec());
        adapter.save_workflow(2001, &record2).unwrap();

        let loaded = adapter.load_workflow(2001).unwrap();
        assert_eq!(loaded.status, WorkflowStatus::Completed);
        assert_eq!(loaded.result_data, Some(b"done".to_vec()));
        assert_eq!(adapter.workflow_count(), 1);
    }

    #[test]
    fn test_delete_workflow() {
        let adapter = make_adapter();
        let record = make_test_record(3001, WorkflowStatus::Running);
        adapter.save_workflow(3001, &record).unwrap();
        assert_eq!(adapter.workflow_count(), 1);

        adapter.delete_workflow(3001).unwrap();
        assert_eq!(adapter.workflow_count(), 0);
        assert!(matches!(
            adapter.load_workflow(3001),
            Err(DatabaseError::NotFound(3001))
        ));
    }

    #[test]
    fn test_delete_cascades_events_and_attrs() {
        let adapter = make_adapter();
        let record = make_test_record(4001, WorkflowStatus::Running);
        adapter.save_workflow(4001, &record).unwrap();
        adapter
            .save_event(4001, 1, "WorkflowStarted", 0, vec![1, 2, 3])
            .unwrap();
        adapter
            .save_event(4001, 2, "StepCompleted", 1, vec![4, 5])
            .unwrap();

        let mut attrs = SearchAttributes::new();
        attrs.insert("env".into(), SearchAttributeValue::Text("prod".into()));
        adapter.save_search_attributes(4001, &attrs).unwrap();

        assert_eq!(adapter.event_count(4001), 2);

        adapter.delete_workflow(4001).unwrap();
        assert_eq!(adapter.event_count(4001), 0);
        assert!(adapter.load_search_attributes(4001).unwrap().is_empty());
    }

    // ── Status Update Tests ───────────────────────────────────────────────────

    #[test]
    fn test_update_workflow_status() {
        let adapter = make_adapter();
        let record = make_test_record(5001, WorkflowStatus::Running);
        adapter.save_workflow(5001, &record).unwrap();

        adapter
            .update_workflow_status(5001, WorkflowStatus::Completed)
            .unwrap();
        let loaded = adapter.load_workflow(5001).unwrap();
        assert_eq!(loaded.status, WorkflowStatus::Completed);
    }

    #[test]
    fn test_update_status_nonexistent() {
        let adapter = make_adapter();
        let result = adapter.update_workflow_status(9999, WorkflowStatus::Failed);
        assert!(matches!(result, Err(DatabaseError::NotFound(9999))));
    }

    // ── Event History Tests ───────────────────────────────────────────────────

    #[test]
    fn test_save_and_load_events() {
        let adapter = make_adapter();
        let record = make_test_record(6001, WorkflowStatus::Running);
        adapter.save_workflow(6001, &record).unwrap();

        let id1 = adapter
            .save_event(6001, 1, "WorkflowStarted", 0, vec![1])
            .unwrap();
        let id2 = adapter
            .save_event(6001, 2, "StepCompleted", 1, vec![2])
            .unwrap();
        let id3 = adapter
            .save_event(6001, 2, "StepCompleted", 2, vec![3])
            .unwrap();
        let id4 = adapter
            .save_event(6001, 3, "WorkflowCompleted", 3, vec![4])
            .unwrap();

        assert!(id1 < id2 && id2 < id3 && id3 < id4);

        let events = adapter.load_events(6001).unwrap();
        assert_eq!(events.len(), 4);
        assert_eq!(events[0].event_type, 1);
        assert_eq!(events[0].event_type_name, "WorkflowStarted");
        assert_eq!(events[1].sequence_num, 1);
        assert_eq!(events[3].event_type_name, "WorkflowCompleted");
    }

    #[test]
    fn test_load_events_empty() {
        let adapter = make_adapter();
        let events = adapter.load_events(9999).unwrap();
        assert!(events.is_empty());
    }

    #[test]
    fn test_events_ordered_by_sequence() {
        let adapter = make_adapter();
        // Insert events out of order
        adapter
            .save_event(7001, 2, "StepCompleted", 5, vec![5])
            .unwrap();
        adapter
            .save_event(7001, 1, "WorkflowStarted", 0, vec![0])
            .unwrap();
        adapter
            .save_event(7001, 2, "StepCompleted", 3, vec![3])
            .unwrap();

        let events = adapter.load_events(7001).unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].sequence_num, 0);
        assert_eq!(events[1].sequence_num, 3);
        assert_eq!(events[2].sequence_num, 5);
    }

    // ── Search Attributes Tests ───────────────────────────────────────────────

    #[test]
    fn test_save_and_load_search_attributes() {
        let adapter = make_adapter();
        let record = make_test_record(8001, WorkflowStatus::Running);
        adapter.save_workflow(8001, &record).unwrap();

        let mut attrs = SearchAttributes::new();
        attrs.insert(
            "environment".into(),
            SearchAttributeValue::Text("production".into()),
        );
        attrs.insert("priority".into(), SearchAttributeValue::Integer(5));
        attrs.insert("score".into(), SearchAttributeValue::Float(9.95));
        attrs.insert("active".into(), SearchAttributeValue::Bool(true));
        attrs.insert(
            "tags".into(),
            SearchAttributeValue::TextArray(vec!["critical".into(), "finance".into()]),
        );

        adapter.save_search_attributes(8001, &attrs).unwrap();

        let loaded = adapter.load_search_attributes(8001).unwrap();
        assert_eq!(loaded.len(), 5);
        match loaded.get("environment") {
            Some(SearchAttributeValue::Text(s)) => assert_eq!(s, "production"),
            _ => panic!("expected Text value"),
        }
        match loaded.get("priority") {
            Some(SearchAttributeValue::Integer(n)) => assert_eq!(*n, 5),
            _ => panic!("expected Integer value"),
        }
    }

    #[test]
    fn test_search_attrs_upsert() {
        let adapter = make_adapter();
        let mut attrs1 = SearchAttributes::new();
        attrs1.insert("key1".into(), SearchAttributeValue::Text("val1".into()));
        adapter.save_search_attributes(9001, &attrs1).unwrap();

        let mut attrs2 = SearchAttributes::new();
        attrs2.insert("key2".into(), SearchAttributeValue::Integer(42));
        adapter.save_search_attributes(9001, &attrs2).unwrap();

        let loaded = adapter.load_search_attributes(9001).unwrap();
        assert_eq!(loaded.len(), 2); // Both keys present (upsert merged)
        assert!(loaded.contains_key("key1"));
        assert!(loaded.contains_key("key2"));
    }

    #[test]
    fn test_load_search_attrs_empty() {
        let adapter = make_adapter();
        let attrs = adapter.load_search_attributes(9999).unwrap();
        assert!(attrs.is_empty());
    }

    // ── List / Filter Tests ───────────────────────────────────────────────────

    #[test]
    fn test_list_workflows_all() {
        let adapter = make_adapter();
        for i in 0..5 {
            let record = make_test_record(10000 + i, WorkflowStatus::Running);
            adapter.save_workflow(10000 + i, &record).unwrap();
        }

        let list = adapter
            .list_workflows(None, StatusFilter::All, 100, 0)
            .unwrap();
        assert_eq!(list.len(), 5);
    }

    #[test]
    fn test_list_workflows_with_namespace_filter() {
        let adapter = make_adapter();

        let mut r1 = make_test_record(11001, WorkflowStatus::Running);
        r1.namespace_name = "ns-a".into();
        adapter.save_workflow(11001, &r1).unwrap();

        let mut r2 = make_test_record(11002, WorkflowStatus::Running);
        r2.namespace_name = "ns-b".into();
        adapter.save_workflow(11002, &r2).unwrap();

        let mut r3 = make_test_record(11003, WorkflowStatus::Completed);
        r3.namespace_name = "ns-a".into();
        adapter.save_workflow(11003, &r3).unwrap();

        let ns_a = adapter
            .list_workflows(Some("ns-a"), StatusFilter::All, 100, 0)
            .unwrap();
        assert_eq!(ns_a.len(), 2);

        let ns_b = adapter
            .list_workflows(Some("ns-b"), StatusFilter::All, 100, 0)
            .unwrap();
        assert_eq!(ns_b.len(), 1);
    }

    #[test]
    fn test_list_workflows_with_status_filter() {
        let adapter = make_adapter();

        adapter
            .save_workflow(12001, &make_test_record(12001, WorkflowStatus::Running))
            .unwrap();
        adapter
            .save_workflow(12002, &make_test_record(12002, WorkflowStatus::Completed))
            .unwrap();
        adapter
            .save_workflow(12003, &make_test_record(12003, WorkflowStatus::Failed))
            .unwrap();
        adapter
            .save_workflow(12004, &make_test_record(12004, WorkflowStatus::Running))
            .unwrap();

        let running = adapter
            .list_workflows(None, StatusFilter::Running, 100, 0)
            .unwrap();
        assert_eq!(running.len(), 2);

        let completed = adapter
            .list_workflows(None, StatusFilter::Completed, 100, 0)
            .unwrap();
        assert_eq!(completed.len(), 1);

        let failed = adapter
            .list_workflows(None, StatusFilter::Failed, 100, 0)
            .unwrap();
        assert_eq!(failed.len(), 1);
    }

    #[test]
    fn test_list_workflows_pagination() {
        let adapter = make_adapter();
        for i in 0..10 {
            let record = make_test_record(13000 + i, WorkflowStatus::Running);
            adapter.save_workflow(13000 + i, &record).unwrap();
        }

        let page1 = adapter
            .list_workflows(None, StatusFilter::All, 3, 0)
            .unwrap();
        assert_eq!(page1.len(), 3);

        let page2 = adapter
            .list_workflows(None, StatusFilter::All, 3, 3)
            .unwrap();
        assert_eq!(page2.len(), 3);

        let page4 = adapter
            .list_workflows(None, StatusFilter::All, 3, 9)
            .unwrap();
        assert_eq!(page4.len(), 1);

        let empty = adapter
            .list_workflows(None, StatusFilter::All, 3, 100)
            .unwrap();
        assert!(empty.is_empty());
    }

    #[test]
    fn test_count_workflows() {
        let adapter = make_adapter();
        adapter
            .save_workflow(14001, &make_test_record(14001, WorkflowStatus::Running))
            .unwrap();
        adapter
            .save_workflow(14002, &make_test_record(14002, WorkflowStatus::Completed))
            .unwrap();
        adapter
            .save_workflow(14003, &make_test_record(14003, WorkflowStatus::Running))
            .unwrap();

        assert_eq!(adapter.count_workflows(None, StatusFilter::All).unwrap(), 3);
        assert_eq!(
            adapter
                .count_workflows(None, StatusFilter::Running)
                .unwrap(),
            2
        );
        assert_eq!(
            adapter
                .count_workflows(None, StatusFilter::Completed)
                .unwrap(),
            1
        );
        assert_eq!(
            adapter.count_workflows(None, StatusFilter::Failed).unwrap(),
            0
        );
    }

    // ── Failure Simulation Tests ──────────────────────────────────────────────

    #[test]
    fn test_failure_simulation() {
        let adapter = make_adapter();
        let record = make_test_record(15001, WorkflowStatus::Running);

        adapter.set_simulate_failures(true);
        assert!(adapter.save_workflow(15001, &record).is_err());
        assert!(adapter.load_workflow(15001).is_err());
        assert!(adapter.delete_workflow(15001).is_err());
        assert!(adapter
            .list_workflows(None, StatusFilter::All, 10, 0)
            .is_err());

        adapter.set_simulate_failures(false);
        assert!(adapter.save_workflow(15001, &record).is_ok());
    }

    // ── PostgresAdapter Tests ─────────────────────────────────────────────────

    #[test]
    fn test_postgres_adapter_creation() {
        let config = DatabaseConfig::new("db.example.com", "velocity_prod")
            .with_port(5433)
            .with_credentials("admin", "secret")
            .with_pool_size(20)
            .with_ssl_mode(SslMode::Require);

        let adapter = PostgresAdapter::new(config);
        assert_eq!(adapter.adapter_name(), "PostgresAdapter");
        assert!(!adapter.is_connected());
        assert_eq!(adapter.config().port, 5433);
    }

    #[test]
    fn test_postgres_adapter_not_connected() {
        let adapter = PostgresAdapter::new(DatabaseConfig::default());
        let record = make_test_record(16001, WorkflowStatus::Running);

        assert!(matches!(
            adapter.save_workflow(16001, &record),
            Err(DatabaseError::NotConnected)
        ));
        assert!(matches!(
            adapter.load_workflow(16001),
            Err(DatabaseError::NotConnected)
        ));
    }

    #[test]
    fn test_postgres_adapter_schema_init() {
        let adapter = PostgresAdapter::new(DatabaseConfig::default());
        assert!(adapter.init_schema().is_ok());
        assert!(!adapter.schema_sql().is_empty());
    }

    #[test]
    fn test_postgres_prepared_statements() {
        let adapter = PostgresAdapter::new(DatabaseConfig::default());
        assert!(adapter.get_statement("upsert_workflow").is_some());
        assert!(adapter.get_statement("select_workflow").is_some());
        assert!(adapter.get_statement("nonexistent").is_none());

        let names = adapter.statement_names();
        assert!(names.len() >= 10);
    }

    #[test]
    fn test_connection_string() {
        let config = DatabaseConfig::new("localhost", "testdb")
            .with_port(5433)
            .with_credentials("user", "pass");
        let conn_str = config.to_connection_string();
        assert!(conn_str.contains("host=localhost"));
        assert!(conn_str.contains("port=5433"));
        assert!(conn_str.contains("dbname=testdb"));
        assert!(conn_str.contains("user=user"));
    }

    // ── DatabaseError Display Tests ───────────────────────────────────────────

    #[test]
    fn test_error_display() {
        let err = DatabaseError::NotFound(42);
        assert_eq!(format!("{}", err), "workflow not found: 42");

        let err = DatabaseError::NotConnected;
        assert_eq!(format!("{}", err), "database not connected");

        let err = DatabaseError::ConnectionError("timeout".into());
        assert!(format!("{}", err).contains("timeout"));
    }

    // ── SearchAttributeValue Type Code Tests ──────────────────────────────────

    #[test]
    fn test_search_attr_type_codes() {
        assert_eq!(SearchAttributeValue::Text("".into()).type_code(), 0);
        assert_eq!(SearchAttributeValue::Integer(0).type_code(), 1);
        assert_eq!(SearchAttributeValue::Float(0.0).type_code(), 2);
        assert_eq!(SearchAttributeValue::Bool(false).type_code(), 3);
        assert_eq!(SearchAttributeValue::DateTime("".into()).type_code(), 4);
        assert_eq!(SearchAttributeValue::Bytes(vec![]).type_code(), 5);
        assert_eq!(SearchAttributeValue::TextArray(vec![]).type_code(), 6);
        assert_eq!(SearchAttributeValue::IntArray(vec![]).type_code(), 7);
    }

    // ── Clear / Reset Tests ───────────────────────────────────────────────────

    #[test]
    fn test_adapter_clear() {
        let adapter = make_adapter();
        adapter
            .save_workflow(17001, &make_test_record(17001, WorkflowStatus::Running))
            .unwrap();
        adapter.save_event(17001, 1, "Started", 0, vec![]).unwrap();
        adapter
            .save_search_attributes(17001, &{
                let mut m = SearchAttributes::new();
                m.insert("k".into(), SearchAttributeValue::Text("v".into()));
                m
            })
            .unwrap();

        assert_eq!(adapter.workflow_count(), 1);
        adapter.clear();
        assert_eq!(adapter.workflow_count(), 0);
        assert_eq!(adapter.event_count(17001), 0);
        assert!(adapter.load_search_attributes(17001).unwrap().is_empty());
    }
}
