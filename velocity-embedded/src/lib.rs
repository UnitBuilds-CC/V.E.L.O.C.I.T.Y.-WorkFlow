//! Velocity Embedded — DBOS-compatible durable execution library.
//!
//! This crate provides a library-style API for durable execution that embeds
//! directly into your application process (no separate server needed).
//!
//! # Features
//! - **Durable functions**: Steps survive crashes and restarts
//! - **Postgres-backed**: All state stored in Postgres tables
//! - **Type-safe**: Full Rust type system support
//! - **Transactional**: Steps can participate in DB transactions
//!
//! # Example
//! ```ignore
//! use velocity_embedded::{EmbeddedEngine, DurableContext};
//!
//! async fn my_workflow(ctx: &DurableContext, name: &str) -> String {
//!     let greeting = ctx.run("greet", || async { format!("Hello, {}!", name) }).await;
//!     greeting
//! }
//! ```

mod durable;
mod postgres_adapter;
mod storage;

pub use durable::{DurableContext, TransientContext, WorkflowHandle, WorkflowStatus};
pub use postgres_adapter::{PostgresAdapter, PostgresConfig};
pub use storage::{InMemoryStorage, StorageBackend, StorageError};

use serde::{Deserialize, Serialize};
use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

// ─── Embedded Engine ─────────────────────────────────────────────────────────

/// Configuration for the embedded engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbeddedConfig {
    /// Database URL (e.g., "postgres://user:pass@localhost/velocity")
    pub database_url: String,
    /// Maximum concurrent workflows
    pub max_concurrent_workflows: usize,
    /// Worker ID for this instance
    pub worker_id: String,
    /// Enable automatic schema migration on startup
    pub auto_migrate: bool,
    /// Polling interval for pending workflows (ms)
    pub poll_interval_ms: u64,
}

impl Default for EmbeddedConfig {
    fn default() -> Self {
        Self {
            database_url: "postgres://localhost:5432/velocity".to_string(),
            max_concurrent_workflows: 100,
            worker_id: format!("worker-{}", std::process::id()),
            auto_migrate: true,
            poll_interval_ms: 100,
        }
    }
}

/// The embedded durable execution engine.
///
/// This is the main entry point for the DBOS-compatible flavor.
/// It wraps the Velocity engine and provides a library-style API.
pub struct EmbeddedEngine {
    config: EmbeddedConfig,
    storage: Arc<Mutex<Box<dyn StorageBackend>>>,
    workflows: Arc<Mutex<HashMap<String, WorkflowRecord>>>,
    sequence_counters: Arc<Mutex<HashMap<String, u64>>>,
    running: Arc<Mutex<bool>>,
}

/// A record of a workflow execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowRecord {
    pub workflow_id: String,
    pub function_name: String,
    pub status: WorkflowStatus,
    pub input: Option<serde_json::Value>,
    pub output: Option<serde_json::Value>,
    pub error: Option<String>,
    pub created_at: u64,
    pub updated_at: u64,
    pub journal: Vec<JournalEntry>,
}

/// A journal entry for crash recovery.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JournalEntry {
    pub sequence: u64,
    pub function_name: String,
    pub input: Option<serde_json::Value>,
    pub output: Option<serde_json::Value>,
    pub error: Option<String>,
    pub completed: bool,
}

impl EmbeddedEngine {
    /// Create a new embedded engine with the given configuration.
    pub fn new(config: EmbeddedConfig) -> Self {
        Self {
            config,
            storage: Arc::new(Mutex::new(Box::new(InMemoryStorage::new()))),
            workflows: Arc::new(Mutex::new(HashMap::new())),
            sequence_counters: Arc::new(Mutex::new(HashMap::new())),
            running: Arc::new(Mutex::new(false)),
        }
    }

    /// Create an engine with a custom storage backend (e.g., Postgres).
    pub fn with_storage(config: EmbeddedConfig, storage: Box<dyn StorageBackend>) -> Self {
        Self {
            config,
            storage: Arc::new(Mutex::new(storage)),
            workflows: Arc::new(Mutex::new(HashMap::new())),
            sequence_counters: Arc::new(Mutex::new(HashMap::new())),
            running: Arc::new(Mutex::new(false)),
        }
    }

    /// Initialize the engine (run migrations, etc.).
    pub fn init(&self) -> Result<(), EmbeddedError> {
        if self.config.auto_migrate {
            let storage = self.storage.lock().map_err(|_| EmbeddedError::LockPoisoned)?;
            storage.init_schema().map_err(|e| EmbeddedError::Storage(e.to_string()))?;
        }
        *self.running.lock().map_err(|_| EmbeddedError::LockPoisoned)? = true;
        Ok(())
    }

    /// Execute a durable workflow function.
    ///
    /// If a workflow with the same ID already exists and completed, returns
    /// the cached result. If it's in-progress, returns a handle to wait on it.
    /// Otherwise, creates a new workflow and executes it.
    pub async fn execute<F, Fut, I, O>(
        &self,
        workflow_id: &str,
        function_name: &str,
        input: I,
        handler: F,
    ) -> Result<WorkflowHandle<O>, EmbeddedError>
    where
        F: FnOnce(DurableContext, I) -> Fut,
        Fut: std::future::Future<Output = Result<O, EmbeddedError>>,
        I: Serialize + DeserializeOwned + Clone,
        O: Serialize + DeserializeOwned + Clone,
    {
        // Check for existing workflow
        {
            let workflows = self.workflows.lock().map_err(|_| EmbeddedError::LockPoisoned)?;
            if let Some(existing) = workflows.get(workflow_id) {
                match existing.status {
                    WorkflowStatus::Completed => {
                        if let Some(ref output) = existing.output {
                            let result: O = serde_json::from_value(output.clone())
                                .map_err(|e| EmbeddedError::Serialization(e.to_string()))?;
                            return Ok(WorkflowHandle::completed(workflow_id.to_string(), result));
                        }
                    }
                    WorkflowStatus::Failed => {
                        return Ok(WorkflowHandle::failed(
                            workflow_id.to_string(),
                            existing.error.clone().unwrap_or_default(),
                        ));
                    }
                    _ => {
                        return Ok(WorkflowHandle::running(workflow_id.to_string()));
                    }
                }
            }
        }

        // Create workflow record
        let now = current_time_ms();
        let input_value = serde_json::to_value(&input)
            .map_err(|e| EmbeddedError::Serialization(e.to_string()))?;

        let record = WorkflowRecord {
            workflow_id: workflow_id.to_string(),
            function_name: function_name.to_string(),
            status: WorkflowStatus::Running,
            input: Some(input_value),
            output: None,
            error: None,
            created_at: now,
            updated_at: now,
            journal: Vec::new(),
        };

        {
            let mut workflows = self.workflows.lock().map_err(|_| EmbeddedError::LockPoisoned)?;
            workflows.insert(workflow_id.to_string(), record);
        }

        // Create durable context
        let ctx = DurableContext::new(
            workflow_id.to_string(),
            self.storage.clone(),
            self.sequence_counters.clone(),
        );

        // Execute the handler
        match handler(ctx, input).await {
            Ok(output) => {
                let output_value = serde_json::to_value(&output)
                    .map_err(|e| EmbeddedError::Serialization(e.to_string()))?;

                let mut workflows = self.workflows.lock().map_err(|_| EmbeddedError::LockPoisoned)?;
                if let Some(record) = workflows.get_mut(workflow_id) {
                    record.status = WorkflowStatus::Completed;
                    record.output = Some(output_value.clone());
                    record.updated_at = current_time_ms();
                }

                // Persist to storage
                if let Ok(storage) = self.storage.lock() {
                    let _ = storage.save_workflow(workflow_id, function_name, &output_value);
                }

                Ok(WorkflowHandle::completed(workflow_id.to_string(), output))
            }
            Err(err) => {
                let mut workflows = self.workflows.lock().map_err(|_| EmbeddedError::LockPoisoned)?;
                if let Some(record) = workflows.get_mut(workflow_id) {
                    record.status = WorkflowStatus::Failed;
                    record.error = Some(err.to_string());
                    record.updated_at = current_time_ms();
                }
                Ok(WorkflowHandle::failed(workflow_id.to_string(), err.to_string()))
            }
        }
    }

    /// Execute a transient (non-durable) function.
    pub async fn execute_transient<F, Fut, O>(
        &self,
        handler: F,
    ) -> Result<O, EmbeddedError>
    where
        F: FnOnce(TransientContext) -> Fut,
        Fut: std::future::Future<Output = Result<O, EmbeddedError>>,
    {
        let ctx = TransientContext::new();
        handler(ctx).await
    }

    /// Get the status of a workflow.
    pub fn get_workflow_status(&self, workflow_id: &str) -> Option<WorkflowRecord> {
        let workflows = self.workflows.lock().ok()?;
        workflows.get(workflow_id).cloned()
    }

    /// List all workflows.
    pub fn list_workflows(&self) -> Vec<WorkflowRecord> {
        let workflows = self.workflows.lock().ok()
            .map(|g| g.values().cloned().collect())
            .unwrap_or_default();
        workflows
    }

    /// Get engine statistics.
    pub fn stats(&self) -> EngineStats {
        let workflows = self.workflows.lock().unwrap_or_else(|e| e.into_inner());
        let total = workflows.len();
        let running = workflows.values().filter(|w| w.status == WorkflowStatus::Running).count();
        let completed = workflows.values().filter(|w| w.status == WorkflowStatus::Completed).count();
        let failed = workflows.values().filter(|w| w.status == WorkflowStatus::Failed).count();

        EngineStats {
            total_workflows: total,
            running,
            completed,
            failed,
            worker_id: self.config.worker_id.clone(),
        }
    }

    /// Shut down the engine.
    pub fn shutdown(&self) -> Result<(), EmbeddedError> {
        *self.running.lock().map_err(|_| EmbeddedError::LockPoisoned)? = false;
        Ok(())
    }
}

/// Engine statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineStats {
    pub total_workflows: usize,
    pub running: usize,
    pub completed: usize,
    pub failed: usize,
    pub worker_id: String,
}

/// Errors from the embedded engine.
#[derive(Debug, Clone)]
pub enum EmbeddedError {
    /// Storage backend error
    Storage(String),
    /// Serialization error
    Serialization(String),
    /// Workflow execution error
    Execution(String),
    /// Lock poisoned
    LockPoisoned,
    /// Not found
    NotFound(String),
}

impl std::fmt::Display for EmbeddedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Storage(msg) => write!(f, "Storage error: {}", msg),
            Self::Serialization(msg) => write!(f, "Serialization error: {}", msg),
            Self::Execution(msg) => write!(f, "Execution error: {}", msg),
            Self::LockPoisoned => write!(f, "Lock poisoned"),
            Self::NotFound(msg) => write!(f, "Not found: {}", msg),
        }
    }
}

impl std::error::Error for EmbeddedError {}

fn current_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> EmbeddedConfig {
        EmbeddedConfig {
            database_url: "memory://test".to_string(),
            auto_migrate: true,
            ..Default::default()
        }
    }

    #[test]
    fn test_engine_creation() {
        let engine = EmbeddedEngine::new(test_config());
        let stats = engine.stats();
        assert_eq!(stats.total_workflows, 0);
        assert_eq!(stats.running, 0);
    }

    #[test]
    fn test_engine_init() {
        let engine = EmbeddedEngine::new(test_config());
        assert!(engine.init().is_ok());
    }

    #[test]
    fn test_workflow_record_serialization() {
        let record = WorkflowRecord {
            workflow_id: "wf-1".to_string(),
            function_name: "test_fn".to_string(),
            status: WorkflowStatus::Running,
            input: Some(serde_json::json!({"key": "value"})),
            output: None,
            error: None,
            created_at: 1000,
            updated_at: 1000,
            journal: vec![],
        };

        let json = serde_json::to_string(&record).unwrap();
        let deserialized: WorkflowRecord = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.workflow_id, "wf-1");
        assert_eq!(deserialized.function_name, "test_fn");
    }

    #[tokio::test]
    async fn test_execute_simple_workflow() {
        let engine = EmbeddedEngine::new(test_config());
        engine.init().unwrap();

        let handle = engine
            .execute("wf-1", "greet", "World".to_string(), |_ctx, input: String| async move {
                Ok(format!("Hello, {}!", input))
            })
            .await
            .unwrap();

        assert!(handle.is_completed());
        assert_eq!(handle.result().unwrap(), "Hello, World!");
    }

    #[tokio::test]
    async fn test_execute_with_durable_step() {
        let engine = EmbeddedEngine::new(test_config());
        engine.init().unwrap();

        let handle = engine
            .execute("wf-2", "compute", 42u32, |mut ctx, input: u32| async move {
                let doubled = ctx.run("double", move || async move { input * 2 }).await?;
                let tripled = ctx.run("triple", move || async move { input * 3 }).await?;
                Ok((doubled, tripled))
            })
            .await
            .unwrap();

        assert!(handle.is_completed());
        let (d, t) = handle.result().unwrap();
        assert_eq!(*d, 84);
        assert_eq!(*t, 126);
    }

    #[tokio::test]
    async fn test_workflow_idempotency() {
        let engine = EmbeddedEngine::new(test_config());
        engine.init().unwrap();

        let h1 = engine
            .execute("wf-idem", "fn", "x".to_string(), |_ctx, input: String| async move {
                Ok(format!("done: {}", input))
            })
            .await
            .unwrap();

        // Second call with same ID returns cached result
        let h2 = engine
            .execute("wf-idem", "fn", "x".to_string(), |_ctx, input: String| async move {
                Ok(format!("done: {}", input))
            })
            .await
            .unwrap();

        assert_eq!(h1.result(), h2.result());
    }

    #[tokio::test]
    async fn test_workflow_failure() {
        let engine = EmbeddedEngine::new(test_config());
        engine.init().unwrap();

        let handle = engine
            .execute("wf-fail", "fail_fn", "x".to_string(), |_ctx, _: String| async move {
                Err::<String, _>(EmbeddedError::Execution("intentional".to_string()))
            })
            .await
            .unwrap();

        assert!(handle.is_failed());
        assert!(handle.error().unwrap().contains("intentional"));
    }

    #[tokio::test]
    async fn test_transient_execution() {
        let engine = EmbeddedEngine::new(test_config());
        engine.init().unwrap();

        let result = engine
            .execute_transient(|_ctx| async { Ok(42u32) })
            .await
            .unwrap();

        assert_eq!(result, 42);
    }

    #[tokio::test]
    async fn test_workflow_status_tracking() {
        let engine = EmbeddedEngine::new(test_config());
        engine.init().unwrap();

        engine
            .execute("wf-status", "fn", "a".to_string(), |_ctx, _: String| async {
                Ok("done".to_string())
            })
            .await
            .unwrap();

        let record = engine.get_workflow_status("wf-status").unwrap();
        assert_eq!(record.status, WorkflowStatus::Completed);
        assert_eq!(record.function_name, "fn");
    }

    #[tokio::test]
    async fn test_list_workflows() {
        let engine = EmbeddedEngine::new(test_config());
        engine.init().unwrap();

        engine
            .execute("wf-a", "fn1", "a".to_string(), |_ctx, _: String| async { Ok("a".to_string()) })
            .await
            .unwrap();
        engine
            .execute("wf-b", "fn2", "b".to_string(), |_ctx, _: String| async { Ok("b".to_string()) })
            .await
            .unwrap();

        let list = engine.list_workflows();
        assert_eq!(list.len(), 2);
    }

    #[tokio::test]
    async fn test_engine_stats() {
        let engine = EmbeddedEngine::new(test_config());
        engine.init().unwrap();

        engine
            .execute("wf-s1", "fn", "x".to_string(), |_ctx, _: String| async { Ok("ok".to_string()) })
            .await
            .unwrap();
        let _ = engine
            .execute::<_, _, _, String>("wf-s2", "fn", "x".to_string(), |_ctx, _: String| async {
                Err::<String, _>(EmbeddedError::Execution("err".to_string()))
            })
            .await;

        let stats = engine.stats();
        assert_eq!(stats.total_workflows, 2);
        assert_eq!(stats.completed, 1);
        assert_eq!(stats.failed, 1);
    }

    #[test]
    fn test_engine_shutdown() {
        let engine = EmbeddedEngine::new(test_config());
        engine.init().unwrap();
        assert!(engine.shutdown().is_ok());
    }

    #[test]
    fn test_journal_entry_serialization() {
        let entry = JournalEntry {
            sequence: 0,
            function_name: "step1".to_string(),
            input: Some(serde_json::json!(42)),
            output: Some(serde_json::json!(84)),
            error: None,
            completed: true,
        };

        let json = serde_json::to_string(&entry).unwrap();
        let back: JournalEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(back.sequence, 0);
        assert!(back.completed);
    }

    #[test]
    fn test_config_defaults() {
        let config = EmbeddedConfig::default();
        assert_eq!(config.max_concurrent_workflows, 100);
        assert!(config.auto_migrate);
        assert_eq!(config.poll_interval_ms, 100);
    }

    #[test]
    fn test_embedded_error_display() {
        let err = EmbeddedError::Storage("connection refused".to_string());
        assert_eq!(format!("{}", err), "Storage error: connection refused");

        let err = EmbeddedError::NotFound("wf-999".to_string());
        assert_eq!(format!("{}", err), "Not found: wf-999");
    }
}
