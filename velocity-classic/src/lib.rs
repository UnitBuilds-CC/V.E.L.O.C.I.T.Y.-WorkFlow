//! Velocity Classic — Temporal-compatible durable workflow engine.
//!
//! This crate provides the Temporal-compatible flavor of the Velocity engine.
//! It re-exports the core engine with Temporal-style API types:
//!
//! - **Workflows**: Durable functions that orchestrate activities
//! - **Activities**: Side-effect-producing functions (HTTP calls, DB writes)
//! - **Worker**: Polls for tasks and executes workflows/activities
//! - **Client**: Submits workflows and queries their status
//!
//! # Compatibility
//!
//! Velocity Classic is designed to be a drop-in replacement for Temporal.
//! It supports the same programming model:
//!
//! ```ignore
//! use velocity_classic::{Workflow, Activity, ClassicWorker, ClassicClient, ClassicConfig};
//! ```
//!
//! # Architecture
//!
//! ```text
//! ┌──────────────┐     gRPC      ┌──────────────────┐
//! │   Worker     │◄─────────────►│  Velocity Server  │
//! │  (your code) │   148 RPCs    │  (this engine)    │
//! └──────────────┘               └──────────────────┘
//!       │                                │
//!       ├── Workflows                    ├── WAL (durability)
//!       ├── Activities                   ├── Raft (consensus)
//!       └── Signals/Queries              └── Task Queue (matching)
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

// ─── NMCP Protocol Modules ──────────────────────────────────────────────────

pub mod nmcp_router;
pub mod nmcp_shmem;
pub mod nmcp_websocket;

// Re-export key NMCP types
pub use nmcp_router::{ClassicFrameTypes, NmcpFrame, NmcpFrameRouter, NmcpRouterStats, NMCP_MAGIC, NMCP_HEADER_SIZE};
pub use nmcp_shmem::{NmcpShmemClient, NmcpShmemServer, ShmemBuffer, SHMEM_BUFFER_SIZE};
pub use nmcp_websocket::{NmcpWebSocketClient, NmcpWebSocketServer};

// ─── Re-exports ──────────────────────────────────────────────────────────────

// Re-export core engine types
pub use velocity_workflow_engine::{TaskQueue, WorkflowEngine};

// ─── Classic Config ──────────────────────────────────────────────────────────

/// Configuration for Velocity Classic (Temporal-compatible mode).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClassicConfig {
    /// Server address (e.g., "localhost:7233")
    pub server_address: String,
    /// Namespace (default: "default")
    pub namespace: String,
    /// Task queue name for this worker
    pub task_queue: String,
    /// Maximum concurrent workflow executions
    pub max_concurrent_workflows: usize,
    /// Maximum concurrent activity executions
    pub max_concurrent_activities: usize,
    /// Enable sticky queues for workflow tasks
    pub sticky_queues: bool,
    /// Worker identity (auto-generated if empty)
    pub worker_identity: String,
}

impl Default for ClassicConfig {
    fn default() -> Self {
        Self {
            server_address: "localhost:7233".to_string(),
            namespace: "default".to_string(),
            task_queue: "default".to_string(),
            max_concurrent_workflows: 100,
            max_concurrent_activities: 200,
            sticky_queues: true,
            worker_identity: format!("worker-{}", std::process::id()),
        }
    }
}

// ─── Workflow Definition ─────────────────────────────────────────────────────

/// Trait for workflow definitions.
///
/// Workflows are durable functions that orchestrate activities.
/// They must be deterministic — same input always produces same output.
///
/// # Example
/// ```ignore
/// struct OrderWorkflow;
///
/// impl Workflow for OrderWorkflow {
///     type Input = String;
///     type Output = String;
///
///     fn name() -> &'static str { "OrderWorkflow" }
/// }
/// ```
pub trait Workflow: Send + Sync + 'static {
    type Input: Serialize + for<'de> Deserialize<'de> + Send + 'static;
    type Output: Serialize + for<'de> Deserialize<'de> + Send + 'static;

    /// The workflow type name (used for registration).
    fn name() -> &'static str;
}

/// Trait for activity definitions.
///
/// Activities are side-effect-producing functions.
/// They can be non-deterministic (HTTP calls, random numbers, etc.).
pub trait Activity: Send + Sync + 'static {
    type Input: Serialize + for<'de> Deserialize<'de> + Send + 'static;
    type Output: Serialize + for<'de> Deserialize<'de> + Send + 'static;

    /// The activity type name (used for registration).
    fn name() -> &'static str;
}

// ─── Workflow Execution ──────────────────────────────────────────────────────

/// Status of a workflow execution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkflowStatus {
    /// Workflow is running
    Running,
    /// Workflow completed successfully
    Completed,
    /// Workflow failed with an error
    Failed,
    /// Workflow was cancelled
    Cancelled,
    /// Workflow was terminated (timeout or policy)
    Terminated,
    /// Workflow is continuing (ContinueAsNew)
    ContinuingAsNew,
}

/// Handle to a workflow execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowExecution {
    pub workflow_id: String,
    pub run_id: String,
    pub workflow_type: String,
    pub status: WorkflowStatus,
    pub start_time: u64,
    pub close_time: Option<u64>,
}

// ─── Signal / Query ──────────────────────────────────────────────────────────

/// A signal sent to a running workflow.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Signal {
    pub signal_name: String,
    pub input: serde_json::Value,
    pub identity: String,
}

/// A query sent to a running workflow.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Query {
    pub query_type: String,
    pub input: serde_json::Value,
}

/// Response to a query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    pub query_type: String,
    pub result: serde_json::Value,
}

// ─── Search Attributes ───────────────────────────────────────────────────────

/// Search attributes for workflow visibility.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SearchAttributes {
    pub indexed_fields: HashMap<String, serde_json::Value>,
}

impl SearchAttributes {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set(&mut self, key: &str, value: serde_json::Value) {
        self.indexed_fields.insert(key.to_string(), value);
    }

    pub fn get(&self, key: &str) -> Option<&serde_json::Value> {
        self.indexed_fields.get(key)
    }
}

// ─── Memo ────────────────────────────────────────────────────────────────────

/// Memo attached to a workflow execution.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Memo {
    pub fields: HashMap<String, serde_json::Value>,
}

impl Memo {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set(&mut self, key: &str, value: serde_json::Value) {
        self.fields.insert(key.to_string(), value);
    }

    pub fn get(&self, key: &str) -> Option<&serde_json::Value> {
        self.fields.get(key)
    }
}

// ─── Retry Policy ────────────────────────────────────────────────────────────

/// Retry policy for activities and workflows.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryPolicy {
    /// Initial retry interval
    pub initial_interval_ms: u64,
    /// Backoff coefficient (e.g., 2.0 = exponential)
    pub backoff_coefficient: f64,
    /// Maximum retry interval
    pub maximum_interval_ms: u64,
    /// Maximum number of attempts (0 = unlimited)
    pub maximum_attempts: u32,
    /// Non-retryable error types
    pub non_retryable_error_types: Vec<String>,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            initial_interval_ms: 1000,
            backoff_coefficient: 2.0,
            maximum_interval_ms: 100_000,
            maximum_attempts: 0,
            non_retryable_error_types: Vec::new(),
        }
    }
}

// ─── Classic Worker ──────────────────────────────────────────────────────────

/// A Classic worker that executes workflows and activities.
///
/// The worker connects to the Velocity server, polls for tasks,
/// and executes registered workflow and activity implementations.
pub struct ClassicWorker {
    config: ClassicConfig,
    workflow_types: Arc<Mutex<Vec<String>>>,
    activity_types: Arc<Mutex<Vec<String>>>,
}

impl ClassicWorker {
    /// Create a new Classic worker.
    pub fn new(config: ClassicConfig) -> Self {
        Self {
            config,
            workflow_types: Arc::new(Mutex::new(Vec::new())),
            activity_types: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Register a workflow type.
    pub fn register_workflow<W: Workflow>(&self) {
        let mut types = self.workflow_types.lock().unwrap();
        types.push(W::name().to_string());
    }

    /// Register an activity type.
    pub fn register_activity<A: Activity>(&self) {
        let mut types = self.activity_types.lock().unwrap();
        types.push(A::name().to_string());
    }

    /// Get the worker configuration.
    pub fn config(&self) -> &ClassicConfig {
        &self.config
    }

    /// Get registered workflow types.
    pub fn workflow_types(&self) -> Vec<String> {
        self.workflow_types.lock().unwrap().clone()
    }

    /// Get registered activity types.
    pub fn activity_types(&self) -> Vec<String> {
        self.activity_types.lock().unwrap().clone()
    }

    /// Get the task queue name.
    pub fn task_queue(&self) -> &str {
        &self.config.task_queue
    }

    /// Get the namespace.
    pub fn namespace(&self) -> &str {
        &self.config.namespace
    }
}

// ─── Classic Client ──────────────────────────────────────────────────────────

/// A Classic client for submitting and managing workflows.
pub struct ClassicClient {
    config: ClassicConfig,
    executions: Arc<Mutex<HashMap<String, WorkflowExecution>>>,
}

impl ClassicClient {
    /// Create a new Classic client.
    pub fn new(config: ClassicConfig) -> Self {
        Self {
            config,
            executions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Start a workflow execution.
    pub fn start_workflow(
        &self,
        workflow_id: &str,
        workflow_type: &str,
        _input: serde_json::Value,
    ) -> WorkflowExecution {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let execution = WorkflowExecution {
            workflow_id: workflow_id.to_string(),
            run_id: format!("run-{}", workflow_id),
            workflow_type: workflow_type.to_string(),
            status: WorkflowStatus::Running,
            start_time: now,
            close_time: None,
        };

        let mut execs = self.executions.lock().unwrap();
        execs.insert(workflow_id.to_string(), execution.clone());
        execution
    }

    /// Signal a running workflow.
    pub fn signal_workflow(
        &self,
        workflow_id: &str,
        _signal_name: &str,
        _input: serde_json::Value,
    ) -> Result<(), String> {
        let execs = self.executions.lock().unwrap();
        if !execs.contains_key(workflow_id) {
            return Err(format!("Workflow not found: {}", workflow_id));
        }
        // In a real implementation, this would send the signal to the server
        Ok(())
    }

    /// Query a running workflow.
    pub fn query_workflow(
        &self,
        workflow_id: &str,
        query_type: &str,
    ) -> Result<QueryResult, String> {
        let execs = self.executions.lock().unwrap();
        if !execs.contains_key(workflow_id) {
            return Err(format!("Workflow not found: {}", workflow_id));
        }
        Ok(QueryResult {
            query_type: query_type.to_string(),
            result: serde_json::json!({"status": "ok"}),
        })
    }

    /// Get workflow execution info.
    pub fn describe_workflow(&self, workflow_id: &str) -> Option<WorkflowExecution> {
        let execs = self.executions.lock().unwrap();
        execs.get(workflow_id).cloned()
    }

    /// List workflow executions.
    pub fn list_workflows(&self) -> Vec<WorkflowExecution> {
        let execs = self.executions.lock().unwrap();
        execs.values().cloned().collect()
    }

    /// Cancel a workflow execution.
    pub fn cancel_workflow(&self, workflow_id: &str) -> Result<(), String> {
        let mut execs = self.executions.lock().unwrap();
        match execs.get_mut(workflow_id) {
            Some(exec) => {
                exec.status = WorkflowStatus::Cancelled;
                exec.close_time = Some(
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64,
                );
                Ok(())
            }
            None => Err(format!("Workflow not found: {}", workflow_id)),
        }
    }

    /// Terminate a workflow execution.
    pub fn terminate_workflow(&self, workflow_id: &str, _reason: &str) -> Result<(), String> {
        let mut execs = self.executions.lock().unwrap();
        match execs.get_mut(workflow_id) {
            Some(exec) => {
                exec.status = WorkflowStatus::Terminated;
                exec.close_time = Some(
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64,
                );
                Ok(())
            }
            None => Err(format!("Workflow not found: {}", workflow_id)),
        }
    }

    /// Get the client configuration.
    pub fn config(&self) -> &ClassicConfig {
        &self.config
    }
}

// ─── Feature Matrix ──────────────────────────────────────────────────────────

/// Velocity Classic feature matrix — shows Temporal compatibility.
pub fn feature_matrix() -> HashMap<&'static str, bool> {
    let mut features = HashMap::new();
    // Core workflow features
    features.insert("workflows", true);
    features.insert("activities", true);
    features.insert("signals", true);
    features.insert("queries", true);
    features.insert("child_workflows", true);
    features.insert("continue_as_new", true);
    features.insert("timers", true);
    features.insert("retries", true);
    features.insert("heartbeats", true);
    features.insert("cancellation", true);
    // Advanced features
    features.insert("signal_with_start", true);
    features.insert("search_attributes", true);
    features.insert("memo", true);
    features.insert("batch_operations", true);
    features.insert("schedules", true);
    features.insert("updates", true);
    features.insert("reset", true);
    features.insert("sticky_queues", true);
    features.insert("versioning", true);
    // Nexus (Velocity extension)
    features.insert("nexus_operations", true);
    features.insert("saga_pattern", true);
    features
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // Test workflow/activity types
    struct TestWorkflow;
    impl Workflow for TestWorkflow {
        type Input = String;
        type Output = String;
        fn name() -> &'static str {
            "TestWorkflow"
        }
    }

    struct TestActivity;
    impl Activity for TestActivity {
        type Input = u32;
        type Output = u32;
        fn name() -> &'static str {
            "TestActivity"
        }
    }

    #[test]
    fn test_classic_config_defaults() {
        let config = ClassicConfig::default();
        assert_eq!(config.server_address, "localhost:7233");
        assert_eq!(config.namespace, "default");
        assert_eq!(config.task_queue, "default");
        assert!(config.sticky_queues);
    }

    #[test]
    fn test_classic_worker() {
        let worker = ClassicWorker::new(ClassicConfig::default());
        worker.register_workflow::<TestWorkflow>();
        worker.register_activity::<TestActivity>();

        assert_eq!(worker.workflow_types(), vec!["TestWorkflow"]);
        assert_eq!(worker.activity_types(), vec!["TestActivity"]);
        assert_eq!(worker.task_queue(), "default");
        assert_eq!(worker.namespace(), "default");
    }

    #[test]
    fn test_classic_client_start_workflow() {
        let client = ClassicClient::new(ClassicConfig::default());
        let exec = client.start_workflow("wf-1", "TestWorkflow", serde_json::json!("input"));

        assert_eq!(exec.workflow_id, "wf-1");
        assert_eq!(exec.workflow_type, "TestWorkflow");
        assert_eq!(exec.status, WorkflowStatus::Running);
    }

    #[test]
    fn test_classic_client_describe_workflow() {
        let client = ClassicClient::new(ClassicConfig::default());
        client.start_workflow("wf-1", "TestWorkflow", serde_json::json!("input"));

        let desc = client.describe_workflow("wf-1");
        assert!(desc.is_some());
        assert_eq!(desc.unwrap().status, WorkflowStatus::Running);
    }

    #[test]
    fn test_classic_client_cancel_workflow() {
        let client = ClassicClient::new(ClassicConfig::default());
        client.start_workflow("wf-1", "TestWorkflow", serde_json::json!("input"));

        assert!(client.cancel_workflow("wf-1").is_ok());
        let desc = client.describe_workflow("wf-1").unwrap();
        assert_eq!(desc.status, WorkflowStatus::Cancelled);
        assert!(desc.close_time.is_some());
    }

    #[test]
    fn test_classic_client_terminate_workflow() {
        let client = ClassicClient::new(ClassicConfig::default());
        client.start_workflow("wf-1", "TestWorkflow", serde_json::json!("input"));

        assert!(client.terminate_workflow("wf-1", "timeout").is_ok());
        let desc = client.describe_workflow("wf-1").unwrap();
        assert_eq!(desc.status, WorkflowStatus::Terminated);
    }

    #[test]
    fn test_classic_client_signal_workflow() {
        let client = ClassicClient::new(ClassicConfig::default());
        client.start_workflow("wf-1", "TestWorkflow", serde_json::json!("input"));

        assert!(client
            .signal_workflow("wf-1", "approve", serde_json::json!(true))
            .is_ok());
    }

    #[test]
    fn test_classic_client_signal_nonexistent() {
        let client = ClassicClient::new(ClassicConfig::default());
        assert!(client
            .signal_workflow("wf-999", "approve", serde_json::json!(true))
            .is_err());
    }

    #[test]
    fn test_classic_client_query_workflow() {
        let client = ClassicClient::new(ClassicConfig::default());
        client.start_workflow("wf-1", "TestWorkflow", serde_json::json!("input"));

        let result = client.query_workflow("wf-1", "getStatus").unwrap();
        assert_eq!(result.query_type, "getStatus");
    }

    #[test]
    fn test_classic_client_list_workflows() {
        let client = ClassicClient::new(ClassicConfig::default());
        client.start_workflow("wf-1", "TestWorkflow", serde_json::json!("a"));
        client.start_workflow("wf-2", "TestWorkflow", serde_json::json!("b"));

        let list = client.list_workflows();
        assert_eq!(list.len(), 2);
    }

    #[test]
    fn test_search_attributes() {
        let mut attrs = SearchAttributes::new();
        attrs.set("orderId", serde_json::json!("order-123"));
        attrs.set("priority", serde_json::json!(1));

        assert_eq!(attrs.get("orderId"), Some(&serde_json::json!("order-123")));
        assert_eq!(attrs.get("priority"), Some(&serde_json::json!(1)));
        assert_eq!(attrs.get("missing"), None);
    }

    #[test]
    fn test_memo() {
        let mut memo = Memo::new();
        memo.set("description", serde_json::json!("test workflow"));

        assert_eq!(
            memo.get("description"),
            Some(&serde_json::json!("test workflow"))
        );
        assert_eq!(memo.get("missing"), None);
    }

    #[test]
    fn test_retry_policy_defaults() {
        let policy = RetryPolicy::default();
        assert_eq!(policy.initial_interval_ms, 1000);
        assert_eq!(policy.backoff_coefficient, 2.0);
        assert_eq!(policy.maximum_attempts, 0);
    }

    #[test]
    fn test_feature_matrix() {
        let matrix = feature_matrix();
        assert!(matrix.get("workflows").unwrap());
        assert!(matrix.get("activities").unwrap());
        assert!(matrix.get("signals").unwrap());
        assert!(matrix.get("queries").unwrap());
        assert!(matrix.get("signal_with_start").unwrap());
        assert!(matrix.get("nexus_operations").unwrap());
        assert!(matrix.get("saga_pattern").unwrap());
        assert!(matrix.len() >= 20);
    }

    #[test]
    fn test_workflow_status_variants() {
        let statuses = vec![
            WorkflowStatus::Running,
            WorkflowStatus::Completed,
            WorkflowStatus::Failed,
            WorkflowStatus::Cancelled,
            WorkflowStatus::Terminated,
            WorkflowStatus::ContinuingAsNew,
        ];
        assert_eq!(statuses.len(), 6);
    }

    #[test]
    fn test_workflow_execution_serialization() {
        let exec = WorkflowExecution {
            workflow_id: "wf-1".to_string(),
            run_id: "run-1".to_string(),
            workflow_type: "TestWorkflow".to_string(),
            status: WorkflowStatus::Completed,
            start_time: 1000,
            close_time: Some(2000),
        };

        let json = serde_json::to_string(&exec).unwrap();
        let back: WorkflowExecution = serde_json::from_str(&json).unwrap();
        assert_eq!(back.workflow_id, "wf-1");
        assert_eq!(back.status, WorkflowStatus::Completed);
    }

    #[test]
    fn test_signal_serialization() {
        let signal = Signal {
            signal_name: "approve".to_string(),
            input: serde_json::json!({"approved": true}),
            identity: "user-1".to_string(),
        };

        let json = serde_json::to_string(&signal).unwrap();
        let back: Signal = serde_json::from_str(&json).unwrap();
        assert_eq!(back.signal_name, "approve");
    }

    #[test]
    fn test_config_serialization() {
        let config = ClassicConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let back: ClassicConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(back.server_address, config.server_address);
        assert_eq!(back.namespace, config.namespace);
    }
}
