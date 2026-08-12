// Copyright (c) VELOCITY Suite. All rights reserved.
// Licensed under the MIT License.

//! VELOCITY Test Framework — Integration testing and workflow test infrastructure.
//!
//! Provides:
//! - `TestServer`: In-memory VELOCITY server for integration tests
//! - `WorkflowTestEnv`: Isolated environment for workflow testing
//! - `MockActivityWorker`: Mock activity execution for tests
//! - `TestAssertions`: Assertion helpers for workflow state verification
//! - `RecordingInterceptor`: Records all operations for verification
//!
//! # Usage
//! ```rust,no_run
//! use velocity_test_framework::{TestServer, TestServerConfig};
//!
//! #[tokio::test]
//! async fn test_workflow() {
//!     let server = TestServer::start(TestServerConfig::default()).await;
//!     let wf_id = server.start_workflow("default", "TestWorkflow", "task-queue", "{}").await;
//!     server.wait_workflow_complete(&wf_id, 5000).await;
//!     assert!(server.is_workflow_completed(&wf_id).await);
//!     server.stop().await;
//! }
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::Notify;
use tokio::time;

// ═══════════════════════════════════════════════════════════════════════════════
// Test Server Configuration
// ═══════════════════════════════════════════════════════════════════════════════

/// Configuration for the test server.
#[derive(Debug, Clone)]
pub struct TestServerConfig {
    /// HTTP port (0 = random).
    pub port: u16,
    /// Default namespace.
    pub namespace: String,
    /// Number of history shards.
    pub shards: u32,
    /// Workflow retention in seconds.
    pub retention_secs: u64,
    /// Enable chaos mode (random failures).
    pub chaos_mode: bool,
    /// Enable debug logging.
    pub debug: bool,
}

impl Default for TestServerConfig {
    fn default() -> Self {
        Self {
            port: 0,
            namespace: "default".into(),
            shards: 4,
            retention_secs: 86400,
            chaos_mode: false,
            debug: false,
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Test Server
// ═══════════════════════════════════════════════════════════════════════════════

/// Workflow execution state in the test server.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct TestWorkflowExecution {
    workflow_id: String,
    run_id: String,
    workflow_type: String,
    namespace: String,
    task_queue: String,
    status: String,
    input: String,
    start_time: i64,
    close_time: Option<i64>,
    history: Vec<TestHistoryEvent>,
    signals: HashMap<String, Vec<String>>,
    queries: HashMap<String, String>,
    result: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestHistoryEvent {
    event_id: u64,
    event_type: String,
    timestamp: i64,
    details: Option<String>,
}

/// In-memory test server for integration testing.
///
/// Provides a lightweight VELOCITY server that runs entirely in-memory,
/// suitable for integration tests without requiring Docker or external services.
pub struct TestServer {
    port: u16,
    base_url: String,
    workflows: Arc<RwLock<HashMap<String, TestWorkflowExecution>>>,
    namespaces: Arc<RwLock<HashMap<String, NamespaceState>>>,
    task_queues: Arc<RwLock<HashMap<String, TaskQueueState>>>,
    event_counter: Arc<AtomicU64>,
    running: Arc<AtomicBool>,
    shutdown: Arc<Notify>,
    config: TestServerConfig,
    operation_log: Arc<RwLock<Vec<OperationRecord>>>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
struct NamespaceState {
    name: String,
    is_active: bool,
    retention_secs: u64,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
struct TaskQueueState {
    name: String,
    pending_tasks: u64,
    active_workers: u64,
}

/// Record of an operation performed on the test server.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationRecord {
    pub timestamp: i64,
    pub operation: String,
    pub target: String,
    pub details: Option<String>,
}

impl TestServer {
    /// Start a new test server with the given configuration.
    pub async fn start(config: TestServerConfig) -> Self {
        let port = if config.port == 0 { 19100 } else { config.port };
        let base_url = format!("http://localhost:{}", port);

        let mut namespaces = HashMap::new();
        namespaces.insert(
            "default".into(),
            NamespaceState {
                name: "default".into(),
                is_active: true,
                retention_secs: config.retention_secs,
            },
        );
        namespaces.insert(
            "system".into(),
            NamespaceState {
                name: "system".into(),
                is_active: true,
                retention_secs: config.retention_secs,
            },
        );

        let server = Self {
            port,
            base_url,
            workflows: Arc::new(RwLock::new(HashMap::new())),
            namespaces: Arc::new(RwLock::new(namespaces)),
            task_queues: Arc::new(RwLock::new(HashMap::new())),
            event_counter: Arc::new(AtomicU64::new(1)),
            running: Arc::new(AtomicBool::new(true)),
            shutdown: Arc::new(Notify::new()),
            config,
            operation_log: Arc::new(RwLock::new(Vec::new())),
        };

        server.log_operation("server_start", "test-server", None);
        server
    }

    /// Get the server's base URL.
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Get the server's port.
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Stop the test server.
    pub async fn stop(&self) {
        self.running.store(false, Ordering::Relaxed);
        self.shutdown.notify_waiters();
        self.log_operation("server_stop", "test-server", None);
    }

    /// Check if the server is running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    // ── Workflow Operations ──────────────────────────────────────────────────

    /// Start a new workflow execution.
    pub async fn start_workflow(
        &self,
        namespace: &str,
        workflow_type: &str,
        task_queue: &str,
        input: &str,
    ) -> String {
        let wf_id = format!(
            "test-wf-{}",
            self.event_counter.fetch_add(1, Ordering::Relaxed)
        );
        let run_id = format!("run-{}", now_millis());

        let execution = TestWorkflowExecution {
            workflow_id: wf_id.clone(),
            run_id,
            workflow_type: workflow_type.to_string(),
            namespace: namespace.to_string(),
            task_queue: task_queue.to_string(),
            status: "running".into(),
            input: input.to_string(),
            start_time: now_millis(),
            close_time: None,
            history: vec![TestHistoryEvent {
                event_id: 1,
                event_type: "WorkflowExecutionStarted".into(),
                timestamp: now_millis(),
                details: Some(format!("type={}, queue={}", workflow_type, task_queue)),
            }],
            signals: HashMap::new(),
            queries: HashMap::new(),
            result: None,
        };

        self.workflows
            .write()
            .unwrap()
            .insert(wf_id.clone(), execution);

        // Update task queue
        self.task_queues
            .write()
            .unwrap()
            .entry(task_queue.to_string())
            .or_insert_with(|| TaskQueueState {
                name: task_queue.to_string(),
                pending_tasks: 0,
                active_workers: 0,
            })
            .pending_tasks += 1;

        self.log_operation(
            "start_workflow",
            &wf_id,
            Some(format!("type={}", workflow_type)),
        );
        wf_id
    }

    /// Start a workflow with a specific ID.
    pub async fn start_workflow_with_id(
        &self,
        workflow_id: &str,
        namespace: &str,
        workflow_type: &str,
        task_queue: &str,
        input: &str,
    ) {
        let run_id = format!("run-{}", now_millis());

        let execution = TestWorkflowExecution {
            workflow_id: workflow_id.to_string(),
            run_id,
            workflow_type: workflow_type.to_string(),
            namespace: namespace.to_string(),
            task_queue: task_queue.to_string(),
            status: "running".into(),
            input: input.to_string(),
            start_time: now_millis(),
            close_time: None,
            history: vec![TestHistoryEvent {
                event_id: 1,
                event_type: "WorkflowExecutionStarted".into(),
                timestamp: now_millis(),
                details: Some(format!("type={}, queue={}", workflow_type, task_queue)),
            }],
            signals: HashMap::new(),
            queries: HashMap::new(),
            result: None,
        };

        self.workflows
            .write()
            .unwrap()
            .insert(workflow_id.to_string(), execution);
        self.log_operation("start_workflow_with_id", workflow_id, None);
    }

    /// Signal a running workflow.
    pub async fn signal_workflow(&self, workflow_id: &str, signal_name: &str, input: &str) -> bool {
        let mut workflows = self.workflows.write().unwrap();
        if let Some(wf) = workflows.get_mut(workflow_id) {
            if wf.status != "running" {
                return false;
            }
            wf.signals
                .entry(signal_name.to_string())
                .or_default()
                .push(input.to_string());
            let event_id = wf.history.len() as u64 + 1;
            wf.history.push(TestHistoryEvent {
                event_id,
                event_type: "WorkflowExecutionSignaled".into(),
                timestamp: now_millis(),
                details: Some(format!("signal={}", signal_name)),
            });
            self.log_operation("signal_workflow", workflow_id, Some(signal_name.into()));
            true
        } else {
            false
        }
    }

    /// Query a running workflow.
    pub async fn query_workflow(&self, workflow_id: &str, query_type: &str) -> Option<String> {
        let workflows = self.workflows.read().unwrap();
        if let Some(wf) = workflows.get(workflow_id) {
            if wf.status != "running" {
                return None;
            }
            // Default query responses
            let result = match query_type {
                "__stack_trace" => Some("[]".into()),
                "__open_sessions" => Some("[]".into()),
                _ => wf.queries.get(query_type).cloned(),
            };
            self.log_operation("query_workflow", workflow_id, Some(query_type.into()));
            result
        } else {
            None
        }
    }

    /// Complete a workflow (test helper).
    pub async fn complete_workflow(&self, workflow_id: &str, result: &str) -> bool {
        let mut workflows = self.workflows.write().unwrap();
        if let Some(wf) = workflows.get_mut(workflow_id) {
            wf.status = "completed".into();
            wf.close_time = Some(now_millis());
            wf.result = Some(result.to_string());
            let event_id = wf.history.len() as u64 + 1;
            wf.history.push(TestHistoryEvent {
                event_id,
                event_type: "WorkflowExecutionCompleted".into(),
                timestamp: now_millis(),
                details: Some(result.to_string()),
            });
            self.log_operation("complete_workflow", workflow_id, None);
            true
        } else {
            false
        }
    }

    /// Fail a workflow (test helper).
    pub async fn fail_workflow(&self, workflow_id: &str, reason: &str) -> bool {
        let mut workflows = self.workflows.write().unwrap();
        if let Some(wf) = workflows.get_mut(workflow_id) {
            wf.status = "failed".into();
            wf.close_time = Some(now_millis());
            wf.result = Some(reason.to_string());
            let event_id = wf.history.len() as u64 + 1;
            wf.history.push(TestHistoryEvent {
                event_id,
                event_type: "WorkflowExecutionFailed".into(),
                timestamp: now_millis(),
                details: Some(reason.to_string()),
            });
            self.log_operation("fail_workflow", workflow_id, Some(reason.into()));
            true
        } else {
            false
        }
    }

    /// Cancel a workflow.
    pub async fn cancel_workflow(&self, workflow_id: &str) -> bool {
        let mut workflows = self.workflows.write().unwrap();
        if let Some(wf) = workflows.get_mut(workflow_id) {
            wf.status = "canceled".into();
            wf.close_time = Some(now_millis());
            let event_id = wf.history.len() as u64 + 1;
            wf.history.push(TestHistoryEvent {
                event_id,
                event_type: "WorkflowExecutionCanceled".into(),
                timestamp: now_millis(),
                details: None,
            });
            self.log_operation("cancel_workflow", workflow_id, None);
            true
        } else {
            false
        }
    }

    /// Terminate a workflow.
    pub async fn terminate_workflow(&self, workflow_id: &str, reason: &str) -> bool {
        let mut workflows = self.workflows.write().unwrap();
        if let Some(wf) = workflows.get_mut(workflow_id) {
            wf.status = "terminated".into();
            wf.close_time = Some(now_millis());
            wf.result = Some(reason.to_string());
            let event_id = wf.history.len() as u64 + 1;
            wf.history.push(TestHistoryEvent {
                event_id,
                event_type: "WorkflowExecutionTerminated".into(),
                timestamp: now_millis(),
                details: Some(reason.to_string()),
            });
            self.log_operation("terminate_workflow", workflow_id, Some(reason.into()));
            true
        } else {
            false
        }
    }

    // ── Workflow Inspection ──────────────────────────────────────────────────

    /// Get the status of a workflow.
    pub async fn get_workflow_status(&self, workflow_id: &str) -> Option<String> {
        self.workflows
            .read()
            .unwrap()
            .get(workflow_id)
            .map(|wf| wf.status.clone())
    }

    /// Check if a workflow is completed.
    pub async fn is_workflow_completed(&self, workflow_id: &str) -> bool {
        self.get_workflow_status(workflow_id)
            .await
            .map(|s| s == "completed")
            .unwrap_or(false)
    }

    /// Check if a workflow is failed.
    pub async fn is_workflow_failed(&self, workflow_id: &str) -> bool {
        self.get_workflow_status(workflow_id)
            .await
            .map(|s| s == "failed")
            .unwrap_or(false)
    }

    /// Wait for a workflow to complete (with timeout in ms).
    pub async fn wait_workflow_complete(&self, workflow_id: &str, timeout_ms: u64) -> bool {
        let start = Instant::now();
        let timeout = Duration::from_millis(timeout_ms);
        while start.elapsed() < timeout {
            if let Some(status) = self.get_workflow_status(workflow_id).await {
                if status == "completed"
                    || status == "failed"
                    || status == "canceled"
                    || status == "terminated"
                {
                    return true;
                }
            }
            time::sleep(Duration::from_millis(50)).await;
        }
        false
    }

    /// Get the workflow result.
    pub async fn get_workflow_result(&self, workflow_id: &str) -> Option<String> {
        self.workflows
            .read()
            .unwrap()
            .get(workflow_id)
            .and_then(|wf| wf.result.clone())
    }

    /// Get the event history for a workflow.
    pub async fn get_workflow_history(&self, workflow_id: &str) -> Vec<TestHistoryEvent> {
        self.workflows
            .read()
            .unwrap()
            .get(workflow_id)
            .map(|wf| wf.history.clone())
            .unwrap_or_default()
    }

    /// Get the number of history events.
    pub async fn get_history_length(&self, workflow_id: &str) -> usize {
        self.get_workflow_history(workflow_id).await.len()
    }

    /// List all workflows.
    pub async fn list_workflows(&self) -> Vec<String> {
        self.workflows.read().unwrap().keys().cloned().collect()
    }

    /// Count workflows by status.
    pub async fn count_workflows_by_status(&self, status: &str) -> usize {
        self.workflows
            .read()
            .unwrap()
            .values()
            .filter(|wf| wf.status == status)
            .count()
    }

    // ── Namespace Operations ─────────────────────────────────────────────────

    /// Create a namespace.
    pub async fn create_namespace(&self, name: &str) -> bool {
        let mut namespaces = self.namespaces.write().unwrap();
        if namespaces.contains_key(name) {
            return false;
        }
        namespaces.insert(
            name.to_string(),
            NamespaceState {
                name: name.to_string(),
                is_active: true,
                retention_secs: self.config.retention_secs,
            },
        );
        self.log_operation("create_namespace", name, None);
        true
    }

    /// Check if a namespace exists.
    pub async fn namespace_exists(&self, name: &str) -> bool {
        self.namespaces.read().unwrap().contains_key(name)
    }

    /// List all namespaces.
    pub async fn list_namespaces(&self) -> Vec<String> {
        self.namespaces.read().unwrap().keys().cloned().collect()
    }

    // ── Operation Log ────────────────────────────────────────────────────────

    /// Get the operation log.
    pub fn get_operation_log(&self) -> Vec<OperationRecord> {
        self.operation_log.read().unwrap().clone()
    }

    /// Count operations of a specific type.
    pub fn count_operations(&self, operation: &str) -> usize {
        self.operation_log
            .read()
            .unwrap()
            .iter()
            .filter(|r| r.operation == operation)
            .count()
    }

    /// Check if a specific operation was performed.
    pub fn has_operation(&self, operation: &str, target: &str) -> bool {
        self.operation_log
            .read()
            .unwrap()
            .iter()
            .any(|r| r.operation == operation && r.target == target)
    }

    /// Clear the operation log.
    pub fn clear_operation_log(&self) {
        self.operation_log.write().unwrap().clear();
    }

    fn log_operation(&self, operation: &str, target: &str, details: Option<String>) {
        self.operation_log.write().unwrap().push(OperationRecord {
            timestamp: now_millis(),
            operation: operation.to_string(),
            target: target.to_string(),
            details,
        });
    }

    // ── Health ───────────────────────────────────────────────────────────────

    /// Check server health.
    pub async fn health_check(&self) -> bool {
        self.is_running()
    }

    /// Get server stats.
    pub async fn get_stats(&self) -> TestServerStats {
        let workflows = self.workflows.read().unwrap();
        let namespaces = self.namespaces.read().unwrap();
        let task_queues = self.task_queues.read().unwrap();

        TestServerStats {
            workflow_count: workflows.len(),
            running_workflows: workflows.values().filter(|w| w.status == "running").count(),
            completed_workflows: workflows
                .values()
                .filter(|w| w.status == "completed")
                .count(),
            failed_workflows: workflows.values().filter(|w| w.status == "failed").count(),
            namespace_count: namespaces.len(),
            task_queue_count: task_queues.len(),
            operation_count: self.operation_log.read().unwrap().len(),
        }
    }
}

/// Server statistics snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestServerStats {
    pub workflow_count: usize,
    pub running_workflows: usize,
    pub completed_workflows: usize,
    pub failed_workflows: usize,
    pub namespace_count: usize,
    pub task_queue_count: usize,
    pub operation_count: usize,
}

// ═══════════════════════════════════════════════════════════════════════════════
// Workflow Test Environment
// ═══════════════════════════════════════════════════════════════════════════════

/// Isolated environment for testing individual workflows.
///
/// Provides a simplified API for workflow testing with automatic cleanup.
pub struct WorkflowTestEnv {
    server: TestServer,
    namespace: String,
    task_queue: String,
    created_workflows: Vec<String>,
}

impl WorkflowTestEnv {
    /// Create a new workflow test environment.
    pub async fn new(namespace: &str, task_queue: &str) -> Self {
        let config = TestServerConfig {
            namespace: namespace.to_string(),
            ..Default::default()
        };
        let server = TestServer::start(config).await;
        Self {
            server,
            namespace: namespace.to_string(),
            task_queue: task_queue.to_string(),
            created_workflows: Vec::new(),
        }
    }

    /// Start a workflow in this environment.
    pub async fn start_workflow(&mut self, workflow_type: &str, input: &str) -> String {
        let wf_id = self
            .server
            .start_workflow(&self.namespace, workflow_type, &self.task_queue, input)
            .await;
        self.created_workflows.push(wf_id.clone());
        wf_id
    }

    /// Complete a workflow (simulate completion).
    pub async fn complete_workflow(&self, workflow_id: &str, result: &str) {
        self.server.complete_workflow(workflow_id, result).await;
    }

    /// Fail a workflow (simulate failure).
    pub async fn fail_workflow(&self, workflow_id: &str, reason: &str) {
        self.server.fail_workflow(workflow_id, reason).await;
    }

    /// Signal a workflow.
    pub async fn signal_workflow(&self, workflow_id: &str, signal_name: &str, input: &str) -> bool {
        self.server
            .signal_workflow(workflow_id, signal_name, input)
            .await
    }

    /// Query a workflow.
    pub async fn query_workflow(&self, workflow_id: &str, query_type: &str) -> Option<String> {
        self.server.query_workflow(workflow_id, query_type).await
    }

    /// Wait for workflow completion.
    pub async fn wait_complete(&self, workflow_id: &str, timeout_ms: u64) -> bool {
        self.server
            .wait_workflow_complete(workflow_id, timeout_ms)
            .await
    }

    /// Assert workflow completed.
    pub async fn assert_completed(&self, workflow_id: &str) {
        assert!(
            self.server.is_workflow_completed(workflow_id).await,
            "Expected workflow '{}' to be completed, but status is {:?}",
            workflow_id,
            self.server.get_workflow_status(workflow_id).await
        );
    }

    /// Assert workflow failed.
    pub async fn assert_failed(&self, workflow_id: &str) {
        assert!(
            self.server.is_workflow_failed(workflow_id).await,
            "Expected workflow '{}' to be failed",
            workflow_id
        );
    }

    /// Assert workflow history contains an event type.
    pub async fn assert_history_contains(&self, workflow_id: &str, event_type: &str) {
        let history = self.server.get_workflow_history(workflow_id).await;
        assert!(
            history.iter().any(|e| e.event_type == event_type),
            "Expected history to contain '{}', but found: {:?}",
            event_type,
            history.iter().map(|e| &e.event_type).collect::<Vec<_>>()
        );
    }

    /// Assert workflow received a signal.
    pub async fn assert_signaled(&self, workflow_id: &str, signal_name: &str) {
        let history = self.server.get_workflow_history(workflow_id).await;
        let has_signal = history.iter().any(|e| {
            e.event_type == "WorkflowExecutionSignaled"
                && e.details
                    .as_ref()
                    .map(|d| d.contains(signal_name))
                    .unwrap_or(false)
        });
        assert!(
            has_signal,
            "Expected workflow '{}' to have signal '{}'",
            workflow_id, signal_name
        );
    }

    /// Get the underlying test server.
    pub fn server(&self) -> &TestServer {
        &self.server
    }

    /// Clean up all created workflows.
    pub async fn cleanup(&mut self) {
        for wf_id in &self.created_workflows {
            self.server.terminate_workflow(wf_id, "cleanup").await;
        }
        self.created_workflows.clear();
        self.server.stop().await;
    }
}

impl Drop for WorkflowTestEnv {
    fn drop(&mut self) {
        // Best-effort cleanup
        let _ = self.server.stop();
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Mock Activity Worker
// ═══════════════════════════════════════════════════════════════════════════════

/// Mock activity worker for testing.
///
/// Records activity invocations and returns configurable responses.
pub struct MockActivityWorker {
    activities: Arc<RwLock<HashMap<String, MockActivityBehavior>>>,
    invocations: Arc<RwLock<Vec<ActivityInvocation>>>,
}

/// Behavior configuration for a mock activity.
#[derive(Debug, Clone)]
pub enum MockActivityBehavior {
    /// Return a successful result.
    ReturnResult(String),
    /// Fail with an error.
    Fail(String),
    /// Time out (never completes).
    Timeout,
    /// Return result after a delay.
    DelayedResult(String, Duration),
    /// Execute a custom function.
    Custom(fn(&str) -> Result<String, String>),
}

/// Record of an activity invocation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActivityInvocation {
    pub activity_type: String,
    pub input: String,
    pub timestamp: i64,
    pub result: Option<String>,
    pub error: Option<String>,
}

impl MockActivityWorker {
    /// Create a new mock activity worker.
    pub fn new() -> Self {
        Self {
            activities: Arc::new(RwLock::new(HashMap::new())),
            invocations: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Register a mock activity with a behavior.
    pub fn register_activity(&self, activity_type: &str, behavior: MockActivityBehavior) {
        self.activities
            .write()
            .unwrap()
            .insert(activity_type.to_string(), behavior);
    }

    /// Register a simple result-returning activity.
    pub fn register_result(&self, activity_type: &str, result: &str) {
        self.register_activity(
            activity_type,
            MockActivityBehavior::ReturnResult(result.to_string()),
        );
    }

    /// Register a failing activity.
    pub fn register_failure(&self, activity_type: &str, error: &str) {
        self.register_activity(activity_type, MockActivityBehavior::Fail(error.to_string()));
    }

    /// Execute a mock activity.
    pub fn execute(&self, activity_type: &str, input: &str) -> Result<String, String> {
        let invocation = ActivityInvocation {
            activity_type: activity_type.to_string(),
            input: input.to_string(),
            timestamp: now_millis(),
            result: None,
            error: None,
        };

        let result = {
            let activities = self.activities.read().unwrap();
            match activities.get(activity_type) {
                Some(MockActivityBehavior::ReturnResult(r)) => Ok(r.clone()),
                Some(MockActivityBehavior::Fail(e)) => Err(e.clone()),
                Some(MockActivityBehavior::Timeout) => {
                    std::thread::sleep(Duration::from_secs(300));
                    Err("Activity timed out".into())
                }
                Some(MockActivityBehavior::DelayedResult(r, _)) => Ok(r.clone()),
                Some(MockActivityBehavior::Custom(f)) => f(input),
                None => Err(format!("Unknown activity type: {}", activity_type)),
            }
        };

        let mut inv = invocation;
        match &result {
            Ok(r) => inv.result = Some(r.clone()),
            Err(e) => inv.error = Some(e.clone()),
        }
        self.invocations.write().unwrap().push(inv);

        result
    }

    /// Get all invocations.
    pub fn get_invocations(&self) -> Vec<ActivityInvocation> {
        self.invocations.read().unwrap().clone()
    }

    /// Count invocations of a specific activity type.
    pub fn count_invocations(&self, activity_type: &str) -> usize {
        self.invocations
            .read()
            .unwrap()
            .iter()
            .filter(|i| i.activity_type == activity_type)
            .count()
    }

    /// Check if an activity was invoked.
    pub fn was_invoked(&self, activity_type: &str) -> bool {
        self.count_invocations(activity_type) > 0
    }

    /// Clear invocation history.
    pub fn clear_history(&self) {
        self.invocations.write().unwrap().clear();
    }
}

impl Default for MockActivityWorker {
    fn default() -> Self {
        Self::new()
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Test Assertions
// ═══════════════════════════════════════════════════════════════════════════════

/// Assertion helpers for workflow testing.
pub struct TestAssertions;

impl TestAssertions {
    /// Assert that a workflow completed successfully.
    pub async fn assert_workflow_completed(server: &TestServer, workflow_id: &str) {
        assert!(
            server.is_workflow_completed(workflow_id).await,
            "Workflow '{}' should be completed, actual status: {:?}",
            workflow_id,
            server.get_workflow_status(workflow_id).await
        );
    }

    /// Assert that a workflow failed.
    pub async fn assert_workflow_failed(server: &TestServer, workflow_id: &str) {
        assert!(
            server.is_workflow_failed(workflow_id).await,
            "Workflow '{}' should be failed",
            workflow_id
        );
    }

    /// Assert workflow history has at least N events.
    pub async fn assert_history_length(server: &TestServer, workflow_id: &str, min_events: usize) {
        let len = server.get_history_length(workflow_id).await;
        assert!(
            len >= min_events,
            "Expected at least {} history events for '{}', got {}",
            min_events,
            workflow_id,
            len
        );
    }

    /// Assert that a specific event type exists in the history.
    pub async fn assert_history_event(server: &TestServer, workflow_id: &str, event_type: &str) {
        let history = server.get_workflow_history(workflow_id).await;
        let found = history.iter().any(|e| e.event_type == event_type);
        assert!(
            found,
            "Expected event '{}' in history of '{}', found: {:?}",
            event_type,
            workflow_id,
            history.iter().map(|e| &e.event_type).collect::<Vec<_>>()
        );
    }

    /// Assert workflow result matches expected.
    pub async fn assert_workflow_result(server: &TestServer, workflow_id: &str, expected: &str) {
        let result = server.get_workflow_result(workflow_id).await;
        assert_eq!(
            result.as_deref(),
            Some(expected),
            "Workflow '{}' result mismatch",
            workflow_id
        );
    }

    /// Assert a namespace exists.
    pub async fn assert_namespace_exists(server: &TestServer, name: &str) {
        assert!(
            server.namespace_exists(name).await,
            "Namespace '{}' should exist",
            name
        );
    }

    /// Assert the total number of workflows.
    pub async fn assert_workflow_count(server: &TestServer, expected: usize) {
        let count = server.list_workflows().await.len();
        assert_eq!(
            count, expected,
            "Expected {} workflows, got {}",
            expected, count
        );
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Recording Interceptor
// ═══════════════════════════════════════════════════════════════════════════════

/// Records all operations for verification in tests.
pub struct RecordingInterceptor {
    records: Arc<RwLock<Vec<InterceptedCall>>>,
    enabled: Arc<AtomicBool>,
}

/// A recorded intercepted call.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InterceptedCall {
    pub method: String,
    pub target: String,
    pub timestamp: i64,
    pub duration_ms: u64,
    pub success: bool,
    pub error: Option<String>,
}

impl RecordingInterceptor {
    /// Create a new recording interceptor.
    pub fn new() -> Self {
        Self {
            records: Arc::new(RwLock::new(Vec::new())),
            enabled: Arc::new(AtomicBool::new(true)),
        }
    }

    /// Record a call.
    pub fn record_call(
        &self,
        method: &str,
        target: &str,
        duration_ms: u64,
        success: bool,
        error: Option<String>,
    ) {
        if !self.enabled.load(Ordering::Relaxed) {
            return;
        }
        self.records.write().unwrap().push(InterceptedCall {
            method: method.to_string(),
            target: target.to_string(),
            timestamp: now_millis(),
            duration_ms,
            success,
            error,
        });
    }

    /// Get all recorded calls.
    pub fn get_records(&self) -> Vec<InterceptedCall> {
        self.records.read().unwrap().clone()
    }

    /// Count calls to a specific method.
    pub fn count_calls(&self, method: &str) -> usize {
        self.records
            .read()
            .unwrap()
            .iter()
            .filter(|r| r.method == method)
            .count()
    }

    /// Check if a method was called.
    pub fn was_called(&self, method: &str) -> bool {
        self.count_calls(method) > 0
    }

    /// Check if a method was called with a specific target.
    pub fn was_called_with(&self, method: &str, target: &str) -> bool {
        self.records
            .read()
            .unwrap()
            .iter()
            .any(|r| r.method == method && r.target == target)
    }

    /// Get failed calls.
    pub fn get_failed_calls(&self) -> Vec<InterceptedCall> {
        self.records
            .read()
            .unwrap()
            .iter()
            .filter(|r| !r.success)
            .cloned()
            .collect()
    }

    /// Clear all records.
    pub fn clear(&self) {
        self.records.write().unwrap().clear();
    }

    /// Enable or disable recording.
    pub fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::Relaxed);
    }

    /// Get the total number of recorded calls.
    pub fn total_calls(&self) -> usize {
        self.records.read().unwrap().len()
    }

    /// Get average call duration for a method.
    pub fn avg_duration(&self, method: &str) -> f64 {
        let records = self.records.read().unwrap();
        let matching: Vec<&InterceptedCall> =
            records.iter().filter(|r| r.method == method).collect();
        if matching.is_empty() {
            return 0.0;
        }
        let sum: u64 = matching.iter().map(|r| r.duration_ms).sum();
        sum as f64 / matching.len() as f64
    }
}

impl Default for RecordingInterceptor {
    fn default() -> Self {
        Self::new()
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════════════════════════

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_server_lifecycle() {
        let server = TestServer::start(TestServerConfig::default()).await;
        assert!(server.is_running());
        assert!(server.health_check().await);
        server.stop().await;
        assert!(!server.is_running());
    }

    #[tokio::test]
    async fn test_workflow_lifecycle() {
        let server = TestServer::start(TestServerConfig::default()).await;

        let wf_id = server
            .start_workflow("default", "TestWorkflow", "test-queue", "{}")
            .await;

        assert_eq!(
            server.get_workflow_status(&wf_id).await,
            Some("running".into())
        );

        server.complete_workflow(&wf_id, "done").await;
        assert!(server.is_workflow_completed(&wf_id).await);
        assert_eq!(
            server.get_workflow_result(&wf_id).await,
            Some("done".into())
        );

        server.stop().await;
    }

    #[tokio::test]
    async fn test_workflow_failure() {
        let server = TestServer::start(TestServerConfig::default()).await;

        let wf_id = server
            .start_workflow("default", "FailWorkflow", "test-queue", "{}")
            .await;

        server.fail_workflow(&wf_id, "something broke").await;
        assert!(server.is_workflow_failed(&wf_id).await);
        assert_eq!(
            server.get_workflow_result(&wf_id).await,
            Some("something broke".into())
        );

        server.stop().await;
    }

    #[tokio::test]
    async fn test_workflow_signals() {
        let server = TestServer::start(TestServerConfig::default()).await;

        let wf_id = server
            .start_workflow("default", "SignalWorkflow", "test-queue", "{}")
            .await;

        assert!(server.signal_workflow(&wf_id, "my-signal", "data").await);
        assert!(!server.signal_workflow("nonexistent", "sig", "data").await);

        let history = server.get_workflow_history(&wf_id).await;
        assert!(history
            .iter()
            .any(|e| e.event_type == "WorkflowExecutionSignaled"));

        server.stop().await;
    }

    #[tokio::test]
    async fn test_workflow_queries() {
        let server = TestServer::start(TestServerConfig::default()).await;

        let wf_id = server
            .start_workflow("default", "QueryWorkflow", "test-queue", "{}")
            .await;

        let trace = server.query_workflow(&wf_id, "__stack_trace").await;
        assert_eq!(trace, Some("[]".into()));

        let unknown = server.query_workflow(&wf_id, "unknown_query").await;
        assert_eq!(unknown, None);

        server.stop().await;
    }

    #[tokio::test]
    async fn test_workflow_cancel_terminate() {
        let server = TestServer::start(TestServerConfig::default()).await;

        let wf1 = server.start_workflow("default", "W1", "q", "{}").await;
        let wf2 = server.start_workflow("default", "W2", "q", "{}").await;

        assert!(server.cancel_workflow(&wf1).await);
        assert_eq!(
            server.get_workflow_status(&wf1).await,
            Some("canceled".into())
        );

        assert!(server.terminate_workflow(&wf2, "test").await);
        assert_eq!(
            server.get_workflow_status(&wf2).await,
            Some("terminated".into())
        );

        server.stop().await;
    }

    #[tokio::test]
    async fn test_namespace_operations() {
        let server = TestServer::start(TestServerConfig::default()).await;

        assert!(server.namespace_exists("default").await);
        assert!(!server.namespace_exists("custom").await);

        assert!(server.create_namespace("custom").await);
        assert!(server.namespace_exists("custom").await);
        assert!(!server.create_namespace("custom").await); // duplicate

        let namespaces = server.list_namespaces().await;
        assert!(namespaces.contains(&"custom".to_string()));

        server.stop().await;
    }

    #[tokio::test]
    async fn test_operation_log() {
        let server = TestServer::start(TestServerConfig::default()).await;

        let wf_id = server.start_workflow("default", "TestWF", "q", "{}").await;
        server.complete_workflow(&wf_id, "ok").await;

        assert!(server.has_operation("start_workflow", &wf_id));
        assert!(server.has_operation("complete_workflow", &wf_id));
        assert_eq!(server.count_operations("start_workflow"), 1);

        let log = server.get_operation_log();
        assert!(log.len() >= 3); // server_start + start_workflow + complete_workflow

        server.clear_operation_log();
        assert_eq!(server.get_operation_log().len(), 0);

        server.stop().await;
    }

    #[tokio::test]
    async fn test_server_stats() {
        let server = TestServer::start(TestServerConfig::default()).await;

        let wf1 = server.start_workflow("default", "W1", "q", "{}").await;
        let _wf2 = server.start_workflow("default", "W2", "q", "{}").await;
        server.complete_workflow(&wf1, "ok").await;

        let stats = server.get_stats().await;
        assert_eq!(stats.workflow_count, 2);
        assert_eq!(stats.completed_workflows, 1);
        assert_eq!(stats.running_workflows, 1);

        server.stop().await;
    }

    #[tokio::test]
    async fn test_count_workflows_by_status() {
        let server = TestServer::start(TestServerConfig::default()).await;

        let wf1 = server.start_workflow("default", "W1", "q", "{}").await;
        let wf2 = server.start_workflow("default", "W2", "q", "{}").await;
        let _wf3 = server.start_workflow("default", "W3", "q", "{}").await;

        server.complete_workflow(&wf1, "ok").await;
        server.fail_workflow(&wf2, "err").await;

        assert_eq!(server.count_workflows_by_status("running").await, 1);
        assert_eq!(server.count_workflows_by_status("completed").await, 1);
        assert_eq!(server.count_workflows_by_status("failed").await, 1);

        server.stop().await;
    }

    #[tokio::test]
    async fn test_workflow_test_env() {
        let mut env = WorkflowTestEnv::new("test-ns", "test-queue").await;

        let wf_id = env.start_workflow("TestWorkflow", "{}").await;
        env.complete_workflow(&wf_id, "success").await;
        env.assert_completed(&wf_id).await;
        env.assert_history_contains(&wf_id, "WorkflowExecutionStarted")
            .await;
        env.assert_history_contains(&wf_id, "WorkflowExecutionCompleted")
            .await;

        env.cleanup().await;
    }

    #[tokio::test]
    async fn test_workflow_test_env_signals() {
        let mut env = WorkflowTestEnv::new("test-ns", "test-queue").await;

        let wf_id = env.start_workflow("SignalWorkflow", "{}").await;
        assert!(env.signal_workflow(&wf_id, "my-signal", "data").await);
        env.assert_signaled(&wf_id, "my-signal").await;

        env.cleanup().await;
    }

    #[tokio::test]
    async fn test_mock_activity_worker() {
        let worker = MockActivityWorker::new();

        worker.register_result("Greet", "Hello, World!");
        worker.register_failure("FailAct", "intentional error");

        let result = worker.execute("Greet", "input");
        assert_eq!(result, Ok("Hello, World!".into()));

        let result = worker.execute("FailAct", "input");
        assert_eq!(result, Err("intentional error".into()));

        let result = worker.execute("Unknown", "input");
        assert!(result.is_err());

        assert_eq!(worker.count_invocations("Greet"), 1);
        assert_eq!(worker.count_invocations("FailAct"), 1);
        assert!(worker.was_invoked("Greet"));
        assert!(!worker.was_invoked("Never"));
        assert_eq!(worker.get_invocations().len(), 3);
    }

    #[tokio::test]
    async fn test_mock_activity_clear() {
        let worker = MockActivityWorker::new();
        worker.register_result("Act", "ok");

        worker.execute("Act", "1");
        worker.execute("Act", "2");
        assert_eq!(worker.count_invocations("Act"), 2);

        worker.clear_history();
        assert_eq!(worker.count_invocations("Act"), 0);
    }

    #[tokio::test]
    async fn test_recording_interceptor() {
        let interceptor = RecordingInterceptor::new();

        interceptor.record_call("StartWorkflow", "wf-1", 50, true, None);
        interceptor.record_call("SignalWorkflow", "wf-1", 10, true, None);
        interceptor.record_call("QueryWorkflow", "wf-1", 5, false, Some("timeout".into()));

        assert_eq!(interceptor.total_calls(), 3);
        assert!(interceptor.was_called("StartWorkflow"));
        assert!(interceptor.was_called_with("StartWorkflow", "wf-1"));
        assert!(!interceptor.was_called("TerminateWorkflow"));

        assert_eq!(interceptor.count_calls("StartWorkflow"), 1);
        assert_eq!(interceptor.get_failed_calls().len(), 1);
        assert_eq!(interceptor.avg_duration("StartWorkflow"), 50.0);

        interceptor.clear();
        assert_eq!(interceptor.total_calls(), 0);
    }

    #[tokio::test]
    async fn test_recording_interceptor_disable() {
        let interceptor = RecordingInterceptor::new();

        interceptor.record_call("A", "t", 1, true, None);
        assert_eq!(interceptor.total_calls(), 1);

        interceptor.set_enabled(false);
        interceptor.record_call("B", "t", 1, true, None);
        assert_eq!(interceptor.total_calls(), 1); // not recorded

        interceptor.set_enabled(true);
        interceptor.record_call("C", "t", 1, true, None);
        assert_eq!(interceptor.total_calls(), 2);
    }

    #[tokio::test]
    async fn test_assertions() {
        let server = TestServer::start(TestServerConfig::default()).await;

        let wf_id = server.start_workflow("default", "TestWF", "q", "{}").await;
        server.complete_workflow(&wf_id, "result").await;

        TestAssertions::assert_workflow_completed(&server, &wf_id).await;
        TestAssertions::assert_history_length(&server, &wf_id, 2).await;
        TestAssertions::assert_history_event(&server, &wf_id, "WorkflowExecutionStarted").await;
        TestAssertions::assert_history_event(&server, &wf_id, "WorkflowExecutionCompleted").await;
        TestAssertions::assert_workflow_result(&server, &wf_id, "result").await;
        TestAssertions::assert_namespace_exists(&server, "default").await;
        TestAssertions::assert_workflow_count(&server, 1).await;

        server.stop().await;
    }

    #[tokio::test]
    async fn test_start_workflow_with_id() {
        let server = TestServer::start(TestServerConfig::default()).await;

        server
            .start_workflow_with_id("my-custom-id", "default", "MyWorkflow", "q", "{}")
            .await;

        assert_eq!(
            server.get_workflow_status("my-custom-id").await,
            Some("running".into())
        );

        server.complete_workflow("my-custom-id", "done").await;
        assert!(server.is_workflow_completed("my-custom-id").await);

        server.stop().await;
    }

    #[tokio::test]
    async fn test_wait_workflow_complete_immediate() {
        let server = TestServer::start(TestServerConfig::default()).await;

        let wf_id = server.start_workflow("default", "W", "q", "{}").await;
        server.complete_workflow(&wf_id, "ok").await;

        assert!(server.wait_workflow_complete(&wf_id, 1000).await);

        server.stop().await;
    }

    #[tokio::test]
    async fn test_wait_workflow_timeout() {
        let server = TestServer::start(TestServerConfig::default()).await;

        let wf_id = server.start_workflow("default", "W", "q", "{}").await;
        // Don't complete it — should timeout

        assert!(!server.wait_workflow_complete(&wf_id, 200).await);

        server.stop().await;
    }
}
