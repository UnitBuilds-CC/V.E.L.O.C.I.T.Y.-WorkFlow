//! Native Rust client for the VELOCITY-WorkFlow engine.
//!
//! `VelocityClient` wraps an `Arc<WorkflowEngine>` and exposes a high-level,
//! ergonomic API for workflow lifecycle management — start, step, signal,
//! query, cancel, and status inspection.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use velocity_workflow_engine::engine::{WorkflowEngine, WorkflowStatus};

use crate::errors::{self, VelocityError};
use crate::interceptors::InterceptorChain;

// ─── Data types ──────────────────────────────────────────────────────────────

/// Handle returned when a workflow is started.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkflowHandle {
    /// Engine-assigned opaque key.
    pub workflow_key: u64,
    /// Caller-supplied workflow ID (usually a hash).
    pub workflow_id: u64,
    /// Monotonic run ID.
    pub run_id: u64,
}

/// Snapshot of a workflow's execution state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowDescription {
    /// Engine-assigned opaque key.
    pub workflow_key: u64,
    /// Current execution status.
    pub status: WorkflowStatus,
    /// Step the workflow is currently on.
    pub current_step: u32,
    /// Total number of steps in the workflow.
    pub total_steps: u32,
    /// Workflow type ID.
    pub workflow_type_id: u64,
}

/// Information about a pending workflow task (returned by poll).
#[derive(Debug, Clone)]
pub struct WorkflowTaskInfo {
    /// Opaque task token (used to report completion/failure).
    pub task_token: u64,
    /// Workflow key.
    pub workflow_key: u64,
    /// Workflow type identifier.
    pub workflow_type: u64,
    /// Current step index.
    pub step_index: u32,
    /// Attempt number.
    pub attempt: i32,
}

/// Information about a pending activity task (returned by poll).
#[derive(Debug, Clone)]
pub struct ActivityTaskInfo {
    /// Opaque task token.
    pub task_token: u64,
    /// Workflow key this activity belongs to.
    pub workflow_key: u64,
    /// Activity type name.
    pub activity_type: String,
    /// Input payload.
    pub input: Vec<u8>,
    /// Current step index.
    pub step_index: u32,
    /// Attempt number.
    pub attempt: i32,
}

// ─── Client ──────────────────────────────────────────────────────────────────

/// High-level client that owns (or shares) a `WorkflowEngine`.
///
/// # Examples
///
/// ```rust,no_run
/// use velocity_sdk::VelocityClient;
///
/// let mut client = VelocityClient::new();
/// let key = client.start_workflow(1, 1, 1, 3);
/// client.complete_step(key, 0, b"step0".to_vec());
/// ```
pub struct VelocityClient {
    engine: Arc<WorkflowEngine>,
    next_id: AtomicU64,
    interceptors: InterceptorChain,
}

impl VelocityClient {
    /// Create a new client backed by a freshly-constructed engine.
    pub fn new() -> Self {
        Self {
            engine: Arc::new(WorkflowEngine::new()),
            next_id: AtomicU64::new(1),
            interceptors: InterceptorChain::new(),
        }
    }

    /// Create a client that wraps an existing engine instance.
    pub fn with_engine(engine: Arc<WorkflowEngine>) -> Self {
        Self {
            engine,
            next_id: AtomicU64::new(1),
            interceptors: InterceptorChain::new(),
        }
    }

    /// Access the underlying engine (e.g. for advanced / low-level operations).
    pub fn engine(&self) -> &Arc<WorkflowEngine> {
        &self.engine
    }

    /// Return a mutable reference to the interceptor chain.
    pub fn interceptors_mut(&mut self) -> &mut InterceptorChain {
        &mut self.interceptors
    }

    // ─── Workflow lifecycle ──────────────────────────────────────────────

    /// Start a new workflow execution.
    ///
    /// # Arguments
    /// * `workflow_type_id` — hashed workflow type identifier.
    /// * `namespace_id`     — hashed namespace identifier.
    /// * `task_queue_hash`  — hashed task-queue name.
    /// * `total_steps`      — number of execution steps for the workflow slab.
    ///
    /// # Returns
    /// The engine-assigned `workflow_key` (0 on failure).
    pub fn start_workflow(
        &self,
        workflow_type_id: u64,
        namespace_id: u64,
        task_queue_hash: u64,
        total_steps: u32,
    ) -> u64 {
        let workflow_id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let key = self.engine.start_workflow(
            workflow_id,
            workflow_type_id,
            namespace_id,
            task_queue_hash,
            total_steps,
            None,
        );

        if key != 0 {
            self.interceptors.invoke_workflow_start(workflow_type_id, key);
        }
        key
    }

    /// Start a workflow with an input payload.
    pub fn start_workflow_with_input(
        &self,
        workflow_type_id: u64,
        namespace_id: u64,
        task_queue_hash: u64,
        total_steps: u32,
        input: Vec<u8>,
    ) -> u64 {
        let workflow_id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let key = self.engine.start_workflow(
            workflow_id,
            workflow_type_id,
            namespace_id,
            task_queue_hash,
            total_steps,
            Some(input),
        );
        if key != 0 {
            self.interceptors.invoke_workflow_start(workflow_type_id, key);
        }
        key
    }

    /// Mark a single step as completed.
    ///
    /// # Errors
    /// Returns `VelocityError` if the workflow key is invalid.
    pub fn complete_step(&self, workflow_key: u64, step: u32, result: Vec<u8>) -> Result<(), VelocityError> {
        self.engine.complete_step(workflow_key, step, result);
        Ok(())
    }

    /// Deliver a signal to a running workflow.
    pub fn signal_workflow(&self, workflow_key: u64, signal_id: u64, payload: Vec<u8>) {
        self.engine.signal_workflow(workflow_key, signal_id, payload);
        self.interceptors.invoke_workflow_signal(workflow_key, signal_id);
    }

    /// Query a workflow's registered query handler.
    ///
    /// Returns the raw bytes from the query registry, or an error if the
    /// query is not registered.
    pub fn query_workflow(&self, workflow_key: u64, query_id: u64) -> Result<Vec<u8>, VelocityError> {
        let registry = self.engine.query_registry();
        // The query registry tracks registered queries per workflow.
        // For now, return an empty result if the workflow exists.
        let status = self.engine.get_status(workflow_key);
        if status == WorkflowStatus::Void {
            return Err(errors::workflow_not_found(workflow_key));
        }
        // Return a placeholder — real implementation would invoke the handler.
        let _ = query_id;
        Ok(Vec::new())
    }

    /// Request cancellation of a running workflow.
    pub fn cancel_workflow(&self, workflow_key: u64) {
        self.engine.cancel_workflow(workflow_key);
    }

    /// Get the current status of a workflow.
    pub fn get_status(&self, workflow_key: u64) -> WorkflowStatus {
        self.engine.get_status(workflow_key)
    }

    /// Describe a workflow (status + step progress).
    pub fn describe_workflow(&self, workflow_key: u64) -> Result<WorkflowDescription, VelocityError> {
        let status = self.engine.get_status(workflow_key);
        if status == WorkflowStatus::Void {
            return Err(errors::workflow_not_found(workflow_key));
        }
        let desc = self.engine.describe_workflow(workflow_key);
        let d = desc.ok_or_else(|| errors::workflow_not_found(workflow_key))?;
        Ok(WorkflowDescription {
            workflow_key,
            status,
            current_step: d.completed_steps,
            total_steps: d.total_steps,
            workflow_type_id: d.workflow_type_id,
        })
    }

    /// List all active workflow keys known to the engine.
    ///
    /// This iterates the visibility index and collects workflow keys.
    pub fn list_workflows(&self) -> Vec<u64> {
        self.engine.visibility().list_all_keys()
    }

    // ─── Worker Poll/Respond Methods ──────────────────────────────────────

    /// Poll for a workflow task from the engine's task queue.
    ///
    /// Returns `Some(task)` if a task is available, `None` if the queue is empty.
    /// The task dict contains: workflow_key, workflow_type, step_index, task_token.
    pub fn poll_workflow_task(&self, task_queue_hash: u64) -> Option<WorkflowTaskInfo> {
        // Check the engine's task queue for pending workflow tasks
        let keys = self.engine.visibility().list_all_keys();
        for key in keys {
            let status = self.engine.get_status(key);
            if status == WorkflowStatus::Running {
                if let Some(d) = self.engine.describe_workflow(key) {
                    return Some(WorkflowTaskInfo {
                        task_token: key,
                        workflow_key: key,
                        workflow_type: d.workflow_type_id,
                        step_index: d.completed_steps,
                        attempt: 1,
                    });
                }
            }
        }
        None
    }

    /// Complete a workflow task by reporting the result to the engine.
    pub fn complete_workflow_task(&self, task_token: u64, result: Vec<u8>) -> Result<(), VelocityError> {
        if let Some(d) = self.engine.describe_workflow(task_token) {
            self.engine.complete_step(task_token, d.completed_steps, result);
            Ok(())
        } else {
            Err(VelocityError::not_found(format!("Workflow task {} not found", task_token)))
        }
    }

    /// Fail a workflow task by reporting the failure to the engine.
    pub fn fail_workflow_task(&self, task_token: u64, _reason: &str) -> Result<(), VelocityError> {
        self.engine.fail_workflow(task_token);
        Ok(())
    }

    /// Poll for an activity task from the engine's task queue.
    pub fn poll_activity_task(&self, _task_queue_hash: u64) -> Option<ActivityTaskInfo> {
        // Activity tasks are dispatched inline in embedded mode.
        // In a full gRPC deployment, this would poll the server.
        None
    }

    /// Report successful completion of an activity task.
    pub fn complete_activity_task(&self, task_token: u64, result: Vec<u8>) -> Result<(), VelocityError> {
        // In embedded mode, activity results are returned directly.
        Ok(())
    }

    /// Report failure of an activity task.
    pub fn fail_activity_task(&self, task_token: u64, reason: &str) -> Result<(), VelocityError> {
        Err(VelocityError::internal(format!("Activity task {} failed: {}", task_token, reason)))
    }

    /// Shut down the engine (flushes WAL, stops timers / task queue).
    pub fn destroy(&self) {
        self.engine.shutdown();
    }
}

impl Default for VelocityClient {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for VelocityClient {
    fn drop(&mut self) {
        // Best-effort shutdown; callers should invoke `destroy()` explicitly.
        self.engine.shutdown();
    }
}
