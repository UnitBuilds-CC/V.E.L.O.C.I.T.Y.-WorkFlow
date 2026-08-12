//! Testing utilities for the VELOCITY-WorkFlow Rust SDK.
//!
//! Provides `TestWorkflowEnvironment` and `MockClient` for unit-testing
//! workflow logic without a running engine or server.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use velocity_workflow_engine::engine::WorkflowStatus;

use crate::errors;
use crate::errors::VelocityError;

// ─── Mock data types ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct MockWorkflow {
    workflow_type_id: u64,
    namespace_id: u64,
    task_queue_hash: u64,
    total_steps: u32,
    current_step: u32,
    status: WorkflowStatus,
    result: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
struct MockSignal {
    signal_id: u64,
    payload: Vec<u8>,
}

// ─── MockClient ──────────────────────────────────────────────────────────────

/// In-memory mock client that mirrors the `VelocityClient` API surface.
///
/// Useful for unit tests that need to verify workflow interactions without
/// depending on the real engine.
pub struct MockClient {
    workflows: HashMap<u64, MockWorkflow>,
    signals: HashMap<u64, Vec<MockSignal>>,
    next_key: AtomicU64,
}

impl MockClient {
    /// Create a new, empty mock client.
    pub fn new() -> Self {
        Self {
            workflows: HashMap::new(),
            signals: HashMap::new(),
            next_key: AtomicU64::new(1),
        }
    }

    /// Start a mock workflow. Returns the assigned key.
    pub fn start_workflow(
        &mut self,
        workflow_type_id: u64,
        namespace_id: u64,
        task_queue_hash: u64,
        total_steps: u32,
    ) -> u64 {
        let key = self.next_key.fetch_add(1, Ordering::Relaxed);
        self.workflows.insert(key, MockWorkflow {
            workflow_type_id,
            namespace_id,
            task_queue_hash,
            total_steps,
            current_step: 0,
            status: WorkflowStatus::Running,
            result: None,
        });
        self.signals.insert(key, Vec::new());
        key
    }

    /// Complete a step on a mock workflow.
    pub fn complete_step(&mut self, workflow_key: u64, step: u32, result: Vec<u8>) -> Result<(), VelocityError> {
        let wf = self.workflows.get_mut(&workflow_key)
            .ok_or_else(|| errors::workflow_not_found(workflow_key))?;
        wf.current_step = step + 1;
        let _ = result;
        Ok(())
    }

    /// Signal a mock workflow.
    pub fn signal_workflow(&mut self, workflow_key: u64, signal_id: u64, payload: Vec<u8>) -> Result<(), VelocityError> {
        if !self.workflows.contains_key(&workflow_key) {
            return Err(errors::workflow_not_found(workflow_key));
        }
        self.signals.entry(workflow_key).or_default().push(MockSignal { signal_id, payload });
        Ok(())
    }

    /// Query a mock workflow (returns empty bytes).
    pub fn query_workflow(&self, workflow_key: u64, _query_id: u64) -> Result<Vec<u8>, VelocityError> {
        if !self.workflows.contains_key(&workflow_key) {
            return Err(errors::workflow_not_found(workflow_key));
        }
        Ok(Vec::new())
    }

    /// Cancel a mock workflow.
    pub fn cancel_workflow(&mut self, workflow_key: u64) -> Result<(), VelocityError> {
        let wf = self.workflows.get_mut(&workflow_key)
            .ok_or_else(|| errors::workflow_not_found(workflow_key))?;
        wf.status = WorkflowStatus::Canceled;
        Ok(())
    }

    /// Get the status of a mock workflow.
    pub fn get_status(&self, workflow_key: u64) -> Result<WorkflowStatus, VelocityError> {
        self.workflows.get(&workflow_key)
            .map(|wf| wf.status)
            .ok_or_else(|| errors::workflow_not_found(workflow_key))
    }

    /// Complete a mock workflow.
    pub fn complete_workflow(&mut self, workflow_key: u64, result: Vec<u8>) -> Result<(), VelocityError> {
        let wf = self.workflows.get_mut(&workflow_key)
            .ok_or_else(|| errors::workflow_not_found(workflow_key))?;
        if wf.status != WorkflowStatus::Running {
            return Err(errors::workflow_already_completed(workflow_key));
        }
        wf.status = WorkflowStatus::Completed;
        wf.result = Some(result);
        Ok(())
    }

    /// List all mock workflow keys.
    pub fn list_workflows(&self) -> Vec<u64> {
        self.workflows.keys().copied().collect()
    }

    /// Get signals received by a workflow.
    pub fn get_signals(&self, workflow_key: u64) -> Vec<(u64, Vec<u8>)> {
        self.signals.get(&workflow_key)
            .map(|sigs| sigs.iter().map(|s| (s.signal_id, s.payload.clone())).collect())
            .unwrap_or_default()
    }
}

impl Default for MockClient {
    fn default() -> Self {
        Self::new()
    }
}

// ─── TestWorkflowEnvironment ─────────────────────────────────────────────────

/// Isolated test environment wrapping a `MockClient`.
///
/// Provides assertion helpers and time-skip support for deterministic tests.
pub struct TestWorkflowEnvironment {
    /// The underlying mock client.
    pub client: MockClient,
    time_offset_secs: i64,
}

impl TestWorkflowEnvironment {
    /// Create a fresh test environment.
    pub fn new() -> Self {
        Self {
            client: MockClient::new(),
            time_offset_secs: 0,
        }
    }

    /// Start a workflow in the test environment.
    pub fn start_workflow(
        &mut self,
        workflow_type_id: u64,
        namespace_id: u64,
        task_queue_hash: u64,
        total_steps: u32,
    ) -> u64 {
        self.client.start_workflow(workflow_type_id, namespace_id, task_queue_hash, total_steps)
    }

    /// Advance the simulated clock.
    pub fn time_skip(&mut self, seconds: i64) {
        self.time_offset_secs += seconds;
    }

    /// Current simulated time as UNIX epoch seconds.
    pub fn current_time_secs(&self) -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64
            + self.time_offset_secs
    }

    /// Assert that a workflow has completed.
    pub fn assert_workflow_completed(&self, workflow_key: u64) -> Result<(), VelocityError> {
        let status = self.client.get_status(workflow_key)?;
        if status != WorkflowStatus::Completed {
            return Err(errors::internal(&format!(
                "Expected workflow {workflow_key} to be completed, but status is {status:?}",
            )));
        }
        Ok(())
    }

    /// Assert that a workflow received a specific signal.
    pub fn assert_signal_received(&self, workflow_key: u64, signal_id: u64) -> Result<(), VelocityError> {
        let signals = self.client.get_signals(workflow_key);
        if !signals.iter().any(|(id, _)| *id == signal_id) {
            return Err(errors::internal(&format!(
                "Expected signal {signal_id} not found for workflow {workflow_key}",
            )));
        }
        Ok(())
    }

    /// Reset the environment to a clean state.
    pub fn reset(&mut self) {
        self.client = MockClient::new();
        self.time_offset_secs = 0;
    }
}

impl Default for TestWorkflowEnvironment {
    fn default() -> Self {
        Self::new()
    }
}
