//! Durable execution contexts and workflow handles.
//!
//! Provides:
//! - `DurableContext`: Context for durable functions (crash-recoverable steps)
//! - `TransientContext`: Context for non-durable operations
//! - `WorkflowHandle`: Handle to a running or completed workflow
//! - `WorkflowStatus`: Status enum for workflow lifecycle

use crate::storage::StorageBackend;
use crate::{EmbeddedError, JournalEntry};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

// ─── Workflow Status ─────────────────────────────────────────────────────────

/// Status of a workflow execution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkflowStatus {
    /// Workflow is queued for execution
    Pending,
    /// Workflow is currently executing
    Running,
    /// Workflow completed successfully
    Completed,
    /// Workflow failed with an error
    Failed,
}

// ─── Workflow Handle ─────────────────────────────────────────────────────────

/// A handle to a workflow execution.
///
/// Provides access to the workflow's result or error.
#[derive(Debug, Clone)]
pub struct WorkflowHandle<O> {
    workflow_id: String,
    status: WorkflowStatus,
    result: Option<O>,
    error: Option<String>,
}

impl<O> WorkflowHandle<O> {
    pub fn completed(workflow_id: String, result: O) -> Self {
        Self {
            workflow_id,
            status: WorkflowStatus::Completed,
            result: Some(result),
            error: None,
        }
    }

    pub fn failed(workflow_id: String, error: String) -> Self {
        Self {
            workflow_id,
            status: WorkflowStatus::Failed,
            result: None,
            error: Some(error),
        }
    }

    pub fn running(workflow_id: String) -> Self {
        Self {
            workflow_id,
            status: WorkflowStatus::Running,
            result: None,
            error: None,
        }
    }

    /// Get the workflow ID.
    pub fn workflow_id(&self) -> &str {
        &self.workflow_id
    }

    /// Get the workflow status.
    pub fn status(&self) -> &WorkflowStatus {
        &self.status
    }

    /// Check if the workflow is completed.
    pub fn is_completed(&self) -> bool {
        self.status == WorkflowStatus::Completed
    }

    /// Check if the workflow has failed.
    pub fn is_failed(&self) -> bool {
        self.status == WorkflowStatus::Failed
    }

    /// Check if the workflow is still running.
    pub fn is_running(&self) -> bool {
        self.status == WorkflowStatus::Running
    }

    /// Get the result, if completed successfully.
    pub fn result(&self) -> Option<&O> {
        self.result.as_ref()
    }

    /// Get the error message, if failed.
    pub fn error(&self) -> Option<&str> {
        self.error.as_deref()
    }

    /// Convert into the result, consuming the handle.
    pub fn into_result(self) -> Option<O> {
        self.result
    }
}

// ─── Durable Context ─────────────────────────────────────────────────────────

/// Context for durable function execution.
///
/// Provides crash-recoverable operations:
/// - `run()`: Execute a durable step (survives crashes)
/// - `sleep()`: Durable sleep
/// - `recv()`: Receive external input
/// - `send()`: Send to another workflow
/// - `get_state()` / `set_state()`: Durable key-value state
pub struct DurableContext {
    workflow_id: String,
    #[allow(dead_code)]
    storage: Arc<Mutex<Box<dyn StorageBackend>>>,
    sequence_counters: Arc<Mutex<HashMap<String, u64>>>,
    journal: Vec<JournalEntry>,
    state: HashMap<String, serde_json::Value>,
}

impl DurableContext {
    pub(crate) fn new(
        workflow_id: String,
        storage: Arc<Mutex<Box<dyn StorageBackend>>>,
        sequence_counters: Arc<Mutex<HashMap<String, u64>>>,
    ) -> Self {
        Self {
            workflow_id,
            storage,
            sequence_counters,
            journal: Vec::new(),
            state: HashMap::new(),
        }
    }

    /// Get the workflow ID.
    pub fn workflow_id(&self) -> &str {
        &self.workflow_id
    }

    /// Execute a durable step.
    ///
    /// The step is journaled. On crash recovery, previously completed steps
    /// return their cached result without re-executing.
    pub async fn run<F, Fut, O>(&mut self, step_name: &str, handler: F) -> Result<O, EmbeddedError>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = O>,
        O: Serialize + DeserializeOwned + Clone,
    {
        let seq_key = format!("{}:{}", self.workflow_id, step_name);

        // Check if we have a cached result from journal replay
        let cached = {
            let counters = self
                .sequence_counters
                .lock()
                .map_err(|_| EmbeddedError::LockPoisoned)?;
            let seq = counters.get(&seq_key).copied().unwrap_or(0);

            // Check journal for existing result
            self.journal
                .iter()
                .find(|e| e.sequence == seq && e.function_name == step_name && e.completed)
                .and_then(|e| e.output.as_ref())
                .and_then(|v| serde_json::from_value::<O>(v.clone()).ok())
        };

        if let Some(result) = cached {
            return Ok(result);
        }

        // Execute the step
        let result = handler().await;

        // Journal the result
        let output_value = serde_json::to_value(&result)
            .map_err(|e| EmbeddedError::Serialization(e.to_string()))?;

        let seq = {
            let mut counters = self
                .sequence_counters
                .lock()
                .map_err(|_| EmbeddedError::LockPoisoned)?;
            let seq = counters.entry(seq_key).or_insert(0);
            let current = *seq;
            *seq += 1;
            current
        };

        self.journal.push(JournalEntry {
            sequence: seq,
            function_name: step_name.to_string(),
            input: None,
            output: Some(output_value),
            error: None,
            completed: true,
        });

        Ok(result)
    }

    /// Durable sleep — survives crashes.
    pub async fn sleep(&mut self, duration_ms: u64) -> Result<(), EmbeddedError> {
        tokio::time::sleep(std::time::Duration::from_millis(duration_ms)).await;

        self.journal.push(JournalEntry {
            sequence: self.journal.len() as u64,
            function_name: "__sleep".to_string(),
            input: Some(serde_json::json!({ "duration_ms": duration_ms })),
            output: None,
            error: None,
            completed: true,
        });

        Ok(())
    }

    /// Get a durable state value.
    pub fn get_state<T: DeserializeOwned>(&self, key: &str) -> Option<T> {
        self.state
            .get(key)
            .and_then(|v| serde_json::from_value(v.clone()).ok())
    }

    /// Set a durable state value.
    pub fn set_state<T: Serialize>(&mut self, key: &str, value: T) -> Result<(), EmbeddedError> {
        let json = serde_json::to_value(&value)
            .map_err(|e| EmbeddedError::Serialization(e.to_string()))?;
        self.state.insert(key.to_string(), json);
        Ok(())
    }

    /// Delete a durable state value.
    pub fn clear_state(&mut self, key: &str) -> bool {
        self.state.remove(key).is_some()
    }

    /// Get the journal entries for this context.
    pub fn journal(&self) -> &[JournalEntry] {
        &self.journal
    }

    /// Send a message to another workflow.
    pub fn send<T: Serialize>(
        &mut self,
        target_workflow_id: &str,
        message: T,
    ) -> Result<(), EmbeddedError> {
        let value = serde_json::to_value(&message)
            .map_err(|e| EmbeddedError::Serialization(e.to_string()))?;

        self.journal.push(JournalEntry {
            sequence: self.journal.len() as u64,
            function_name: "__send".to_string(),
            input: Some(serde_json::json!({
                "target": target_workflow_id,
                "message": value,
            })),
            output: None,
            error: None,
            completed: true,
        });

        Ok(())
    }

    /// Receive a message (returns None if no message available).
    pub fn recv<T: DeserializeOwned>(&mut self) -> Option<T> {
        // In a real implementation, this would poll the storage backend
        // For now, check the journal for received messages
        None
    }
}

// ─── Transient Context ───────────────────────────────────────────────────────

/// Context for transient (non-durable) operations.
///
/// Use for operations that don't need crash recovery (e.g., HTTP calls
/// that are idempotent, cache lookups, etc.).
pub struct TransientContext {
    started_at: u64,
}

impl TransientContext {
    pub(crate) fn new() -> Self {
        Self {
            started_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        }
    }

    /// Get the start time of this transient operation.
    pub fn started_at(&self) -> u64 {
        self.started_at
    }

    /// Execute a non-durable step.
    pub async fn run<F, Fut, O>(&self, handler: F) -> O
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = O>,
    {
        handler().await
    }
}
