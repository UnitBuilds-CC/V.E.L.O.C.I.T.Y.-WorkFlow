//! Batch operations for workflow management.
//! Supports bulk terminate, cancel, signal, and status queries across
//! many workflows in a single operation — mirroring Temporal's BatchService.

use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Mutex,
};

use crate::engine::{WorkflowEngine, WorkflowStatus};

// ─── Batch Operation Types ────────────────────────────────────────────────────

/// The type of batch operation to perform.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub enum BatchOperationType {
    Terminate = 0,
    Cancel = 1,
    Signal = 2,
    QueryStatus = 3,
}

/// Result of a single workflow operation within a batch.
#[derive(Debug, Clone)]
pub struct BatchItemResult {
    pub workflow_key: u64,
    pub success: bool,
    pub error_message: Option<String>,
}

/// Aggregate result of a batch operation.
#[derive(Debug, Clone)]
pub struct BatchResult {
    pub batch_id: u64,
    pub operation: BatchOperationType,
    pub total: usize,
    pub succeeded: usize,
    pub failed: usize,
    pub item_results: Vec<BatchItemResult>,
}

/// A batch operation descriptor.
#[derive(Debug, Clone)]
pub struct BatchOperation {
    pub batch_id: u64,
    pub operation: BatchOperationType,
    pub workflow_keys: Vec<u64>,
    pub signal_name_id: Option<u64>,
    pub signal_payload: Option<Vec<u8>>,
    pub status: BatchStatus,
    pub result: Option<BatchResult>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub enum BatchStatus {
    Pending = 0,
    Running = 1,
    Completed = 2,
    Failed = 3,
}

// ─── Batch Executor ──────────────────────────────────────────────────────────

/// Executes batch operations against the workflow engine.
pub struct BatchExecutor {
    batches: Mutex<HashMap<u64, BatchOperation>>,
    next_batch_id: AtomicU64,
}

impl BatchExecutor {
    pub fn new() -> Self {
        Self {
            batches: Mutex::new(HashMap::new()),
            next_batch_id: AtomicU64::new(1),
        }
    }

    /// Submit a batch terminate operation for the given workflow keys.
    pub fn submit_terminate(&self, engine: &WorkflowEngine, workflow_keys: Vec<u64>) -> u64 {
        let batch_id = self.next_batch_id.fetch_add(1, Ordering::Relaxed);
        let result = self.execute_batch(
            engine,
            batch_id,
            BatchOperationType::Terminate,
            &workflow_keys,
            None,
            None,
        );

        let mut batches = self.batches.lock().unwrap();
        batches.insert(
            batch_id,
            BatchOperation {
                batch_id,
                operation: BatchOperationType::Terminate,
                workflow_keys,
                signal_name_id: None,
                signal_payload: None,
                status: BatchStatus::Completed,
                result: Some(result),
            },
        );

        batch_id
    }

    /// Submit a batch cancel operation.
    pub fn submit_cancel(&self, engine: &WorkflowEngine, workflow_keys: Vec<u64>) -> u64 {
        let batch_id = self.next_batch_id.fetch_add(1, Ordering::Relaxed);
        let result = self.execute_batch(
            engine,
            batch_id,
            BatchOperationType::Cancel,
            &workflow_keys,
            None,
            None,
        );

        let mut batches = self.batches.lock().unwrap();
        batches.insert(
            batch_id,
            BatchOperation {
                batch_id,
                operation: BatchOperationType::Cancel,
                workflow_keys,
                signal_name_id: None,
                signal_payload: None,
                status: BatchStatus::Completed,
                result: Some(result),
            },
        );

        batch_id
    }

    /// Submit a batch signal operation.
    pub fn submit_signal(
        &self,
        engine: &WorkflowEngine,
        workflow_keys: Vec<u64>,
        signal_name_id: u64,
        payload: Vec<u8>,
    ) -> u64 {
        let batch_id = self.next_batch_id.fetch_add(1, Ordering::Relaxed);
        let result = self.execute_batch(
            engine,
            batch_id,
            BatchOperationType::Signal,
            &workflow_keys,
            Some(signal_name_id),
            Some(payload.clone()),
        );

        let mut batches = self.batches.lock().unwrap();
        batches.insert(
            batch_id,
            BatchOperation {
                batch_id,
                operation: BatchOperationType::Signal,
                workflow_keys,
                signal_name_id: Some(signal_name_id),
                signal_payload: Some(payload),
                status: BatchStatus::Completed,
                result: Some(result),
            },
        );

        batch_id
    }

    /// Submit a batch query (status check) operation.
    pub fn submit_query_status(&self, engine: &WorkflowEngine, workflow_keys: Vec<u64>) -> u64 {
        let batch_id = self.next_batch_id.fetch_add(1, Ordering::Relaxed);
        let result = self.execute_batch(
            engine,
            batch_id,
            BatchOperationType::QueryStatus,
            &workflow_keys,
            None,
            None,
        );

        let mut batches = self.batches.lock().unwrap();
        batches.insert(
            batch_id,
            BatchOperation {
                batch_id,
                operation: BatchOperationType::QueryStatus,
                workflow_keys,
                signal_name_id: None,
                signal_payload: None,
                status: BatchStatus::Completed,
                result: Some(result),
            },
        );

        batch_id
    }

    /// Execute a batch operation synchronously and return the result.
    fn execute_batch(
        &self,
        engine: &WorkflowEngine,
        batch_id: u64,
        operation: BatchOperationType,
        workflow_keys: &[u64],
        signal_name_id: Option<u64>,
        signal_payload: Option<Vec<u8>>,
    ) -> BatchResult {
        let mut item_results = Vec::with_capacity(workflow_keys.len());
        let mut succeeded = 0;
        let mut failed = 0;

        for &key in workflow_keys {
            let success = match operation {
                BatchOperationType::Terminate => {
                    let status = engine.get_status(key);
                    if status == WorkflowStatus::Running {
                        engine.terminate_workflow(key);
                        true
                    } else {
                        failed += 1;
                        item_results.push(BatchItemResult {
                            workflow_key: key,
                            success: false,
                            error_message: Some(format!(
                                "Workflow not running (status={:?})",
                                status
                            )),
                        });
                        continue;
                    }
                }
                BatchOperationType::Cancel => {
                    let status = engine.get_status(key);
                    if status == WorkflowStatus::Running {
                        engine.cancel_workflow(key);
                        true
                    } else {
                        failed += 1;
                        item_results.push(BatchItemResult {
                            workflow_key: key,
                            success: false,
                            error_message: Some(format!(
                                "Workflow not running (status={:?})",
                                status
                            )),
                        });
                        continue;
                    }
                }
                BatchOperationType::Signal => {
                    let status = engine.get_status(key);
                    if status == WorkflowStatus::Running {
                        if let (Some(sig_id), Some(ref payload)) = (signal_name_id, &signal_payload)
                        {
                            engine.signal_workflow(key, sig_id, payload.clone());
                            true
                        } else {
                            failed += 1;
                            item_results.push(BatchItemResult {
                                workflow_key: key,
                                success: false,
                                error_message: Some("Missing signal name or payload".to_string()),
                            });
                            continue;
                        }
                    } else {
                        failed += 1;
                        item_results.push(BatchItemResult {
                            workflow_key: key,
                            success: false,
                            error_message: Some(format!(
                                "Workflow not running (status={:?})",
                                status
                            )),
                        });
                        continue;
                    }
                }
                BatchOperationType::QueryStatus => {
                    // Query always succeeds — it's read-only
                    true
                }
            };

            if success {
                succeeded += 1;
                item_results.push(BatchItemResult {
                    workflow_key: key,
                    success: true,
                    error_message: None,
                });
            }
        }

        BatchResult {
            batch_id,
            operation,
            total: workflow_keys.len(),
            succeeded,
            failed,
            item_results,
        }
    }

    /// Get the result of a previously submitted batch operation.
    pub fn get_result(&self, batch_id: u64) -> Option<BatchResult> {
        self.batches
            .lock()
            .unwrap()
            .get(&batch_id)
            .and_then(|b| b.result.clone())
    }

    /// Get the status of a batch operation.
    pub fn get_status(&self, batch_id: u64) -> Option<BatchStatus> {
        self.batches
            .lock()
            .unwrap()
            .get(&batch_id)
            .map(|b| b.status)
    }

    /// Get the total number of batch operations submitted.
    pub fn batch_count(&self) -> usize {
        self.batches.lock().unwrap().len()
    }

    /// List all batch operations with their status and results.
    pub fn list_all(&self) -> Vec<(u64, BatchStatus, Option<BatchResult>)> {
        let batches = self.batches.lock().unwrap();
        batches
            .iter()
            .map(|(id, entry)| (*id, entry.status, entry.result.clone()))
            .collect()
    }
}

impl Default for BatchExecutor {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_engine_with_workflows(count: usize) -> (WorkflowEngine, Vec<u64>) {
        let engine = WorkflowEngine::new();
        let mut keys = Vec::new();
        for i in 0..count {
            let key = engine.start_workflow(1000 + i as u64, 1, 0, 42, 3, None);
            keys.push(key);
        }
        (engine, keys)
    }

    #[test]
    fn test_batch_terminate() {
        let (engine, keys) = setup_engine_with_workflows(5);

        let executor = BatchExecutor::new();
        let batch_id = executor.submit_terminate(&engine, keys.clone());

        let result = executor.get_result(batch_id).unwrap();
        assert_eq!(result.total, 5);
        assert_eq!(result.succeeded, 5);
        assert_eq!(result.failed, 0);

        // All workflows should be terminated
        for &key in &keys {
            assert_eq!(engine.get_status(key), WorkflowStatus::Terminated);
        }

        engine.shutdown();
    }

    #[test]
    fn test_batch_cancel() {
        let (engine, keys) = setup_engine_with_workflows(3);

        let executor = BatchExecutor::new();
        let batch_id = executor.submit_cancel(&engine, keys.clone());

        let result = executor.get_result(batch_id).unwrap();
        assert_eq!(result.total, 3);
        assert_eq!(result.succeeded, 3);

        for &key in &keys {
            assert_eq!(engine.get_status(key), WorkflowStatus::Canceled);
        }

        engine.shutdown();
    }

    #[test]
    fn test_batch_signal() {
        let (engine, keys) = setup_engine_with_workflows(3);

        let executor = BatchExecutor::new();
        let batch_id = executor.submit_signal(&engine, keys.clone(), 999, vec![1, 2, 3]);

        let result = executor.get_result(batch_id).unwrap();
        assert_eq!(result.succeeded, 3);

        // Each workflow should have the signal
        for &key in &keys {
            assert!(engine.has_signal(key, 999));
        }

        engine.shutdown();
    }

    #[test]
    fn test_batch_query_status() {
        let (engine, keys) = setup_engine_with_workflows(4);

        // Complete one workflow
        engine.complete_workflow(keys[0], Some(vec![42]));

        let executor = BatchExecutor::new();
        let batch_id = executor.submit_query_status(&engine, keys.clone());

        let result = executor.get_result(batch_id).unwrap();
        assert_eq!(result.total, 4);
        assert_eq!(result.succeeded, 4); // Query always succeeds

        engine.shutdown();
    }

    #[test]
    fn test_batch_terminate_mixed_status() {
        let (engine, keys) = setup_engine_with_workflows(3);

        // Complete first, cancel second, leave third running
        engine.complete_workflow(keys[0], None);
        engine.cancel_workflow(keys[1]);

        let executor = BatchExecutor::new();
        let batch_id = executor.submit_terminate(&engine, keys.clone());

        let result = executor.get_result(batch_id).unwrap();
        assert_eq!(result.total, 3);
        assert_eq!(result.succeeded, 1); // Only the running one
        assert_eq!(result.failed, 2); // Already completed/canceled

        engine.shutdown();
    }

    #[test]
    fn test_batch_count() {
        let (engine, keys) = setup_engine_with_workflows(2);
        let executor = BatchExecutor::new();

        assert_eq!(executor.batch_count(), 0);
        executor.submit_terminate(&engine, keys.clone());
        assert_eq!(executor.batch_count(), 1);
        executor.submit_cancel(&engine, keys);
        assert_eq!(executor.batch_count(), 2);

        engine.shutdown();
    }
}
