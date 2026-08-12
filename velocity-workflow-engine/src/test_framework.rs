//! Workflow Test Framework — deterministic testing environment for workflow code.
//!
//! Provides an isolated test environment with:
//! - Activity mocking and result injection
//! - Signal/update simulation during workflow execution
//! - Workflow execution assertions and inspection
//! - Run-to-completion helpers
//! - Automatic cleanup
//!
//! Matches and exceeds Temporal's testing framework capabilities.

use std::collections::HashMap;

use crate::engine::{WorkflowEngine, WorkflowStatus};
use crate::visibility::SearchAttributeValue;

// ─── Test Workflow Environment ─────────────────────────────────────────────

/// Isolated test environment for workflow testing.
pub struct TestWorkflowEnvironment {
    engine: WorkflowEngine,
    /// Mock activity results: (workflow_type_id, step) -> result
    activity_mocks: HashMap<(u64, u32), MockActivityResult>,
}

/// Mock result for an activity.
#[derive(Debug, Clone)]
pub enum MockActivityResult {
    /// Return a successful result.
    Success(Vec<u8>),
    /// Return a failure.
    Failure(String),
    /// Simulate a timeout.
    Timeout,
    /// Block indefinitely (for testing cancellation).
    Block,
}

/// Result of a test workflow execution.
#[derive(Debug, Clone)]
pub struct TestWorkflowResult {
    pub workflow_key: u64,
    pub status: WorkflowStatus,
    pub steps_completed: u32,
    pub total_steps: u32,
}

impl TestWorkflowEnvironment {
    /// Create a new test environment.
    pub fn new() -> Self {
        Self {
            engine: WorkflowEngine::new(),
            activity_mocks: HashMap::new(),
        }
    }

    /// Create a test environment with WAL persistence.
    pub fn with_wal(wal_path: &str) -> std::io::Result<Self> {
        Ok(Self {
            engine: WorkflowEngine::with_wal(wal_path, 1024 * 1024)?,
            activity_mocks: HashMap::new(),
        })
    }

    /// Get a reference to the underlying engine.
    pub fn engine(&self) -> &WorkflowEngine {
        &self.engine
    }

    // ─── Activity Mocking ────────────────────────────────────────────

    /// Mock an activity result for a specific workflow type and step.
    pub fn mock_activity(&mut self, workflow_type_id: u64, step: u32, result: MockActivityResult) {
        self.activity_mocks.insert((workflow_type_id, step), result);
    }

    /// Mock all activities for a workflow type to return the same result.
    pub fn mock_all_activities(&mut self, workflow_type_id: u64, total_steps: u32, result: Vec<u8>) {
        for step in 0..total_steps {
            self.activity_mocks.insert(
                (workflow_type_id, step),
                MockActivityResult::Success(result.clone()),
            );
        }
    }

    /// Get the mock result for an activity.
    pub fn get_mock(&self, workflow_type_id: u64, step: u32) -> Option<&MockActivityResult> {
        self.activity_mocks.get(&(workflow_type_id, step))
    }

    // ─── Workflow Lifecycle ──────────────────────────────────────────

    /// Start a workflow.
    pub fn start_workflow(
        &self,
        workflow_id: u64,
        workflow_type_id: u64,
        namespace_id: u64,
        task_queue_hash: u64,
        total_steps: u32,
        input: Option<Vec<u8>>,
    ) -> u64 {
        self.engine.start_workflow(
            workflow_id, workflow_type_id, namespace_id, task_queue_hash, total_steps, input,
        )
    }

    /// Start a workflow with search attributes.
    pub fn start_workflow_with_attrs(
        &self,
        workflow_id: u64,
        workflow_type_id: u64,
        namespace_id: u64,
        task_queue_hash: u64,
        total_steps: u32,
        input: Option<Vec<u8>>,
        attrs: HashMap<String, SearchAttributeValue>,
    ) -> u64 {
        self.engine.start_workflow_with_attrs(
            workflow_id, workflow_type_id, namespace_id, task_queue_hash, total_steps, input, attrs,
        )
    }

    /// Start a child workflow.
    pub fn start_child_workflow(
        &self,
        parent_key: u64,
        child_workflow_id: u64,
        workflow_type_id: u64,
        task_queue_hash: u64,
        total_steps: u32,
        input: Option<Vec<u8>>,
    ) -> u64 {
        self.engine.start_child_workflow(
            parent_key, child_workflow_id, workflow_type_id, task_queue_hash, total_steps, input,
        )
    }

    /// Complete a step in a workflow.
    pub fn complete_step(&self, workflow_key: u64, step: u32, result: Vec<u8>) {
        self.engine.complete_step(workflow_key, step, result);
    }

    /// Complete the workflow with a final result.
    pub fn complete_workflow(&self, workflow_key: u64, result: Option<Vec<u8>>) {
        self.engine.complete_workflow(workflow_key, result);
    }

    /// Fail the workflow.
    pub fn fail_workflow(&self, workflow_key: u64) {
        self.engine.fail_workflow(workflow_key);
    }

    /// Cancel the workflow.
    pub fn cancel_workflow(&self, workflow_key: u64) {
        self.engine.cancel_workflow(workflow_key);
    }

    /// Terminate the workflow.
    pub fn terminate_workflow(&self, workflow_key: u64) {
        self.engine.terminate_workflow(workflow_key);
    }

    // ─── Signal & Update Simulation ──────────────────────────────────

    /// Send a signal to a workflow.
    pub fn signal_workflow(&self, workflow_key: u64, signal_name_id: u64, payload: Vec<u8>) {
        self.engine.signal_workflow(workflow_key, signal_name_id, payload);
    }

    /// Send an update to a workflow.
    pub fn update_workflow(&self, workflow_key: u64, update_name_id: u64, payload: Vec<u8>) {
        self.engine.update_workflow(workflow_key, update_name_id, payload);
    }

    // ─── Assertions & Inspection ─────────────────────────────────────

    /// Get the current status of a workflow.
    pub fn get_status(&self, workflow_key: u64) -> WorkflowStatus {
        self.engine.get_status(workflow_key)
    }

    /// Get the current step of a workflow.
    pub fn get_current_step(&self, workflow_key: u64) -> u32 {
        self.engine.get_current_step(workflow_key)
    }

    /// Check if a step is completed.
    pub fn is_step_completed(&self, workflow_key: u64, step: u32) -> bool {
        self.engine.is_step_completed(workflow_key, step)
    }

    /// Get the step result.
    pub fn get_step_result(&self, workflow_key: u64, step: u32) -> Option<Vec<u8>> {
        self.engine.get_step_result(workflow_key, step)
    }

    /// Get the total steps for a workflow.
    pub fn get_total_steps(&self, workflow_key: u64) -> u32 {
        self.engine.get_total_steps(workflow_key)
    }

    /// Get the active workflow count.
    pub fn workflow_count(&self) -> usize {
        self.engine.workflow_count()
    }

    /// Build a test result for a workflow.
    pub fn build_test_result(&self, workflow_key: u64) -> TestWorkflowResult {
        let status = self.get_status(workflow_key);
        let total_steps = self.get_total_steps(workflow_key);
        let mut steps_completed = 0u32;
        for step in 0..total_steps {
            if self.is_step_completed(workflow_key, step) {
                steps_completed += 1;
            }
        }
        TestWorkflowResult {
            workflow_key,
            status,
            steps_completed,
            total_steps,
        }
    }

    /// Assert workflow status.
    pub fn assert_status(&self, workflow_key: u64, expected: WorkflowStatus) {
        let actual = self.get_status(workflow_key);
        assert_eq!(actual, expected, "Workflow {} status mismatch", workflow_key);
    }

    /// Assert workflow is completed.
    pub fn assert_completed(&self, workflow_key: u64) {
        self.assert_status(workflow_key, WorkflowStatus::Completed);
    }

    /// Assert workflow is failed.
    pub fn assert_failed(&self, workflow_key: u64) {
        self.assert_status(workflow_key, WorkflowStatus::Failed);
    }

    /// Assert workflow is running.
    pub fn assert_running(&self, workflow_key: u64) {
        self.assert_status(workflow_key, WorkflowStatus::Running);
    }

    /// Assert a step result.
    pub fn assert_step_result(&self, workflow_key: u64, step: u32, expected: &[u8]) {
        assert!(
            self.is_step_completed(workflow_key, step),
            "Step {} should be completed for workflow {}", step, workflow_key
        );
        let actual = self.get_step_result(workflow_key, step).unwrap_or_default();
        assert_eq!(actual, expected, "Step {} result mismatch for workflow {}", step, workflow_key);
    }

    /// Assert workflow count.
    pub fn assert_workflow_count(&self, expected: usize) {
        let actual = self.workflow_count();
        assert_eq!(actual, expected, "Workflow count mismatch");
    }

    // ─── Run-to-Completion Helper ─────────────────────────────────────

    /// Run a workflow to completion by auto-completing all steps using mocks.
    pub fn run_workflow_to_completion(
        &self,
        workflow_key: u64,
        workflow_type_id: u64,
        final_result: Option<Vec<u8>>,
    ) -> TestWorkflowResult {
        let total_steps = self.get_total_steps(workflow_key);
        for step in 0..total_steps {
            let step_result = if let Some(mock) = self.get_mock(workflow_type_id, step) {
                match mock {
                    MockActivityResult::Success(data) => data.clone(),
                    MockActivityResult::Failure(_) => {
                        self.fail_workflow(workflow_key);
                        return self.build_test_result(workflow_key);
                    }
                    MockActivityResult::Timeout => {
                        self.fail_workflow(workflow_key);
                        return self.build_test_result(workflow_key);
                    }
                    MockActivityResult::Block => {
                        return self.build_test_result(workflow_key);
                    }
                }
            } else {
                vec![] // Default empty success
            };
            self.complete_step(workflow_key, step, step_result);
        }
        self.complete_workflow(workflow_key, final_result);
        self.build_test_result(workflow_key)
    }

    /// Shutdown the test environment.
    pub fn shutdown(&self) {
        self.engine.shutdown();
    }
}

impl Default for TestWorkflowEnvironment {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for TestWorkflowEnvironment {
    fn drop(&mut self) {
        self.engine.shutdown();
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_env_basic_workflow() {
        let env = TestWorkflowEnvironment::new();
        let key = env.start_workflow(1, 100, 0, 42, 3, None);
        env.assert_running(key);
        assert_eq!(env.get_total_steps(key), 3);
        assert_eq!(env.workflow_count(), 1);
        env.shutdown();
    }

    #[test]
    fn test_run_to_completion() {
        let env = TestWorkflowEnvironment::new();
        let key = env.start_workflow(1, 100, 0, 42, 3, None);
        let result = env.run_workflow_to_completion(key, 100, Some(vec![42]));
        assert_eq!(result.status, WorkflowStatus::Completed);
        assert_eq!(result.steps_completed, 3);
        assert_eq!(result.total_steps, 3);
        env.shutdown();
    }

    #[test]
    fn test_activity_mock_failure() {
        let mut env = TestWorkflowEnvironment::new();
        env.mock_activity(100, 1, MockActivityResult::Failure("timeout".into()));
        let key = env.start_workflow(1, 100, 0, 42, 3, None);
        let result = env.run_workflow_to_completion(key, 100, None);
        assert_eq!(result.status, WorkflowStatus::Failed);
        assert_eq!(result.steps_completed, 1);
        env.shutdown();
    }

    #[test]
    fn test_activity_mock_block() {
        let mut env = TestWorkflowEnvironment::new();
        env.mock_activity(100, 2, MockActivityResult::Block);
        let key = env.start_workflow(1, 100, 0, 42, 3, None);
        let result = env.run_workflow_to_completion(key, 100, None);
        assert_eq!(result.status, WorkflowStatus::Running);
        assert_eq!(result.steps_completed, 2);
        env.shutdown();
    }

    #[test]
    fn test_signal_during_execution() {
        let env = TestWorkflowEnvironment::new();
        let key = env.start_workflow(1, 100, 0, 42, 3, None);
        env.signal_workflow(key, 1, vec![1, 2, 3]);
        env.complete_workflow(key, Some(vec![99]));
        env.assert_completed(key);
        env.shutdown();
    }

    #[test]
    fn test_child_workflow() {
        let env = TestWorkflowEnvironment::new();
        let parent = env.start_workflow(1, 100, 0, 42, 3, None);
        let child = env.start_child_workflow(parent, 2, 200, 42, 2, None);
        env.assert_running(parent);
        env.assert_running(child);
        assert_eq!(env.workflow_count(), 2);
        let cr = env.run_workflow_to_completion(child, 200, Some(vec![1]));
        assert_eq!(cr.status, WorkflowStatus::Completed);
        let pr = env.run_workflow_to_completion(parent, 100, Some(vec![2]));
        assert_eq!(pr.status, WorkflowStatus::Completed);
        env.shutdown();
    }

    #[test]
    fn test_cancel_workflow() {
        let env = TestWorkflowEnvironment::new();
        let key = env.start_workflow(1, 100, 0, 42, 5, None);
        env.complete_step(key, 0, vec![]);
        env.complete_step(key, 1, vec![]);
        env.cancel_workflow(key);
        env.assert_status(key, WorkflowStatus::Canceled);
        assert_eq!(env.get_current_step(key), 2);
        env.shutdown();
    }

    #[test]
    fn test_step_results() {
        let env = TestWorkflowEnvironment::new();
        let key = env.start_workflow(1, 100, 0, 42, 3, None);
        env.complete_step(key, 0, vec![10]);
        env.complete_step(key, 1, vec![20]);
        env.complete_step(key, 2, vec![30]);
        env.assert_step_result(key, 0, &[10]);
        env.assert_step_result(key, 1, &[20]);
        env.assert_step_result(key, 2, &[30]);
        env.shutdown();
    }

    #[test]
    fn test_build_test_result() {
        let env = TestWorkflowEnvironment::new();
        let key = env.start_workflow(1, 100, 0, 42, 5, None);
        env.complete_step(key, 0, vec![]);
        env.complete_step(key, 1, vec![]);
        let result = env.build_test_result(key);
        assert_eq!(result.status, WorkflowStatus::Running);
        assert_eq!(result.steps_completed, 2);
        assert_eq!(result.total_steps, 5);
        env.shutdown();
    }

    #[test]
    fn test_mock_all_activities() {
        let mut env = TestWorkflowEnvironment::new();
        env.mock_all_activities(100, 3, vec![42]);
        let key = env.start_workflow(1, 100, 0, 42, 3, None);
        let result = env.run_workflow_to_completion(key, 100, Some(vec![99]));
        assert_eq!(result.status, WorkflowStatus::Completed);
        assert_eq!(result.steps_completed, 3);
        env.assert_step_result(key, 0, &[42]);
        env.assert_step_result(key, 1, &[42]);
        env.assert_step_result(key, 2, &[42]);
        env.shutdown();
    }

    #[test]
    fn test_search_attributes_in_test() {
        let env = TestWorkflowEnvironment::new();
        let mut attrs = HashMap::new();
        attrs.insert("OrderId".to_string(), SearchAttributeValue::String("ORD-123".to_string()));
        let key = env.start_workflow_with_attrs(1, 100, 0, 42, 3, None, attrs);
        env.assert_running(key);
        let vis = env.engine().visibility().list_by_search_attribute(
            "OrderId",
            &SearchAttributeValue::String("ORD-123".to_string()),
        );
        assert_eq!(vis.len(), 1);
        env.shutdown();
    }

    #[test]
    fn test_update_during_execution() {
        let env = TestWorkflowEnvironment::new();
        let key = env.start_workflow(1, 100, 0, 42, 3, None);
        env.update_workflow(key, 1, vec![5, 6, 7]);
        let result = env.run_workflow_to_completion(key, 100, Some(vec![99]));
        assert_eq!(result.status, WorkflowStatus::Completed);
        env.shutdown();
    }

    #[test]
    fn test_multiple_workflow_types() {
        let mut env = TestWorkflowEnvironment::new();
        env.mock_activity(100, 0, MockActivityResult::Success(vec![1]));
        env.mock_activity(200, 0, MockActivityResult::Success(vec![2]));
        let k1 = env.start_workflow(1, 100, 0, 42, 1, None);
        let k2 = env.start_workflow(2, 200, 0, 42, 1, None);
        let r1 = env.run_workflow_to_completion(k1, 100, None);
        let r2 = env.run_workflow_to_completion(k2, 200, None);
        assert_eq!(r1.status, WorkflowStatus::Completed);
        assert_eq!(r2.status, WorkflowStatus::Completed);
        env.assert_step_result(k1, 0, &[1]);
        env.assert_step_result(k2, 0, &[2]);
        env.shutdown();
    }

    #[test]
    fn test_terminate_workflow() {
        let env = TestWorkflowEnvironment::new();
        let key = env.start_workflow(1, 100, 0, 42, 3, None);
        env.terminate_workflow(key);
        env.assert_status(key, WorkflowStatus::Terminated);
        env.shutdown();
    }

    #[test]
    fn test_activity_mock_timeout() {
        let mut env = TestWorkflowEnvironment::new();
        env.mock_activity(100, 0, MockActivityResult::Timeout);
        let key = env.start_workflow(1, 100, 0, 42, 3, None);
        let result = env.run_workflow_to_completion(key, 100, None);
        assert_eq!(result.status, WorkflowStatus::Failed);
        assert_eq!(result.steps_completed, 0);
        env.shutdown();
    }
}
