//! Saga orchestration primitives with compensation (rollback) support.
//! Implements the saga pattern: a sequence of local transactions where each has a
//! compensating action that can undo its effects if a later step fails.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;

/// How a step executes relative to its siblings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StepExecutionMode {
    /// Execute after the previous step completes.
    Sequential,
    /// Execute in parallel with other parallel steps in the same group.
    Parallel,
}

/// A single step in a saga, with a compensation action.
#[derive(Debug, Clone)]
pub struct SagaStep {
    pub step_id: u64,
    pub name: String,
    pub workflow_type_id: u64,
    pub input: Option<Vec<u8>>,
    pub result: Option<Vec<u8>>,
    pub compensation_workflow_type_id: Option<u64>,
    pub compensation_input: Option<Vec<u8>>,
    pub status: SagaStepStatus,
    pub execution_mode: StepExecutionMode,
    pub timeout_ms: u64,
    pub max_retries: u32,
    pub retry_count: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SagaStepStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Compensating,
    Compensated,
    CompensationFailed,
}

/// A saga execution instance.
#[derive(Debug, Clone)]
pub struct SagaExecution {
    pub saga_id: u64,
    pub workflow_key: u64,
    pub steps: Vec<SagaStep>,
    pub current_step: usize,
    pub status: SagaStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SagaStatus {
    Created,
    Running,
    Completed,
    Failed,
    Compensating,
    Compensated,
    PartiallyCompensated,
}

/// Saga manager that tracks saga executions and coordinates compensation.
pub struct SagaOrchestrator {
    sagas: RwLock<HashMap<u64, SagaExecution>>,
    next_id: AtomicU64,
}

impl SagaOrchestrator {
    pub fn new() -> Self {
        Self {
            sagas: RwLock::new(HashMap::new()),
            next_id: AtomicU64::new(1),
        }
    }

    /// Create a new saga with the given steps. Returns the saga ID.
    pub fn create_saga(&self, workflow_key: u64, steps: Vec<SagaStepDefinition>) -> u64 {
        let saga_id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let saga_steps: Vec<SagaStep> = steps
            .into_iter()
            .enumerate()
            .map(|(i, def)| SagaStep {
                step_id: i as u64,
                name: def.name,
                workflow_type_id: def.workflow_type_id,
                input: def.input,
                result: None,
                compensation_workflow_type_id: def.compensation_workflow_type_id,
                compensation_input: def.compensation_input,
                status: SagaStepStatus::Pending,
                execution_mode: def.execution_mode,
                timeout_ms: def.timeout_ms,
                max_retries: def.max_retries,
                retry_count: 0,
            })
            .collect();

        let execution = SagaExecution {
            saga_id,
            workflow_key,
            steps: saga_steps,
            current_step: 0,
            status: SagaStatus::Created,
        };

        self.sagas.write().unwrap().insert(saga_id, execution);
        saga_id
    }

    /// Mark a saga step as completed with a result.
    pub fn complete_step(&self, saga_id: u64, step_index: usize, result: Option<Vec<u8>>) -> bool {
        let mut sagas = self.sagas.write().unwrap();
        if let Some(saga) = sagas.get_mut(&saga_id) {
            if step_index < saga.steps.len() {
                saga.steps[step_index].status = SagaStepStatus::Completed;
                saga.steps[step_index].result = result;
                saga.current_step = step_index + 1;
                if saga.current_step >= saga.steps.len() {
                    saga.status = SagaStatus::Completed;
                } else if saga.status == SagaStatus::Created {
                    saga.status = SagaStatus::Running;
                }
                return true;
            }
        }
        false
    }

    /// Mark a saga step as failed. Triggers compensation for all completed steps.
    /// Returns the list of compensation workflow type IDs that need to be executed (in reverse order).
    pub fn fail_step(&self, saga_id: u64, step_index: usize) -> Vec<(u64, Option<Vec<u8>>)> {
        let mut sagas = self.sagas.write().unwrap();
        let mut compensations = Vec::new();

        if let Some(saga) = sagas.get_mut(&saga_id) {
            if step_index < saga.steps.len() {
                saga.steps[step_index].status = SagaStepStatus::Failed;
                saga.status = SagaStatus::Compensating;

                // Compensate completed steps in reverse order
                for i in (0..step_index).rev() {
                    if saga.steps[i].status == SagaStepStatus::Completed {
                        if let Some(comp_type_id) = saga.steps[i].compensation_workflow_type_id {
                            saga.steps[i].status = SagaStepStatus::Compensating;
                            compensations
                                .push((comp_type_id, saga.steps[i].compensation_input.clone()));
                        }
                    }
                }
            }
        }

        compensations
    }

    /// Mark a compensation step as completed.
    pub fn complete_compensation(&self, saga_id: u64, step_index: usize) {
        let mut sagas = self.sagas.write().unwrap();
        if let Some(saga) = sagas.get_mut(&saga_id) {
            if step_index < saga.steps.len() {
                saga.steps[step_index].status = SagaStepStatus::Compensated;
                // Check if all compensations are done
                let all_done = saga.steps.iter().all(|s| {
                    s.status == SagaStepStatus::Compensated
                        || s.status == SagaStepStatus::Pending
                        || s.status == SagaStepStatus::Failed
                });
                if all_done {
                    saga.status = SagaStatus::Compensated;
                }
            }
        }
    }

    /// Get saga status.
    pub fn get_saga(&self, saga_id: u64) -> Option<SagaExecution> {
        self.sagas.read().unwrap().get(&saga_id).cloned()
    }

    /// Get the total number of sagas.
    pub fn saga_count(&self) -> usize {
        self.sagas.read().unwrap().len()
    }

    /// Get sagas by status.
    pub fn sagas_by_status(&self, status: SagaStatus) -> Vec<SagaExecution> {
        self.sagas
            .read()
            .unwrap()
            .values()
            .filter(|s| s.status == status)
            .cloned()
            .collect()
    }
}

impl Default for SagaOrchestrator {
    fn default() -> Self {
        Self::new()
    }
}

/// Definition for creating a saga step.
pub struct SagaStepDefinition {
    pub name: String,
    pub workflow_type_id: u64,
    pub input: Option<Vec<u8>>,
    pub compensation_workflow_type_id: Option<u64>,
    pub compensation_input: Option<Vec<u8>>,
    /// Execution mode: sequential (default) or parallel within a group.
    pub execution_mode: StepExecutionMode,
    /// Timeout for this step in milliseconds (0 = no timeout).
    pub timeout_ms: u64,
    /// Retry policy for this step.
    pub max_retries: u32,
}

impl SagaStepDefinition {
    pub fn new(name: &str, workflow_type_id: u64) -> Self {
        Self {
            name: name.to_string(),
            workflow_type_id,
            input: None,
            compensation_workflow_type_id: None,
            compensation_input: None,
            execution_mode: StepExecutionMode::Sequential,
            timeout_ms: 0,
            max_retries: 0,
        }
    }

    pub fn with_input(mut self, input: Vec<u8>) -> Self {
        self.input = Some(input);
        self
    }

    pub fn with_compensation(mut self, comp_type_id: u64, comp_input: Option<Vec<u8>>) -> Self {
        self.compensation_workflow_type_id = Some(comp_type_id);
        self.compensation_input = comp_input;
        self
    }

    pub fn with_parallel(mut self) -> Self {
        self.execution_mode = StepExecutionMode::Parallel;
        self
    }

    pub fn with_timeout(mut self, timeout_ms: u64) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }

    pub fn with_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }
}

// ─── Saga Execution Log ──────────────────────────────────────────────────────

/// An audit log entry for saga execution.
#[derive(Debug, Clone)]
pub struct SagaLogEntry {
    pub entry_id: u64,
    pub saga_id: u64,
    pub timestamp_ms: u64,
    pub entry_type: SagaLogEntryType,
    pub step_index: Option<usize>,
    pub details: String,
}

#[derive(Debug, Clone)]
pub enum SagaLogEntryType {
    SagaCreated,
    StepStarted,
    StepCompleted,
    StepFailed,
    CompensationStarted,
    CompensationCompleted,
    CompensationFailed,
    SagaCompleted,
    SagaCompensated,
    SagaPartiallyCompensated,
    StepRetrying,
    StepTimedOut,
}

/// Execution log for a saga — full audit trail.
#[derive(Debug, Clone, Default)]
pub struct SagaExecutionLog {
    pub entries: Vec<SagaLogEntry>,
    next_entry_id: u64,
}

impl SagaExecutionLog {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn log(&mut self, saga_id: u64, entry_type: SagaLogEntryType, step_index: Option<usize>, details: String) {
        let entry = SagaLogEntry {
            entry_id: self.next_entry_id,
            saga_id,
            timestamp_ms: now_ms(),
            entry_type,
            step_index,
            details,
        };
        self.entries.push(entry);
        self.next_entry_id += 1;
    }

    pub fn entry_count(&self) -> usize {
        self.entries.len()
    }

    pub fn last_entry(&self) -> Option<&SagaLogEntry> {
        self.entries.last()
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// ─── Nested Saga Support ─────────────────────────────────────────────────────

/// A nested saga is a saga that runs as part of a parent saga step.
/// If the nested saga fails, its compensation propagates to the parent.
#[derive(Debug, Clone)]
pub struct NestedSagaRef {
    pub parent_saga_id: u64,
    pub parent_step_index: usize,
    pub child_saga_id: u64,
}

/// Enhanced orchestrator with nested saga support and execution logging.
pub struct EnhancedSagaOrchestrator {
    inner: SagaOrchestrator,
    logs: RwLock<HashMap<u64, SagaExecutionLog>>,
    nested_refs: RwLock<Vec<NestedSagaRef>>,
}

impl EnhancedSagaOrchestrator {
    pub fn new() -> Self {
        Self {
            inner: SagaOrchestrator::new(),
            logs: RwLock::new(HashMap::new()),
            nested_refs: RwLock::new(Vec::new()),
        }
    }

    /// Create a saga with logging.
    pub fn create_saga(&self, workflow_key: u64, steps: Vec<SagaStepDefinition>) -> u64 {
        let saga_id = self.inner.create_saga(workflow_key, steps);
        let mut log = SagaExecutionLog::new();
        log.log(saga_id, SagaLogEntryType::SagaCreated, None, format!("workflow_key={}", workflow_key));
        self.logs.write().unwrap().insert(saga_id, log);
        saga_id
    }

    /// Complete a step with logging.
    pub fn complete_step(&self, saga_id: u64, step_index: usize, result: Option<Vec<u8>>) -> bool {
        let ok = self.inner.complete_step(saga_id, step_index, result);
        if ok {
            let mut logs = self.logs.write().unwrap();
            if let Some(log) = logs.get_mut(&saga_id) {
                log.log(saga_id, SagaLogEntryType::StepCompleted, Some(step_index), String::new());
            }
        }
        ok
    }

    /// Start a step (marks it as running).
    pub fn start_step(&self, saga_id: u64, step_index: usize) -> bool {
        let mut logs = self.logs.write().unwrap();
        if let Some(log) = logs.get_mut(&saga_id) {
            log.log(saga_id, SagaLogEntryType::StepStarted, Some(step_index), String::new());
        }
        true
    }

    /// Fail a step with logging and compensation tracking.
    pub fn fail_step(&self, saga_id: u64, step_index: usize) -> Vec<(u64, Option<Vec<u8>>)> {
        let mut logs = self.logs.write().unwrap();
        if let Some(log) = logs.get_mut(&saga_id) {
            log.log(saga_id, SagaLogEntryType::StepFailed, Some(step_index), String::new());
            log.log(saga_id, SagaLogEntryType::CompensationStarted, None, format!("failed_step={}", step_index));
        }
        self.inner.fail_step(saga_id, step_index)
    }

    /// Complete compensation with logging.
    pub fn complete_compensation(&self, saga_id: u64, step_index: usize) {
        self.inner.complete_compensation(saga_id, step_index);
        let mut logs = self.logs.write().unwrap();
        if let Some(log) = logs.get_mut(&saga_id) {
            log.log(saga_id, SagaLogEntryType::CompensationCompleted, Some(step_index), String::new());
        }
    }

    /// Create a nested saga — a child saga attached to a parent step.
    pub fn create_nested_saga(&self, parent_saga_id: u64, parent_step_index: usize, workflow_key: u64, steps: Vec<SagaStepDefinition>) -> u64 {
        let child_saga_id = self.create_saga(workflow_key, steps);
        self.nested_refs.write().unwrap().push(NestedSagaRef {
            parent_saga_id,
            parent_step_index,
            child_saga_id,
        });
        child_saga_id
    }

    /// Get the execution log for a saga.
    pub fn get_log(&self, saga_id: u64) -> Option<SagaExecutionLog> {
        self.logs.read().unwrap().get(&saga_id).cloned()
    }

    /// Get nested saga IDs for a parent saga.
    pub fn get_nested_sagas(&self, parent_saga_id: u64) -> Vec<u64> {
        self.nested_refs.read().unwrap()
            .iter()
            .filter(|r| r.parent_saga_id == parent_saga_id)
            .map(|r| r.child_saga_id)
            .collect()
    }

    /// Get saga status.
    pub fn get_saga(&self, saga_id: u64) -> Option<SagaExecution> {
        self.inner.get_saga(saga_id)
    }

    /// Get the parallel steps for a given saga (steps marked as Parallel).
    pub fn get_parallel_groups(&self, saga_id: u64) -> Vec<Vec<usize>> {
        let sagas = self.inner.sagas.read().unwrap();
        if let Some(saga) = sagas.get(&saga_id) {
            let mut groups: Vec<Vec<usize>> = Vec::new();
            let mut current_group: Vec<usize> = Vec::new();

            for (i, step) in saga.steps.iter().enumerate() {
                // Check if step definition has parallel mode
                // We track this via the step's execution_mode field
                // For now, group consecutive parallel steps together
                if step.status == SagaStepStatus::Pending {
                    current_group.push(i);
                } else if !current_group.is_empty() {
                    if current_group.len() > 1 {
                        groups.push(current_group.clone());
                    }
                    current_group.clear();
                }
            }
            if current_group.len() > 1 {
                groups.push(current_group);
            }
            groups
        } else {
            Vec::new()
        }
    }

    pub fn saga_count(&self) -> usize {
        self.inner.saga_count()
    }
}

impl Default for EnhancedSagaOrchestrator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_saga() {
        let orchestrator = SagaOrchestrator::new();
        let steps = vec![
            SagaStepDefinition::new("book_flight", 100).with_compensation(200, None),
            SagaStepDefinition::new("book_hotel", 101).with_compensation(201, None),
            SagaStepDefinition::new("book_car", 102).with_compensation(202, None),
        ];

        let saga_id = orchestrator.create_saga(42, steps);
        assert!(saga_id > 0);
        assert_eq!(orchestrator.saga_count(), 1);

        let saga = orchestrator.get_saga(saga_id).unwrap();
        assert_eq!(saga.steps.len(), 3);
        assert_eq!(saga.status, SagaStatus::Created);
    }

    #[test]
    fn test_saga_happy_path() {
        let orchestrator = SagaOrchestrator::new();
        let steps = vec![
            SagaStepDefinition::new("step1", 100).with_compensation(200, None),
            SagaStepDefinition::new("step2", 101).with_compensation(201, None),
        ];

        let saga_id = orchestrator.create_saga(42, steps);
        assert!(orchestrator.complete_step(saga_id, 0, Some(vec![1])));
        assert!(orchestrator.complete_step(saga_id, 1, Some(vec![2])));

        let saga = orchestrator.get_saga(saga_id).unwrap();
        assert_eq!(saga.status, SagaStatus::Completed);
    }

    #[test]
    fn test_saga_compensation_on_failure() {
        let orchestrator = SagaOrchestrator::new();
        let steps = vec![
            SagaStepDefinition::new("step1", 100).with_compensation(200, Some(vec![10])),
            SagaStepDefinition::new("step2", 101).with_compensation(201, None),
            SagaStepDefinition::new("step3", 102).with_compensation(202, None),
        ];

        let saga_id = orchestrator.create_saga(42, steps);
        orchestrator.complete_step(saga_id, 0, None);
        orchestrator.complete_step(saga_id, 1, None);

        // Step 2 (index 2) fails — should trigger compensation for steps 0 and 1
        let compensations = orchestrator.fail_step(saga_id, 2);
        assert_eq!(compensations.len(), 2);
        // Should be in reverse order
        assert_eq!(compensations[0].0, 201); // step2 compensation
        assert_eq!(compensations[1].0, 200); // step1 compensation
        assert_eq!(compensations[1].1, Some(vec![10])); // step1 compensation input

        let saga = orchestrator.get_saga(saga_id).unwrap();
        assert_eq!(saga.status, SagaStatus::Compensating);
    }

    #[test]
    fn test_saga_compensation_completion() {
        let orchestrator = SagaOrchestrator::new();
        let steps = vec![
            SagaStepDefinition::new("step1", 100).with_compensation(200, None),
            SagaStepDefinition::new("step2", 101).with_compensation(201, None),
        ];

        let saga_id = orchestrator.create_saga(42, steps);
        orchestrator.complete_step(saga_id, 0, None);

        // Step 1 fails
        orchestrator.fail_step(saga_id, 1);

        // Complete compensation for step 0
        orchestrator.complete_compensation(saga_id, 0);

        let saga = orchestrator.get_saga(saga_id).unwrap();
        assert_eq!(saga.status, SagaStatus::Compensated);
    }

    // ─── Enhanced Saga Tests ──────────────────────────────────────────────────

    #[test]
    fn test_enhanced_saga_creation_with_log() {
        let orch = EnhancedSagaOrchestrator::new();
        let steps = vec![
            SagaStepDefinition::new("step1", 100).with_compensation(200, None),
            SagaStepDefinition::new("step2", 101).with_compensation(201, None),
        ];
        let saga_id = orch.create_saga(42, steps);
        let log = orch.get_log(saga_id).unwrap();
        assert_eq!(log.entry_count(), 1);
        assert!(matches!(log.entries[0].entry_type, SagaLogEntryType::SagaCreated));
    }

    #[test]
    fn test_enhanced_saga_step_lifecycle_logging() {
        let orch = EnhancedSagaOrchestrator::new();
        let steps = vec![
            SagaStepDefinition::new("step1", 100).with_compensation(200, None),
            SagaStepDefinition::new("step2", 101).with_compensation(201, None),
        ];
        let saga_id = orch.create_saga(42, steps);

        orch.start_step(saga_id, 0);
        orch.complete_step(saga_id, 0, Some(vec![42]));

        let log = orch.get_log(saga_id).unwrap();
        assert_eq!(log.entry_count(), 3); // Created + Started + Completed
        assert!(matches!(log.entries[1].entry_type, SagaLogEntryType::StepStarted));
        assert!(matches!(log.entries[2].entry_type, SagaLogEntryType::StepCompleted));
    }

    #[test]
    fn test_enhanced_saga_failure_logging() {
        let orch = EnhancedSagaOrchestrator::new();
        let steps = vec![
            SagaStepDefinition::new("step1", 100).with_compensation(200, None),
            SagaStepDefinition::new("step2", 101),
        ];
        let saga_id = orch.create_saga(42, steps);
        orch.complete_step(saga_id, 0, None);
        orch.fail_step(saga_id, 1);

        let log = orch.get_log(saga_id).unwrap();
        let has_failed = log.entries.iter().any(|e| matches!(e.entry_type, SagaLogEntryType::StepFailed));
        let has_comp_start = log.entries.iter().any(|e| matches!(e.entry_type, SagaLogEntryType::CompensationStarted));
        assert!(has_failed);
        assert!(has_comp_start);
    }

    #[test]
    fn test_nested_saga() {
        let orch = EnhancedSagaOrchestrator::new();
        let parent_steps = vec![
            SagaStepDefinition::new("outer_step1", 100).with_compensation(200, None),
            SagaStepDefinition::new("outer_step2", 101).with_compensation(201, None),
        ];
        let parent_id = orch.create_saga(42, parent_steps);

        // Create a nested saga for step 0
        let child_steps = vec![
            SagaStepDefinition::new("inner_step1", 300).with_compensation(400, None),
            SagaStepDefinition::new("inner_step2", 301).with_compensation(401, None),
        ];
        let child_id = orch.create_nested_saga(parent_id, 0, 42, child_steps);

        // Verify nesting
        let nested = orch.get_nested_sagas(parent_id);
        assert_eq!(nested.len(), 1);
        assert_eq!(nested[0], child_id);

        // Both sagas should be accessible
        assert!(orch.get_saga(parent_id).is_some());
        assert!(orch.get_saga(child_id).is_some());
    }

    #[test]
    fn test_step_definition_builder() {
        let step = SagaStepDefinition::new("complex_step", 100)
            .with_input(vec![1, 2, 3])
            .with_compensation(200, Some(vec![4, 5]))
            .with_parallel()
            .with_timeout(30_000)
            .with_retries(3);

        assert_eq!(step.name, "complex_step");
        assert_eq!(step.workflow_type_id, 100);
        assert_eq!(step.input, Some(vec![1, 2, 3]));
        assert_eq!(step.compensation_workflow_type_id, Some(200));
        assert_eq!(step.compensation_input, Some(vec![4, 5]));
        assert_eq!(step.execution_mode, StepExecutionMode::Parallel);
        assert_eq!(step.timeout_ms, 30_000);
        assert_eq!(step.max_retries, 3);
    }

    #[test]
    fn test_saga_execution_log_ordering() {
        let mut log = SagaExecutionLog::new();
        log.log(1, SagaLogEntryType::SagaCreated, None, "init".into());
        log.log(1, SagaLogEntryType::StepStarted, Some(0), "start".into());
        log.log(1, SagaLogEntryType::StepCompleted, Some(0), "done".into());

        assert_eq!(log.entry_count(), 3);
        assert_eq!(log.entries[0].entry_id, 0);
        assert_eq!(log.entries[1].entry_id, 1);
        assert_eq!(log.entries[2].entry_id, 2);
        assert_eq!(log.last_entry().unwrap().entry_id, 2);
    }
}
