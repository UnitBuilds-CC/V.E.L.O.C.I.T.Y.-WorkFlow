//! Saga orchestration primitives with compensation (rollback) support.
//! Implements the saga pattern: a sequence of local transactions where each has a
//! compensating action that can undo its effects if a later step fails.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, RwLock};

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
}

impl SagaStepDefinition {
    pub fn new(name: &str, workflow_type_id: u64) -> Self {
        Self {
            name: name.to_string(),
            workflow_type_id,
            input: None,
            compensation_workflow_type_id: None,
            compensation_input: None,
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
}
