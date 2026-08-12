//! Worker Determinism Checker — ensures deterministic workflow execution.
//!
//! Workflow code must be deterministic for replay to work correctly.
//! This module tracks side effects and detects non-deterministic operations.

use std::collections::HashMap;
use std::sync::Mutex;

/// Severity of a determinism violation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ViolationSeverity {
    Warning,
    Error,
    Fatal,
}

/// Type of workflow operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationType {
    Signal,
    Query,
    Timer,
    ChildWorkflow,
    Activity,
    SideEffect,
    RandomNumber,
    SystemTime,
    FileIO,
    NetworkIO,
}

/// A workflow operation to check for determinism.
#[derive(Debug, Clone)]
pub struct WorkflowOperation {
    pub name: String,
    pub op_type: OperationType,
    pub step: u32,
}

/// A determinism violation.
#[derive(Debug, Clone)]
pub struct DeterminismViolation {
    pub operation: String,
    pub step: u32,
    pub reason: String,
    pub severity: ViolationSeverity,
}

/// A recorded side effect.
#[derive(Debug, Clone)]
pub struct RecordedSideEffect {
    pub id: u64,
    pub operation: String,
    pub result: Vec<u8>,
    pub timestamp: u64,
}

/// Result of a determinism check.
#[derive(Debug, Clone)]
pub struct DeterminismResult {
    pub is_deterministic: bool,
    pub violations: Vec<DeterminismViolation>,
    pub side_effect_count: usize,
}

/// Checks and enforces deterministic execution.
pub struct DeterminismChecker {
    violations: Mutex<Vec<DeterminismViolation>>,
    side_effects: Mutex<Vec<RecordedSideEffect>>,
    side_effect_index: Mutex<u64>,
    replay_mode: Mutex<bool>,
}

impl DeterminismChecker {
    pub fn new() -> Self {
        Self {
            violations: Mutex::new(Vec::new()),
            side_effects: Mutex::new(Vec::new()),
            side_effect_index: Mutex::new(0),
            replay_mode: Mutex::new(false),
        }
    }

    /// Enable or disable replay mode.
    pub fn set_replay_mode(&self, replay: bool) {
        *self.replay_mode.lock().unwrap() = replay;
    }

    /// Check determinism for a workflow step.
    pub fn check_determinism(&self, workflow_key: u64, step: u32) -> DeterminismResult {
        let violations = self.violations.lock().unwrap();
        let side_effects = self.side_effects.lock().unwrap();

        let step_violations: Vec<_> = violations.iter()
            .filter(|v| v.step == step)
            .cloned()
            .collect();

        DeterminismResult {
            is_deterministic: step_violations.is_empty(),
            violations: step_violations,
            side_effect_count: side_effects.len(),
        }
    }

    /// Record a side effect.
    pub fn record_side_effect(&self, operation: &str, result: &[u8], timestamp: u64) -> u64 {
        let mut effects = self.side_effects.lock().unwrap();
        let mut idx = self.side_effect_index.lock().unwrap();
        let id = *idx;
        *idx += 1;

        effects.push(RecordedSideEffect {
            id,
            operation: operation.to_string(),
            result: result.to_vec(),
            timestamp,
        });
        id
    }

    /// Replay a side effect by ID.
    pub fn replay_side_effect(&self, side_effect_id: u64) -> Option<Vec<u8>> {
        let effects = self.side_effects.lock().unwrap();
        effects.iter()
            .find(|e| e.id == side_effect_id)
            .map(|e| e.result.clone())
    }

    /// Validate that operations don't contain non-deterministic ops.
    pub fn validate_no_nondeterministic_ops(&self, operations: &[WorkflowOperation]) -> Vec<DeterminismViolation> {
        let mut violations = Vec::new();

        for op in operations {
            match op.op_type {
                OperationType::RandomNumber => {
                    violations.push(DeterminismViolation {
                        operation: op.name.clone(),
                        step: op.step,
                        reason: "Random number generation is non-deterministic. Use side effects instead.".to_string(),
                        severity: ViolationSeverity::Fatal,
                    });
                }
                OperationType::SystemTime => {
                    violations.push(DeterminismViolation {
                        operation: op.name.clone(),
                        step: op.step,
                        reason: "System time is non-deterministic. Use workflow timers instead.".to_string(),
                        severity: ViolationSeverity::Fatal,
                    });
                }
                OperationType::FileIO => {
                    violations.push(DeterminismViolation {
                        operation: op.name.clone(),
                        step: op.step,
                        reason: "File I/O is non-deterministic. Use activities instead.".to_string(),
                        severity: ViolationSeverity::Error,
                    });
                }
                OperationType::NetworkIO => {
                    violations.push(DeterminismViolation {
                        operation: op.name.clone(),
                        step: op.step,
                        reason: "Network I/O is non-deterministic. Use activities instead.".to_string(),
                        severity: ViolationSeverity::Error,
                    });
                }
                _ => {} // Signal, Query, Timer, ChildWorkflow, Activity, SideEffect are OK
            }
        }

        if !violations.is_empty() {
            let mut stored = self.violations.lock().unwrap();
            stored.extend(violations.clone());
        }

        violations
    }

    /// Get total violation count.
    pub fn violation_count(&self) -> usize {
        self.violations.lock().unwrap().len()
    }

    /// Get side effect count.
    pub fn side_effect_count(&self) -> usize {
        self.side_effects.lock().unwrap().len()
    }

    /// Clear all violations.
    pub fn clear_violations(&self) {
        self.violations.lock().unwrap().clear();
    }

    /// Clear all side effects.
    pub fn clear_side_effects(&self) {
        self.side_effects.lock().unwrap().clear();
        *self.side_effect_index.lock().unwrap() = 0;
    }

    /// Get all violations.
    pub fn all_violations(&self) -> Vec<DeterminismViolation> {
        self.violations.lock().unwrap().clone()
    }

    /// Get all side effects.
    pub fn all_side_effects(&self) -> Vec<RecordedSideEffect> {
        self.side_effects.lock().unwrap().clone()
    }

    /// Check if in replay mode.
    pub fn is_replay_mode(&self) -> bool {
        *self.replay_mode.lock().unwrap()
    }
}

impl Default for DeterminismChecker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_and_replay_side_effect() {
        let checker = DeterminismChecker::new();
        let id = checker.record_side_effect("generate-uuid", b"abc-123", 1000);
        let result = checker.replay_side_effect(id).unwrap();
        assert_eq!(result, b"abc-123");
    }

    #[test]
    fn test_replay_nonexistent() {
        let checker = DeterminismChecker::new();
        assert!(checker.replay_side_effect(999).is_none());
    }

    #[test]
    fn test_validate_random_number() {
        let checker = DeterminismChecker::new();
        let ops = vec![
            WorkflowOperation {
                name: "generate-id".to_string(),
                op_type: OperationType::RandomNumber,
                step: 1,
            },
        ];

        let violations = checker.validate_no_nondeterministic_ops(&ops);
        assert_eq!(violations.len(), 1);
        assert_eq!(violations[0].severity, ViolationSeverity::Fatal);
    }

    #[test]
    fn test_validate_system_time() {
        let checker = DeterminismChecker::new();
        let ops = vec![
            WorkflowOperation {
                name: "get-timestamp".to_string(),
                op_type: OperationType::SystemTime,
                step: 1,
            },
        ];

        let violations = checker.validate_no_nondeterministic_ops(&ops);
        assert_eq!(violations.len(), 1);
        assert_eq!(violations[0].severity, ViolationSeverity::Fatal);
    }

    #[test]
    fn test_validate_file_io() {
        let checker = DeterminismChecker::new();
        let ops = vec![
            WorkflowOperation {
                name: "read-config".to_string(),
                op_type: OperationType::FileIO,
                step: 1,
            },
        ];

        let violations = checker.validate_no_nondeterministic_ops(&ops);
        assert_eq!(violations.len(), 1);
        assert_eq!(violations[0].severity, ViolationSeverity::Error);
    }

    #[test]
    fn test_validate_safe_operations() {
        let checker = DeterminismChecker::new();
        let ops = vec![
            WorkflowOperation { name: "signal".to_string(), op_type: OperationType::Signal, step: 1 },
            WorkflowOperation { name: "query".to_string(), op_type: OperationType::Query, step: 2 },
            WorkflowOperation { name: "timer".to_string(), op_type: OperationType::Timer, step: 3 },
            WorkflowOperation { name: "activity".to_string(), op_type: OperationType::Activity, step: 4 },
            WorkflowOperation { name: "child".to_string(), op_type: OperationType::ChildWorkflow, step: 5 },
            WorkflowOperation { name: "effect".to_string(), op_type: OperationType::SideEffect, step: 6 },
        ];

        let violations = checker.validate_no_nondeterministic_ops(&ops);
        assert!(violations.is_empty());
    }

    #[test]
    fn test_check_determinism_clean() {
        let checker = DeterminismChecker::new();
        let result = checker.check_determinism(1, 1);
        assert!(result.is_deterministic);
        assert_eq!(result.violations.len(), 0);
    }

    #[test]
    fn test_check_determinism_with_violations() {
        let checker = DeterminismChecker::new();
        let ops = vec![
            WorkflowOperation {
                name: "random-op".to_string(),
                op_type: OperationType::RandomNumber,
                step: 1,
            },
        ];
        checker.validate_no_nondeterministic_ops(&ops);

        let result = checker.check_determinism(1, 1);
        assert!(!result.is_deterministic);
        assert_eq!(result.violations.len(), 1);
    }

    #[test]
    fn test_multiple_side_effects() {
        let checker = DeterminismChecker::new();
        let id1 = checker.record_side_effect("op1", b"result1", 100);
        let id2 = checker.record_side_effect("op2", b"result2", 200);
        let id3 = checker.record_side_effect("op3", b"result3", 300);

        assert_eq!(checker.side_effect_count(), 3);
        assert_eq!(checker.replay_side_effect(id1).unwrap(), b"result1");
        assert_eq!(checker.replay_side_effect(id2).unwrap(), b"result2");
        assert_eq!(checker.replay_side_effect(id3).unwrap(), b"result3");
    }

    #[test]
    fn test_clear_violations() {
        let checker = DeterminismChecker::new();
        let ops = vec![
            WorkflowOperation {
                name: "bad-op".to_string(),
                op_type: OperationType::RandomNumber,
                step: 1,
            },
        ];
        checker.validate_no_nondeterministic_ops(&ops);
        assert_eq!(checker.violation_count(), 1);

        checker.clear_violations();
        assert_eq!(checker.violation_count(), 0);
    }

    #[test]
    fn test_replay_mode() {
        let checker = DeterminismChecker::new();
        assert!(!checker.is_replay_mode());
        checker.set_replay_mode(true);
        assert!(checker.is_replay_mode());
        checker.set_replay_mode(false);
        assert!(!checker.is_replay_mode());
    }
}
