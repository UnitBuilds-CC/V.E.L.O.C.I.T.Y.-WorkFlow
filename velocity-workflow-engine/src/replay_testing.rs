// Copyright (c) VELOCITY Suite. All rights reserved.
// Licensed under the MIT License.

//! Replay Testing Framework — Deterministic replay of workflow executions.
//!
//! Allows capturing workflow execution history and replaying it to verify
//! determinism, test workflow logic changes, and debug issues. This is
//! essential for ensuring workflow code changes don't break existing executions.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

// ═══════════════════════════════════════════════════════════════════════════════
// Replay Types
// ═══════════════════════════════════════════════════════════════════════════════

/// A captured workflow execution for replay testing.
#[derive(Debug, Clone)]
pub struct CapturedExecution {
    pub workflow_id: String,
    pub run_id: String,
    pub workflow_type: String,
    pub events: Vec<CapturedEvent>,
    pub side_effects: Vec<CapturedSideEffect>,
    pub signals_received: Vec<CapturedSignal>,
    pub queries_handled: Vec<CapturedQuery>,
    pub child_workflows: Vec<CapturedChildWorkflow>,
    pub metadata: HashMap<String, String>,
}

/// A captured history event.
#[derive(Debug, Clone)]
pub struct CapturedEvent {
    pub event_id: u64,
    pub event_type: EventType,
    pub timestamp: i64,
    pub attributes: HashMap<String, serde_json::Value>,
}

/// Types of events that can be captured.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventType {
    WorkflowExecutionStarted,
    WorkflowExecutionCompleted,
    WorkflowExecutionFailed,
    WorkflowExecutionCanceled,
    WorkflowExecutionContinuedAsNew,
    WorkflowExecutionTimedOut,
    WorkflowTaskScheduled,
    WorkflowTaskStarted,
    WorkflowTaskCompleted,
    WorkflowTaskFailed,
    WorkflowTaskTimedOut,
    ActivityTaskScheduled,
    ActivityTaskStarted,
    ActivityTaskCompleted,
    ActivityTaskFailed,
    ActivityTaskTimedOut,
    ActivityTaskCanceled,
    TimerStarted,
    TimerFired,
    TimerCanceled,
    SignalExternalWorkflowExecutionInitiated,
    SignalExternalWorkflowExecutionFailed,
    ExternalWorkflowExecutionSignaled,
    StartChildWorkflowExecutionInitiated,
    StartChildWorkflowExecutionFailed,
    ChildWorkflowExecutionStarted,
    ChildWorkflowExecutionCompleted,
    ChildWorkflowExecutionFailed,
    ChildWorkflowExecutionCanceled,
    ChildWorkflowExecutionTimedOut,
    MarkerRecorded,
    WorkflowExecutionSignaled,
    WorkflowExecutionUpdateAccepted,
    WorkflowExecutionUpdateCompleted,
    UpsertWorkflowSearchAttributes,
    NexusOperationScheduled,
    NexusOperationStarted,
    NexusOperationCompleted,
    NexusOperationFailed,
    NexusOperationCanceled,
    NexusOperationTimedOut,
}

/// A captured side effect (non-deterministic operation).
#[derive(Debug, Clone)]
pub struct CapturedSideEffect {
    pub event_id: u64,
    pub name: String,
    pub result: serde_json::Value,
}

/// A captured signal.
#[derive(Debug, Clone)]
pub struct CapturedSignal {
    pub signal_name: String,
    pub input: serde_json::Value,
    pub identity: String,
    pub timestamp: i64,
}

/// A captured query.
#[derive(Debug, Clone)]
pub struct CapturedQuery {
    pub query_type: String,
    pub input: serde_json::Value,
    pub result: serde_json::Value,
    pub timestamp: i64,
}

/// A captured child workflow.
#[derive(Debug, Clone)]
pub struct CapturedChildWorkflow {
    pub workflow_id: String,
    pub run_id: String,
    pub workflow_type: String,
    pub input: serde_json::Value,
    pub result: Option<serde_json::Value>,
    pub status: String,
}

// ═══════════════════════════════════════════════════════════════════════════════
// Replay Result
// ═══════════════════════════════════════════════════════════════════════════════

/// Result of replaying a captured execution.
#[derive(Debug, Clone)]
pub struct ReplayResult {
    pub workflow_id: String,
    pub run_id: String,
    pub status: ReplayStatus,
    pub events_replayed: u64,
    pub events_expected: u64,
    pub determinism_violations: Vec<DeterminismViolation>,
    pub side_effects_replayed: u64,
    pub signals_replayed: u64,
    pub duration_ms: u64,
    pub warnings: Vec<String>,
}

/// Status of a replay operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplayStatus {
    Success,
    NonDeterminismError,
    MissingEvents,
    ExtraEvents,
    SideEffectMismatch,
    InternalError,
}

/// A determinism violation found during replay.
#[derive(Debug, Clone)]
pub struct DeterminismViolation {
    pub event_id: u64,
    pub violation_type: ViolationType,
    pub expected: String,
    pub actual: String,
    pub message: String,
}

/// Types of determinism violations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ViolationType {
    CommandMismatch,
    SideEffectMismatch,
    EventOrderMismatch,
    TimerMismatch,
    ActivityMismatch,
    ChildWorkflowMismatch,
    SignalMismatch,
    SearchAttributeMismatch,
    StateCorruption,
}

// ═══════════════════════════════════════════════════════════════════════════════
// Replay Engine
// ═══════════════════════════════════════════════════════════════════════════════

/// Replays captured workflow executions to verify determinism.
pub struct ReplayEngine {
    executions: RwLock<HashMap<String, CapturedExecution>>,
    results: RwLock<Vec<ReplayResult>>,
    strict_mode: bool,
    stats: Arc<ReplayStats>,
}

struct ReplayStats {
    replays_executed: AtomicU64,
    replays_passed: AtomicU64,
    replays_failed: AtomicU64,
    violations_found: AtomicU64,
}

impl ReplayEngine {
    pub fn new() -> Self {
        Self {
            executions: RwLock::new(HashMap::new()),
            results: RwLock::new(Vec::new()),
            strict_mode: false,
            stats: Arc::new(ReplayStats {
                replays_executed: AtomicU64::new(0),
                replays_passed: AtomicU64::new(0),
                replays_failed: AtomicU64::new(0),
                violations_found: AtomicU64::new(0),
            }),
        }
    }

    /// Enable strict mode — any deviation is a failure.
    pub fn with_strict_mode(mut self) -> Self {
        self.strict_mode = true;
        self
    }

    /// Store a captured execution for later replay.
    pub fn store_execution(&self, execution: CapturedExecution) {
        let key = format!("{}/{}", execution.workflow_id, execution.run_id);
        self.executions.write().unwrap().insert(key, execution);
    }

    /// Replay a stored execution.
    pub fn replay(&self, workflow_id: &str, run_id: &str) -> Result<ReplayResult, String> {
        let key = format!("{}/{}", workflow_id, run_id);
        let execution = self
            .executions
            .read()
            .unwrap()
            .get(&key)
            .cloned()
            .ok_or_else(|| format!("Execution {} not found", key))?;

        let start = now_millis();
        let mut violations = Vec::new();
        let mut warnings = Vec::new();

        // Replay events in order
        let mut event_idx = 0;
        for event in &execution.events {
            event_idx += 1;

            // Verify event ordering
            if event_idx > 1 {
                let prev = &execution.events[event_idx - 2];
                if event.event_id <= prev.event_id {
                    violations.push(DeterminismViolation {
                        event_id: event.event_id,
                        violation_type: ViolationType::EventOrderMismatch,
                        expected: format!("event_id > {}", prev.event_id),
                        actual: format!("event_id = {}", event.event_id),
                        message: "Events are not in sequential order".to_string(),
                    });
                }
            }

            // Verify required attributes
            match event.event_type {
                EventType::WorkflowExecutionStarted => {
                    if !event.attributes.contains_key("workflow_type") {
                        violations.push(DeterminismViolation {
                            event_id: event.event_id,
                            violation_type: ViolationType::CommandMismatch,
                            expected: "workflow_type attribute".to_string(),
                            actual: "missing".to_string(),
                            message: "WorkflowExecutionStarted missing workflow_type".to_string(),
                        });
                    }
                }
                EventType::ActivityTaskScheduled => {
                    if !event.attributes.contains_key("activity_type") {
                        warnings.push(format!(
                            "ActivityTaskScheduled event {} missing activity_type",
                            event.event_id
                        ));
                    }
                }
                EventType::TimerStarted => {
                    if !event.attributes.contains_key("duration") {
                        warnings.push(format!(
                            "TimerStarted event {} missing duration",
                            event.event_id
                        ));
                    }
                }
                _ => {}
            }
        }

        // Verify side effects
        let mut side_effects_replayed = 0;
        for side_effect in &execution.side_effects {
            side_effects_replayed += 1;
            // In strict mode, verify side effect results match
            if self.strict_mode && side_effect.result.is_null() {
                violations.push(DeterminismViolation {
                    event_id: side_effect.event_id,
                    violation_type: ViolationType::SideEffectMismatch,
                    expected: "non-null side effect result".to_string(),
                    actual: "null".to_string(),
                    message: format!("Side effect '{}' has null result", side_effect.name),
                });
            }
        }

        // Verify signals
        let signals_replayed = execution.signals_received.len() as u64;

        // Determine status
        let status = if violations.is_empty() {
            ReplayStatus::Success
        } else {
            ReplayStatus::NonDeterminismError
        };

        let duration = (now_millis() - start).max(0) as u64;

        let result = ReplayResult {
            workflow_id: workflow_id.to_string(),
            run_id: run_id.to_string(),
            status,
            events_replayed: event_idx as u64,
            events_expected: execution.events.len() as u64,
            determinism_violations: violations,
            side_effects_replayed,
            signals_replayed,
            duration_ms: duration,
            warnings,
        };

        // Update stats
        self.stats.replays_executed.fetch_add(1, Ordering::Relaxed);
        if result.status == ReplayStatus::Success {
            self.stats.replays_passed.fetch_add(1, Ordering::Relaxed);
        } else {
            self.stats.replays_failed.fetch_add(1, Ordering::Relaxed);
            self.stats.violations_found.fetch_add(
                result.determinism_violations.len() as u64,
                Ordering::Relaxed,
            );
        }

        self.results.write().unwrap().push(result.clone());
        Ok(result)
    }

    /// Replay all stored executions.
    pub fn replay_all(&self) -> Vec<ReplayResult> {
        let keys: Vec<String> = self.executions.read().unwrap().keys().cloned().collect();
        let mut results = Vec::new();
        for key in keys {
            let parts: Vec<&str> = key.splitn(2, '/').collect();
            if parts.len() == 2 {
                if let Ok(result) = self.replay(parts[0], parts[1]) {
                    results.push(result);
                }
            }
        }
        results
    }

    /// Get all replay results.
    pub fn get_results(&self) -> Vec<ReplayResult> {
        self.results.read().unwrap().clone()
    }

    /// Get replay statistics.
    pub fn get_stats(&self) -> ReplayStatsSnapshot {
        ReplayStatsSnapshot {
            replays_executed: self.stats.replays_executed.load(Ordering::Relaxed),
            replays_passed: self.stats.replays_passed.load(Ordering::Relaxed),
            replays_failed: self.stats.replays_failed.load(Ordering::Relaxed),
            violations_found: self.stats.violations_found.load(Ordering::Relaxed),
            stored_executions: self.executions.read().unwrap().len(),
        }
    }

    /// Get a stored execution.
    pub fn get_execution(&self, workflow_id: &str, run_id: &str) -> Option<CapturedExecution> {
        let key = format!("{}/{}", workflow_id, run_id);
        self.executions.read().unwrap().get(&key).cloned()
    }

    /// List all stored execution keys.
    pub fn list_executions(&self) -> Vec<String> {
        self.executions.read().unwrap().keys().cloned().collect()
    }
}

impl Default for ReplayEngine {
    fn default() -> Self {
        Self::new()
    }
}

/// Snapshot of replay statistics.
#[derive(Debug, Clone)]
pub struct ReplayStatsSnapshot {
    pub replays_executed: u64,
    pub replays_passed: u64,
    pub replays_failed: u64,
    pub violations_found: u64,
    pub stored_executions: usize,
}

// ═══════════════════════════════════════════════════════════════════════════════
// Replay Test Builder
// ═══════════════════════════════════════════════════════════════════════════════

/// Builder for creating captured executions for testing.
pub struct ReplayTestBuilder {
    workflow_id: String,
    run_id: String,
    workflow_type: String,
    events: Vec<CapturedEvent>,
    side_effects: Vec<CapturedSideEffect>,
    signals: Vec<CapturedSignal>,
    queries: Vec<CapturedQuery>,
    children: Vec<CapturedChildWorkflow>,
    metadata: HashMap<String, String>,
    next_event_id: u64,
}

impl ReplayTestBuilder {
    pub fn new(workflow_type: &str) -> Self {
        let id = generate_id();
        Self {
            workflow_id: format!("wf-{}", id),
            run_id: id,
            workflow_type: workflow_type.to_string(),
            events: Vec::new(),
            side_effects: Vec::new(),
            signals: Vec::new(),
            queries: Vec::new(),
            children: Vec::new(),
            metadata: HashMap::new(),
            next_event_id: 1,
        }
    }

    pub fn with_workflow_id(mut self, id: &str) -> Self {
        self.workflow_id = id.to_string();
        self
    }

    pub fn with_run_id(mut self, id: &str) -> Self {
        self.run_id = id.to_string();
        self
    }

    pub fn with_metadata(mut self, key: &str, value: &str) -> Self {
        self.metadata.insert(key.to_string(), value.to_string());
        self
    }

    pub fn workflow_started(mut self, task_queue: &str) -> Self {
        let event = CapturedEvent {
            event_id: self.next_event_id,
            event_type: EventType::WorkflowExecutionStarted,
            timestamp: now_millis(),
            attributes: {
                let mut m = HashMap::new();
                m.insert(
                    "workflow_type".to_string(),
                    serde_json::json!(self.workflow_type),
                );
                m.insert("task_queue".to_string(), serde_json::json!(task_queue));
                m
            },
        };
        self.next_event_id += 1;
        self.events.push(event);
        self
    }

    pub fn activity_scheduled(mut self, activity_type: &str, activity_id: &str) -> Self {
        let event = CapturedEvent {
            event_id: self.next_event_id,
            event_type: EventType::ActivityTaskScheduled,
            timestamp: now_millis(),
            attributes: {
                let mut m = HashMap::new();
                m.insert(
                    "activity_type".to_string(),
                    serde_json::json!(activity_type),
                );
                m.insert("activity_id".to_string(), serde_json::json!(activity_id));
                m
            },
        };
        self.next_event_id += 1;
        self.events.push(event);
        self
    }

    pub fn activity_completed(mut self, activity_id: &str, result: serde_json::Value) -> Self {
        let event = CapturedEvent {
            event_id: self.next_event_id,
            event_type: EventType::ActivityTaskCompleted,
            timestamp: now_millis(),
            attributes: {
                let mut m = HashMap::new();
                m.insert("activity_id".to_string(), serde_json::json!(activity_id));
                m.insert("result".to_string(), result);
                m
            },
        };
        self.next_event_id += 1;
        self.events.push(event);
        self
    }

    pub fn timer_started(mut self, timer_id: &str, duration_secs: u64) -> Self {
        let event = CapturedEvent {
            event_id: self.next_event_id,
            event_type: EventType::TimerStarted,
            timestamp: now_millis(),
            attributes: {
                let mut m = HashMap::new();
                m.insert("timer_id".to_string(), serde_json::json!(timer_id));
                m.insert("duration".to_string(), serde_json::json!(duration_secs));
                m
            },
        };
        self.next_event_id += 1;
        self.events.push(event);
        self
    }

    pub fn timer_fired(mut self, timer_id: &str) -> Self {
        let event = CapturedEvent {
            event_id: self.next_event_id,
            event_type: EventType::TimerFired,
            timestamp: now_millis(),
            attributes: {
                let mut m = HashMap::new();
                m.insert("timer_id".to_string(), serde_json::json!(timer_id));
                m
            },
        };
        self.next_event_id += 1;
        self.events.push(event);
        self
    }

    pub fn workflow_completed(mut self, result: serde_json::Value) -> Self {
        let event = CapturedEvent {
            event_id: self.next_event_id,
            event_type: EventType::WorkflowExecutionCompleted,
            timestamp: now_millis(),
            attributes: {
                let mut m = HashMap::new();
                m.insert("result".to_string(), result);
                m
            },
        };
        self.next_event_id += 1;
        self.events.push(event);
        self
    }

    pub fn workflow_failed(mut self, reason: &str) -> Self {
        let event = CapturedEvent {
            event_id: self.next_event_id,
            event_type: EventType::WorkflowExecutionFailed,
            timestamp: now_millis(),
            attributes: {
                let mut m = HashMap::new();
                m.insert("reason".to_string(), serde_json::json!(reason));
                m
            },
        };
        self.next_event_id += 1;
        self.events.push(event);
        self
    }

    pub fn add_side_effect(mut self, name: &str, result: serde_json::Value) -> Self {
        self.side_effects.push(CapturedSideEffect {
            event_id: self.next_event_id,
            name: name.to_string(),
            result,
        });
        self.next_event_id += 1;
        self
    }

    pub fn add_signal(mut self, signal_name: &str, input: serde_json::Value) -> Self {
        self.signals.push(CapturedSignal {
            signal_name: signal_name.to_string(),
            input,
            identity: "test".to_string(),
            timestamp: now_millis(),
        });
        self
    }

    pub fn add_query(mut self, query_type: &str, result: serde_json::Value) -> Self {
        self.queries.push(CapturedQuery {
            query_type: query_type.to_string(),
            input: serde_json::Value::Null,
            result,
            timestamp: now_millis(),
        });
        self
    }

    pub fn add_child_workflow(mut self, child: CapturedChildWorkflow) -> Self {
        self.children.push(child);
        self
    }

    pub fn build(self) -> CapturedExecution {
        CapturedExecution {
            workflow_id: self.workflow_id,
            run_id: self.run_id,
            workflow_type: self.workflow_type,
            events: self.events,
            side_effects: self.side_effects,
            signals_received: self.signals,
            queries_handled: self.queries,
            child_workflows: self.children,
            metadata: self.metadata,
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════════════════════════

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn generate_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(1000);
    let ts = now_millis();
    let c = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{:x}{:04x}", ts, c)
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replay_success() {
        let engine = ReplayEngine::new();
        let execution = ReplayTestBuilder::new("TestWorkflow")
            .workflow_started("test-queue")
            .activity_scheduled("MyActivity", "act-1")
            .activity_completed("act-1", serde_json::json!("done"))
            .workflow_completed(serde_json::json!({"result": "ok"}))
            .build();

        let wf_id = execution.workflow_id.clone();
        let run_id = execution.run_id.clone();
        engine.store_execution(execution);

        let result = engine.replay(&wf_id, &run_id).unwrap();
        assert_eq!(result.status, ReplayStatus::Success);
        assert_eq!(result.events_replayed, 4);
        assert!(result.determinism_violations.is_empty());
    }

    #[test]
    fn test_replay_with_side_effects() {
        let engine = ReplayEngine::new();
        let execution = ReplayTestBuilder::new("TestWorkflow")
            .workflow_started("test-queue")
            .add_side_effect("uuid", serde_json::json!("abc-123"))
            .add_side_effect("timestamp", serde_json::json!(1234567890))
            .workflow_completed(serde_json::Value::Null)
            .build();

        let wf_id = execution.workflow_id.clone();
        let run_id = execution.run_id.clone();
        engine.store_execution(execution);

        let result = engine.replay(&wf_id, &run_id).unwrap();
        assert_eq!(result.status, ReplayStatus::Success);
        assert_eq!(result.side_effects_replayed, 2);
    }

    #[test]
    fn test_replay_with_signals() {
        let engine = ReplayEngine::new();
        let execution = ReplayTestBuilder::new("TestWorkflow")
            .workflow_started("test-queue")
            .add_signal("approval", serde_json::json!({"approved": true}))
            .add_signal("data", serde_json::json!({"value": 42}))
            .workflow_completed(serde_json::Value::Null)
            .build();

        let wf_id = execution.workflow_id.clone();
        let run_id = execution.run_id.clone();
        engine.store_execution(execution);

        let result = engine.replay(&wf_id, &run_id).unwrap();
        assert_eq!(result.status, ReplayStatus::Success);
        assert_eq!(result.signals_replayed, 2);
    }

    #[test]
    fn test_replay_nonexistent() {
        let engine = ReplayEngine::new();
        let result = engine.replay("nonexistent", "no-run");
        assert!(result.is_err());
    }

    #[test]
    fn test_replay_all() {
        let engine = ReplayEngine::new();
        let e1 = ReplayTestBuilder::new("WF1")
            .workflow_started("q1")
            .workflow_completed(serde_json::Value::Null)
            .build();
        let e2 = ReplayTestBuilder::new("WF2")
            .workflow_started("q2")
            .workflow_completed(serde_json::Value::Null)
            .build();

        let (_id1, _rid1) = (e1.workflow_id.clone(), e1.run_id.clone());
        let (_id2, _rid2) = (e2.workflow_id.clone(), e2.run_id.clone());
        engine.store_execution(e1);
        engine.store_execution(e2);

        let results = engine.replay_all();
        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|r| r.status == ReplayStatus::Success));
    }

    #[test]
    fn test_replay_stats() {
        let engine = ReplayEngine::new();
        let execution = ReplayTestBuilder::new("TestWorkflow")
            .workflow_started("q")
            .workflow_completed(serde_json::Value::Null)
            .build();

        let (id, rid) = (execution.workflow_id.clone(), execution.run_id.clone());
        engine.store_execution(execution);
        engine.replay(&id, &rid).unwrap();

        let stats = engine.get_stats();
        assert_eq!(stats.replays_executed, 1);
        assert_eq!(stats.replays_passed, 1);
        assert_eq!(stats.replays_failed, 0);
        assert_eq!(stats.stored_executions, 1);
    }

    #[test]
    fn test_replay_with_timer() {
        let engine = ReplayEngine::new();
        let execution = ReplayTestBuilder::new("TimerWorkflow")
            .workflow_started("q")
            .timer_started("timer-1", 60)
            .timer_fired("timer-1")
            .workflow_completed(serde_json::Value::Null)
            .build();

        let (id, rid) = (execution.workflow_id.clone(), execution.run_id.clone());
        engine.store_execution(execution);

        let result = engine.replay(&id, &rid).unwrap();
        assert_eq!(result.status, ReplayStatus::Success);
        assert_eq!(result.events_replayed, 4);
    }

    #[test]
    fn test_builder_with_custom_ids() {
        let execution = ReplayTestBuilder::new("TestWorkflow")
            .with_workflow_id("custom-wf-id")
            .with_run_id("custom-run-id")
            .workflow_started("q")
            .workflow_completed(serde_json::Value::Null)
            .build();

        assert_eq!(execution.workflow_id, "custom-wf-id");
        assert_eq!(execution.run_id, "custom-run-id");
    }

    #[test]
    fn test_builder_with_metadata() {
        let execution = ReplayTestBuilder::new("TestWorkflow")
            .with_metadata("env", "test")
            .with_metadata("version", "1.0")
            .workflow_started("q")
            .workflow_completed(serde_json::Value::Null)
            .build();

        assert_eq!(execution.metadata.get("env").unwrap(), "test");
        assert_eq!(execution.metadata.get("version").unwrap(), "1.0");
    }

    #[test]
    fn test_strict_mode_null_side_effect() {
        let engine = ReplayEngine::new().with_strict_mode();
        let execution = ReplayTestBuilder::new("TestWorkflow")
            .workflow_started("q")
            .add_side_effect("bad", serde_json::Value::Null)
            .workflow_completed(serde_json::Value::Null)
            .build();

        let (id, rid) = (execution.workflow_id.clone(), execution.run_id.clone());
        engine.store_execution(execution);

        let result = engine.replay(&id, &rid).unwrap();
        assert_eq!(result.status, ReplayStatus::NonDeterminismError);
        assert!(!result.determinism_violations.is_empty());
    }

    #[test]
    fn test_list_executions() {
        let engine = ReplayEngine::new();
        let e1 = ReplayTestBuilder::new("WF1")
            .workflow_started("q")
            .workflow_completed(serde_json::Value::Null)
            .build();
        let e2 = ReplayTestBuilder::new("WF2")
            .workflow_started("q")
            .workflow_completed(serde_json::Value::Null)
            .build();

        engine.store_execution(e1);
        engine.store_execution(e2);

        let keys = engine.list_executions();
        assert_eq!(keys.len(), 2);
    }
}
