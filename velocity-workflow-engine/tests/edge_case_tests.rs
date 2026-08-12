//! Edge case and error path tests for the VELOCITY-WorkFlow engine.
//!
//! These tests exercise boundary conditions, invalid inputs, concurrent access,
//! and error paths that the happy-path integration tests do not cover.

use velocity_workflow_engine::db_adapter::{
    DatabaseAdapter, InMemoryAdapter, StatusFilter, WorkflowRecord,
};
use velocity_workflow_engine::engine::{WorkflowEngine, WorkflowStatus};
use velocity_workflow_engine::hot_swap::{HotSwapRegistry, HotSwapResult};
use velocity_workflow_engine::namespace::{NamespaceConfig, NamespaceRegistry};
use velocity_workflow_engine::observability::{
    LogLevel, MetricsExporter, SpanTracker, StructuredLogger,
};
use velocity_workflow_engine::retry::{
    CircuitBreaker, CircuitBreakerConfig, CircuitState, RetryPolicy,
};
use velocity_workflow_engine::search_index::SearchAttributeIndex;
use velocity_workflow_engine::task_queue::{TaskItem, TaskKind, TaskQueue};
use velocity_workflow_engine::timer_engine::TimerEngine;
use velocity_workflow_engine::visibility::SearchAttributeValue;

use std::collections::HashMap;
use std::sync::{Arc, Barrier};
use std::time::Duration;

// ═══════════════════════════════════════════════════════════════════════════════
// Workflow Edge Cases
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_start_workflow_with_zero_steps() {
    let engine = WorkflowEngine::new();
    let key = engine.start_workflow(1, 1, 0, 42, 0, None);
    // Workflow with zero steps should be created; status depends on engine policy.
    // It should at least not panic.
    let status = engine.get_status(key);
    assert!(status == WorkflowStatus::Running || status == WorkflowStatus::Completed);
}

#[test]
fn test_complete_already_completed_workflow() {
    let engine = WorkflowEngine::new();
    let key = engine.start_workflow(1, 1, 0, 42, 1, None);
    engine.complete_step(key, 0, b"s0".to_vec());
    engine.complete_workflow(key, Some(b"done".to_vec()));
    assert_eq!(engine.get_status(key), WorkflowStatus::Completed);

    // Second completion should be a no-op or keep the status Completed
    engine.complete_workflow(key, Some(b"again".to_vec()));
    assert_eq!(engine.get_status(key), WorkflowStatus::Completed);
}

#[test]
fn test_signal_completed_workflow() {
    let engine = WorkflowEngine::new();
    let key = engine.start_workflow(1, 1, 0, 42, 1, None);
    engine.complete_step(key, 0, b"s0".to_vec());
    engine.complete_workflow(key, Some(b"done".to_vec()));
    assert_eq!(engine.get_status(key), WorkflowStatus::Completed);

    // Signaling a completed workflow: engine may accept or reject.
    // Verify it does not panic.
    engine.signal_workflow(key, 50, b"late-signal".to_vec());
    // Status should remain Completed
    assert_eq!(engine.get_status(key), WorkflowStatus::Completed);
}

#[test]
fn test_query_completed_workflow() {
    let engine = WorkflowEngine::new();
    let key = engine.start_workflow(1, 1, 0, 42, 1, None);
    engine.complete_step(key, 0, b"s0".to_vec());

    // Register a query handler before completion
    engine
        .query_registry()
        .register_handler(key, 1, Box::new(|_input| b"state=done".to_vec()));
    engine.complete_workflow(key, Some(b"done".to_vec()));

    // Query should still work on completed workflows (read-only)
    let result = engine.query_registry().execute_query(key, 1, &[]);
    assert_eq!(result, Some(b"state=done".to_vec()));
}

#[test]
fn test_cancel_completed_workflow() {
    let engine = WorkflowEngine::new();
    let key = engine.start_workflow(1, 1, 0, 42, 1, None);
    engine.complete_step(key, 0, b"s0".to_vec());
    engine.complete_workflow(key, Some(b"done".to_vec()));
    assert_eq!(engine.get_status(key), WorkflowStatus::Completed);

    // Cancelling a completed workflow: the engine allows the transition.
    // Verify the engine does not panic; status may change to Canceled or stay Completed.
    engine.cancel_workflow(key);
    let final_status = engine.get_status(key);
    assert!(final_status == WorkflowStatus::Completed || final_status == WorkflowStatus::Canceled);
}

#[test]
fn test_complete_step_twice() {
    let engine = WorkflowEngine::new();
    let key = engine.start_workflow(1, 1, 0, 42, 2, None);

    engine.complete_step(key, 0, b"first".to_vec());
    // Second completion of same step should be no-op or error (not panic)
    engine.complete_step(key, 0, b"second".to_vec());

    // Workflow should still be functional
    engine.complete_step(key, 1, b"s1".to_vec());
    engine.complete_workflow(key, Some(b"done".to_vec()));
    assert_eq!(engine.get_status(key), WorkflowStatus::Completed);
}

#[test]
fn test_complete_step_out_of_order() {
    let engine = WorkflowEngine::new();
    let key = engine.start_workflow(1, 1, 0, 42, 3, None);

    // Complete step 2 before step 0 and 1
    engine.complete_step(key, 2, b"s2".to_vec());
    engine.complete_step(key, 0, b"s0".to_vec());
    engine.complete_step(key, 1, b"s1".to_vec());

    engine.complete_workflow(key, Some(b"done".to_vec()));
    assert_eq!(engine.get_status(key), WorkflowStatus::Completed);
}

#[test]
fn test_start_workflow_duplicate_id() {
    let engine = WorkflowEngine::new();
    let key1 = engine.start_workflow(42, 1, 0, 42, 1, None);
    let key2 = engine.start_workflow(42, 1, 0, 42, 1, None);

    // Both should return valid keys; they may be the same (dedup) or different.
    // Engine should not panic either way.
    assert!(engine.get_status(key1) == WorkflowStatus::Running);
    assert!(engine.get_status(key2) == WorkflowStatus::Running);
}

#[test]
fn test_workflow_with_max_steps() {
    let engine = WorkflowEngine::new();
    let step_count = 1000u32;
    let key = engine.start_workflow(1, 1, 0, 42, step_count, None);
    assert_eq!(engine.get_total_steps(key), step_count);
    assert_eq!(engine.get_status(key), WorkflowStatus::Running);

    // Complete all steps
    for i in 0..step_count {
        engine.complete_step(key, i, format!("step{}", i).into_bytes());
    }
    engine.complete_workflow(key, Some(b"max-steps-done".to_vec()));
    assert_eq!(engine.get_status(key), WorkflowStatus::Completed);
}

#[test]
fn test_concurrent_signals_same_workflow() {
    let engine = Arc::new(WorkflowEngine::new());
    let key = engine.start_workflow(1, 1, 0, 42, 1, None);

    let barrier = Arc::new(Barrier::new(10));
    let mut handles = vec![];
    for i in 0..10 {
        let eng = engine.clone();
        let b = barrier.clone();
        handles.push(std::thread::spawn(move || {
            b.wait();
            eng.signal_workflow(key, i, format!("sig-{}", i).into_bytes());
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    // All 10 signals should be recorded
    for i in 0..10 {
        assert!(engine.has_signal(key, i), "signal {} should exist", i);
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Namespace Edge Cases
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_register_duplicate_namespace() {
    let registry = NamespaceRegistry::new();
    let config1 = NamespaceConfig::new(1, "production");
    assert!(registry.register(config1).is_ok());

    let config2 = NamespaceConfig::new(2, "production");
    assert!(registry.register(config2).is_err());
}

#[test]
fn test_deregister_nonexistent_namespace() {
    let registry = NamespaceRegistry::new();
    // Deleting a namespace that doesn't exist should return an error
    let result = registry.delete(999);
    assert!(result.is_err());
}

#[test]
fn test_workflow_in_nonexistent_namespace() {
    let engine = WorkflowEngine::new();
    // Start a workflow with a namespace ID that has no explicit registration.
    // The engine should still create the workflow (namespace IDs are just integers).
    let key = engine.start_workflow(1, 1, 99, 42, 1, None);
    assert_eq!(engine.get_status(key), WorkflowStatus::Running);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Task Queue Edge Cases
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_poll_empty_queue() {
    let tq = TaskQueue::new();
    // Polling an empty queue should return None immediately (non-blocking)
    let result = tq.try_poll(42);
    assert!(result.is_none());
}

#[test]
fn test_poll_with_priority() {
    let tq = TaskQueue::new();
    let hash = 42u64;

    // Enqueue low priority first, then high priority
    tq.enqueue(
        hash,
        TaskItem {
            task_id: 0,
            kind: TaskKind::WorkflowTask,
            workflow_key: 1,
            task_queue_hash: hash,
            step_index: 0,
            activity_name_id: 0,
            attempt: 1,
            priority: 1,
            deadline_ms: 0,
        },
    );
    tq.enqueue(
        hash,
        TaskItem {
            task_id: 0,
            kind: TaskKind::WorkflowTask,
            workflow_key: 2,
            task_queue_hash: hash,
            step_index: 0,
            activity_name_id: 0,
            attempt: 1,
            priority: 10,
            deadline_ms: 0,
        },
    );

    // Higher priority (lower number) should be dispatched first
    let first = tq.try_poll(hash).unwrap();
    assert_eq!(first.workflow_key, 1); // priority 1 < 10, so this goes first
}

#[test]
fn test_concurrent_poll_same_queue() {
    let tq = Arc::new(TaskQueue::new());
    let hash = 42u64;

    // Enqueue exactly one task
    tq.enqueue(
        hash,
        TaskItem {
            task_id: 0,
            kind: TaskKind::WorkflowTask,
            workflow_key: 1,
            task_queue_hash: hash,
            step_index: 0,
            activity_name_id: 0,
            attempt: 1,
            priority: 0,
            deadline_ms: 0,
        },
    );

    let barrier = Arc::new(Barrier::new(5));
    let received = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let mut handles = vec![];

    for _ in 0..5 {
        let tq = tq.clone();
        let b = barrier.clone();
        let recv = received.clone();
        handles.push(std::thread::spawn(move || {
            b.wait();
            if tq.try_poll(hash).is_some() {
                recv.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    // Only one thread should have received the task
    assert_eq!(received.load(std::sync::atomic::Ordering::Relaxed), 1);
}

#[test]
fn test_queue_with_thousand_tasks() {
    let tq = TaskQueue::new();
    let hash = 42u64;
    let start = std::time::Instant::now();

    for i in 0..1000 {
        tq.enqueue(
            hash,
            TaskItem {
                task_id: 0,
                kind: TaskKind::WorkflowTask,
                workflow_key: i,
                task_queue_hash: hash,
                step_index: 0,
                activity_name_id: 0,
                attempt: 1,
                priority: 0,
                deadline_ms: 0,
            },
        );
    }

    assert_eq!(tq.pending_count(hash), 1000);
    let elapsed = start.elapsed();
    // Enqueuing 1000 tasks should be fast (< 1 second)
    assert!(
        elapsed.as_secs() < 1,
        "enqueuing 1000 tasks took too long: {:?}",
        elapsed
    );

    // Drain all tasks
    let mut count = 0;
    while tq.try_poll(hash).is_some() {
        count += 1;
    }
    assert_eq!(count, 1000);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Timer Edge Cases
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_cancel_already_fired_timer() {
    let engine = TimerEngine::new();
    // Schedule a timer with zero duration (fires immediately)
    let timer_id = engine.schedule(1, Duration::from_millis(0));
    // Give it a moment to fire
    std::thread::sleep(Duration::from_millis(10));

    // Cancelling an already-fired timer should be a no-op (return false or true)
    let cancelled = engine.cancel(timer_id);
    // Either outcome is acceptable — just verify no panic
    let _ = cancelled;
}

#[test]
fn test_schedule_timer_with_zero_duration() {
    let engine = TimerEngine::new();
    let timer_id = engine.schedule(1, Duration::from_millis(0));
    assert!(timer_id > 0);
    assert_eq!(engine.pending_count(), 1);
}

#[test]
fn test_mass_timer_cancellation() {
    let engine = TimerEngine::new();
    let mut timer_ids = vec![];

    for i in 0..1000 {
        let id = engine.schedule(i, Duration::from_secs(3600));
        timer_ids.push(id);
    }
    assert_eq!(engine.pending_count(), 1000);

    // Cancel all timers
    let mut cancelled = 0;
    for id in timer_ids {
        if engine.cancel(id) {
            cancelled += 1;
        }
    }
    assert_eq!(cancelled, 1000);
    assert_eq!(engine.pending_count(), 0);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Signal / Query Edge Cases
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_signal_with_empty_payload() {
    let engine = WorkflowEngine::new();
    let key = engine.start_workflow(1, 1, 0, 42, 1, None);

    engine.signal_workflow(key, 1, vec![]);
    assert!(engine.has_signal(key, 1));
}

#[test]
fn test_signal_with_large_payload() {
    let engine = WorkflowEngine::new();
    let key = engine.start_workflow(1, 1, 0, 42, 1, None);

    // 1 MB payload
    let large_payload = vec![0xABu8; 1_048_576];
    engine.signal_workflow(key, 2, large_payload.clone());
    assert!(engine.has_signal(key, 2));
}

#[test]
fn test_query_nonexistent_query_id() {
    let engine = WorkflowEngine::new();
    let key = engine.start_workflow(1, 1, 0, 42, 1, None);

    // Query with no registered handler should return None
    let result = engine.query_registry().execute_query(key, 999, &[]);
    assert!(result.is_none());
}

#[test]
fn test_rapid_signal_burst() {
    let engine = WorkflowEngine::new();
    let key = engine.start_workflow(1, 1, 0, 42, 1, None);

    // 100 signals in quick succession
    for i in 0..100 {
        engine.signal_workflow(key, i, format!("burst-{}", i).into_bytes());
    }

    // All should be recorded
    for i in 0..100 {
        assert!(
            engine.has_signal(key, i),
            "signal {} missing after burst",
            i
        );
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Search Index Edge Cases
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_index_duplicate_attribute() {
    let idx = SearchAttributeIndex::new();
    // Index the same attribute twice for the same workflow — should update
    idx.index_attribute(1, "color", &SearchAttributeValue::String("red".into()));
    idx.index_attribute(1, "color", &SearchAttributeValue::String("blue".into()));

    // The index uses set-based storage; re-indexing adds the new value to the set.
    // Verify the new value is present.
    let blue = idx.exact_match("color", &SearchAttributeValue::String("blue".into()));
    assert_eq!(blue.len(), 1, "new value should be indexed");
    assert!(blue.contains(&1));
}

#[test]
fn test_query_nonexistent_attribute() {
    let idx = SearchAttributeIndex::new();
    idx.index_attribute(1, "color", &SearchAttributeValue::String("red".into()));

    let results = idx.exact_match("nonexistent", &SearchAttributeValue::String("x".into()));
    assert!(results.is_empty());
}

#[test]
fn test_range_query_inverted_bounds() {
    let idx = SearchAttributeIndex::new();
    for i in 0..10 {
        idx.index_attribute(i, "priority", &SearchAttributeValue::Integer(i as i64));
    }

    // Range query where low > high: the underlying BTreeMap panics on inverted bounds.
    // Verify this is the documented behaviour by catching the panic.
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        idx.range_integer("priority", 8, 3)
    }));
    assert!(result.is_err(), "inverted range should panic in BTreeMap");
}

// ═══════════════════════════════════════════════════════════════════════════════
// Hot-Swap Edge Cases
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_apply_patch_to_nonexistent_workflow() {
    let registry = HotSwapRegistry::new();
    let patch_id = registry.register_patch(100, "Fix handler", vec![(0, 42)]);

    // Apply to a workflow key that doesn't exist in the engine
    let result = registry.apply_patch(patch_id, 99999);
    // Should still succeed (patch is registered for the type) or return NoMatchingWorkflows
    match result {
        HotSwapResult::Applied { .. } | HotSwapResult::NoMatchingWorkflows => {}
        _ => panic!("unexpected result: {:?}", result),
    }
}

#[test]
fn test_rollback_without_any_patches() {
    let registry = HotSwapRegistry::new();
    // Rollback on a workflow that has no patches applied
    let result = registry.rollback(42);
    assert!(!result, "rollback without patches should return false");
}

#[test]
fn test_double_rollback() {
    let registry = HotSwapRegistry::new();
    let patch_id = registry.register_patch(100, "Fix", vec![(0, 42)]);
    registry.apply_patch(patch_id, 1001);

    // First rollback should succeed
    assert!(registry.rollback(1001));
    // Second rollback should fail (nothing left to roll back)
    assert!(!registry.rollback(1001));
}

// ═══════════════════════════════════════════════════════════════════════════════
// DB Adapter Edge Cases
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_load_nonexistent_workflow() {
    let adapter = InMemoryAdapter::new();
    let result = adapter.load_workflow(99999);
    assert!(result.is_err());
}

#[test]
fn test_delete_nonexistent_workflow() {
    let adapter = InMemoryAdapter::new();
    // Deleting a non-existent workflow should succeed (idempotent)
    let result = adapter.delete_workflow(99999);
    assert!(result.is_ok());
}

#[test]
fn test_list_empty_database() {
    let adapter = InMemoryAdapter::new();
    let workflows = adapter
        .list_workflows(None, StatusFilter::All, 100, 0)
        .unwrap();
    assert!(workflows.is_empty());
    assert_eq!(adapter.workflow_count(), 0);
}

#[test]
fn test_save_and_load_special_characters() {
    let adapter = InMemoryAdapter::new();
    let record = WorkflowRecord {
        workflow_key: 42,
        workflow_id: 1,
        run_id: 1,
        workflow_type_id: 100,
        namespace_id: 0,
        namespace_name: "tëst-ünïcödé".to_string(),
        task_queue_hash: 42,
        current_step: 0,
        total_steps: 1,
        merkle_root: vec![0u8; 32],
        step_bitmask: vec![0u8; 32],
        status: WorkflowStatus::Running,
        step_results: HashMap::new(),
        signal_buffer: HashMap::new(),
        update_buffer: HashMap::new(),
        input_data: Some("héllo wörld \0 null bytes 🚀".as_bytes().to_vec()),
        result_data: None,
        parent_key: None,
        child_keys: vec![],
        event_sequence: 0,
    };
    adapter.save_workflow(42, &record).unwrap();

    let loaded = adapter.load_workflow(42).unwrap();
    assert_eq!(loaded.namespace_name, "tëst-ünïcödé");
    assert_eq!(
        loaded.input_data.as_ref().unwrap(),
        "héllo wörld \0 null bytes 🚀".as_bytes()
    );
}

// ═══════════════════════════════════════════════════════════════════════════════
// Observability Edge Cases
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_log_with_empty_fields() {
    let logger = StructuredLogger::new(LogLevel::Debug, "test-service");
    // Logging with empty fields should not panic
    let logged = logger.log_event(LogLevel::Info, "test_event", &[]);
    assert!(logged);
}

#[test]
fn test_export_metrics_before_init() {
    // Creating a fresh MetricsExporter and exporting should work (empty output)
    let exporter = MetricsExporter::new();
    let output = exporter.export_prometheus();
    // Should be valid (possibly empty or with defaults)
    assert!(output.is_empty() || output.contains("#"));
}

#[test]
fn test_span_without_parent() {
    let tracker = SpanTracker::new();
    let span_id = tracker.start_span("root-span", None);
    assert!(span_id > 0);
    assert!(tracker.end_span(span_id));
}

// ═══════════════════════════════════════════════════════════════════════════════
// Retry / Circuit Breaker Edge Cases
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_retry_with_max_attempts_one() {
    let policy = RetryPolicy::defaults().with_max_attempts(1);
    assert_eq!(policy.max_attempts, 1);

    // Delay computation should still work
    let delay = policy.compute_delay(0);
    assert!(delay.as_millis() > 0);
}

#[test]
fn test_retry_with_zero_backoff() {
    let policy = RetryPolicy::defaults()
        .with_max_attempts(5)
        .with_initial_interval_ms(100)
        .with_backoff_coefficient(1.0); // No exponential growth

    let d0 = policy.compute_delay(0);
    let d1 = policy.compute_delay(1);
    let d4 = policy.compute_delay(4);
    // With coefficient 1.0, all delays should be ~100ms (±jitter)
    assert!(d0.as_millis() >= 75 && d0.as_millis() <= 125);
    assert!(d1.as_millis() >= 75 && d1.as_millis() <= 125);
    assert!(d4.as_millis() >= 75 && d4.as_millis() <= 125);
}

#[test]
fn test_circuit_breaker_immediate_recovery() {
    let config = CircuitBreakerConfig {
        failure_threshold: 2,
        recovery_timeout_ms: 50,
        half_open_max_calls: 1,
    };
    let cb = CircuitBreaker::new(config);

    // Trip the breaker
    cb.record_failure();
    cb.record_failure();
    assert_eq!(cb.state(), CircuitState::Open);

    // Wait for recovery
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(cb.state(), CircuitState::HalfOpen);

    // Single success should close the circuit
    cb.record_success();
    assert_eq!(cb.state(), CircuitState::Closed);
}

#[test]
fn test_circuit_breaker_half_open_failure() {
    let config = CircuitBreakerConfig {
        failure_threshold: 2,
        recovery_timeout_ms: 50,
        half_open_max_calls: 1,
    };
    let cb = CircuitBreaker::new(config);

    // Trip the breaker
    cb.record_failure();
    cb.record_failure();
    assert_eq!(cb.state(), CircuitState::Open);

    // Wait for recovery
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(cb.state(), CircuitState::HalfOpen);

    // Failure in half-open should re-open the circuit
    cb.record_failure();
    assert_eq!(cb.state(), CircuitState::Open);
}
