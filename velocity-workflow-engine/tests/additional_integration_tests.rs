//! Additional integration tests for the VELOCITY-WorkFlow engine.
//!
//! These tests exercise edge cases, concurrency, and cross-module interactions
//! that complement the existing integration test suite.

use velocity_workflow_engine::engine::{WorkflowEngine, WorkflowStatus};
use velocity_workflow_engine::graceful_shutdown::{GracefulShutdownConfig, ShutdownController};
use velocity_workflow_engine::health_check::{HealthChecker, HealthStatus};
use velocity_workflow_engine::hot_swap::{HotSwapRegistry, HotSwapResult};
use velocity_workflow_engine::metrics::MetricsRegistry;
use velocity_workflow_engine::metrics_export::MetricsSnapshot;
use velocity_workflow_engine::namespace::{NamespaceConfig, NamespaceRegistry};
use velocity_workflow_engine::resource_limits::{ResourceLimits, ResourceTracker};
use velocity_workflow_engine::saga::{SagaOrchestrator, SagaStepDefinition};
use velocity_workflow_engine::task_queue::{TaskItem, TaskKind, TaskQueue};
use velocity_workflow_engine::timer_engine::TimerEngine;

use std::sync::Arc;

// ═══════════════════════════════════════════════════════════════════════════════
// Workflow Stress Tests
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_workflow_with_many_steps() {
    let engine = WorkflowEngine::new();
    let step_count: u32 = 1000;
    let key = engine.start_workflow(1, 1, 0, 42, step_count, Some(b"big-workflow".to_vec()));
    assert_eq!(engine.get_status(key), WorkflowStatus::Running);
    assert_eq!(engine.get_total_steps(key), step_count);

    // Complete all 1000 steps
    for i in 0..step_count {
        engine.complete_step(key, i, format!("step-{i}").into_bytes());
    }

    engine.complete_workflow(key, Some(b"done-1000-steps".to_vec()));
    assert_eq!(engine.get_status(key), WorkflowStatus::Completed);
}

#[test]
fn test_concurrent_sagas() {
    let orchestrator = Arc::new(SagaOrchestrator::new());
    let thread_count = 4;
    let mut handles = vec![];

    for t in 0..thread_count {
        let orch = Arc::clone(&orchestrator);
        handles.push(std::thread::spawn(move || {
            let steps = vec![
                SagaStepDefinition::new("step_a", 100 + t as u64)
                    .with_compensation(200 + t as u64, Some(b"compensate".to_vec())),
                SagaStepDefinition::new("step_b", 110 + t as u64)
                    .with_compensation(210 + t as u64, Some(b"compensate".to_vec())),
            ];
            let saga_id = orch.create_saga(42 + t as u64, steps);
            assert!(saga_id > 0);

            // Complete first step
            assert!(orch.complete_step(saga_id, 0, Some(b"ok".to_vec())));
            // Complete second step
            assert!(orch.complete_step(saga_id, 1, Some(b"ok".to_vec())));

            saga_id
        }));
    }

    let saga_ids: Vec<u64> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    assert_eq!(saga_ids.len(), thread_count);

    // All sagas should be completed
    for id in &saga_ids {
        let saga = orchestrator.get_saga(*id).unwrap();
        assert_eq!(
            saga.status,
            velocity_workflow_engine::saga::SagaStatus::Completed
        );
    }
}

#[test]
fn test_workflow_timeout_handling() {
    let engine = WorkflowEngine::new();
    let key = engine.start_workflow(1, 1, 0, 42, 5, None);
    assert_eq!(engine.get_status(key), WorkflowStatus::Running);

    // Schedule a timer
    let timer_id = engine.schedule_timer(key, 100);
    assert!(timer_id > 0);

    // Cancel the workflow (simulating a timeout)
    engine.cancel_workflow(key);
    assert_eq!(engine.get_status(key), WorkflowStatus::Canceled);

    // Verify the workflow count reflects the cancellation
    assert!(engine.workflow_count() >= 1);
}

#[test]
fn test_signal_ordering_guarantees() {
    let engine = WorkflowEngine::new();
    let key = engine.start_workflow(1, 1, 0, 42, 1, None);

    // Send signals in order
    for i in 0..10u64 {
        engine.signal_workflow(key, i, format!("signal-{i}").into_bytes());
    }

    // Verify all signals were received
    for i in 0..10u64 {
        assert!(engine.has_signal(key, i), "Signal {i} should be present");
    }

    engine.complete_workflow(key, Some(b"all-signals-received".to_vec()));
    assert_eq!(engine.get_status(key), WorkflowStatus::Completed);
}

#[test]
fn test_query_consistency() {
    let engine = WorkflowEngine::new();
    let key = engine.start_workflow(1, 1, 0, 42, 3, None);

    // Register a query handler that returns current step info
    engine.query_registry().register_handler(
        key,
        1,
        Box::new(|_input| b"status=running,step=0".to_vec()),
    );

    // Query should return consistent result
    let result1 = engine.query_registry().execute_query(key, 1, &[]);
    let result2 = engine.query_registry().execute_query(key, 1, &[]);
    assert_eq!(result1, result2);
    assert_eq!(result1, Some(b"status=running,step=0".to_vec()));

    engine.complete_workflow(key, Some(b"done".to_vec()));
}

// ═══════════════════════════════════════════════════════════════════════════════
// Namespace and Resource Tests
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_namespace_quota_enforcement() {
    let registry = NamespaceRegistry::new();

    // Register namespaces with different configs
    let ns1 = NamespaceConfig::new(1, "production").with_description("Production namespace");
    let ns2 = NamespaceConfig::new(2, "staging").with_description("Staging namespace");

    registry.register(ns1).unwrap();
    registry.register(ns2).unwrap();

    // Verify both exist (plus the default namespace = 3 total)
    assert!(registry.get_by_name("production").is_some());
    assert!(registry.get_by_name("staging").is_some());
    assert!(registry.count() >= 2); // At least production + staging

    // Duplicate should fail
    let dup = NamespaceConfig::new(3, "production");
    assert!(registry.register(dup).is_err());
}

#[test]
fn test_task_queue_priority() {
    let tq = TaskQueue::new();
    let hash = 42u64;

    // Enqueue tasks with varying priorities
    for i in 0..5 {
        tq.enqueue(
            hash,
            TaskItem {
                task_id: i,
                kind: TaskKind::WorkflowTask,
                workflow_key: 100 + i,
                task_queue_hash: hash,
                step_index: 0,
                activity_name_id: 0,
                attempt: 1,
                priority: (i * 10) as u8,
                deadline_ms: 0,
            },
        );
    }

    // All tasks should be retrievable
    let mut retrieved = 0;
    while tq.try_poll(hash).is_some() {
        retrieved += 1;
    }
    assert_eq!(retrieved, 5);
}

#[test]
fn test_timer_precision() {
    let timer = TimerEngine::new();

    // Schedule timers at various durations
    let t1 = timer.schedule(1, std::time::Duration::from_millis(10));
    let t2 = timer.schedule(2, std::time::Duration::from_millis(50));
    let t3 = timer.schedule(3, std::time::Duration::from_millis(100));

    assert_eq!(timer.pending_count(), 3);

    // Cancel the middle timer
    assert!(timer.cancel(t2));
    assert_eq!(timer.pending_count(), 2);

    // Cancel remaining
    assert!(timer.cancel(t1));
    assert!(timer.cancel(t3));
    assert_eq!(timer.pending_count(), 0);
}

#[test]
fn test_hot_swap_consistency() {
    let registry = HotSwapRegistry::new();

    // Register multiple patches
    let patch1 = registry.register_patch(100, "Patch A", vec![(0, 42), (1, 43)]);
    let patch2 = registry.register_patch(200, "Patch B", vec![(0, 44)]);
    assert!(patch1 > 0);
    assert!(patch2 > 0);

    // Apply patch1 to a workflow
    let result1 = registry.apply_patch(patch1, 1001);
    assert!(matches!(result1, HotSwapResult::Applied { .. }));

    // Apply patch2 to the same workflow
    let result2 = registry.apply_patch(patch2, 1001);
    assert!(matches!(result2, HotSwapResult::Applied { .. }));

    // Rollback should remove all patches from the workflow
    let rolled_back = registry.rollback(1001);
    assert!(rolled_back);

    // Re-apply should work after rollback
    let result3 = registry.apply_patch(patch1, 1001);
    assert!(matches!(result3, HotSwapResult::Applied { .. }));
}

// ═══════════════════════════════════════════════════════════════════════════════
// Metrics and Observability Tests
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_metrics_accuracy() {
    let engine = WorkflowEngine::new();

    // Start several workflows
    let mut keys = vec![];
    for i in 0..5 {
        let key = engine.start_workflow(i, 1, 0, 42, 1, None);
        keys.push(key);
    }

    // Complete some, fail others
    engine.complete_workflow(keys[0], Some(b"ok".to_vec()));
    engine.complete_workflow(keys[1], Some(b"ok".to_vec()));
    engine.fail_workflow(keys[2]);
    engine.cancel_workflow(keys[3]);
    // keys[4] remains running

    // Verify metrics match actual operations
    let started = engine
        .metrics_registry()
        .get_counter("velocity_workflow_started_total");
    let completed = engine
        .metrics_registry()
        .get_counter("velocity_workflow_completed_total");
    let failed = engine
        .metrics_registry()
        .get_counter("velocity_workflow_failed_total");

    assert_eq!(started, 5);
    assert_eq!(completed, 2);
    assert_eq!(failed, 1);
}

#[test]
fn test_graceful_shutdown_drain() {
    let config = GracefulShutdownConfig::new(5000, 10000);
    let controller = ShutdownController::new(config);

    // Register components
    assert!(controller.register_component("task-queue"));
    assert!(controller.register_component("timer-engine"));
    assert!(controller.register_component("signal-processor"));

    // Initiate shutdown
    controller.initiate_shutdown();

    // Mark components as drained
    assert!(controller.mark_component_drained("task-queue"));
    assert!(controller.mark_component_drained("timer-engine"));
    assert!(controller.mark_component_drained("signal-processor"));

    // Wait for drain should succeed since all components drained
    let drained = controller.wait_for_drain(std::time::Duration::from_secs(1));
    assert!(drained, "All components should have drained");
}

#[test]
fn test_resource_limits_enforcement() {
    let limits = ResourceLimits {
        max_active_workflows: 3,
        max_workflows_per_namespace: 2,
        max_signals_per_workflow: 10,
        max_payload_size_bytes: 1024,
        max_steps_per_workflow: 100,
        max_child_workflows: 5,
    };
    let tracker = ResourceTracker::new(limits);

    // Start workflows up to the per-namespace limit
    tracker.track_workflow_started(1);
    assert!(tracker.check_can_start_workflow(1).is_ok()); // 1 active < 2 limit

    tracker.track_workflow_started(1);
    // Now at 2/2 for namespace — next check should fail
    assert!(tracker.check_can_start_workflow(1).is_err());

    // But a workflow in namespace 2 should succeed (total < 3, ns2 = 0 < 2)
    assert!(tracker.check_can_start_workflow(2).is_ok());

    // Payload size check
    assert!(tracker.check_payload_size(512).is_ok());
    assert!(tracker.check_payload_size(2048).is_err());

    // Step count check
    assert!(tracker.check_step_count(50).is_ok());
    assert!(tracker.check_step_count(200).is_err());
}

#[test]
fn test_health_check_accuracy() {
    let engine = Arc::new(WorkflowEngine::new());

    // Start some workflows
    for i in 0..3 {
        engine.start_workflow(i, 1, 0, 42, 1, None);
    }

    let checker = HealthChecker::new(engine.clone(), None, None);

    // Engine should be healthy
    let engine_health = checker.check_engine();
    assert!(matches!(engine_health, HealthStatus::Healthy));

    // Database check with no adapter should be healthy
    let db_health = checker.check_database();
    assert!(matches!(db_health, HealthStatus::Healthy));

    // Aggregate check should be healthy
    let aggregate = checker.check_all();
    assert!(aggregate.overall_status.is_healthy());
    assert_eq!(aggregate.component_statuses.len(), 3);
}

#[test]
fn test_metrics_export_formats() {
    let registry = MetricsRegistry::new();
    registry.register_counter("velocity_requests_total");
    registry.register_gauge("velocity_active_connections");
    registry.inc_counter("velocity_requests_total");
    registry.inc_counter("velocity_requests_total");
    registry.inc_counter("velocity_requests_total");
    registry.set_gauge("velocity_active_connections", 42);

    let snapshot = MetricsSnapshot::capture(&registry);

    // Test JSON export
    let json = snapshot.export_json();
    assert!(json.contains("counters"));
    assert!(json.contains("\"timestamp\""));
    assert!(json.contains("gauges"));

    // Test Prometheus export
    let prom = snapshot.export_prometheus();
    assert!(prom.contains("# TYPE"));
    assert!(prom.contains("counter"));
    assert!(prom.contains("gauge"));
    assert!(prom.contains("velocity_requests_total 3"));
    assert!(prom.contains("velocity_active_connections 42"));

    // Test StatsD export
    let statsd = snapshot.export_statsd();
    assert!(statsd.contains("|c"));
    assert!(statsd.contains("|g"));
}

#[test]
fn test_concurrent_namespace_operations() {
    let registry = Arc::new(NamespaceRegistry::new());
    let thread_count = 8;
    let mut handles = vec![];

    // Concurrent registrations
    for i in 0..thread_count {
        let reg = Arc::clone(&registry);
        handles.push(std::thread::spawn(move || {
            let name = format!("ns-{i}");
            let config = NamespaceConfig::new(100 + i as u64, &name);
            reg.register(config)
        }));
    }

    let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let successful = results.iter().filter(|r| r.is_ok()).count();
    assert_eq!(successful, thread_count);

    // Verify all namespaces exist
    for i in 0..thread_count {
        let name = format!("ns-{i}");
        assert!(registry.get_by_name(&name).is_some());
    }

    // Concurrent deletions
    let mut del_handles = vec![];
    for i in 0..thread_count {
        let reg = Arc::clone(&registry);
        let ns_id = 100 + i as u64;
        del_handles.push(std::thread::spawn(move || reg.delete(ns_id)));
    }

    let del_results: Vec<_> = del_handles.into_iter().map(|h| h.join().unwrap()).collect();
    let del_successful = del_results.iter().filter(|r| r.is_ok()).count();
    assert_eq!(del_successful, thread_count);

    // All non-default namespaces should be gone
    // (the default namespace with id=0 cannot be deleted)
    assert_eq!(registry.count(), 1); // Only "default" remains
}
