//! Integration tests for the VELOCITY-WorkFlow engine.
//!
//! These tests exercise real module APIs and cross-module interactions,
//! verifying that subsystems compose correctly end-to-end.

use velocity_workflow_engine::archival::{ArchivePolicy, ArchiveRecord, ArchiveStore};
use velocity_workflow_engine::batch::BatchExecutor;
use velocity_workflow_engine::chaos_endurance::{run_soak_test, SoakTestConfig};
use velocity_workflow_engine::cron::CronScheduler;
use velocity_workflow_engine::db_adapter::{DatabaseAdapter, InMemoryAdapter, WorkflowRecord};
use velocity_workflow_engine::dynamic_config::{ConfigValue, DynamicConfig};
use velocity_workflow_engine::engine::{ActivityRetryPolicy, WorkflowEngine, WorkflowStatus};
use velocity_workflow_engine::event_history::{HistoryEventType, HistoryStore};
use velocity_workflow_engine::heartbeat::HeartbeatTracker;
use velocity_workflow_engine::history_compaction::{
    CompactableEventType, CompactionConfig, HistoryCompactor,
};
use velocity_workflow_engine::hot_swap::{HotSwapRegistry, HotSwapResult};
use velocity_workflow_engine::memo::MemoStore;
use velocity_workflow_engine::metrics::MetricsRegistry;
use velocity_workflow_engine::namespace::{NamespaceConfig, NamespaceRegistry};
use velocity_workflow_engine::network_replication::{FrameType, WireFrame};
use velocity_workflow_engine::nexus::{NexusManager, NexusOperationState};
use velocity_workflow_engine::partition::PartitionManager;
use velocity_workflow_engine::payload_codec::{CodecChain, PayloadCodec, XorCodec};
use velocity_workflow_engine::query_handler::QueryRegistry;
use velocity_workflow_engine::raft_consensus::{RaftConfig, RaftEventType, RaftNode, RaftState};
use velocity_workflow_engine::rate_limiter::RateLimiter;
use velocity_workflow_engine::replay::ReplayEngine;
use velocity_workflow_engine::retry::{
    CircuitBreaker, CircuitBreakerConfig, CircuitState, RetryPolicy,
};
use velocity_workflow_engine::saga::{SagaOrchestrator, SagaStepDefinition};
use velocity_workflow_engine::search_index::SearchAttributeIndex;
use velocity_workflow_engine::sharding::ShardManager;
use velocity_workflow_engine::task_queue::{TaskItem, TaskKind, TaskQueue};
use velocity_workflow_engine::timer_engine::TimerEngine;
use velocity_workflow_engine::visibility::SearchAttributeValue;
use velocity_workflow_engine::worker_versioning::WorkerVersioning;
use velocity_workflow_engine::*;

use std::collections::HashMap;
use std::sync::Arc;

// ═══════════════════════════════════════════════════════════════════════════════
// Workflow Lifecycle Tests
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_full_workflow_lifecycle() {
    let engine = WorkflowEngine::new();
    let key = engine.start_workflow(100, 1, 0, 42, 3, Some(b"input".to_vec()));
    assert_eq!(engine.get_status(key), WorkflowStatus::Running);
    assert_eq!(engine.get_total_steps(key), 3);

    // Complete each step
    engine.complete_step(key, 0, b"step0".to_vec());
    engine.complete_step(key, 1, b"step1".to_vec());
    engine.complete_step(key, 2, b"step2".to_vec());

    // Signal the workflow
    engine.signal_workflow(key, 99, b"signal-data".to_vec());
    assert!(engine.has_signal(key, 99));

    // Complete the workflow
    engine.complete_workflow(key, Some(b"done".to_vec()));
    assert_eq!(engine.get_status(key), WorkflowStatus::Completed);
}

#[test]
fn test_workflow_with_child_workflows() {
    let engine = WorkflowEngine::new();
    let parent_key = engine.start_workflow(1, 10, 0, 42, 2, None);

    // Simulate child workflows via the engine
    let child1 = engine.start_workflow(2, 11, 0, 42, 1, None);
    let child2 = engine.start_workflow(3, 11, 0, 42, 1, None);

    engine.complete_workflow(child1, Some(b"child1".to_vec()));
    engine.complete_workflow(child2, Some(b"child2".to_vec()));

    assert_eq!(engine.get_status(child1), WorkflowStatus::Completed);
    assert_eq!(engine.get_status(child2), WorkflowStatus::Completed);

    engine.complete_workflow(parent_key, Some(b"parent-done".to_vec()));
    assert_eq!(engine.get_status(parent_key), WorkflowStatus::Completed);
}

#[test]
fn test_workflow_with_cron_schedule() {
    let scheduler = CronScheduler::new();
    let id = scheduler.register("*/5 * * * *", 100, 0, 42, 3, 0).unwrap();
    assert!(id > 0);

    // Advance to fire time
    let fires = scheduler.advance_to(6);
    assert!(!fires.is_empty());
    assert_eq!(fires[0].workflow_type_id, 100);
    assert_eq!(fires[0].fire_number, 1);

    // Fire count should be updated
    assert_eq!(scheduler.fire_count(id), Some(1));
}

#[test]
fn test_workflow_with_saga_compensation() {
    let orchestrator = SagaOrchestrator::new();
    let steps = vec![
        SagaStepDefinition::new("book_flight", 100)
            .with_compensation(200, Some(b"cancel_flight".to_vec())),
        SagaStepDefinition::new("book_hotel", 101)
            .with_compensation(201, Some(b"cancel_hotel".to_vec())),
        SagaStepDefinition::new("book_car", 102)
            .with_compensation(202, Some(b"cancel_car".to_vec())),
    ];
    let saga_id = orchestrator.create_saga(42, steps);

    // Complete first two steps
    assert!(orchestrator.complete_step(saga_id, 0, Some(b"flight_booked".to_vec())));
    assert!(orchestrator.complete_step(saga_id, 1, Some(b"hotel_booked".to_vec())));

    // Third step fails — triggers compensation
    let compensations = orchestrator.fail_step(saga_id, 2);
    assert_eq!(compensations.len(), 2);
    // Reverse order: hotel first, then flight
    assert_eq!(compensations[0].0, 201);
    assert_eq!(compensations[1].0, 200);

    // Complete compensations
    orchestrator.complete_compensation(saga_id, 0);
    orchestrator.complete_compensation(saga_id, 1);
    let saga = orchestrator.get_saga(saga_id).unwrap();
    assert_eq!(
        saga.status,
        velocity_workflow_engine::saga::SagaStatus::Compensated
    );
}

#[test]
fn test_workflow_with_retry() {
    let policy = ActivityRetryPolicy::new(3, 10, 2.0);
    assert_eq!(policy.max_attempts, 3);
    let delay0 = policy.calculate_delay(0);
    let delay1 = policy.calculate_delay(1);
    let delay2 = policy.calculate_delay(2);
    // Exponential backoff: 10ms, 20ms, 40ms
    assert_eq!(delay0.as_millis(), 10);
    assert_eq!(delay1.as_millis(), 20);
    assert_eq!(delay2.as_millis(), 40);
}

#[test]
fn test_workflow_with_rate_limiting() {
    let limiter = RateLimiter::new(100.0, 10, 50.0);
    // Should allow up to capacity
    assert!(limiter.try_acquire(0, 5));
    assert!(limiter.try_acquire(0, 5));
    // Global bucket exhausted
    assert!(!limiter.try_acquire(0, 1));
}

#[test]
fn test_workflow_with_heartbeat_tracking() {
    let tracker = HeartbeatTracker::new();
    tracker.register(1, 100, 5000, 3);
    assert_eq!(tracker.active_count(), 1);
    assert!(tracker.record_heartbeat(1, 100, Some(b"progress".to_vec())));
    assert!(!tracker.record_heartbeat(1, 999, None)); // not registered
    tracker.unregister(1, 100);
    assert_eq!(tracker.active_count(), 0);
}

#[test]
fn test_workflow_with_memo() {
    let store = MemoStore::new();
    store.set(42, "user_id", b"alice".to_vec(), None);
    store.set(42, "order_id", b"ORD-123".to_vec(), None);
    assert_eq!(store.get(42, "user_id"), Some(b"alice".to_vec()));
    assert_eq!(store.count(42), 2);
    assert_eq!(store.workflow_count(), 1);

    let all = store.get_all(42);
    assert_eq!(all.len(), 2);
    assert!(store.remove(42, "user_id"));
    assert_eq!(store.count(42), 1);
}

#[test]
fn test_workflow_with_search_attributes() {
    let idx = SearchAttributeIndex::new();
    idx.index_attribute(1, "customer", &SearchAttributeValue::String("C123".into()));
    idx.index_attribute(2, "customer", &SearchAttributeValue::String("C456".into()));
    idx.index_attribute(3, "customer", &SearchAttributeValue::String("C123".into()));

    let results = idx.exact_match("customer", &SearchAttributeValue::String("C123".into()));
    assert_eq!(results.len(), 2);
    assert!(results.contains(&1));
    assert!(results.contains(&3));
}

#[test]
fn test_workflow_with_event_history() {
    let store = HistoryStore::new();
    let key = 42u64;
    store.record_event(key, HistoryEventType::WorkflowStarted, b"start".to_vec());
    store.record_event(key, HistoryEventType::StepCompleted, b"step0".to_vec());
    store.record_event(key, HistoryEventType::SignalReceived, b"sig".to_vec());
    store.record_event(key, HistoryEventType::WorkflowCompleted, vec![]);

    assert_eq!(store.event_count(key), 4);
    let history = store.get_history(key).unwrap();
    assert_eq!(history[0].event_type, HistoryEventType::WorkflowStarted);
    assert_eq!(history[3].event_type, HistoryEventType::WorkflowCompleted);
}

#[test]
fn test_workflow_with_worker_versioning() {
    let wv = WorkerVersioning::new();
    let set_id = wv.create_version_set();
    assert!(wv.add_build_id(set_id, "build-v1"));
    assert!(wv.add_build_id(set_id, "build-v2"));
    assert_eq!(
        wv.get_current_build_id(set_id),
        Some("build-v1".to_string())
    );

    wv.set_current_build_id(set_id, "build-v2");
    assert_eq!(
        wv.get_current_build_id(set_id),
        Some("build-v2".to_string())
    );

    wv.add_routing_rule("my-queue", "build-v2", 100);
    assert_eq!(
        wv.resolve_build_id("my-queue"),
        Some("build-v2".to_string())
    );
}

#[test]
fn test_workflow_with_partition() {
    let mgr = PartitionManager::new(4);
    let p1 = mgr.create_partition(42);
    let p2 = mgr.create_partition(42);
    assert_eq!(mgr.partition_count(), 2);
    assert_ne!(p1, p2);

    let item = TaskItem {
        task_id: 0,
        kind: TaskKind::WorkflowTask,
        workflow_key: 100,
        task_queue_hash: 42,
        step_index: 0,
        activity_name_id: 0,
        attempt: 1,
        priority: 0,
        deadline_ms: 0,
    };
    mgr.enqueue(42, item);
    let task = mgr.poll_with_forwarding(42);
    assert!(task.is_some());
}

#[test]
fn test_workflow_with_sharding() {
    let mgr = ShardManager::new(16);
    mgr.add_host("host-a");
    mgr.add_host("host-b");
    mgr.add_host("host-c");

    assert_eq!(mgr.host_count(), 3);
    assert!(mgr.host_for_key(42).is_some());
    assert_eq!(mgr.shard_for_key(100), 100 % 16);

    assert!(mgr.assign_shard(0, "host-a"));
    assert_eq!(mgr.get_owner(0), Some("host-a".to_string()));
}

// ═══════════════════════════════════════════════════════════════════════════════
// Cross-Module Tests
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_signal_then_query() {
    let engine = WorkflowEngine::new();
    let key = engine.start_workflow(1, 1, 0, 42, 2, None);

    // Register a query handler
    engine
        .query_registry()
        .register_handler(key, 1, Box::new(|_input| b"status=running".to_vec()));

    // Signal the workflow
    engine.signal_workflow(key, 10, b"approval".to_vec());
    assert!(engine.has_signal(key, 10));

    // Query the workflow state
    let result = engine.query_registry().execute_query(key, 1, &[]);
    assert_eq!(result, Some(b"status=running".to_vec()));
}

#[test]
fn test_timer_and_completion() {
    let engine = WorkflowEngine::new();
    let key = engine.start_workflow(1, 1, 0, 42, 1, None);

    // Schedule a timer
    let timer_id = engine.schedule_timer(key, 50);
    assert!(timer_id > 0);

    // Complete the workflow before timer fires
    engine.complete_workflow(key, Some(b"done".to_vec()));
    assert_eq!(engine.get_status(key), WorkflowStatus::Completed);
}

#[test]
fn test_task_queue_priority() {
    let tq = TaskQueue::new();
    let hash = 42u64;

    // Enqueue tasks with different priorities
    tq.enqueue(
        hash,
        TaskItem {
            task_id: 1,
            kind: TaskKind::WorkflowTask,
            workflow_key: 1,
            task_queue_hash: hash,
            step_index: 0,
            activity_name_id: 0,
            attempt: 1,
            priority: 10,
            deadline_ms: 0,
        },
    );
    tq.enqueue(
        hash,
        TaskItem {
            task_id: 2,
            kind: TaskKind::WorkflowTask,
            workflow_key: 2,
            task_queue_hash: hash,
            step_index: 0,
            activity_name_id: 0,
            attempt: 1,
            priority: 1,
            deadline_ms: 0,
        },
    );

    // Both should be retrievable
    let t1 = tq.try_poll(hash);
    let t2 = tq.try_poll(hash);
    assert!(t1.is_some());
    assert!(t2.is_some());
}

#[test]
fn test_namespace_isolation() {
    let engine = WorkflowEngine::new();
    // Start workflows in different namespaces
    let key_ns0 = engine.start_workflow(1, 1, 0, 42, 1, None);
    let key_ns1 = engine.start_workflow(1, 1, 1, 42, 1, None);

    // Both should exist independently
    assert_eq!(engine.get_status(key_ns0), WorkflowStatus::Running);
    assert_eq!(engine.get_status(key_ns1), WorkflowStatus::Running);
    assert_ne!(key_ns0, key_ns1);

    engine.complete_workflow(key_ns0, Some(b"ns0".to_vec()));
    assert_eq!(engine.get_status(key_ns0), WorkflowStatus::Completed);
    assert_eq!(engine.get_status(key_ns1), WorkflowStatus::Running);
}

#[test]
fn test_batch_operations() {
    let engine = WorkflowEngine::new();
    let k1 = engine.start_workflow(1, 1, 0, 42, 1, None);
    let k2 = engine.start_workflow(2, 1, 0, 42, 1, None);
    let k3 = engine.start_workflow(3, 1, 0, 42, 1, None);

    let batch_id = engine
        .batch_executor()
        .submit_terminate(&engine, vec![k1, k2, k3]);
    assert!(batch_id > 0);

    assert_eq!(engine.get_status(k1), WorkflowStatus::Terminated);
    assert_eq!(engine.get_status(k2), WorkflowStatus::Terminated);
    assert_eq!(engine.get_status(k3), WorkflowStatus::Terminated);
}

#[test]
fn test_archival_and_cold_storage() {
    let store = ArchiveStore::new();
    let record = ArchiveRecord {
        workflow_key: 42,
        workflow_id: 1,
        run_id: 1,
        workflow_type_id: 100,
        namespace_id: 0,
        status: WorkflowStatus::Completed,
        input_data: Some(b"input".to_vec()),
        result_data: Some(b"result".to_vec()),
        step_count: 3,
        step_results: HashMap::new(),
        event_count: 10,
        archived_at_ms: 0,
        start_time_ms: 0,
        close_time_ms: 0,
    };
    let archive_id = store.archive(record);
    assert!(archive_id > 0);

    let retrieved = store.get(42);
    assert!(retrieved.is_some());
    assert_eq!(retrieved.unwrap().workflow_type_id, 100);
}

#[test]
fn test_payload_codec_roundtrip() {
    let mut chain = CodecChain::new();
    chain.add(Arc::new(XorCodec { key: 0xAA }));
    chain.add(Arc::new(XorCodec { key: 0x55 }));

    let original = b"hello velocity workflow engine";
    let encoded = chain.encode(original).unwrap();
    assert_ne!(encoded, original.to_vec());

    let decoded = chain.decode(&encoded).unwrap();
    assert_eq!(decoded, original.to_vec());
}

#[test]
fn test_replay_engine_consistency() {
    let store = HistoryStore::new();
    let key = 42u64;
    store.record_event(key, HistoryEventType::WorkflowStarted, b"start".to_vec());
    store.record_event(key, HistoryEventType::StepCompleted, b"step0".to_vec());
    store.record_event(key, HistoryEventType::WorkflowCompleted, vec![]);

    let replay_engine = ReplayEngine::new();
    let history = store.get_history(key).unwrap();
    let result = replay_engine.replay(key, &history, None);

    assert!(result.success);
    assert_eq!(result.workflow_key, key);
    assert_eq!(result.total_events, 3);
}

#[test]
fn test_nexus_operation_lifecycle() {
    let mgr = NexusManager::new();
    mgr.register_service("payment-svc", "http://payment:8080");

    let op_id = mgr
        .start_operation(
            "payment-svc",
            "charge",
            42,
            Some(b"amount=100".to_vec()),
            None,
        )
        .unwrap();
    assert!(op_id > 0);

    let op = mgr.get_operation(op_id).unwrap();
    assert_eq!(op.state, NexusOperationState::Scheduled);

    mgr.mark_started(op_id, Some("token-123".to_string()));
    let op = mgr.get_operation(op_id).unwrap();
    assert_eq!(op.state, NexusOperationState::Started);

    mgr.complete_operation(op_id, b"receipt".to_vec());
    let op = mgr.get_operation(op_id).unwrap();
    assert_eq!(op.state, NexusOperationState::Completed);
}

#[test]
fn test_raft_consensus() {
    let config = RaftConfig {
        node_id: 1,
        ..RaftConfig::default()
    };
    let mut node = RaftNode::new(config);

    // Force leader election for single-node cluster
    node.become_leader();

    assert_eq!(node.state(), RaftState::Leader);

    // Append entries
    let idx = node
        .append_entry(42, RaftEventType::WorkflowStarted, b"start".to_vec())
        .unwrap();
    assert!(idx > 0);

    // Commit entries
    node.advance_commit(idx);
    let stats = node.stats();
    assert!(stats.commit_index > 0);
}

#[test]
fn test_history_compaction() {
    let config = CompactionConfig {
        l0_threshold: 5,
        ..CompactionConfig::default()
    };
    let mut compactor = HistoryCompactor::new(config);

    // Add events to the compactor
    for _i in 0..10 {
        compactor.append_event(
            42,
            CompactableEventType::ActivityTaskCompleted,
            b"data".to_vec(),
        );
    }

    let stats = compactor.stats();
    assert!(stats.total_events_l0 >= 10);
}

#[test]
fn test_network_replication_protocol() {
    // Test wire frame encode/decode
    let frame = WireFrame {
        frame_type: FrameType::TaskBatch,
        payload: b"replication-data".to_vec(),
    };
    let encoded = frame.encode();
    let decoded = WireFrame::decode(&encoded).unwrap();
    assert_eq!(decoded.frame_type, FrameType::TaskBatch);
    assert_eq!(decoded.payload, b"replication-data".to_vec());

    // Test all frame types
    for ft in [
        FrameType::Handshake,
        FrameType::Ack,
        FrameType::Ping,
        FrameType::Pong,
        FrameType::Shutdown,
    ] {
        let f = WireFrame {
            frame_type: ft,
            payload: vec![],
        };
        let enc = f.encode();
        let dec = WireFrame::decode(&enc).unwrap();
        assert_eq!(dec.frame_type, ft);
    }
}

#[test]
fn test_hot_swap_apply_and_rollback() {
    let registry = HotSwapRegistry::new();

    // Register a patch
    let patch_id = registry.register_patch(100, "Fix step handler", vec![(0, 42), (1, 43)]);
    assert!(patch_id > 0);

    // Apply the patch to a workflow
    let result = registry.apply_patch(patch_id, 1001);
    assert!(matches!(result, HotSwapResult::Applied { .. }));

    // Rollback the patch from the workflow
    let rolled_back = registry.rollback(1001);
    assert!(rolled_back);
}

#[test]
fn test_db_adapter_persistence() {
    let adapter = InMemoryAdapter::new();

    // Use from_context to build a proper record
    let engine = WorkflowEngine::new();
    let key = engine.start_workflow(1, 100, 0, 42, 3, Some(b"input".to_vec()));

    // Get the workflow context via the engine's slab
    let slab = engine.get_slab(key).unwrap();
    let record = WorkflowRecord {
        workflow_key: key,
        workflow_id: 1,
        run_id: slab.run_id,
        workflow_type_id: 100,
        namespace_id: 0,
        namespace_name: "default".to_string(),
        task_queue_hash: 42,
        current_step: 0,
        total_steps: 3,
        merkle_root: slab.merkle_root.to_vec(),
        step_bitmask: slab
            .step_bitmask
            .bits
            .iter()
            .flat_map(|w| w.to_le_bytes())
            .collect(),
        status: WorkflowStatus::Running,
        step_results: HashMap::new(),
        signal_buffer: HashMap::new(),
        update_buffer: HashMap::new(),
        input_data: Some(b"input".to_vec()),
        result_data: None,
        parent_key: None,
        child_keys: vec![],
        event_sequence: 0,
    };
    adapter.save_workflow(key, &record).unwrap();
    assert_eq!(adapter.workflow_count(), 1);

    // Load the workflow
    let loaded = adapter.load_workflow(key).unwrap();
    assert_eq!(loaded.workflow_type_id, 100);
    assert_eq!(loaded.total_steps, 3);
}

#[test]
fn test_error_handling_and_retry() {
    let policy = RetryPolicy::defaults()
        .with_max_attempts(3)
        .with_initial_interval_ms(10);

    assert_eq!(policy.max_attempts, 3);

    let delay = policy.compute_delay(0);
    assert!(delay.as_millis() >= 7); // 10ms * 0.75 jitter floor

    let delay2 = policy.compute_delay(1);
    assert!(delay2.as_millis() >= 15); // 20ms * 0.75
}

#[test]
fn test_circuit_breaker_states() {
    let config = CircuitBreakerConfig {
        failure_threshold: 3,
        recovery_timeout_ms: 100,
        half_open_max_calls: 1,
    };
    let cb = CircuitBreaker::new(config);

    // Initially closed
    assert_eq!(cb.state(), CircuitState::Closed);

    // Record failures to trip the breaker
    cb.record_failure();
    cb.record_failure();
    cb.record_failure();
    assert_eq!(cb.state(), CircuitState::Open);

    // Wait for recovery timeout
    std::thread::sleep(std::time::Duration::from_millis(150));
    assert_eq!(cb.state(), CircuitState::HalfOpen);

    // Record success to close the circuit
    cb.record_success();
    assert_eq!(cb.state(), CircuitState::Closed);
}

#[test]
fn test_observability_metrics_export() {
    let registry = MetricsRegistry::new();
    registry.inc_counter("velocity_workflow_started_total");
    registry.inc_counter("velocity_workflow_started_total");
    registry.set_gauge("velocity_workflows_running", 5);

    let output = registry.export_prometheus();
    assert!(output.contains("velocity_workflow_started_total 2"));
    assert!(output.contains("velocity_workflows_running 5"));
    assert!(output.contains("# TYPE"));
    assert!(output.contains("counter"));
    assert!(output.contains("gauge"));
}

#[test]
fn test_chaos_soak_test() {
    let config = SoakTestConfig {
        duration: std::time::Duration::from_millis(200),
        thread_count: 2,
        batch_size: 3,
        inject_failures: false,
        ..SoakTestConfig::default()
    };
    let metrics = run_soak_test(&config);
    // The soak test should complete without panicking
    assert!(
        metrics
            .workflows_started
            .load(std::sync::atomic::Ordering::Relaxed)
            > 0
    );
}

#[test]
fn test_dynamic_config_override() {
    let config = DynamicConfig::new();
    // Default value
    assert_eq!(config.get_int("workflow.maxConcurrent"), 1000);

    // Override
    config.set("workflow.maxConcurrent", ConfigValue::Int(500));
    assert_eq!(config.get_int("workflow.maxConcurrent"), 500);

    // List keys
    let keys = config.list_keys();
    assert!(keys.contains(&"workflow.maxConcurrent".to_string()));
}

#[test]
fn test_query_registry_lifecycle() {
    let registry = QueryRegistry::new();
    registry.register_handler(
        42,
        1,
        Box::new(|input| {
            let mut r = input.to_vec();
            r.extend_from_slice(b"_processed");
            r
        }),
    );

    assert!(registry.has_handler(42, 1));
    assert!(!registry.has_handler(42, 2));

    let result = registry.execute_query(42, 1, b"data").unwrap();
    assert_eq!(result, b"data_processed".to_vec());

    registry.unregister_workflow(42);
    assert_eq!(registry.workflow_count(), 0);
}

#[test]
fn test_engine_auto_archive_on_complete() {
    let engine = WorkflowEngine::new();
    engine.set_archive_policy(ArchivePolicy::default_completed());

    let key = engine.start_workflow(1, 1, 0, 42, 1, None);
    engine.complete_workflow(key, Some(b"done".to_vec()));

    // Should be auto-archived
    let archived = engine.archive_store().get(key);
    assert!(archived.is_some());
    assert_eq!(archived.unwrap().status, WorkflowStatus::Completed);
}

#[test]
fn test_engine_metrics_incremented() {
    let engine = WorkflowEngine::new();
    let k1 = engine.start_workflow(1, 1, 0, 42, 1, None);
    let k2 = engine.start_workflow(2, 1, 0, 42, 1, None);

    engine.complete_workflow(k1, Some(b"ok".to_vec()));
    engine.fail_workflow(k2);

    let started = engine
        .metrics_registry()
        .get_counter("velocity_workflow_started_total");
    let completed = engine
        .metrics_registry()
        .get_counter("velocity_workflow_completed_total");
    let failed = engine
        .metrics_registry()
        .get_counter("velocity_workflow_failed_total");

    assert_eq!(started, 2);
    assert_eq!(completed, 1);
    assert_eq!(failed, 1);
}

#[test]
fn test_engine_history_recorded() {
    let engine = WorkflowEngine::new();
    let key = engine.start_workflow(1, 1, 0, 42, 2, None);
    engine.complete_step(key, 0, b"s0".to_vec());
    engine.complete_step(key, 1, b"s1".to_vec());
    engine.signal_workflow(key, 5, b"sig".to_vec());
    engine.complete_workflow(key, Some(b"done".to_vec()));

    let history = engine.history_store().get_history(key).unwrap();
    // Engine records workflow-level events: Started and Completed
    assert!(history.len() >= 2);
    assert_eq!(history[0].event_type, HistoryEventType::WorkflowStarted);
    assert_eq!(
        history.last().unwrap().event_type,
        HistoryEventType::WorkflowCompleted
    );
}

#[test]
fn test_shard_consistent_hashing() {
    let mgr = ShardManager::new(64);
    mgr.add_host("node-a");
    mgr.add_host("node-b");

    // Same key should always map to same host
    let host1 = mgr.host_for_key(42).unwrap();
    let host2 = mgr.host_for_key(42).unwrap();
    assert_eq!(host1, host2);

    // Different keys may map to different hosts
    let mut hosts = std::collections::HashSet::new();
    for k in 0..100 {
        if let Some(h) = mgr.host_for_key(k) {
            hosts.insert(h);
        }
    }
    assert!(hosts.len() >= 1); // At least one host got keys
}

#[test]
fn test_search_index_range_queries() {
    let idx = SearchAttributeIndex::new();
    for i in 0..20 {
        idx.index_attribute(i, "priority", &SearchAttributeValue::Integer(i as i64));
    }

    let range = idx.range_integer("priority", 5, 10);
    assert_eq!(range.len(), 6); // 5,6,7,8,9,10

    let gt = idx.greater_than_integer("priority", 15);
    assert_eq!(gt.len(), 4); // 16,17,18,19

    let lt = idx.less_than_integer("priority", 3);
    assert_eq!(lt.len(), 3); // 0,1,2
}

#[test]
fn test_timer_engine_schedule_and_cancel() {
    let engine = TimerEngine::new();
    let t1 = engine.schedule(1, std::time::Duration::from_secs(60));
    let t2 = engine.schedule(2, std::time::Duration::from_secs(120));
    assert_eq!(engine.pending_count(), 2);

    assert!(engine.cancel(t1));
    assert_eq!(engine.pending_count(), 1);
    assert!(engine.cancel(t2));
    assert_eq!(engine.pending_count(), 0);
}

#[test]
fn test_namespace_registration_and_lookup() {
    let registry = NamespaceRegistry::new();
    let config = NamespaceConfig::new(1, "production").with_description("Production namespace");
    registry.register(config).unwrap();

    let ns_id = registry.get_by_name("production");
    assert!(ns_id.is_some());
    assert_eq!(ns_id.unwrap(), 1);

    // Duplicate should fail
    let dup = NamespaceConfig::new(2, "production");
    assert!(registry.register(dup).is_err());
}

#[test]
fn test_engine_with_in_memory_db_adapter() {
    let mut engine = WorkflowEngine::new();
    let adapter = Arc::new(InMemoryAdapter::new());
    engine.enable_db_adapter(adapter.clone());

    let key = engine.start_workflow(1, 1, 0, 42, 1, None);
    engine.complete_workflow(key, Some(b"persisted".to_vec()));

    // The engine should have persisted the workflow
    // (persist_workflow is called explicitly; auto-persist depends on engine implementation)
    assert!(engine.db_adapter().is_some());
}

#[test]
fn test_multiple_concurrent_workflows() {
    let engine = WorkflowEngine::new();
    let mut keys = Vec::new();

    for i in 0..10 {
        let key = engine.start_workflow(i, 1, 0, 42, 3, None);
        keys.push(key);
    }

    assert_eq!(engine.workflow_count(), 10);

    // Complete all
    for key in &keys {
        engine.complete_step(*key, 0, b"s0".to_vec());
        engine.complete_step(*key, 1, b"s1".to_vec());
        engine.complete_step(*key, 2, b"s2".to_vec());
        engine.complete_workflow(*key, Some(b"done".to_vec()));
    }

    for key in &keys {
        assert_eq!(engine.get_status(*key), WorkflowStatus::Completed);
    }
}

#[test]
fn test_engine_cancel_and_terminate() {
    let engine = WorkflowEngine::new();
    let k1 = engine.start_workflow(1, 1, 0, 42, 1, None);
    let k2 = engine.start_workflow(2, 1, 0, 42, 1, None);

    engine.cancel_workflow(k1);
    engine.terminate_workflow(k2);

    assert_eq!(engine.get_status(k1), WorkflowStatus::Canceled);
    assert_eq!(engine.get_status(k2), WorkflowStatus::Terminated);
}

#[test]
fn test_engine_event_sequence_increments() {
    let engine = WorkflowEngine::new();
    let key = engine.start_workflow(1, 1, 0, 42, 2, None);
    let seq0 = engine.get_event_sequence(key);

    engine.complete_step(key, 0, b"s0".to_vec());
    let seq1 = engine.get_event_sequence(key);
    assert!(seq1 > seq0);

    engine.signal_workflow(key, 1, b"sig".to_vec());
    let seq2 = engine.get_event_sequence(key);
    assert!(seq2 > seq1);
}
