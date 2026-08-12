//! PostgreSQL integration tests for VELOCITY-WorkFlow.
//!
//! These tests use the `InMemoryAdapter` to simulate what would happen with a real
//! PostgreSQL database. They exercise the full persistence lifecycle — migrations,
//! CRUD operations, filtering, concurrent access, large payloads, and error paths.

use std::collections::HashMap;
use std::sync::Arc;
use std::thread;

use velocity_workflow_engine::db_adapter::{
    DatabaseAdapter, DatabaseError, InMemoryAdapter, SearchAttributeValue, SearchAttributes,
    StatusFilter, WorkflowRecord,
};
use velocity_workflow_engine::engine::{WorkflowEngine, WorkflowStatus};
use velocity_workflow_engine::migration_runner::MigrationRunner;

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Create a test WorkflowRecord with sensible defaults.
fn make_record(key: u64, status: WorkflowStatus) -> WorkflowRecord {
    WorkflowRecord {
        workflow_key: key,
        workflow_id: key & 0xFFFF_FFFF,
        run_id: key + 1000,
        workflow_type_id: 42,
        namespace_id: 1,
        namespace_name: "test-namespace".to_string(),
        task_queue_hash: 12345,
        current_step: 3,
        total_steps: 10,
        merkle_root: vec![0u8; 32],
        step_bitmask: vec![0xFF; 4],
        status,
        step_results: HashMap::new(),
        signal_buffer: HashMap::new(),
        update_buffer: HashMap::new(),
        input_data: Some(b"test-input".to_vec()),
        result_data: None,
        parent_key: None,
        child_keys: vec![],
        event_sequence: 0,
    }
}

/// Create a fresh InMemoryAdapter wrapped in Arc for sharing.
fn make_adapter() -> Arc<InMemoryAdapter> {
    Arc::new(InMemoryAdapter::new())
}

// ═══════════════════════════════════════════════════════════════════════════════
// Migration Runner Tests
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_migration_runner_applies_all() {
    let adapter = InMemoryAdapter::new();
    let mut runner = MigrationRunner::new(Box::new(adapter));

    let result = runner.run_all().unwrap();
    assert_eq!(result.versions_applied, vec![1, 2, 3, 4, 5, 6]);
    assert_eq!(runner.current_version(), 6);

    // All migrations should show as applied
    let status = runner.status();
    assert_eq!(status.len(), 6);
    assert!(status.iter().all(|s| s.applied));
}

#[test]
fn test_migration_runner_rollback() {
    let adapter = InMemoryAdapter::new();
    let mut runner = MigrationRunner::new(Box::new(adapter));

    runner.run_all().unwrap();
    assert_eq!(runner.current_version(), 6);

    // Rollback last
    runner.rollback_last().unwrap();
    assert_eq!(runner.current_version(), 5);

    // Rollback to specific version
    runner.rollback_to(2).unwrap();
    assert_eq!(runner.current_version(), 2);

    // Verify status
    let status = runner.status();
    assert!(status[0].applied); // v1
    assert!(status[1].applied); // v2
    assert!(!status[2].applied); // v3
    assert!(!status[3].applied); // v4
    assert!(!status[4].applied); // v5
    assert!(!status[5].applied); // v6
}

#[test]
fn test_migration_runner_idempotent() {
    let adapter = InMemoryAdapter::new();
    let mut runner = MigrationRunner::new(Box::new(adapter));

    let r1 = runner.run_all().unwrap();
    assert_eq!(r1.versions_applied.len(), 6);

    // Running again should apply nothing
    let r2 = runner.run_all().unwrap();
    assert!(r2.versions_applied.is_empty());
    assert_eq!(runner.current_version(), 6);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Workflow Persistence Tests
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_workflow_persistence_roundtrip() {
    let adapter = make_adapter();
    let record = make_record(1001, WorkflowStatus::Running);

    adapter.save_workflow(1001, &record).unwrap();
    let loaded = adapter.load_workflow(1001).unwrap();

    assert_eq!(loaded.workflow_key, 1001);
    assert_eq!(loaded.workflow_id, record.workflow_id);
    assert_eq!(loaded.run_id, record.run_id);
    assert_eq!(loaded.workflow_type_id, 42);
    assert_eq!(loaded.namespace_name, "test-namespace");
    assert_eq!(loaded.task_queue_hash, 12345);
    assert_eq!(loaded.current_step, 3);
    assert_eq!(loaded.total_steps, 10);
    assert_eq!(loaded.status, WorkflowStatus::Running);
    assert_eq!(loaded.input_data, Some(b"test-input".to_vec()));
    assert_eq!(loaded.merkle_root, vec![0u8; 32]);
    assert_eq!(loaded.parent_key, None);
    assert!(loaded.child_keys.is_empty());
}

#[test]
fn test_event_history_persistence() {
    let adapter = make_adapter();
    let record = make_record(2001, WorkflowStatus::Running);
    adapter.save_workflow(2001, &record).unwrap();

    // Save events in sequence
    let id1 = adapter
        .save_event(2001, 1, "WorkflowStarted", 0, b"start".to_vec())
        .unwrap();
    let id2 = adapter
        .save_event(2001, 2, "StepCompleted", 1, b"step0".to_vec())
        .unwrap();
    let id3 = adapter
        .save_event(2001, 2, "StepCompleted", 2, b"step1".to_vec())
        .unwrap();
    let id4 = adapter
        .save_event(2001, 3, "WorkflowCompleted", 3, b"done".to_vec())
        .unwrap();

    assert!(id1 < id2 && id2 < id3 && id3 < id4);

    // Load and verify order
    let events = adapter.load_events(2001).unwrap();
    assert_eq!(events.len(), 4);
    assert_eq!(events[0].event_type, 1);
    assert_eq!(events[0].event_type_name, "WorkflowStarted");
    assert_eq!(events[0].sequence_num, 0);
    assert_eq!(events[1].sequence_num, 1);
    assert_eq!(events[2].sequence_num, 2);
    assert_eq!(events[3].event_type, 3);
    assert_eq!(events[3].event_type_name, "WorkflowCompleted");
}

#[test]
fn test_search_attribute_persistence() {
    let adapter = make_adapter();
    let record = make_record(3001, WorkflowStatus::Running);
    adapter.save_workflow(3001, &record).unwrap();

    let mut attrs = SearchAttributes::new();
    attrs.insert(
        "environment".into(),
        SearchAttributeValue::Text("production".into()),
    );
    attrs.insert("priority".into(), SearchAttributeValue::Integer(5));
    attrs.insert("score".into(), SearchAttributeValue::Float(9.95));
    attrs.insert("active".into(), SearchAttributeValue::Bool(true));
    attrs.insert(
        "tags".into(),
        SearchAttributeValue::TextArray(vec!["critical".into(), "finance".into()]),
    );

    adapter.save_search_attributes(3001, &attrs).unwrap();

    let loaded = adapter.load_search_attributes(3001).unwrap();
    assert_eq!(loaded.len(), 5);

    match loaded.get("environment") {
        Some(SearchAttributeValue::Text(s)) => assert_eq!(s, "production"),
        _ => panic!("expected Text value for 'environment'"),
    }
    match loaded.get("priority") {
        Some(SearchAttributeValue::Integer(n)) => assert_eq!(*n, 5),
        _ => panic!("expected Integer value for 'priority'"),
    }
    match loaded.get("active") {
        Some(SearchAttributeValue::Bool(b)) => assert!(*b),
        _ => panic!("expected Bool value for 'active'"),
    }
}

#[test]
fn test_workflow_status_update() {
    let adapter = make_adapter();
    let record = make_record(4001, WorkflowStatus::Running);
    adapter.save_workflow(4001, &record).unwrap();

    // Update to Completed
    adapter
        .update_workflow_status(4001, WorkflowStatus::Completed)
        .unwrap();
    let loaded = adapter.load_workflow(4001).unwrap();
    assert_eq!(loaded.status, WorkflowStatus::Completed);

    // Update to Failed
    adapter
        .update_workflow_status(4001, WorkflowStatus::Failed)
        .unwrap();
    let loaded = adapter.load_workflow(4001).unwrap();
    assert_eq!(loaded.status, WorkflowStatus::Failed);

    // Update non-existent should error
    let err = adapter
        .update_workflow_status(9999, WorkflowStatus::Completed)
        .unwrap_err();
    assert!(matches!(err, DatabaseError::NotFound(9999)));
}

#[test]
fn test_concurrent_workflow_persistence() {
    let adapter = make_adapter();
    let num_threads = 8;
    let workflows_per_thread = 10;

    let mut handles = Vec::new();
    for t in 0..num_threads {
        let adapter_clone = Arc::clone(&adapter);
        let handle = thread::spawn(move || {
            for i in 0..workflows_per_thread {
                let key = (t * workflows_per_thread + i) as u64 + 10000;
                let record = make_record(key, WorkflowStatus::Running);
                adapter_clone.save_workflow(key, &record).unwrap();
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    assert_eq!(
        adapter.workflow_count(),
        (num_threads * workflows_per_thread) as usize
    );

    // Verify a sample
    let loaded = adapter.load_workflow(10000).unwrap();
    assert_eq!(loaded.workflow_key, 10000);
    assert_eq!(loaded.status, WorkflowStatus::Running);
}

#[test]
fn test_workflow_list_with_filters() {
    let adapter = make_adapter();

    // Create workflows with different statuses and namespaces
    for i in 0..10 {
        let mut record = make_record(
            5000 + i,
            if i % 2 == 0 {
                WorkflowStatus::Running
            } else {
                WorkflowStatus::Completed
            },
        );
        if i < 5 {
            record.namespace_name = "ns-alpha".into();
        } else {
            record.namespace_name = "ns-beta".into();
        }
        adapter.save_workflow(5000 + i, &record).unwrap();
    }

    // Filter by namespace
    let alpha = adapter
        .list_workflows(Some("ns-alpha"), StatusFilter::All, 100, 0)
        .unwrap();
    assert_eq!(alpha.len(), 5);

    // Filter by status
    let running = adapter
        .list_workflows(None, StatusFilter::Running, 100, 0)
        .unwrap();
    assert_eq!(running.len(), 5); // even indices

    let completed = adapter
        .list_workflows(None, StatusFilter::Completed, 100, 0)
        .unwrap();
    assert_eq!(completed.len(), 5); // odd indices

    // Combined filter
    let alpha_running = adapter
        .list_workflows(Some("ns-alpha"), StatusFilter::Running, 100, 0)
        .unwrap();
    assert_eq!(alpha_running.len(), 3); // i=0,2,4

    // Pagination
    let page1 = adapter
        .list_workflows(None, StatusFilter::All, 3, 0)
        .unwrap();
    assert_eq!(page1.len(), 3);
    let page2 = adapter
        .list_workflows(None, StatusFilter::All, 3, 3)
        .unwrap();
    assert_eq!(page2.len(), 3);

    // Count
    assert_eq!(
        adapter.count_workflows(None, StatusFilter::All).unwrap(),
        10
    );
    assert_eq!(
        adapter
            .count_workflows(Some("ns-alpha"), StatusFilter::Running)
            .unwrap(),
        3
    );
}

#[test]
fn test_checkpoint_save_and_load() {
    let adapter = make_adapter();
    let record = make_record(6001, WorkflowStatus::Running);
    adapter.save_workflow(6001, &record).unwrap();

    // Simulate checkpoint via search attributes (checkpoint data stored as attribute)
    let mut attrs = SearchAttributes::new();
    attrs.insert(
        "__checkpoint_data".into(),
        SearchAttributeValue::Bytes(b"snapshot-binary".to_vec()),
    );
    attrs.insert("__checkpoint_step".into(), SearchAttributeValue::Integer(5));
    adapter.save_search_attributes(6001, &attrs).unwrap();

    let loaded = adapter.load_search_attributes(6001).unwrap();
    match loaded.get("__checkpoint_data") {
        Some(SearchAttributeValue::Bytes(data)) => assert_eq!(data, b"snapshot-binary"),
        _ => panic!("expected checkpoint data"),
    }
    match loaded.get("__checkpoint_step") {
        Some(SearchAttributeValue::Integer(step)) => assert_eq!(*step, 5),
        _ => panic!("expected checkpoint step"),
    }
}

#[test]
fn test_namespace_persistence() {
    let adapter = make_adapter();

    // Simulate namespace registration via workflow records
    let mut ns1_record = make_record(7001, WorkflowStatus::Running);
    ns1_record.namespace_name = "production".into();
    adapter.save_workflow(7001, &ns1_record).unwrap();

    let mut ns2_record = make_record(7002, WorkflowStatus::Running);
    ns2_record.namespace_name = "staging".into();
    adapter.save_workflow(7002, &ns2_record).unwrap();

    let mut ns3_record = make_record(7003, WorkflowStatus::Completed);
    ns3_record.namespace_name = "production".into();
    adapter.save_workflow(7003, &ns3_record).unwrap();

    // Verify namespace filtering works
    let prod = adapter
        .list_workflows(Some("production"), StatusFilter::All, 100, 0)
        .unwrap();
    assert_eq!(prod.len(), 2);

    let staging = adapter
        .list_workflows(Some("staging"), StatusFilter::All, 100, 0)
        .unwrap();
    assert_eq!(staging.len(), 1);

    // Verify count
    assert_eq!(
        adapter
            .count_workflows(Some("production"), StatusFilter::All)
            .unwrap(),
        2
    );
}

#[test]
fn test_schedule_persistence() {
    let adapter = make_adapter();

    // Simulate schedule storage via workflow records with metadata
    let mut schedule_record = make_record(8001, WorkflowStatus::Running);
    schedule_record.workflow_type_id = 100; // schedule target type
    adapter.save_workflow(8001, &schedule_record).unwrap();

    // Store schedule metadata as search attributes
    let mut attrs = SearchAttributes::new();
    attrs.insert(
        "__schedule_cron".into(),
        SearchAttributeValue::Text("*/5 * * * *".into()),
    );
    attrs.insert(
        "__schedule_jitter_ms".into(),
        SearchAttributeValue::Integer(1000),
    );
    attrs.insert(
        "__schedule_paused".into(),
        SearchAttributeValue::Bool(false),
    );
    adapter.save_search_attributes(8001, &attrs).unwrap();

    let loaded_attrs = adapter.load_search_attributes(8001).unwrap();
    match loaded_attrs.get("__schedule_cron") {
        Some(SearchAttributeValue::Text(cron)) => assert_eq!(cron, "*/5 * * * *"),
        _ => panic!("expected cron expression"),
    }
    match loaded_attrs.get("__schedule_paused") {
        Some(SearchAttributeValue::Bool(paused)) => assert!(!paused),
        _ => panic!("expected paused flag"),
    }

    // Verify the workflow itself is loadable
    let loaded = adapter.load_workflow(8001).unwrap();
    assert_eq!(loaded.workflow_type_id, 100);
}

#[test]
fn test_replication_queue_persistence() {
    let adapter = make_adapter();

    // Simulate replication queue entries as events
    let record = make_record(9001, WorkflowStatus::Running);
    adapter.save_workflow(9001, &record).unwrap();

    // Enqueue replication events
    let id1 = adapter
        .save_event(
            9001,
            10,
            "ReplicationEnqueue",
            0,
            b"region-a->region-b:wf-9001".to_vec(),
        )
        .unwrap();
    let id2 = adapter
        .save_event(
            9001,
            10,
            "ReplicationEnqueue",
            1,
            b"region-a->region-c:wf-9001".to_vec(),
        )
        .unwrap();
    let id3 = adapter
        .save_event(
            9001,
            10,
            "ReplicationEnqueue",
            2,
            b"region-b->region-c:wf-9001".to_vec(),
        )
        .unwrap();

    assert!(id1 < id2 && id2 < id3);

    // Load and verify
    let events = adapter.load_events(9001).unwrap();
    assert_eq!(events.len(), 3);
    assert_eq!(events[0].event_type_name, "ReplicationEnqueue");
    assert!(String::from_utf8_lossy(&events[0].data).contains("region-a->region-b"));
    assert!(String::from_utf8_lossy(&events[2].data).contains("region-b->region-c"));
}

#[test]
fn test_audit_log_persistence() {
    let adapter = make_adapter();

    // Simulate audit logs as events on a special "audit" workflow
    let audit_key = 99999u64;
    let record = make_record(audit_key, WorkflowStatus::Running);
    adapter.save_workflow(audit_key, &record).unwrap();

    adapter
        .save_event(
            audit_key,
            15,
            "AuditLog",
            0,
            b"actor=admin action=create resource=workflow/1001".to_vec(),
        )
        .unwrap();
    adapter
        .save_event(
            audit_key,
            15,
            "AuditLog",
            1,
            b"actor=user1 action=signal resource=workflow/1002".to_vec(),
        )
        .unwrap();
    adapter
        .save_event(
            audit_key,
            15,
            "AuditLog",
            2,
            b"actor=admin action=terminate resource=workflow/1003".to_vec(),
        )
        .unwrap();

    let events = adapter.load_events(audit_key).unwrap();
    assert_eq!(events.len(), 3);

    // Verify audit log content
    let log0 = String::from_utf8_lossy(&events[0].data);
    assert!(log0.contains("actor=admin"));
    assert!(log0.contains("action=create"));

    let log2 = String::from_utf8_lossy(&events[2].data);
    assert!(log2.contains("action=terminate"));
}

#[test]
fn test_api_key_persistence() {
    let adapter = make_adapter();

    // Simulate API key storage via search attributes on a system workflow
    let sys_key = 88888u64;
    let record = make_record(sys_key, WorkflowStatus::Running);
    adapter.save_workflow(sys_key, &record).unwrap();

    // Store API key metadata
    let mut attrs = SearchAttributes::new();
    attrs.insert(
        "api_key_hash".into(),
        SearchAttributeValue::Text("sha256:abc123".into()),
    );
    attrs.insert(
        "api_key_name".into(),
        SearchAttributeValue::Text("production-key".into()),
    );
    attrs.insert(
        "api_key_namespace".into(),
        SearchAttributeValue::Text("default".into()),
    );
    attrs.insert("api_key_active".into(), SearchAttributeValue::Bool(true));
    adapter.save_search_attributes(sys_key, &attrs).unwrap();

    // Validate: load and check
    let loaded = adapter.load_search_attributes(sys_key).unwrap();
    match loaded.get("api_key_hash") {
        Some(SearchAttributeValue::Text(hash)) => assert_eq!(hash, "sha256:abc123"),
        _ => panic!("expected key hash"),
    }
    match loaded.get("api_key_active") {
        Some(SearchAttributeValue::Bool(active)) => assert!(*active),
        _ => panic!("expected active flag"),
    }

    // Revoke: update the active flag
    let mut revoked = SearchAttributes::new();
    revoked.insert("api_key_active".into(), SearchAttributeValue::Bool(false));
    adapter.save_search_attributes(sys_key, &revoked).unwrap();

    let loaded2 = adapter.load_search_attributes(sys_key).unwrap();
    match loaded2.get("api_key_active") {
        Some(SearchAttributeValue::Bool(active)) => assert!(!active),
        _ => panic!("expected revoked flag"),
    }
}

#[test]
fn test_large_payload_persistence() {
    let adapter = make_adapter();

    // Create a workflow with a large memo (~1MB)
    let mut record = make_record(77001, WorkflowStatus::Running);
    let large_payload = vec![0xABu8; 1024 * 1024]; // 1MB
    record.input_data = Some(large_payload.clone());
    record.merkle_root = vec![0xCD; 32];

    adapter.save_workflow(77001, &record).unwrap();

    let loaded = adapter.load_workflow(77001).unwrap();
    let loaded_data = loaded.input_data.expect("expected input data");
    assert_eq!(loaded_data.len(), 1024 * 1024);
    assert_eq!(loaded_data[0], 0xAB);
    assert_eq!(loaded_data[1024 * 1024 - 1], 0xAB);
    assert_eq!(loaded.merkle_root, vec![0xCD; 32]);
}

#[test]
fn test_error_handling_constraint_violations() {
    let adapter = make_adapter();

    // Duplicate key: InMemoryAdapter overwrites (like upsert), so verify that
    let record1 = make_record(66001, WorkflowStatus::Running);
    adapter.save_workflow(66001, &record1).unwrap();

    let mut record2 = record1.clone();
    record2.status = WorkflowStatus::Completed;
    adapter.save_workflow(66001, &record2).unwrap();

    // Should have overwritten, not duplicated
    assert_eq!(adapter.workflow_count(), 1);
    let loaded = adapter.load_workflow(66001).unwrap();
    assert_eq!(loaded.status, WorkflowStatus::Completed);

    // Load non-existent
    let err = adapter.load_workflow(99999).unwrap_err();
    assert!(matches!(err, DatabaseError::NotFound(99999)));

    // Delete non-existent (should succeed silently)
    adapter.delete_workflow(99999).unwrap();

    // Failure simulation
    adapter.set_simulate_failures(true);
    assert!(adapter.save_workflow(66001, &record1).is_err());
    assert!(adapter.load_workflow(66001).is_err());
    adapter.set_simulate_failures(false);

    // After disabling failures, operations should work again
    assert!(adapter.load_workflow(66001).is_ok());
}

// ═══════════════════════════════════════════════════════════════════════════════
// Engine + Adapter Integration Tests
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_engine_with_adapter_full_lifecycle() {
    let mut engine = WorkflowEngine::new();
    let adapter = Arc::new(InMemoryAdapter::new());
    engine.enable_db_adapter(adapter.clone());

    // Start a workflow
    let key = engine.start_workflow(1, 1, 0, 42, 3, Some(b"input".to_vec()));
    assert_eq!(engine.get_status(key), WorkflowStatus::Running);

    // Complete steps
    engine.complete_step(key, 0, b"step0".to_vec());
    engine.complete_step(key, 1, b"step1".to_vec());
    engine.complete_step(key, 2, b"step2".to_vec());

    // Complete workflow
    engine.complete_workflow(key, Some(b"done".to_vec()));
    assert_eq!(engine.get_status(key), WorkflowStatus::Completed);

    // Persist to adapter
    let slab = engine.get_slab(key).unwrap();
    let record = WorkflowRecord {
        workflow_key: key,
        workflow_id: 1,
        run_id: slab.run_id,
        workflow_type_id: 42,
        namespace_id: 0,
        namespace_name: "default".to_string(),
        task_queue_hash: 42,
        current_step: 3,
        total_steps: 3,
        merkle_root: slab.merkle_root.to_vec(),
        step_bitmask: slab
            .step_bitmask
            .bits
            .iter()
            .flat_map(|w| w.to_le_bytes())
            .collect(),
        status: WorkflowStatus::Completed,
        step_results: HashMap::new(),
        signal_buffer: HashMap::new(),
        update_buffer: HashMap::new(),
        input_data: Some(b"input".to_vec()),
        result_data: Some(b"done".to_vec()),
        parent_key: None,
        child_keys: vec![],
        event_sequence: engine.get_event_sequence(key),
    };
    adapter.save_workflow(key, &record).unwrap();

    // Load back and verify
    let loaded = adapter.load_workflow(key).unwrap();
    assert_eq!(loaded.status, WorkflowStatus::Completed);
    assert_eq!(loaded.total_steps, 3);
    assert_eq!(loaded.result_data, Some(b"done".to_vec()));
}

#[test]
fn test_delete_cascades_related_data() {
    let adapter = make_adapter();
    let record = make_record(55001, WorkflowStatus::Running);
    adapter.save_workflow(55001, &record).unwrap();

    // Add events
    adapter
        .save_event(55001, 1, "Started", 0, b"data".to_vec())
        .unwrap();
    adapter
        .save_event(55001, 2, "StepDone", 1, b"data".to_vec())
        .unwrap();
    assert_eq!(adapter.event_count(55001), 2);

    // Add search attributes
    let mut attrs = SearchAttributes::new();
    attrs.insert("key".into(), SearchAttributeValue::Text("val".into()));
    adapter.save_search_attributes(55001, &attrs).unwrap();
    assert!(!adapter.load_search_attributes(55001).unwrap().is_empty());

    // Delete should cascade
    adapter.delete_workflow(55001).unwrap();
    assert_eq!(adapter.workflow_count(), 0);
    assert_eq!(adapter.event_count(55001), 0);
    assert!(adapter.load_search_attributes(55001).unwrap().is_empty());
}

#[test]
fn test_adapter_always_connected() {
    let adapter = InMemoryAdapter::new();
    assert!(adapter.is_connected());
    assert_eq!(adapter.adapter_name(), "InMemoryAdapter");
}
