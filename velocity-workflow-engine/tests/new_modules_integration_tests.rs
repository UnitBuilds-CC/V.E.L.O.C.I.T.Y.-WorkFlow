//! Integration tests for new modules: update, reachability, deployment, codec_server,
//! worker_sessions, worker_determinism.

use velocity_workflow_engine::*;

// === Update API Tests ===

#[test]
fn test_update_handler_execution() {
    let mut controller = UpdateController::new();
    controller.register_handler("increment", |args| {
        let val: i32 = String::from_utf8_lossy(args).parse().unwrap_or(0);
        Ok(format!("{}", val + 1).into_bytes())
    });

    let result = controller.submit_update(UpdateRequest {
        workflow_key: 1,
        update_id: "u1".to_string(),
        update_name: "increment".to_string(),
        args: b"41".to_vec(),
        wait_policy: UpdateWaitPolicy::Completed,
    });

    assert_eq!(result.status, UpdateStatus::Completed);
    assert_eq!(result.result.unwrap(), b"42");
}

#[test]
fn test_update_with_multiple_handlers() {
    let mut controller = UpdateController::new();
    controller.register_handler("add", |args| Ok(args.to_vec()));
    controller.register_handler("multiply", |args| {
        let val: i32 = String::from_utf8_lossy(args).parse().unwrap_or(0);
        Ok(format!("{}", val * 2).into_bytes())
    });

    let r1 = controller.submit_update(UpdateRequest {
        workflow_key: 1,
        update_id: "u1".to_string(),
        update_name: "add".to_string(),
        args: b"hello".to_vec(),
        wait_policy: UpdateWaitPolicy::Completed,
    });
    assert_eq!(r1.status, UpdateStatus::Completed);

    let r2 = controller.submit_update(UpdateRequest {
        workflow_key: 1,
        update_id: "u2".to_string(),
        update_name: "multiply".to_string(),
        args: b"21".to_vec(),
        wait_policy: UpdateWaitPolicy::Completed,
    });
    assert_eq!(r2.status, UpdateStatus::Completed);
    assert_eq!(r2.result.unwrap(), b"42");
}

#[test]
fn test_update_rejection_flow() {
    let mut controller = UpdateController::new();
    controller.register_handler("validate", |_args| {
        Err("validation failed: invalid input".to_string())
    });

    let result = controller.submit_update(UpdateRequest {
        workflow_key: 1,
        update_id: "u1".to_string(),
        update_name: "validate".to_string(),
        args: b"bad-data".to_vec(),
        wait_policy: UpdateWaitPolicy::Completed,
    });

    assert_eq!(result.status, UpdateStatus::Rejected);
    assert!(result.failure.unwrap().contains("validation failed"));
}

// === Reachability Tests ===

#[test]
fn test_reachability_worker_poll_tracking() {
    let tracker = ReachabilityTracker::new();

    // Simulate workers polling
    tracker.record_poll("orders", 1000);
    tracker.record_poll("orders", 1001);
    tracker.record_poll("payments", 1002);

    let result = tracker.check_task_queue("orders");
    assert!(result.is_reachable);
    assert_eq!(result.worker_count, 2);

    let result = tracker.check_task_queue("payments");
    assert!(result.is_reachable);
    assert_eq!(result.worker_count, 1);
}

#[test]
fn test_reachability_after_disconnect() {
    let tracker = ReachabilityTracker::new();
    tracker.record_poll("orders", 1000);
    tracker.record_poll("orders", 1001);

    // Both workers disconnect
    tracker.record_disconnect("orders");
    tracker.record_disconnect("orders");

    let result = tracker.check_task_queue("orders");
    assert!(!result.is_reachable);
    assert_eq!(result.worker_count, 0);
}

#[test]
fn test_reachability_mixed_queues() {
    let tracker = ReachabilityTracker::new();
    tracker.record_poll("active", 1000);
    tracker.record_poll("dead", 500);
    tracker.record_disconnect("dead");

    let reachable = tracker.list_reachable();
    assert!(reachable.contains(&"active".to_string()));
    assert!(!reachable.contains(&"dead".to_string()));

    let unreachable = tracker.list_unreachable();
    assert!(unreachable.contains(&"dead".to_string()));
}

// === Deployment API Tests ===

#[test]
fn test_deployment_full_lifecycle() {
    let manager = DeploymentManager::new();

    // Create
    let d = manager.create_deployment("d1", "production", "v1.0.0", 1000);
    assert_eq!(d.status, DeploymentStatus::Active);

    // Activate
    manager.activate_deployment("d1").unwrap();
    let current = manager.get_current_deployment("production").unwrap();
    assert_eq!(current.id, "d1");

    // Drain
    manager.drain_deployment("d1").unwrap();
    let d = manager.get_deployment("d1").unwrap();
    assert_eq!(d.status, DeploymentStatus::Draining);

    // Complete drainage
    manager.complete_drainage("d1").unwrap();
    let d = manager.get_deployment("d1").unwrap();
    assert_eq!(d.status, DeploymentStatus::Drained);
}

#[test]
fn test_deployment_series_management() {
    let manager = DeploymentManager::new();
    manager.create_deployment("d1", "prod", "v1.0", 1000);
    manager.create_deployment("d2", "prod", "v1.1", 2000);
    manager.create_deployment("d3", "staging", "v1.0", 1500);

    assert_eq!(manager.list_deployments(None).len(), 3);
    assert_eq!(manager.list_deployments(Some("prod")).len(), 2);

    manager.activate_deployment("d1").unwrap();
    let current = manager.get_current_deployment("prod").unwrap();
    assert_eq!(current.build_id, "v1.0");

    // Promote v1.1
    manager.activate_deployment("d2").unwrap();
    let current = manager.get_current_deployment("prod").unwrap();
    assert_eq!(current.build_id, "v1.1");
}

#[test]
fn test_deployment_task_queue_binding() {
    let manager = DeploymentManager::new();
    manager.create_deployment("d1", "prod", "v1.0", 1000);
    manager.add_task_queue("d1", "orders").unwrap();
    manager.add_task_queue("d1", "payments").unwrap();

    let d = manager.get_deployment("d1").unwrap();
    assert_eq!(d.task_queues.len(), 2);
    assert!(d.task_queues.contains(&"orders".to_string()));
    assert!(d.task_queues.contains(&"payments".to_string()));
}

// === Codec Server Tests ===

#[test]
fn test_codec_server_identity_roundtrip() {
    let server = CodecServer::new();
    let data = b"hello world";

    let encoded = server.handle_encode(&CodecRequest {
        codec_name: "identity".to_string(),
        payloads: vec![data.to_vec()],
        namespace: None,
    });
    assert!(encoded.error.is_none());
    assert_eq!(encoded.payloads[0], data);

    let decoded = server.handle_decode(&CodecRequest {
        codec_name: "identity".to_string(),
        payloads: encoded.payloads,
        namespace: None,
    });
    assert!(decoded.error.is_none());
    assert_eq!(decoded.payloads[0], data);
}

#[test]
fn test_codec_server_base64_roundtrip() {
    let server = CodecServer::new();
    let data = b"the quick brown fox jumps over the lazy dog";

    let encoded = server.handle_encode(&CodecRequest {
        codec_name: "base64".to_string(),
        payloads: vec![data.to_vec()],
        namespace: None,
    });
    assert!(encoded.error.is_none());
    assert_ne!(encoded.payloads[0], data); // Should be different

    let decoded = server.handle_decode(&CodecRequest {
        codec_name: "base64".to_string(),
        payloads: encoded.payloads,
        namespace: None,
    });
    assert!(decoded.error.is_none());
    assert_eq!(decoded.payloads[0], data);
}

#[test]
fn test_codec_server_batch_encode() {
    let server = CodecServer::new();
    let payloads = vec![
        b"payload-one".to_vec(),
        b"payload-two".to_vec(),
        b"payload-three".to_vec(),
    ];

    let response = server.handle_encode(&CodecRequest {
        codec_name: "identity".to_string(),
        payloads,
        namespace: None,
    });

    assert!(response.error.is_none());
    assert_eq!(response.payloads.len(), 3);
}

// === Worker Sessions Tests ===

#[test]
fn test_session_lifecycle() {
    let config = SessionConfig {
        heartbeat_timeout_ms: 5000,
        max_executions_per_session: 10,
        session_idle_timeout_ms: 30000,
    };
    let mgr = SessionManager::new(config);

    let id = mgr.create_session_at("worker-1", "orders", 1000);
    let s = mgr.get_session(&id).unwrap();
    assert_eq!(s.status, SessionStatus::Open);
    assert_eq!(s.worker_id, "worker-1");

    mgr.heartbeat(&id, 2000).unwrap();
    mgr.record_execution(&id).unwrap();

    let s = mgr.get_session(&id).unwrap();
    assert_eq!(s.execution_count, 1);
    assert_eq!(s.last_heartbeat, 2000);

    mgr.close_session(&id).unwrap();
    let s = mgr.get_session(&id).unwrap();
    assert_eq!(s.status, SessionStatus::Closed);
}

#[test]
fn test_session_auto_close_on_max_executions() {
    let config = SessionConfig {
        heartbeat_timeout_ms: 5000,
        max_executions_per_session: 3,
        session_idle_timeout_ms: 30000,
    };
    let mgr = SessionManager::new(config);

    let id = mgr.create_session("worker-1", "orders");
    mgr.record_execution(&id).unwrap();
    mgr.record_execution(&id).unwrap();

    let s = mgr.get_session(&id).unwrap();
    assert_eq!(s.status, SessionStatus::Open);

    // 3rd execution hits max
    mgr.record_execution(&id).unwrap();
    let s = mgr.get_session(&id).unwrap();
    assert_eq!(s.status, SessionStatus::Closed);
}

// === Worker Determinism Tests ===

#[test]
fn test_determinism_side_effect_recording() {
    let checker = DeterminismChecker::new();

    let id1 = checker.record_side_effect("generate-uuid", b"uuid-123", 1000);
    let id2 = checker.record_side_effect("generate-timestamp", b"1609459200", 2000);

    assert_eq!(checker.side_effect_count(), 2);
    assert_eq!(checker.replay_side_effect(id1).unwrap(), b"uuid-123");
    assert_eq!(checker.replay_side_effect(id2).unwrap(), b"1609459200");
}

#[test]
fn test_determinism_detects_random_number() {
    let checker = DeterminismChecker::new();

    let ops = vec![WorkflowOperation {
        name: "generate-id".to_string(),
        op_type: OperationType::RandomNumber,
        step: 1,
    }];

    let violations = checker.validate_no_nondeterministic_ops(&ops);
    assert_eq!(violations.len(), 1);
    assert_eq!(violations[0].severity, ViolationSeverity::Fatal);
    assert!(violations[0].reason.contains("non-deterministic"));
}

#[test]
fn test_determinism_allows_safe_operations() {
    let checker = DeterminismChecker::new();

    let ops = vec![
        WorkflowOperation {
            name: "signal-handler".to_string(),
            op_type: OperationType::Signal,
            step: 1,
        },
        WorkflowOperation {
            name: "query-handler".to_string(),
            op_type: OperationType::Query,
            step: 2,
        },
        WorkflowOperation {
            name: "timer".to_string(),
            op_type: OperationType::Timer,
            step: 3,
        },
        WorkflowOperation {
            name: "activity".to_string(),
            op_type: OperationType::Activity,
            step: 4,
        },
        WorkflowOperation {
            name: "child-wf".to_string(),
            op_type: OperationType::ChildWorkflow,
            step: 5,
        },
        WorkflowOperation {
            name: "side-effect".to_string(),
            op_type: OperationType::SideEffect,
            step: 6,
        },
    ];

    let violations = checker.validate_no_nondeterministic_ops(&ops);
    assert!(violations.is_empty());
    assert_eq!(checker.violation_count(), 0);
}

#[test]
fn test_determinism_replay_mode() {
    let checker = DeterminismChecker::new();
    assert!(!checker.is_replay_mode());

    checker.set_replay_mode(true);
    assert!(checker.is_replay_mode());

    // Record side effects during normal execution
    let id = checker.record_side_effect("uuid", b"abc", 1000);

    // Switch to replay mode
    checker.set_replay_mode(true);
    let replayed = checker.replay_side_effect(id).unwrap();
    assert_eq!(replayed, b"abc");
}

// === Cross-Module Integration Tests ===

#[test]
fn test_update_and_reachability_combined() {
    let mut controller = UpdateController::new();
    let tracker = ReachabilityTracker::new();

    // Register an update handler
    controller.register_handler("process-order", |args| {
        Ok(format!("processed: {} bytes", args.len()).into_bytes())
    });

    // Record worker polling
    tracker.record_poll("orders", 1000);

    // Check reachability
    let reachability = tracker.check_task_queue("orders");
    assert!(reachability.is_reachable);

    // Submit update
    let result = controller.submit_update(UpdateRequest {
        workflow_key: 1,
        update_id: "u1".to_string(),
        update_name: "process-order".to_string(),
        args: b"order-data".to_vec(),
        wait_policy: UpdateWaitPolicy::Completed,
    });
    assert_eq!(result.status, UpdateStatus::Completed);
}

#[test]
fn test_deployment_and_codec_combined() {
    let manager = DeploymentManager::new();
    let server = CodecServer::new();

    // Create deployment
    manager.create_deployment("d1", "prod", "v1.0", 1000);
    manager.add_task_queue("d1", "orders").unwrap();

    // Use codec to encode deployment info
    let info = format!("deployment:d1,queues:{}", "orders");
    let info_bytes = info.as_bytes().to_vec();
    let encoded = server.handle_encode(&CodecRequest {
        codec_name: "base64".to_string(),
        payloads: vec![info_bytes],
        namespace: None,
    });
    assert!(encoded.error.is_none());

    // Decode back
    let decoded = server.handle_decode(&CodecRequest {
        codec_name: "base64".to_string(),
        payloads: encoded.payloads,
        namespace: None,
    });
    assert_eq!(
        String::from_utf8(decoded.payloads[0].clone()).unwrap(),
        info
    );
}

#[test]
fn test_session_with_determinism_check() {
    let config = SessionConfig::default();
    let sessions = SessionManager::new(config);
    let checker = DeterminismChecker::new();

    // Create session
    let session_id = sessions.create_session("worker-1", "orders");

    // Record side effect
    let effect_id = checker.record_side_effect("generate-order-id", b"ORD-12345", 1000);

    // Record execution in session
    sessions.record_execution(&session_id).unwrap();

    // Replay side effect
    let replayed = checker.replay_side_effect(effect_id).unwrap();
    assert_eq!(replayed, b"ORD-12345");

    // Verify session state
    let s = sessions.get_session(&session_id).unwrap();
    assert_eq!(s.execution_count, 1);
}
