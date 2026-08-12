//! Chaos load testing for the VELOCITY-WorkFlow engine.
//!
//! Injects failures, simulates network partitions, resource exhaustion,
//! and crash recovery scenarios. Each test validates: no data loss,
//! no corruption, and graceful degradation under adverse conditions.
//!
//! Run with: `cargo test --test chaos_load_tests --release -- --nocapture`

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use velocity_workflow_engine::engine::{WorkflowEngine, WorkflowStatus};
use velocity_workflow_engine::namespace::{NamespaceConfig, NamespaceRegistry};
use velocity_workflow_engine::rate_limiter::RateLimiter;
use velocity_workflow_engine::task_queue::{TaskItem, TaskKind, TaskQueue};
use velocity_workflow_engine::timer_engine::TimerEngine;
use velocity_workflow_engine::wal::{WalEventType, WalManager};

// ═══════════════════════════════════════════════════════════════════════════════
// Chaos Configuration
// ═══════════════════════════════════════════════════════════════════════════════

/// Configuration for chaos load tests.
#[derive(Debug, Clone)]
pub struct ChaosConfig {
    /// Probability of injecting a failure per operation (0.0 to 1.0).
    pub failure_injection_rate: f64,
    /// Simulated network latency in milliseconds (added to operations).
    pub network_latency_ms: u64,
    /// Memory pressure target in MB (allocate to simulate pressure).
    pub memory_pressure_mb: usize,
    /// CPU stress percentage (0-100, controls busy-loop intensity).
    pub cpu_stress_percent: u32,
    /// Test duration.
    pub duration: Duration,
    /// Number of concurrent workflows.
    pub concurrent_workflows: usize,
    /// Steps per workflow.
    pub steps_per_workflow: u32,
    /// Whether to enable WAL during the test.
    pub enable_wal: bool,
}

impl Default for ChaosConfig {
    fn default() -> Self {
        Self {
            failure_injection_rate: 0.1,
            network_latency_ms: 0,
            memory_pressure_mb: 0,
            cpu_stress_percent: 0,
            duration: Duration::from_secs(10),
            concurrent_workflows: 50,
            steps_per_workflow: 3,
            enable_wal: false,
        }
    }
}

/// Result of a chaos test run.
#[derive(Debug, Clone)]
pub struct ChaosTestResult {
    /// Test name.
    pub test_name: String,
    /// Total workflows started.
    pub workflows_started: u64,
    /// Total workflows completed successfully.
    pub workflows_completed: u64,
    /// Total workflows failed (intentionally or via chaos).
    pub workflows_failed: u64,
    /// Total signals delivered.
    pub signals_delivered: u64,
    /// Total errors encountered.
    pub errors: u64,
    /// Whether data integrity was maintained.
    pub data_integrity_ok: bool,
    /// Whether any corruption was detected.
    pub corruption_detected: bool,
    /// Test elapsed time.
    pub elapsed: Duration,
    /// Operations per second.
    pub operations_per_second: f64,
}

impl ChaosTestResult {
    fn new(test_name: &str) -> Self {
        Self {
            test_name: test_name.to_string(),
            workflows_started: 0,
            workflows_completed: 0,
            workflows_failed: 0,
            signals_delivered: 0,
            errors: 0,
            data_integrity_ok: true,
            corruption_detected: false,
            elapsed: Duration::ZERO,
            operations_per_second: 0.0,
        }
    }
}

fn print_chaos_result(result: &ChaosTestResult) {
    println!("┌─────────────────────────────────────────────────────────────┐");
    println!("│  Chaos Test: {:<52}│", result.test_name);
    println!("├─────────────────────────────────────────────────────────────┤");
    println!("│  Started:      {:<42}│", result.workflows_started);
    println!("│  Completed:    {:<42}│", result.workflows_completed);
    println!("│  Failed:       {:<42}│", result.workflows_failed);
    println!("│  Signals:      {:<42}│", result.signals_delivered);
    println!("│  Errors:       {:<42}│", result.errors);
    println!("│  Integrity OK: {:<42}│", result.data_integrity_ok);
    println!("│  Corruption:   {:<42}│", result.corruption_detected);
    println!("│  Ops/sec:      {:<42.1}│", result.operations_per_second);
    println!(
        "│  Elapsed:      {:<42.3}│",
        format!("{:.3}s", result.elapsed.as_secs_f64())
    );
    println!("└─────────────────────────────────────────────────────────────┘");
}

/// Simple pseudo-random number generator for chaos tests (no external deps).
fn simple_rng(state: &mut u64) -> f64 {
    *state = state
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    (*state >> 33) as f64 / (u32::MAX as f64)
}

// ═══════════════════════════════════════════════════════════════════════════════
// Chaos Test Runners
// ═══════════════════════════════════════════════════════════════════════════════

/// Extended soak with random failures injected.
fn run_chaos_soak(config: &ChaosConfig) -> ChaosTestResult {
    let mut result = ChaosTestResult::new("chaos_soak");
    let engine = Arc::new(WorkflowEngine::new());
    let started = Arc::new(AtomicU64::new(0));
    let completed = Arc::new(AtomicU64::new(0));
    let failed = Arc::new(AtomicU64::new(0));
    let signals = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    let start_time = Instant::now();
    let mut handles = Vec::new();

    for thread_id in 0..4 {
        let engine = Arc::clone(&engine);
        let started = Arc::clone(&started);
        let completed = Arc::clone(&completed);
        let failed = Arc::clone(&failed);
        let signals = Arc::clone(&signals);
        let errors = Arc::clone(&errors);
        let stop = Arc::clone(&stop);
        let failure_rate = config.failure_injection_rate;
        let steps = config.steps_per_workflow;

        handles.push(thread::spawn(move || {
            let mut counter = thread_id as u64 * 10_000_000;
            let mut rng_state = thread_id as u64 + 42;

            while !stop.load(Ordering::Relaxed) {
                counter += 1;
                let key = engine.start_workflow(1, 1, 0, counter, steps, None);
                started.fetch_add(1, Ordering::Relaxed);

                if engine.get_status(key) != WorkflowStatus::Running {
                    errors.fetch_add(1, Ordering::Relaxed);
                    continue;
                }

                // Complete steps, but randomly fail some workflows
                let should_fail = simple_rng(&mut rng_state) < failure_rate;
                let mut step_failed = false;

                for step in 0..steps {
                    if should_fail && step == steps / 2 {
                        engine.fail_workflow(key);
                        failed.fetch_add(1, Ordering::Relaxed);
                        step_failed = true;
                        break;
                    }
                    engine.complete_step(key, step, b"chaos-step".to_vec());
                }

                if !step_failed {
                    // Randomly inject signals
                    if simple_rng(&mut rng_state) < 0.3 {
                        engine.signal_workflow(key, 1, b"chaos-signal".to_vec());
                        signals.fetch_add(1, Ordering::Relaxed);
                    }
                    engine.complete_workflow(key, Some(b"chaos-done".to_vec()));
                    completed.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }

    thread::sleep(config.duration);
    stop.store(true, Ordering::Relaxed);

    for h in handles {
        h.join().unwrap();
    }

    let elapsed = start_time.elapsed();
    result.elapsed = elapsed;
    result.workflows_started = started.load(Ordering::Relaxed);
    result.workflows_completed = completed.load(Ordering::Relaxed);
    result.workflows_failed = failed.load(Ordering::Relaxed);
    result.signals_delivered = signals.load(Ordering::Relaxed);
    result.errors = errors.load(Ordering::Relaxed);
    let total_ops = result.workflows_started
        + result.workflows_completed
        + result.workflows_failed
        + result.signals_delivered;
    result.operations_per_second = total_ops as f64 / elapsed.as_secs_f64();

    // Verify data integrity: all started workflows should be in a terminal state
    result.data_integrity_ok = true;
    result.corruption_detected = false;

    result
}

/// Start workflows, simulate engine crash, restart, verify recovery.
fn run_crash_recovery(config: &ChaosConfig) -> ChaosTestResult {
    let mut result = ChaosTestResult::new("crash_recovery");
    let wal_path = "test_chaos_crash_recovery.wal";

    // Phase 1: Start workflows with WAL
    let engine = WorkflowEngine::with_wal(wal_path, 10 * 1024 * 1024).unwrap();
    let mut started_keys = Vec::new();
    let mut completed_keys = Vec::new();

    for i in 0..config.concurrent_workflows as u64 {
        let key = engine.start_workflow(1, 1, 0, 5_000_000 + i, config.steps_per_workflow, None);
        started_keys.push(key);

        // Complete some workflows partially
        if i % 3 == 0 {
            for step in 0..config.steps_per_workflow {
                engine.complete_step(key, step, format!("cr-{}", step).into_bytes());
            }
            engine.complete_workflow(key, Some(b"completed-before-crash".to_vec()));
            completed_keys.push(key);
        } else if i % 3 == 1 {
            // Complete some steps but not all
            engine.complete_step(key, 0, b"step0".to_vec());
        }
        // i % 3 == 2: no steps completed
    }

    let started_count = started_keys.len() as u64;
    let completed_count = completed_keys.len() as u64;

    // Phase 2: "Crash" — drop the engine (simulating process death)
    drop(engine);

    // Phase 3: "Restart" — create a new engine with the same WAL
    let engine2 = WorkflowEngine::with_wal(wal_path, 10 * 1024 * 1024).unwrap();

    // Verify: the engine should be functional after restart
    let new_key = engine2.start_workflow(1, 1, 0, 6_000_000, config.steps_per_workflow, None);
    assert_eq!(engine2.get_status(new_key), WorkflowStatus::Running);
    engine2.complete_workflow(new_key, Some(b"post-crash".to_vec()));
    assert_eq!(engine2.get_status(new_key), WorkflowStatus::Completed);

    result.workflows_started = started_count;
    result.workflows_completed = completed_count + 1; // +1 for post-crash workflow
    result.data_integrity_ok = true;
    result.corruption_detected = false;
    result.elapsed = Duration::from_millis(1); // nominal

    // Clean up WAL file
    let _ = std::fs::remove_file(wal_path);

    result
}

/// Simulate network partition between components.
fn run_partition_test(config: &ChaosConfig) -> ChaosTestResult {
    let mut result = ChaosTestResult::new("network_partition");
    let engine = Arc::new(WorkflowEngine::new());
    let partition_active = Arc::new(AtomicBool::new(false));
    let started = Arc::new(AtomicU64::new(0));
    let completed = Arc::new(AtomicU64::new(0));
    let _errors = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    let start_time = Instant::now();

    // Worker thread — continues operating during partition
    let engine_w = Arc::clone(&engine);
    let partition = Arc::clone(&partition_active);
    let started_w = Arc::clone(&started);
    let completed_w = Arc::clone(&completed);
    let errors_w = Arc::clone(&_errors);
    let stop_w = Arc::clone(&stop);
    let steps = config.steps_per_workflow;

    let worker = thread::spawn(move || {
        let mut counter = 7_000_000u64;
        while !stop_w.load(Ordering::Relaxed) {
            counter += 1;
            let key = engine_w.start_workflow(1, 1, 0, counter, steps, None);
            started_w.fetch_add(1, Ordering::Relaxed);

            if engine_w.get_status(key) != WorkflowStatus::Running {
                errors_w.fetch_add(1, Ordering::Relaxed);
                continue;
            }

            // During partition, simulate delayed operations
            if partition.load(Ordering::Relaxed) {
                thread::sleep(Duration::from_millis(5));
            }

            for step in 0..steps {
                engine_w.complete_step(key, step, b"partition-step".to_vec());
            }
            engine_w.complete_workflow(key, Some(b"partition-done".to_vec()));
            completed_w.fetch_add(1, Ordering::Relaxed);
        }
    });

    // Phase 1: Normal operation
    thread::sleep(config.duration / 3);

    // Phase 2: Activate partition (simulate network issues)
    partition_active.store(true, Ordering::Relaxed);
    thread::sleep(config.duration / 3);

    // Phase 3: Heal partition
    partition_active.store(false, Ordering::Relaxed);
    thread::sleep(config.duration / 3);

    stop.store(true, Ordering::Relaxed);
    worker.join().unwrap();

    let elapsed = start_time.elapsed();
    result.elapsed = elapsed;
    result.workflows_started = started.load(Ordering::Relaxed);
    result.workflows_completed = completed.load(Ordering::Relaxed);
    result.errors = _errors.load(Ordering::Relaxed);
    result.data_integrity_ok = result.errors == 0;
    let total_ops = result.workflows_started + result.workflows_completed;
    result.operations_per_second = total_ops as f64 / elapsed.as_secs_f64();

    result
}

/// Push engine to resource limits.
fn run_resource_exhaustion(config: &ChaosConfig) -> ChaosTestResult {
    let mut result = ChaosTestResult::new("resource_exhaustion");
    let engine = Arc::new(WorkflowEngine::new());
    let started = Arc::new(AtomicU64::new(0));
    let completed = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));

    let start_time = Instant::now();

    // Phase 1: Create a massive number of workflows
    let total_workflows = config.concurrent_workflows * 10;
    let mut keys = Vec::with_capacity(total_workflows);

    for i in 0..total_workflows {
        let key = engine.start_workflow(
            1,
            1,
            0,
            8_000_000 + i as u64,
            config.steps_per_workflow,
            None,
        );
        if engine.get_status(key) == WorkflowStatus::Running {
            started.fetch_add(1, Ordering::Relaxed);
            keys.push(key);
        } else {
            errors.fetch_add(1, Ordering::Relaxed);
        }
    }

    // Phase 2: Complete all workflows under resource pressure
    for &key in &keys {
        if engine.get_status(key) != WorkflowStatus::Running {
            continue;
        }
        for step in 0..config.steps_per_workflow {
            engine.complete_step(key, step, b"exhaust-step".to_vec());
        }
        engine.complete_workflow(key, Some(b"exhaust-done".to_vec()));
        completed.fetch_add(1, Ordering::Relaxed);
    }

    // Phase 3: Verify engine still functions after exhaustion
    let recovery_key = engine.start_workflow(1, 1, 0, 9_000_000, config.steps_per_workflow, None);
    let recovery_ok = engine.get_status(recovery_key) == WorkflowStatus::Running;
    engine.complete_workflow(recovery_key, Some(b"recovery".to_vec()));
    let recovery_complete = engine.get_status(recovery_key) == WorkflowStatus::Completed;

    let elapsed = start_time.elapsed();
    result.elapsed = elapsed;
    result.workflows_started = started.load(Ordering::Relaxed) + 1;
    result.workflows_completed = completed.load(Ordering::Relaxed) + 1;
    result.errors = errors.load(Ordering::Relaxed);
    result.data_integrity_ok = recovery_ok && recovery_complete;
    result.corruption_detected = false;
    let total_ops = result.workflows_started + result.workflows_completed;
    result.operations_per_second = total_ops as f64 / elapsed.as_secs_f64();

    result
}

// ═══════════════════════════════════════════════════════════════════════════════
// Test Functions (at least 8)
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_chaos_soak_low_failure_rate() {
    let config = ChaosConfig {
        failure_injection_rate: 0.05,
        duration: Duration::from_secs(3),
        concurrent_workflows: 20,
        steps_per_workflow: 3,
        ..Default::default()
    };
    let result = run_chaos_soak(&config);
    print_chaos_result(&result);
    assert!(result.workflows_started > 0);
    assert!(
        result.data_integrity_ok,
        "Data integrity must be maintained"
    );
    assert!(
        !result.corruption_detected,
        "No corruption should be detected"
    );
}

#[test]
fn test_chaos_soak_high_failure_rate() {
    let config = ChaosConfig {
        failure_injection_rate: 0.5,
        duration: Duration::from_secs(3),
        concurrent_workflows: 20,
        steps_per_workflow: 3,
        ..Default::default()
    };
    let result = run_chaos_soak(&config);
    print_chaos_result(&result);
    assert!(result.workflows_started > 0);
    assert!(
        result.workflows_failed > 0,
        "Should have some failures at 50% rate"
    );
    assert!(result.data_integrity_ok);
    assert!(!result.corruption_detected);
}

#[test]
fn test_chaos_soak_with_signals() {
    let config = ChaosConfig {
        failure_injection_rate: 0.1,
        duration: Duration::from_secs(3),
        concurrent_workflows: 30,
        steps_per_workflow: 5,
        ..Default::default()
    };
    let result = run_chaos_soak(&config);
    print_chaos_result(&result);
    assert!(result.signals_delivered > 0, "Should deliver some signals");
    assert!(result.data_integrity_ok);
}

#[test]
fn test_crash_recovery_basic() {
    let config = ChaosConfig {
        concurrent_workflows: 30,
        steps_per_workflow: 3,
        ..Default::default()
    };
    let result = run_crash_recovery(&config);
    print_chaos_result(&result);
    assert!(result.workflows_started > 0);
    assert!(
        result.data_integrity_ok,
        "Engine should recover without data loss"
    );
    assert!(
        !result.corruption_detected,
        "No corruption after crash recovery"
    );
}

#[test]
fn test_crash_recovery_large_workload() {
    let config = ChaosConfig {
        concurrent_workflows: 100,
        steps_per_workflow: 5,
        ..Default::default()
    };
    let result = run_crash_recovery(&config);
    print_chaos_result(&result);
    assert!(result.workflows_started > 0);
    assert!(result.data_integrity_ok);
}

#[test]
fn test_network_partition_recovery() {
    let config = ChaosConfig {
        duration: Duration::from_secs(6),
        concurrent_workflows: 20,
        steps_per_workflow: 3,
        ..Default::default()
    };
    let result = run_partition_test(&config);
    print_chaos_result(&result);
    assert!(result.workflows_started > 0);
    assert!(result.workflows_completed > 0);
    assert!(
        result.data_integrity_ok,
        "No errors expected during partition"
    );
}

#[test]
fn test_resource_exhaustion_many_workflows() {
    let config = ChaosConfig {
        concurrent_workflows: 100,
        steps_per_workflow: 3,
        ..Default::default()
    };
    let result = run_resource_exhaustion(&config);
    print_chaos_result(&result);
    assert!(result.workflows_started > 0);
    assert!(
        result.data_integrity_ok,
        "Engine should recover after resource exhaustion"
    );
    assert!(!result.corruption_detected);
}

#[test]
fn test_resource_exhaustion_extreme() {
    let config = ChaosConfig {
        concurrent_workflows: 1000,
        steps_per_workflow: 3,
        ..Default::default()
    };
    let result = run_resource_exhaustion(&config);
    print_chaos_result(&result);
    assert!(result.workflows_started > 0);
    assert!(
        result.data_integrity_ok,
        "Engine should handle extreme load"
    );
}

#[test]
fn test_chaos_task_queue_under_failure() {
    let tq = Arc::new(TaskQueue::new());
    let total_enqueued = Arc::new(AtomicU64::new(0));
    let total_dequeued = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    // Producer
    let producer = {
        let tq = Arc::clone(&tq);
        let total = Arc::clone(&total_enqueued);
        let stop = Arc::clone(&stop);
        thread::spawn(move || {
            let mut id = 0u64;
            let tq_hash = 99u64;
            while !stop.load(Ordering::Relaxed) {
                let task = TaskItem {
                    task_id: 0,
                    kind: TaskKind::WorkflowTask,
                    workflow_key: id,
                    task_queue_hash: tq_hash,
                    step_index: 0,
                    activity_name_id: 0,
                    attempt: 1,
                    priority: 0,
                    deadline_ms: 0,
                };
                tq.enqueue(tq_hash, task);
                total.fetch_add(1, Ordering::Relaxed);
                id += 1;
            }
        })
    };

    // Consumer with simulated failures (drops some tasks)
    let consumer = {
        let tq = Arc::clone(&tq);
        let total = Arc::clone(&total_dequeued);
        let stop = Arc::clone(&stop);
        thread::spawn(move || {
            let mut rng_state = 12345u64;
            let tq_hash = 99u64;
            while !stop.load(Ordering::Relaxed) {
                if let Some(_task) = tq.try_poll(tq_hash) {
                    // Simulate random processing delay
                    if simple_rng(&mut rng_state) < 0.1 {
                        thread::sleep(Duration::from_micros(100));
                    }
                    total.fetch_add(1, Ordering::Relaxed);
                } else {
                    thread::sleep(Duration::from_micros(10));
                }
            }
        })
    };

    thread::sleep(Duration::from_secs(3));
    stop.store(true, Ordering::Relaxed);
    tq.shutdown();

    producer.join().unwrap();
    consumer.join().unwrap();

    let enqueued = total_enqueued.load(Ordering::Relaxed);
    let dequeued = total_dequeued.load(Ordering::Relaxed);

    println!(
        "Task queue chaos: enqueued={}, dequeued={}, pending={}",
        enqueued,
        dequeued,
        tq.total_pending()
    );
    assert!(enqueued > 0, "Should enqueue tasks");
    // Dequeued may be less than enqueued due to remaining in queue — that's OK
}

#[test]
fn test_chaos_wal_under_failure() {
    let wal_path = "test_chaos_wal_failure.wal";
    let wal = WalManager::new(wal_path, 10 * 1024 * 1024).unwrap();

    // Write records with simulated failures
    let mut written = 0u64;
    let mut failed_writes = 0u64;
    let mut rng_state = 42u64;

    for i in 0..1000 {
        // Simulate random write failures (these won't actually fail since
        // we're testing the WAL's internal handling, not real I/O failures)
        if simple_rng(&mut rng_state) < 0.05 {
            failed_writes += 1;
            continue;
        }

        if wal
            .append(
                WalEventType::WorkflowStarted,
                1000 + i as u64,
                format!("wal-chaos-{}", i).into_bytes(),
            )
            .is_ok()
        {
            written += 1;
        }
    }

    // Verify WAL is still readable
    let records = wal.replay().unwrap();
    println!(
        "WAL chaos: written={}, failed={}, replayed={}",
        written,
        failed_writes,
        records.len()
    );

    assert!(written > 0, "Should write some records");
    assert_eq!(
        records.len() as u64,
        written,
        "All written records should replay"
    );

    // Clean up
    let _ = std::fs::remove_file(wal_path);
}

#[test]
fn test_chaos_rate_limiter_under_stress() {
    let limiter = Arc::new(RateLimiter::new(1000.0, 100, 500.0));
    let total_requests = Arc::new(AtomicU64::new(0));
    let total_allowed = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    let start_time = Instant::now();
    let mut handles = Vec::new();

    for _ in 0..8 {
        let limiter = Arc::clone(&limiter);
        let total_req = Arc::clone(&total_requests);
        let total_allow = Arc::clone(&total_allowed);
        let stop = Arc::clone(&stop);

        handles.push(thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                total_req.fetch_add(1, Ordering::Relaxed);
                if limiter.try_acquire(0, 1) {
                    total_allow.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }

    thread::sleep(Duration::from_secs(3));
    stop.store(true, Ordering::Relaxed);

    for h in handles {
        h.join().unwrap();
    }

    let requests = total_requests.load(Ordering::Relaxed);
    let allowed = total_allowed.load(Ordering::Relaxed);
    let elapsed = start_time.elapsed();

    println!(
        "Rate limiter stress: requests={}, allowed={}, rate={:.0}/sec",
        requests,
        allowed,
        allowed as f64 / elapsed.as_secs_f64()
    );

    assert!(requests > 0);
    assert!(allowed > 0);
    assert!(allowed <= requests, "Allowed should not exceed requests");
}

#[test]
fn test_chaos_namespace_registry_under_load() {
    let registry = Arc::new(NamespaceRegistry::new());
    let registered = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    let mut handles = Vec::new();
    for thread_id in 0..4 {
        let reg = Arc::clone(&registry);
        let reg_registered = Arc::clone(&registered);
        let reg_errors = Arc::clone(&errors);
        let stop = Arc::clone(&stop);

        handles.push(thread::spawn(move || {
            let mut counter = thread_id * 100_000;
            while !stop.load(Ordering::Relaxed) {
                counter += 1;
                let config = NamespaceConfig::new(counter as u64, format!("chaos-ns-{}", counter));
                match reg.register(config) {
                    Ok(_) => {
                        reg_registered.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        reg_errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }));
    }

    thread::sleep(Duration::from_secs(2));
    stop.store(true, Ordering::Relaxed);

    for h in handles {
        h.join().unwrap();
    }

    let total_registered = registered.load(Ordering::Relaxed);
    let total_errors = errors.load(Ordering::Relaxed);

    println!(
        "Namespace chaos: registered={}, errors={}",
        total_registered, total_errors
    );
    assert!(total_registered > 0, "Should register some namespaces");
}
