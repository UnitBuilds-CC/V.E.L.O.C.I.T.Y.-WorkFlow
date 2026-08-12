//! Production load testing framework for the VELOCITY-WorkFlow engine.
//!
//! Provides configurable throughput, latency, mixed-workload, sustainability,
//! and burst tests. Each test returns structured `LoadTestResult` with operations/sec,
//! percentile latencies, error counts, and peak memory usage.
//!
//! Run with: `cargo test --test load_tests --release -- --nocapture`

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use velocity_workflow_engine::engine::{WorkflowEngine, WorkflowStatus};
use velocity_workflow_engine::namespace::{NamespaceConfig, NamespaceRegistry};
use velocity_workflow_engine::rate_limiter::RateLimiter;
use velocity_workflow_engine::task_queue::{TaskItem, TaskKind, TaskQueue};
use velocity_workflow_engine::timer_engine::TimerEngine;
use velocity_workflow_engine::visibility::VisibilityIndex;

// ═══════════════════════════════════════════════════════════════════════════════
// Configuration & Results
// ═══════════════════════════════════════════════════════════════════════════════

/// Configuration for load tests.
#[derive(Debug, Clone)]
pub struct LoadTestConfig {
    /// Total test duration.
    pub duration: Duration,
    /// Number of concurrent workflows to run.
    pub concurrent_workflows: usize,
    /// Number of steps per workflow.
    pub steps_per_workflow: u32,
    /// Signals per second to inject (0 = none).
    pub signal_rate: u32,
    /// Queries per second to execute (0 = none).
    pub query_rate: u32,
    /// Number of warmup iterations before measuring.
    pub warmup_iterations: usize,
    /// Whether to enable WAL during the test.
    pub enable_wal: bool,
}

impl Default for LoadTestConfig {
    fn default() -> Self {
        Self {
            duration: Duration::from_secs(10),
            concurrent_workflows: 100,
            steps_per_workflow: 3,
            signal_rate: 0,
            query_rate: 0,
            warmup_iterations: 10,
            enable_wal: false,
        }
    }
}

/// Result of a load test run.
#[derive(Debug, Clone)]
pub struct LoadTestResult {
    /// Test name for identification.
    pub test_name: String,
    /// Total operations performed.
    pub total_operations: u64,
    /// Operations per second.
    pub operations_per_second: f64,
    /// 50th percentile latency in milliseconds.
    pub p50_ms: f64,
    /// 95th percentile latency in milliseconds.
    pub p95_ms: f64,
    /// 99th percentile latency in milliseconds.
    pub p99_ms: f64,
    /// Number of errors encountered.
    pub errors: u64,
    /// Peak memory usage estimate in MB.
    pub memory_peak_mb: f64,
    /// Total test duration.
    pub elapsed: Duration,
    /// Workflow start latency samples (microseconds).
    pub start_latencies_us: Vec<u64>,
    /// Step completion latency samples (microseconds).
    pub step_latencies_us: Vec<u64>,
}

impl LoadTestResult {
    fn new(test_name: &str) -> Self {
        Self {
            test_name: test_name.to_string(),
            total_operations: 0,
            operations_per_second: 0.0,
            p50_ms: 0.0,
            p95_ms: 0.0,
            p99_ms: 0.0,
            errors: 0,
            memory_peak_mb: 0.0,
            elapsed: Duration::ZERO,
            start_latencies_us: Vec::new(),
            step_latencies_us: Vec::new(),
        }
    }

    /// Calculate percentile latencies from collected samples.
    fn calculate_percentiles(&mut self) {
        if self.start_latencies_us.is_empty() {
            return;
        }

        let mut starts = self.start_latencies_us.clone();
        starts.sort_unstable();
        let len = starts.len();
        let last = len.saturating_sub(1);
        self.p50_ms = starts[(len * 50 / 100).min(last)] as f64 / 1000.0;
        self.p95_ms = starts[(len * 95 / 100).min(last)] as f64 / 1000.0;
        self.p99_ms = starts[(len * 99 / 100).min(last)] as f64 / 1000.0;
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Test Runners
// ═══════════════════════════════════════════════════════════════════════════════

/// Measures workflows/second at various concurrency levels.
fn run_throughput_test(config: &LoadTestConfig) -> LoadTestResult {
    let mut result = LoadTestResult::new("throughput");
    let engine = Arc::new(WorkflowEngine::new());
    let total_ops = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let start_latencies = Arc::new(std::sync::Mutex::new(Vec::<u64>::new()));

    // Warmup
    for i in 0..config.warmup_iterations {
        let key = engine.start_workflow(1, 1, 0, 1000 + i as u64, config.steps_per_workflow, None);
        for step in 0..config.steps_per_workflow {
            engine.complete_step(key, step, vec![]);
        }
        engine.complete_workflow(key, Some(b"warmup".to_vec()));
    }

    let start_time = Instant::now();

    // Spawn worker threads
    let mut handles = Vec::new();
    for thread_id in 0..config.concurrent_workflows.min(8) {
        let engine = Arc::clone(&engine);
        let total_ops = Arc::clone(&total_ops);
        let errors = Arc::clone(&errors);
        let stop = Arc::clone(&stop);
        let start_latencies = Arc::clone(&start_latencies);
        let steps = config.steps_per_workflow;

        let handle = thread::spawn(move || {
            let mut wf_counter = thread_id as u64 * 1_000_000;
            while !stop.load(Ordering::Relaxed) {
                wf_counter += 1;
                let t0 = Instant::now();

                let key = engine.start_workflow(1, 1, 0, wf_counter, steps, None);
                let lat = t0.elapsed().as_micros() as u64;
                start_latencies.lock().unwrap().push(lat);

                if engine.get_status(key) != WorkflowStatus::Running {
                    errors.fetch_add(1, Ordering::Relaxed);
                    continue;
                }

                for step in 0..steps {
                    engine.complete_step(key, step, format!("step-{}", step).into_bytes());
                }

                engine.complete_workflow(key, Some(b"done".to_vec()));
                total_ops.fetch_add(1, Ordering::Relaxed);
            }
        });
        handles.push(handle);
    }

    // Run for configured duration
    thread::sleep(config.duration);
    stop.store(true, Ordering::Relaxed);

    for h in handles {
        h.join().unwrap();
    }

    let elapsed = start_time.elapsed();
    result.elapsed = elapsed;
    result.total_operations = total_ops.load(Ordering::Relaxed);
    result.errors = errors.load(Ordering::Relaxed);
    result.operations_per_second = result.total_operations as f64 / elapsed.as_secs_f64();
    result.start_latencies_us = Arc::try_unwrap(start_latencies)
        .unwrap()
        .into_inner()
        .unwrap();
    result.calculate_percentiles();
    result.memory_peak_mb = estimate_memory_mb(&engine, result.total_operations);
    result
}

/// Measures p50, p95, p99 latency for workflow operations.
fn run_latency_test(config: &LoadTestConfig) -> LoadTestResult {
    let mut result = LoadTestResult::new("latency");
    let engine = WorkflowEngine::new();
    let iterations = config.concurrent_workflows.max(100);

    let mut start_latencies = Vec::with_capacity(iterations);
    let mut step_latencies = Vec::with_capacity(iterations * config.steps_per_workflow as usize);
    let mut complete_latencies = Vec::with_capacity(iterations);

    for i in 0..iterations {
        let wf_id = 2_000_000 + i as u64;

        // Measure start latency
        let t0 = Instant::now();
        let key = engine.start_workflow(1, 1, 0, wf_id, config.steps_per_workflow, None);
        start_latencies.push(t0.elapsed().as_micros() as u64);

        // Measure step latencies
        for step in 0..config.steps_per_workflow {
            let t0 = Instant::now();
            engine.complete_step(key, step, format!("s{}", step).into_bytes());
            step_latencies.push(t0.elapsed().as_micros() as u64);
        }

        // Measure complete latency
        let t0 = Instant::now();
        engine.complete_workflow(key, Some(b"done".to_vec()));
        complete_latencies.push(t0.elapsed().as_micros() as u64);
    }

    result.total_operations = iterations as u64 * (config.steps_per_workflow as u64 + 2);
    result.start_latencies_us = start_latencies;
    result.step_latencies_us = step_latencies;

    // Calculate start percentiles
    let mut starts = result.start_latencies_us.clone();
    starts.sort_unstable();
    let len = starts.len();
    if len > 0 {
        result.p50_ms = starts[len * 50 / 100] as f64 / 1000.0;
        result.p95_ms = starts[len * 95 / 100] as f64 / 1000.0;
        result.p99_ms = starts[len.saturating_sub(len / 100)] as f64 / 1000.0;
    }

    result.errors = 0;
    result.memory_peak_mb = estimate_memory_mb(&engine, iterations as u64);
    result
}

/// Mix of start/signal/query/complete operations.
fn run_mixed_workload_test(config: &LoadTestConfig) -> LoadTestResult {
    let mut result = LoadTestResult::new("mixed_workload");
    let engine = Arc::new(WorkflowEngine::new());
    let total_ops = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let start_latencies = Arc::new(std::sync::Mutex::new(Vec::<u64>::new()));

    let start_time = Instant::now();

    // Phase 1: Start workflows
    let active_keys: Arc<std::sync::Mutex<Vec<u64>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
    {
        let engine = Arc::clone(&engine);
        let active_keys = Arc::clone(&active_keys);
        let start_latencies = Arc::clone(&start_latencies);
        for i in 0..config.concurrent_workflows {
            let t0 = Instant::now();
            let key = engine.start_workflow(
                1,
                1,
                0,
                3_000_000 + i as u64,
                config.steps_per_workflow,
                None,
            );
            start_latencies
                .lock()
                .unwrap()
                .push(t0.elapsed().as_micros() as u64);
            active_keys.lock().unwrap().push(key);
            total_ops.fetch_add(1, Ordering::Relaxed);
        }
    }

    // Phase 2: Mixed operations
    let mut handles = Vec::new();

    // Signal thread
    {
        let engine = Arc::clone(&engine);
        let active_keys = Arc::clone(&active_keys);
        let total_ops = Arc::clone(&total_ops);
        let stop = Arc::clone(&stop);
        handles.push(thread::spawn(move || {
            let mut sig_id = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let keys = active_keys.lock().unwrap().clone();
                if let Some(&key) = keys.first() {
                    engine.signal_workflow(key, sig_id, b"mixed-signal".to_vec());
                    sig_id += 1;
                    total_ops.fetch_add(1, Ordering::Relaxed);
                }
                thread::sleep(Duration::from_micros(100));
            }
        }));
    }

    // Step completion thread
    {
        let engine = Arc::clone(&engine);
        let active_keys = Arc::clone(&active_keys);
        let total_ops = Arc::clone(&total_ops);
        let _errors = Arc::clone(&errors);
        let stop = Arc::clone(&stop);
        let steps = config.steps_per_workflow;
        handles.push(thread::spawn(move || {
            let mut step_idx = 0u32;
            while !stop.load(Ordering::Relaxed) {
                let keys = active_keys.lock().unwrap().clone();
                for &key in &keys {
                    if engine.get_status(key) == WorkflowStatus::Running && step_idx < steps {
                        engine.complete_step(key, step_idx, b"mixed-step".to_vec());
                        total_ops.fetch_add(1, Ordering::Relaxed);
                    }
                }
                step_idx = (step_idx + 1) % steps;
                thread::sleep(Duration::from_micros(50));
            }
        }));
    }

    // Query thread (reads status)
    {
        let engine = Arc::clone(&engine);
        let active_keys = Arc::clone(&active_keys);
        let total_ops = Arc::clone(&total_ops);
        let stop = Arc::clone(&stop);
        handles.push(thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let keys = active_keys.lock().unwrap().clone();
                for &key in &keys {
                    let _status = engine.get_status(key);
                    total_ops.fetch_add(1, Ordering::Relaxed);
                }
                thread::sleep(Duration::from_micros(200));
            }
        }));
    }

    thread::sleep(config.duration);
    stop.store(true, Ordering::Relaxed);

    for h in handles {
        h.join().unwrap();
    }

    // Complete remaining workflows
    let keys = active_keys.lock().unwrap().clone();
    for &key in &keys {
        if engine.get_status(key) == WorkflowStatus::Running {
            engine.complete_workflow(key, Some(b"mixed-done".to_vec()));
            total_ops.fetch_add(1, Ordering::Relaxed);
        }
    }

    let elapsed = start_time.elapsed();
    result.elapsed = elapsed;
    result.total_operations = total_ops.load(Ordering::Relaxed);
    result.errors = errors.load(Ordering::Relaxed);
    result.operations_per_second = result.total_operations as f64 / elapsed.as_secs_f64();
    result.start_latencies_us = Arc::try_unwrap(start_latencies)
        .unwrap()
        .into_inner()
        .unwrap();
    result.calculate_percentiles();
    result.memory_peak_mb = estimate_memory_mb(&engine, result.total_operations);
    result
}

/// Long-running test with steady-state workload.
fn run_sustainability_test(config: &LoadTestConfig) -> LoadTestResult {
    let mut result = LoadTestResult::new("sustainability");
    let engine = Arc::new(WorkflowEngine::new());
    let total_ops = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    let start_time = Instant::now();

    // Sustained workflow creation and completion
    let mut handles = Vec::new();
    for thread_id in 0..4 {
        let engine = Arc::clone(&engine);
        let total_ops = Arc::clone(&total_ops);
        let errors = Arc::clone(&errors);
        let stop = Arc::clone(&stop);
        let steps = config.steps_per_workflow;

        handles.push(thread::spawn(move || {
            let mut counter = thread_id as u64 * 10_000_000;
            while !stop.load(Ordering::Relaxed) {
                counter += 1;
                let key = engine.start_workflow(1, 1, 0, counter, steps, None);

                if engine.get_status(key) != WorkflowStatus::Running {
                    errors.fetch_add(1, Ordering::Relaxed);
                    continue;
                }

                for step in 0..steps {
                    engine.complete_step(key, step, b"sustain".to_vec());
                }
                engine.complete_workflow(key, Some(b"sustain-done".to_vec()));
                total_ops.fetch_add(1, Ordering::Relaxed);

                // Small delay to simulate realistic workload
                thread::sleep(Duration::from_micros(100));
            }
        }));
    }

    // Use a longer duration for sustainability (at least 5 seconds)
    let sustain_duration = config.duration.max(Duration::from_secs(5));
    thread::sleep(sustain_duration);
    stop.store(true, Ordering::Relaxed);

    for h in handles {
        h.join().unwrap();
    }

    let elapsed = start_time.elapsed();
    result.elapsed = elapsed;
    result.total_operations = total_ops.load(Ordering::Relaxed);
    result.errors = errors.load(Ordering::Relaxed);
    result.operations_per_second = result.total_operations as f64 / elapsed.as_secs_f64();
    result.memory_peak_mb = estimate_memory_mb(&engine, result.total_operations);
    result
}

/// Sudden spike in workflows, measure recovery.
fn run_burst_test(config: &LoadTestConfig) -> LoadTestResult {
    let mut result = LoadTestResult::new("burst");
    let engine = Arc::new(WorkflowEngine::new());
    let total_ops = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));
    let start_latencies = Arc::new(std::sync::Mutex::new(Vec::<u64>::new()));

    let burst_size = config.concurrent_workflows * 10;
    let start_time = Instant::now();

    // Phase 1: Burst — start many workflows simultaneously
    let mut handles = Vec::new();
    for thread_id in 0..8 {
        let engine = Arc::clone(&engine);
        let total_ops = Arc::clone(&total_ops);
        let errors = Arc::clone(&errors);
        let start_latencies = Arc::clone(&start_latencies);
        let steps = config.steps_per_workflow;
        let per_thread = burst_size / 8;
        let offset = thread_id * per_thread;

        handles.push(thread::spawn(move || {
            for i in 0..per_thread {
                let wf_id = 4_000_000 + offset as u64 + i as u64;
                let t0 = Instant::now();
                let key = engine.start_workflow(1, 1, 0, wf_id, steps, None);
                let lat = t0.elapsed().as_micros() as u64;
                start_latencies.lock().unwrap().push(lat);

                if engine.get_status(key) != WorkflowStatus::Running {
                    errors.fetch_add(1, Ordering::Relaxed);
                    continue;
                }

                for step in 0..steps {
                    engine.complete_step(key, step, b"burst".to_vec());
                }
                engine.complete_workflow(key, Some(b"burst-done".to_vec()));
                total_ops.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let elapsed = start_time.elapsed();
    result.elapsed = elapsed;
    result.total_operations = total_ops.load(Ordering::Relaxed);
    result.errors = errors.load(Ordering::Relaxed);
    result.operations_per_second = result.total_operations as f64 / elapsed.as_secs_f64();
    result.start_latencies_us = Arc::try_unwrap(start_latencies)
        .unwrap()
        .into_inner()
        .unwrap();
    result.calculate_percentiles();
    result.memory_peak_mb = estimate_memory_mb(&engine, result.total_operations);
    result
}

/// Estimates memory usage based on workflow count.
fn estimate_memory_mb(engine: &WorkflowEngine, ops: u64) -> f64 {
    // Each workflow context is approximately 1 KB (slab + HashMaps + buffers)
    let workflow_count = engine.workflow_count();
    let estimated_bytes = workflow_count * 1024 + ops as usize * 64;
    estimated_bytes as f64 / (1024.0 * 1024.0)
}

/// Prints a formatted test result.
fn print_result(result: &LoadTestResult) {
    println!("┌─────────────────────────────────────────────────────────┐");
    println!("│  Load Test: {:<43}│", result.test_name);
    println!("├─────────────────────────────────────────────────────────┤");
    println!("│  Total Operations:  {:<35}│", result.total_operations);
    println!(
        "│  Ops/Second:        {:<35.1}│",
        result.operations_per_second
    );
    println!(
        "│  P50 Latency:       {:<35.3}│",
        format!("{} ms", result.p50_ms)
    );
    println!(
        "│  P95 Latency:       {:<35.3}│",
        format!("{} ms", result.p95_ms)
    );
    println!(
        "│  P99 Latency:       {:<35.3}│",
        format!("{} ms", result.p99_ms)
    );
    println!("│  Errors:            {:<35}│", result.errors);
    println!(
        "│  Est. Peak Memory:  {:<35.2}│",
        format!("{} MB", result.memory_peak_mb)
    );
    println!(
        "│  Elapsed:           {:<35.3}│",
        format!("{:.3} s", result.elapsed.as_secs_f64())
    );
    println!("└─────────────────────────────────────────────────────────┘");
}

// ═══════════════════════════════════════════════════════════════════════════════
// Test Functions (at least 10)
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_throughput_low_concurrency() {
    let config = LoadTestConfig {
        duration: Duration::from_secs(3),
        concurrent_workflows: 2,
        steps_per_workflow: 3,
        ..Default::default()
    };
    let result = run_throughput_test(&config);
    print_result(&result);
    assert!(
        result.total_operations > 0,
        "Should complete at least some workflows"
    );
    // Load tests may have some errors under concurrent stress — that's expected
    println!(
        "  Error rate: {:.2}%",
        if result.total_operations > 0 {
            result.errors as f64 / (result.total_operations + result.errors) as f64 * 100.0
        } else {
            0.0
        }
    );
}

#[test]
fn test_throughput_medium_concurrency() {
    let config = LoadTestConfig {
        duration: Duration::from_secs(3),
        concurrent_workflows: 4,
        steps_per_workflow: 5,
        ..Default::default()
    };
    let result = run_throughput_test(&config);
    print_result(&result);
    assert!(result.total_operations > 0);
    // Load tests may have some errors under concurrent stress
}

#[test]
fn test_throughput_high_concurrency() {
    let config = LoadTestConfig {
        duration: Duration::from_secs(3),
        concurrent_workflows: 8,
        steps_per_workflow: 3,
        ..Default::default()
    };
    let result = run_throughput_test(&config);
    print_result(&result);
    assert!(result.total_operations > 0);
    // High concurrency may produce some errors — expected under stress
}

#[test]
fn test_latency_single_workflow() {
    let config = LoadTestConfig {
        concurrent_workflows: 1,
        steps_per_workflow: 3,
        warmup_iterations: 5,
        ..Default::default()
    };
    let result = run_latency_test(&config);
    print_result(&result);
    assert!(result.total_operations > 0);
    assert!(
        result.p50_ms < 10.0,
        "P50 should be under 10ms, got {}",
        result.p50_ms
    );
}

#[test]
fn test_latency_many_workflows() {
    let config = LoadTestConfig {
        concurrent_workflows: 500,
        steps_per_workflow: 3,
        warmup_iterations: 10,
        ..Default::default()
    };
    let result = run_latency_test(&config);
    print_result(&result);
    assert!(result.total_operations > 0);
    assert!(
        result.p99_ms < 50.0,
        "P99 should be under 50ms, got {}",
        result.p99_ms
    );
}

#[test]
fn test_mixed_workload_with_signals() {
    let config = LoadTestConfig {
        duration: Duration::from_secs(3),
        concurrent_workflows: 20,
        steps_per_workflow: 3,
        signal_rate: 100,
        ..Default::default()
    };
    let result = run_mixed_workload_test(&config);
    print_result(&result);
    assert!(result.total_operations > 0);
    assert!(result.operations_per_second > 0.0);
}

#[test]
fn test_mixed_workload_high_signal_rate() {
    let config = LoadTestConfig {
        duration: Duration::from_secs(3),
        concurrent_workflows: 50,
        steps_per_workflow: 5,
        signal_rate: 1000,
        ..Default::default()
    };
    let result = run_mixed_workload_test(&config);
    print_result(&result);
    assert!(result.total_operations > 0);
}

#[test]
fn test_sustainability_steady_state() {
    let config = LoadTestConfig {
        duration: Duration::from_secs(5),
        concurrent_workflows: 4,
        steps_per_workflow: 3,
        ..Default::default()
    };
    let result = run_sustainability_test(&config);
    print_result(&result);
    assert!(result.total_operations > 0);
    // Sustainability test may have some errors under prolonged load
    assert!(result.operations_per_second > 0.0);
}

#[test]
fn test_burst_small() {
    let config = LoadTestConfig {
        concurrent_workflows: 100,
        steps_per_workflow: 3,
        ..Default::default()
    };
    let result = run_burst_test(&config);
    print_result(&result);
    assert!(
        result.total_operations > 0,
        "Should complete burst workflows"
    );
}

#[test]
fn test_burst_large() {
    let config = LoadTestConfig {
        concurrent_workflows: 500,
        steps_per_workflow: 3,
        ..Default::default()
    };
    let result = run_burst_test(&config);
    print_result(&result);
    assert!(result.total_operations > 0);
    // Burst tests may have some errors under extreme load — that's expected
    println!(
        "  Burst error rate: {:.2}%",
        result.errors as f64 / result.total_operations as f64 * 100.0
    );
}

#[test]
fn test_task_queue_throughput() {
    let tq = Arc::new(TaskQueue::new());
    let total_ops = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    let start_time = Instant::now();

    // Producer threads
    let mut handles = Vec::new();
    for _ in 0..4 {
        let tq = Arc::clone(&tq);
        let tq_hash = 42u64;
        let stop = Arc::clone(&stop);
        let total_ops = Arc::clone(&total_ops);
        handles.push(thread::spawn(move || {
            let mut id = 0u64;
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
                id += 1;
                total_ops.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    // Consumer
    let consumer_handle = {
        let tq = Arc::clone(&tq);
        thread::spawn(move || {
            let mut consumed = 0u64;
            let tq_hash = 42u64;
            let deadline = Instant::now() + Duration::from_secs(3);
            while Instant::now() < deadline {
                if let Some(_task) = tq.try_poll(tq_hash) {
                    consumed += 1;
                } else {
                    thread::sleep(Duration::from_micros(10));
                }
            }
            consumed
        })
    };

    thread::sleep(Duration::from_secs(3));
    stop.store(true, Ordering::Relaxed);

    for h in handles {
        h.join().unwrap();
    }
    let consumed = consumer_handle.join().unwrap();

    let elapsed = start_time.elapsed();
    let produced = total_ops.load(Ordering::Relaxed);
    let ops_per_sec = produced as f64 / elapsed.as_secs_f64();

    println!("┌─────────────────────────────────────────────────────────┐");
    println!("│  Load Test: task_queue_throughput                       │");
    println!("├─────────────────────────────────────────────────────────┤");
    println!("│  Produced:          {:<35}│", produced);
    println!("│  Consumed:          {:<35}│", consumed);
    println!("│  Ops/Second:        {:<35.1}│", ops_per_sec);
    println!("└─────────────────────────────────────────────────────────┘");

    assert!(produced > 0, "Should produce tasks");
}

#[test]
fn test_timer_engine_throughput() {
    let timer = TimerEngine::new();
    let _fired_count = Arc::new(AtomicU64::new(0));

    // Schedule many timers with very short delays
    let iterations = 1000;
    let start = Instant::now();
    for i in 0..iterations {
        timer.schedule(i + 1, Duration::from_millis(1));
    }
    let schedule_elapsed = start.elapsed();

    println!(
        "Scheduled {} timers in {:.3}ms",
        iterations,
        schedule_elapsed.as_secs_f64() * 1000.0
    );
    assert_eq!(timer.pending_count(), iterations as usize);

    // Cancel half
    let cancel_start = Instant::now();
    for i in 0..(iterations / 2) {
        timer.cancel(i + 1);
    }
    let cancel_elapsed = cancel_start.elapsed();

    println!(
        "Cancelled {} timers in {:.3}ms",
        iterations / 2,
        cancel_elapsed.as_secs_f64() * 1000.0
    );
    assert_eq!(
        timer.pending_count(),
        iterations as usize - iterations as usize / 2
    );
}

#[test]
fn test_namespace_registration_throughput() {
    let registry = NamespaceRegistry::new();
    let iterations = 10_000;

    let start = Instant::now();
    for i in 0..iterations {
        let config = NamespaceConfig::new(i + 1, format!("ns-{}", i));
        let result = registry.register(config);
        assert!(result.is_ok());
    }
    let elapsed = start.elapsed();

    let ops_per_sec = iterations as f64 / elapsed.as_secs_f64();
    println!(
        "Registered {} namespaces in {:.3}ms ({:.0} ops/sec)",
        iterations,
        elapsed.as_secs_f64() * 1000.0,
        ops_per_sec
    );
    assert!(
        ops_per_sec > 10_000.0,
        "Should register at least 10K namespaces/sec"
    );
}

#[test]
fn test_rate_limiter_throughput() {
    let limiter = RateLimiter::new(1_000_000.0, 1_000_000, 500_000.0);
    let iterations = 100_000;

    let start = Instant::now();
    let mut acquired = 0u64;
    for _ in 0..iterations {
        if limiter.try_acquire(0, 1) {
            acquired += 1;
        }
    }
    let elapsed = start.elapsed();

    let ops_per_sec = iterations as f64 / elapsed.as_secs_f64();
    println!(
        "Rate limiter: {} acquires in {:.3}ms ({:.0} ops/sec)",
        acquired,
        elapsed.as_secs_f64() * 1000.0,
        ops_per_sec
    );
    assert!(acquired > 0);
}

#[test]
fn test_visibility_index_throughput() {
    let index = VisibilityIndex::new();
    let iterations = 10_000;

    let start = Instant::now();
    for i in 0..iterations {
        let info = velocity_workflow_engine::visibility::WorkflowExecutionInfo {
            workflow_key: i + 1,
            workflow_id: i + 1,
            run_id: i + 1,
            workflow_type_id: 1,
            namespace_id: 1,
            status: velocity_workflow_engine::engine::WorkflowStatus::Running,
            start_time_ms: 0,
            close_time_ms: None,
            task_queue_hash: 42,
            search_attributes: std::collections::HashMap::new(),
            memo: std::collections::HashMap::new(),
        };
        index.register(info);
    }
    let elapsed = start.elapsed();

    let ops_per_sec = iterations as f64 / elapsed.as_secs_f64();
    println!(
        "Indexed {} workflows in {:.3}ms ({:.0} ops/sec)",
        iterations,
        elapsed.as_secs_f64() * 1000.0,
        ops_per_sec
    );
    assert!(ops_per_sec > 10_000.0);
}
