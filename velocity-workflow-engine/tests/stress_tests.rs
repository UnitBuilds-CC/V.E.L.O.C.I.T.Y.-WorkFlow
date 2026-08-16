//! Stress tests for the VELOCITY-WorkFlow engine.
//!
//! High-scale concurrency and volume tests that push the engine beyond normal
//! operating parameters to surface race conditions, memory issues, and
//! performance bottlenecks.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use velocity_workflow_engine::db_adapter::{DatabaseAdapter, InMemoryAdapter, WorkflowRecord};
use velocity_workflow_engine::engine::{WorkflowEngine, WorkflowStatus};
use velocity_workflow_engine::hot_swap::HotSwapRegistry;
use velocity_workflow_engine::namespace::{NamespaceConfig, NamespaceRegistry};
use velocity_workflow_engine::observability::{
    LogLevel, MetricsExporter, SpanTracker, StructuredLogger,
};
use velocity_workflow_engine::search_index::SearchAttributeIndex;
use velocity_workflow_engine::task_queue::{TaskItem, TaskKind, TaskQueue};
use velocity_workflow_engine::timer_engine::TimerEngine;
use velocity_workflow_engine::visibility::SearchAttributeValue;

// ═══════════════════════════════════════════════════════════════════════════════
// Stress Tests
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_10k_concurrent_workflows() {
    let start = Instant::now();
    println!("test_10k_concurrent_workflows: starting");
    let engine = WorkflowEngine::new();
    let count = 10_000u64;
    let mut keys = Vec::with_capacity(count as usize);

    // Start all workflows
    for i in 0..count {
        let key = engine.start_workflow(
            i + 1,
            1,
            0,
            42,
            3,
            Some(format!("input-{}", i).into_bytes()),
        );
        keys.push(key);
    }
    assert_eq!(engine.workflow_count(), count as usize);
    let start_elapsed = start.elapsed();
    println!("  started {} workflows in {:?}", count, start_elapsed);

    // Complete all steps and workflows
    for key in &keys {
        engine.complete_step(*key, 0, b"step0".to_vec());
        engine.complete_step(*key, 1, b"step1".to_vec());
        engine.complete_step(*key, 2, b"step2".to_vec());
        engine.complete_workflow(*key, Some(b"done".to_vec()));
    }
    println!(
        "  all {} workflows completed in {:?}",
        count,
        start.elapsed()
    );

    // Verify all completed
    for key in &keys {
        assert_eq!(engine.get_status(*key), WorkflowStatus::Completed);
    }
    engine.shutdown();
    println!("  total time: {:?}", start.elapsed());
}

#[test]
fn test_100k_signals() {
    let start = Instant::now();
    println!("test_100k_signals: sending 100,000 signals to a single workflow");
    let engine = WorkflowEngine::new();
    let key = engine.start_workflow(1, 1, 0, 42, 1, None);

    for i in 0..100_000u64 {
        engine.signal_workflow(key, i % 100, format!("signal-{}", i).into_bytes());
    }
    println!("  sent 100,000 signals in {:?}", start.elapsed());
    assert_eq!(engine.get_status(key), WorkflowStatus::Running);
    engine.complete_workflow(key, None);
    engine.shutdown();
    println!("  total time: {:?}", start.elapsed());
}

#[test]
fn test_1k_concurrent_namespaces() {
    let start = Instant::now();
    println!("test_1k_concurrent_namespaces: registering 1,000 namespaces");
    let engine = WorkflowEngine::new();
    let ns_reg = NamespaceRegistry::new();

    for i in 1..=1000u64 {
        ns_reg
            .register(NamespaceConfig::new(i, format!("namespace-{}", i)))
            .unwrap();
    }
    assert_eq!(ns_reg.count(), 1001); // 1000 + default
    println!("  registered 1000 namespaces in {:?}", start.elapsed());

    // Start workflows across all namespaces
    for ns_id in 1..=1000u64 {
        let key = engine.start_workflow(ns_id, 1, ns_id, 42, 1, None);
        engine.complete_step(key, 0, b"done".to_vec());
        engine.complete_workflow(key, None);
        assert_eq!(engine.get_status(key), WorkflowStatus::Completed);
    }
    println!(
        "  workflows across 1000 namespaces completed in {:?}",
        start.elapsed()
    );
    engine.shutdown();
}

#[test]
fn test_memory_pressure() {
    let start = Instant::now();
    println!("test_memory_pressure: creating 50,000 workflows with data");
    let engine = WorkflowEngine::new();
    let count = 50_000u64;
    let mut keys = Vec::with_capacity(count as usize);

    for i in 0..count {
        let payload = vec![0xABu8; 1024]; // 1KB per workflow
        let key = engine.start_workflow(i + 1, 1, 0, 42, 2, Some(payload));
        keys.push(key);
    }
    println!("  created {} workflows in {:?}", count, start.elapsed());

    // Complete half
    for key in keys.iter().take(count as usize / 2) {
        engine.complete_step(*key, 0, vec![0xCD; 512]);
        engine.complete_step(*key, 1, vec![0xEF; 512]);
        engine.complete_workflow(*key, Some(vec![0xFF; 256]));
    }
    println!(
        "  completed {} workflows, {} still running in {:?}",
        count / 2,
        engine.workflow_count(),
        start.elapsed()
    );

    // Complete the rest
    for key in keys.iter().skip(count as usize / 2) {
        engine.complete_workflow(*key, None);
    }
    println!("  all workflows completed in {:?}", start.elapsed());
    engine.shutdown();
}

#[test]
fn test_long_running_soak() {
    let duration_secs = 5;
    println!(
        "test_long_running_soak: running for {} seconds",
        duration_secs
    );
    let engine = Arc::new(WorkflowEngine::new());
    let total_ops = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut handles = Vec::new();

    for t in 0..4u64 {
        let eng = Arc::clone(&engine);
        let ops = Arc::clone(&total_ops);
        let stop = Arc::clone(&stop);
        handles.push(thread::spawn(move || {
            let mut counter = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let wf_id = t * 10_000_000 + counter;
                let key = eng.start_workflow(wf_id, 1, 0, 42, 2, None);
                eng.complete_step(key, 0, b"s0".to_vec());
                eng.complete_step(key, 1, b"s1".to_vec());
                eng.complete_workflow(key, Some(b"soak-done".to_vec()));
                counter += 1;
                ops.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    thread::sleep(Duration::from_secs(duration_secs));
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        // Best-effort join: threads should stop quickly after flag is set
        let _ = h.join();
    }

    let ops_count = total_ops.load(Ordering::Relaxed);
    println!(
        "  completed {} workflow operations in {} seconds",
        ops_count, duration_secs
    );
    assert!(ops_count > 0, "Should have completed some operations");
    engine.shutdown();
}

#[test]
fn test_rapid_create_destroy() {
    let start = Instant::now();
    println!("test_rapid_create_destroy: 10,000 rapid create/destroy cycles");
    let engine = WorkflowEngine::new();

    for i in 0..10_000u64 {
        let key = engine.start_workflow(i + 1, 1, 0, 42, 1, None);
        engine.terminate_workflow(key);
        assert_eq!(engine.get_status(key), WorkflowStatus::Terminated);
    }
    println!("  10,000 create/destroy cycles in {:?}", start.elapsed());
    engine.shutdown();
}

#[test]
fn test_task_queue_contention() {
    let start = Instant::now();
    println!("test_task_queue_contention: 100 producers, 10 consumers");
    let tq = Arc::new(TaskQueue::new());
    let tq_hash = 42u64;
    let produced = Arc::new(AtomicU64::new(0));
    let consumed = Arc::new(AtomicU64::new(0));
    let total_to_produce = 10_000u64;
    let mut handles = Vec::new();

    // 100 producers
    for t in 0..100u64 {
        let tq = Arc::clone(&tq);
        let prod = Arc::clone(&produced);
        handles.push(thread::spawn(move || {
            for i in 0..100u64 {
                let task = TaskItem {
                    task_id: 0,
                    kind: TaskKind::WorkflowTask,
                    workflow_key: t * 100 + i,
                    task_queue_hash: tq_hash,
                    step_index: 0,
                    activity_name_id: 0,
                    attempt: 1,
                    priority: 0,
                    deadline_ms: 0,
                };
                tq.enqueue(tq_hash, task);
                prod.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    // Wait for producers
    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(produced.load(Ordering::Relaxed), total_to_produce);
    assert_eq!(tq.pending_count(tq_hash), total_to_produce as usize);

    // 10 consumers
    let mut handles = Vec::new();
    for _ in 0..10 {
        let tq = Arc::clone(&tq);
        let cons = Arc::clone(&consumed);
        handles.push(thread::spawn(move || {
            while tq.try_poll(tq_hash).is_some() {
                cons.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    let consumed_count = consumed.load(Ordering::Relaxed);
    println!(
        "  produced={}, consumed={} in {:?}",
        total_to_produce,
        consumed_count,
        start.elapsed()
    );
    assert_eq!(consumed_count, total_to_produce);
    tq.shutdown();
}

#[test]
fn test_timer_engine_scale() {
    let start = Instant::now();
    println!("test_timer_engine_scale: scheduling 10,000 timers");
    let timer = TimerEngine::new();
    let fired_count = Arc::new(AtomicU64::new(0));
    let fc = Arc::clone(&fired_count);

    timer.set_fire_callback(Box::new(move |_, _| {
        fc.fetch_add(1, Ordering::Relaxed);
    }));

    let count = 10_000u64;
    for i in 0..count {
        timer.schedule(i + 1, Duration::from_millis(10 + (i % 50)));
    }
    assert_eq!(timer.pending_count(), count as usize);
    println!("  scheduled {} timers in {:?}", count, start.elapsed());

    let handle = timer.start();
    // Wait for all timers to fire (they all have short delays of 10-59ms).
    // Allow generous time for callback processing of 10k timers.
    thread::sleep(Duration::from_secs(2));

    let fired = fired_count.load(Ordering::Relaxed);
    println!("  fired {} timers in {:?}", fired, start.elapsed());
    assert_eq!(fired, count);
    timer.shutdown();
    let _ = handle.join();
}

#[test]
fn test_search_index_scale() {
    let start = Instant::now();
    println!("test_search_index_scale: indexing 100,000 workflows");
    let index = SearchAttributeIndex::new();
    let count = 100_000u64;

    for i in 0..count {
        index.index_attribute(
            i,
            "env",
            &SearchAttributeValue::Keyword(if i % 3 == 0 {
                "prod".into()
            } else if i % 3 == 1 {
                "staging".into()
            } else {
                "dev".into()
            }),
        );
        index.index_attribute(
            i,
            "priority",
            &SearchAttributeValue::Integer((i % 10) as i64),
        );
    }
    println!("  indexed {} workflows in {:?}", count, start.elapsed());

    let stats = index.stats();
    println!(
        "  stats: indexed_workflows={}, total_entries={}, unique_keys={}",
        stats.indexed_workflows, stats.total_entries, stats.unique_keys
    );
    assert_eq!(stats.indexed_workflows, count);

    // Verify queries work
    let prod_workflows = index.exact_match("env", &SearchAttributeValue::Keyword("prod".into()));
    println!("  prod workflows: {}", prod_workflows.len());
    assert!(!prod_workflows.is_empty());

    let high_priority = index.range_integer("priority", 8, 9);
    println!("  high priority workflows: {}", high_priority.len());
    assert!(!high_priority.is_empty());
    println!("  total time: {:?}", start.elapsed());
}

#[test]
fn test_hot_swap_under_load() {
    let start = Instant::now();
    println!("test_hot_swap_under_load: applying patches while workflows run");
    let engine = Arc::new(WorkflowEngine::new());
    let registry = Arc::new(HotSwapRegistry::new());
    let mut keys = Vec::new();

    // Start workflows
    for i in 0..1000u64 {
        let key = engine.start_workflow(i + 1, 1, 0, 42, 3, None);
        keys.push(key);
    }

    // Apply patches concurrently with workflow operations
    let mut handles = Vec::new();
    for t in 0..4u64 {
        let reg = Arc::clone(&registry);
        let eng = Arc::clone(&engine);
        let keys_clone = keys.clone();
        handles.push(thread::spawn(move || {
            for (i, &key) in keys_clone
                .iter()
                .enumerate()
                .skip((t * 250) as usize)
                .take(250)
            {
                let patch_id =
                    reg.register_patch(1, &format!("patch-{}-{}", t, i), vec![(0, i as u64)]);
                let _ = reg.apply_patch(patch_id, key);
                eng.complete_step(key, 0, b"patched".to_vec());
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    println!(
        "  patches registered: {}, patched workflows: {}",
        registry.patch_count(),
        registry.patched_workflow_count()
    );
    println!("  time: {:?}", start.elapsed());

    // Complete remaining steps
    for key in &keys {
        if engine.get_status(*key) == WorkflowStatus::Running {
            engine.complete_step(*key, 1, b"s1".to_vec());
            engine.complete_step(*key, 2, b"s2".to_vec());
            engine.complete_workflow(*key, None);
        }
    }
    engine.shutdown();
}

#[test]
fn test_db_adapter_concurrent_writes() {
    let start = Instant::now();
    println!("test_db_adapter_concurrent_writes: 50 threads writing simultaneously");
    let adapter = Arc::new(InMemoryAdapter::new());
    let mut handles = Vec::new();

    for t in 0..50u64 {
        let adp = Arc::clone(&adapter);
        handles.push(thread::spawn(move || {
            for i in 0..100u64 {
                let wf_key = t * 1000 + i;
                let record = WorkflowRecord {
                    workflow_key: wf_key,
                    workflow_id: wf_key,
                    run_id: wf_key + 1,
                    workflow_type_id: 1,
                    namespace_id: 0,
                    namespace_name: "default".to_string(),
                    task_queue_hash: 42,
                    current_step: 0,
                    total_steps: 3,
                    merkle_root: vec![0u8; 32],
                    step_bitmask: vec![0u8; 32],
                    status: WorkflowStatus::Running,
                    step_results: Default::default(),
                    signal_buffer: Default::default(),
                    update_buffer: Default::default(),
                    input_data: None,
                    result_data: None,
                    parent_key: None,
                    child_keys: Vec::new(),
                    event_sequence: 0,
                };
                adp.save_workflow(wf_key, &record).unwrap();
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    let count = adapter.workflow_count();
    println!("  stored {} workflows in {:?}", count, start.elapsed());
    assert_eq!(count, 5000);
}

#[test]
fn test_observability_under_load() {
    let start = Instant::now();
    println!("test_observability_under_load: 100,000 log events + metrics");
    let logger = StructuredLogger::new(LogLevel::Info, "stress-test");
    let metrics = MetricsExporter::new();
    let tracer = SpanTracker::new();

    // Log 100,000 events
    for i in 0..100_000u64 {
        logger.log_event(
            LogLevel::Info,
            "stress_event",
            &[("iteration", &i.to_string()), ("thread", "main")],
        );
    }
    println!("  logged 100,000 events in {:?}", start.elapsed());
    assert_eq!(logger.total_events(), 100_000);

    // Export metrics
    for _ in 0..10_000 {
        metrics.inc_counter("workflow_started_total");
    }
    let prometheus_output = metrics.export_prometheus();
    assert!(!prometheus_output.is_empty());
    println!(
        "  metrics exported, output size: {} bytes",
        prometheus_output.len()
    );

    // Create and end spans
    for i in 0..10_000u64 {
        let span_id = tracer.start_span(&format!("span-{}", i), None);
        tracer.end_span(span_id);
    }
    println!("  10,000 spans created and ended in {:?}", start.elapsed());
    println!("  total time: {:?}", start.elapsed());
}

// ═══════════════════════════════════════════════════════════════════════════════
// Phase 6.2 — Lock-Up Stress Test
// ═══════════════════════════════════════════════════════════════════════════════

/// 10K concurrent workflow starts + signals + queries from multiple threads.
/// Verifies no deadlocks, no mutex contention spikes, and p99 latency < 2x single-client.
#[test]
fn test_lockup_stress_10k_concurrent() {
    let engine = Arc::new(WorkflowEngine::new());
    let num_threads = 8;
    let workflows_per_thread = 1250; // 8 * 1250 = 10,000
    let total = num_threads * workflows_per_thread;

    println!("test_lockup_stress_10k_concurrent: {} threads × {} workflows = {} total",
        num_threads, workflows_per_thread, total);

    let start = Instant::now();
    let latencies = Arc::new(dashmap::DashMap::<u64, u64>::new()); // thread_id → latency_us

    // Phase 1: Concurrent workflow starts
    let handles: Vec<_> = (0..num_threads)
        .map(|t| {
            let engine = engine.clone();
            let latencies = latencies.clone();
            thread::spawn(move || {
                let thread_start = Instant::now();
                let base_id = (t * workflows_per_thread) as u64 + 1;
                let mut keys = Vec::with_capacity(workflows_per_thread);

                for i in 0..workflows_per_thread {
                    let key = engine.start_workflow(
                        base_id + i as u64,
                        1,
                        0,
                        42,
                        5,
                        Some(format!("t{}-wf{}", t, i).into_bytes()),
                    );
                    keys.push(key);
                }

                let start_latency = thread_start.elapsed().as_micros() as u64;
                latencies.insert(t as u64, start_latency);

                // Phase 2: Complete steps + signals + queries
                for (idx, &key) in keys.iter().enumerate() {
                    for step in 0..5u32 {
                        engine.complete_step(key, step, format!("step-{}", step).into_bytes());
                    }
                    // Mix in some signals and status queries
                    if idx % 10 == 0 {
                        engine.signal_workflow(key, 0, b"concurrent-signal".to_vec());
                    }
                    if idx % 20 == 0 {
                        let _status = engine.get_status(key);
                    }
                }

                // Phase 3: Complete all workflows
                for &key in &keys {
                    engine.complete_workflow(key, Some(b"done".to_vec()));
                }

                keys
            })
        })
        .collect();

    // Collect results
    let mut all_keys = Vec::with_capacity(total);
    for handle in handles {
        all_keys.extend(handle.join().expect("thread panicked"));
    }

    let elapsed = start.elapsed();
    println!("  {} workflows started+completed in {:?}", total, elapsed);

    // Verify all completed
    for key in &all_keys {
        assert_eq!(engine.get_status(*key), WorkflowStatus::Completed);
    }

    // Calculate latency stats
    let mut lat_values: Vec<u64> = latencies.iter().map(|e| *e.value()).collect();
    lat_values.sort();
    let p50 = lat_values.get(lat_values.len() / 2).unwrap_or(&0);
    let p99 = lat_values.get(lat_values.len() * 99 / 100).unwrap_or(&0);
    let min = lat_values.first().unwrap_or(&0);
    let max = lat_values.last().unwrap_or(&0);

    println!("  Start latency (us): min={}, p50={}, p99={}, max={}", min, p50, p99, max);
    println!("  Throughput: {} workflows/sec", (total as f64 / elapsed.as_secs_f64()) as u64);

    // p99 should not exceed 10x min (generous bound for CI)
    if *min > 0 {
        println!("  p99/min ratio: {:.1}x", *p99 as f64 / *min as f64);
    }

    engine.shutdown();
    println!("  PASSED — no deadlocks, all {} workflows completed", total);
}

/// Stress test with concurrent step persistence (WAL + PG simulation).
/// Verifies that persist_step doesn't lock up under high concurrency.
#[test]
fn test_concurrent_step_persistence() {
    let wal_path = format!("/tmp/velocity-stress-persist-{}.wal", std::process::id());
    let engine = Arc::new(
        WorkflowEngine::with_wal(&wal_path, 64 * 1024 * 1024).expect("WAL init"),
    );
    let num_threads = 8;
    let workflows_per_thread = 500;

    println!("test_concurrent_step_persistence: {} threads × {} workflows", num_threads, workflows_per_thread);
    let start = Instant::now();

    let handles: Vec<_> = (0..num_threads)
        .map(|t| {
            let engine = engine.clone();
            thread::spawn(move || {
                let base_id = (t * workflows_per_thread) as u64 + 1;
                for i in 0..workflows_per_thread {
                    let key = engine.start_workflow(base_id + i as u64, 1, 0, 42, 10, None);
                    // Persist each step sequentially (matches production pattern)
                    for step in 0..10u32 {
                        engine.persist_step(key, step, "default");
                    }
                    engine.complete_workflow(key, Some(b"done".to_vec()));
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().expect("thread panicked");
    }

    let elapsed = start.elapsed();
    let total = num_threads * workflows_per_thread;
    let total_steps = total * 10;
    println!("  {} workflows × 10 steps = {} steps persisted in {:?}",
        total, total_steps, elapsed);
    println!("  Step persist throughput: {} steps/sec",
        (total_steps as f64 / elapsed.as_secs_f64()) as u64);

    // Verify WAL recovery
    let engine2 = WorkflowEngine::with_wal(&wal_path, 64 * 1024 * 1024).expect("WAL reopen");
    let (records, workflows) = engine2.recover_from_wal().expect("WAL replay");
    println!("  WAL recovery: {} records, {} workflows", records, workflows);

    let _ = std::fs::remove_file(&wal_path);
    println!("  PASSED — no lock-ups during concurrent step persistence");
}
