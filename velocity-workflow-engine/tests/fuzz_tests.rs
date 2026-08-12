//! Fuzz testing framework for the VELOCITY-WorkFlow engine.
//!
//! Property-based testing using seeded random generation for reproducible runs.
//! Each test prints its seed so failures can be reproduced exactly.

use std::panic::catch_unwind;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use velocity_workflow_engine::cron::CronExpression;
use velocity_workflow_engine::db_adapter::{DatabaseAdapter, InMemoryAdapter};
use velocity_workflow_engine::engine::{WorkflowEngine, WorkflowStatus};
use velocity_workflow_engine::hardware_integration::compute_simple_merkle_root;
use velocity_workflow_engine::hot_swap::HotSwapRegistry;
use velocity_workflow_engine::namespace::{NamespaceConfig, NamespaceRegistry};
use velocity_workflow_engine::observability::{
    LogLevel, MetricsExporter, SpanTracker, StructuredLogger,
};
use velocity_workflow_engine::rate_limiter::RateLimiter;
use velocity_workflow_engine::retry::{CircuitBreaker, CircuitBreakerConfig, RetryPolicy};
use velocity_workflow_engine::search_index::SearchAttributeIndex;
use velocity_workflow_engine::timer_engine::TimerEngine;
use velocity_workflow_engine::visibility::SearchAttributeValue;

// ═══════════════════════════════════════════════════════════════════════════════
// Fuzz Infrastructure
// ═══════════════════════════════════════════════════════════════════════════════

/// Configuration for a fuzz run.
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct FuzzConfig {
    seed: u64,
    iteration_count: usize,
    max_workflows: u64,
    max_steps: u32,
    max_signals: u64,
}

impl FuzzConfig {
    fn quick() -> Self {
        Self {
            seed: 42,
            iteration_count: 200,
            max_workflows: 50,
            max_steps: 20,
            max_signals: 50,
        }
    }
    fn standard() -> Self {
        Self {
            seed: 12345,
            iteration_count: 500,
            max_workflows: 100,
            max_steps: 50,
            max_signals: 100,
        }
    }
}

/// Seeded pseudo-random number generator (xoshiro128**).
struct RandomGenerator {
    state: [u32; 4],
}

impl RandomGenerator {
    fn new(seed: u64) -> Self {
        let s = seed.wrapping_mul(0x9E3779B97F4A7C15);
        let mut gen = Self {
            state: [s as u32, (s >> 32) as u32, 0x12345678, 0x9ABCDEF0],
        };
        // Warm up
        for _ in 0..20 {
            gen.next_u32();
        }
        gen
    }

    fn next_u32(&mut self) -> u32 {
        let result = self.state[1].wrapping_mul(5).rotate_left(7).wrapping_mul(9);
        let t = self.state[1] << 9;
        self.state[2] ^= self.state[0];
        self.state[3] ^= self.state[1];
        self.state[1] ^= self.state[2];
        self.state[0] ^= self.state[3];
        self.state[2] ^= t;
        self.state[3] = self.state[3].rotate_left(11);
        result
    }

    fn next_u64(&mut self) -> u64 {
        let lo = self.next_u32() as u64;
        let hi = self.next_u32() as u64;
        (hi << 32) | lo
    }

    fn range(&mut self, lo: u64, hi: u64) -> u64 {
        if lo >= hi {
            return lo;
        }
        lo + (self.next_u64() % (hi - lo + 1))
    }

    fn range_u32(&mut self, lo: u32, hi: u32) -> u32 {
        if lo >= hi {
            return lo;
        }
        lo + (self.next_u32() % (hi - lo + 1))
    }

    fn random_workflow_id(&mut self) -> u64 {
        self.range(1, 1_000_000)
    }
    fn random_step_count(&mut self) -> u32 {
        self.range_u32(1, 50)
    }
    fn random_signal_id(&mut self) -> u64 {
        self.range(1, 10_000)
    }
    #[allow(dead_code)]
    fn random_namespace_id(&mut self) -> u64 {
        self.range(0, 10)
    }
    fn random_payload(&mut self, max_len: usize) -> Vec<u8> {
        let len = self.range(0, max_len as u64) as usize;
        (0..len).map(|_| self.next_u32() as u8).collect()
    }
}

/// Helper: run a closure with catch_unwind, return true if it didn't panic.
fn no_panic<F: FnOnce() + std::panic::UnwindSafe>(f: F) -> bool {
    catch_unwind(f).is_ok()
}

// ═══════════════════════════════════════════════════════════════════════════════
// Fuzz Tests
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn fuzz_workflow_lifecycle() {
    let cfg = FuzzConfig::quick();
    println!(
        "fuzz_workflow_lifecycle: seed={}, iterations={}",
        cfg.seed, cfg.iteration_count
    );
    let mut rng = RandomGenerator::new(cfg.seed);
    let engine = WorkflowEngine::new();
    let mut active_keys: Vec<u64> = Vec::new();
    let mut completed = 0u64;
    let mut panicked = 0u64;

    for _ in 0..cfg.iteration_count {
        let op = rng.range(0, 4);
        let ok = no_panic(std::panic::AssertUnwindSafe(|| {
            match op {
                0 => {
                    // start
                    let wf_id = rng.random_workflow_id();
                    let steps = rng.random_step_count();
                    let key =
                        engine.start_workflow(wf_id, 1, 0, 42, steps, Some(rng.random_payload(64)));
                    active_keys.push(key);
                }
                1 if !active_keys.is_empty() => {
                    // complete step
                    let idx = rng.range(0, active_keys.len() as u64 - 1) as usize;
                    let key = active_keys[idx];
                    let step = rng.range_u32(0, engine.get_total_steps(key).saturating_sub(1));
                    engine.complete_step(key, step, rng.random_payload(128));
                }
                2 if !active_keys.is_empty() => {
                    // signal
                    let idx = rng.range(0, active_keys.len() as u64 - 1) as usize;
                    let key = active_keys[idx];
                    engine.signal_workflow(key, rng.random_signal_id(), rng.random_payload(64));
                }
                3 if !active_keys.is_empty() => {
                    // complete workflow
                    let idx = rng.range(0, active_keys.len() as u64 - 1) as usize;
                    let key = active_keys.remove(idx);
                    engine.complete_workflow(key, Some(b"fuzz-done".to_vec()));
                    completed += 1;
                }
                _ => {} // query or no-op on empty
            }
        }));
        if !ok {
            panicked += 1;
        }
    }
    // Cleanup remaining
    for key in &active_keys {
        engine.complete_workflow(*key, None);
    }
    println!(
        "  completed={}, panicked={}, remaining={}",
        completed,
        panicked,
        active_keys.len()
    );
    assert_eq!(panicked, 0, "Fuzz run had panics");
    engine.shutdown();
}

#[test]
fn fuzz_concurrent_operations() {
    let cfg = FuzzConfig::quick();
    println!("fuzz_concurrent_operations: seed={}", cfg.seed);
    let engine = Arc::new(WorkflowEngine::new());
    let mut handles = Vec::new();

    for t in 0..4u64 {
        let eng = Arc::clone(&engine);
        let seed = cfg.seed + t * 1000;
        handles.push(thread::spawn(move || {
            let mut rng = RandomGenerator::new(seed);
            for _ in 0..100 {
                let wf_id = rng.random_workflow_id() + t * 1_000_000;
                let key = eng.start_workflow(wf_id, 1, 0, 42, 3, None);
                eng.complete_step(key, 0, b"s0".to_vec());
                eng.signal_workflow(key, rng.random_signal_id(), b"sig".to_vec());
                eng.complete_step(key, 1, b"s1".to_vec());
                eng.complete_step(key, 2, b"s2".to_vec());
                eng.complete_workflow(key, Some(b"done".to_vec()));
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    assert!(engine.workflow_count() >= 400);
    println!("  total workflows: {}", engine.workflow_count());
    engine.shutdown();
}

#[test]
fn fuzz_signal_storm() {
    let cfg = FuzzConfig::quick();
    println!("fuzz_signal_storm: seed={}", cfg.seed);
    let engine = WorkflowEngine::new();
    let key = engine.start_workflow(1, 1, 0, 42, 1, None);
    let mut rng = RandomGenerator::new(cfg.seed);
    let mut count = 0u64;

    for _ in 0..1000 {
        let sig_id = rng.random_signal_id();
        let payload = rng.random_payload(256);
        engine.signal_workflow(key, sig_id, payload);
        count += 1;
    }
    println!("  sent {} signals to single workflow", count);
    assert_eq!(engine.get_status(key), WorkflowStatus::Running);
    engine.complete_workflow(key, None);
    engine.shutdown();
}

#[test]
fn fuzz_step_ordering() {
    let cfg = FuzzConfig::quick();
    println!("fuzz_step_ordering: seed={}", cfg.seed);
    let mut rng = RandomGenerator::new(cfg.seed);
    let mut no_panic_count = 0u64;

    for _ in 0..cfg.iteration_count {
        let engine = WorkflowEngine::new();
        let steps = rng.range_u32(2, 10);
        let key = engine.start_workflow(rng.random_workflow_id(), 1, 0, 42, steps, None);

        // Complete steps in random order
        let mut order: Vec<u32> = (0..steps).collect();
        for i in (1..order.len()).rev() {
            let j = rng.range(0, i as u64) as usize;
            order.swap(i, j);
        }
        for step in &order {
            if no_panic(std::panic::AssertUnwindSafe(|| {
                engine.complete_step(key, *step, format!("r{}", step).into_bytes());
            })) {
                no_panic_count += 1;
            }
        }
        engine.complete_workflow(key, None);
        engine.shutdown();
    }
    println!("  step completions without panic: {}", no_panic_count);
}

#[test]
fn fuzz_namespace_isolation() {
    let cfg = FuzzConfig::quick();
    println!("fuzz_namespace_isolation: seed={}", cfg.seed);
    let engine = WorkflowEngine::new();
    let ns_reg = NamespaceRegistry::new();
    let mut rng = RandomGenerator::new(cfg.seed);

    for ns_id in 1..=5u64 {
        ns_reg
            .register(NamespaceConfig::new(ns_id, format!("ns-{}", ns_id)))
            .unwrap();
    }

    for _ in 0..cfg.iteration_count {
        let ns = rng.range(0, 5);
        let wf_id = rng.random_workflow_id();
        let key = engine.start_workflow(wf_id, 1, ns, 42, 2, None);
        engine.complete_step(key, 0, b"s0".to_vec());
        engine.complete_step(key, 1, b"s1".to_vec());
        engine.complete_workflow(key, Some(b"done".to_vec()));
        assert_eq!(engine.get_status(key), WorkflowStatus::Completed);
    }
    println!("  namespace count: {}", ns_reg.count());
    engine.shutdown();
}

#[test]
fn fuzz_timer_cancellation() {
    let cfg = FuzzConfig::quick();
    println!("fuzz_timer_cancellation: seed={}", cfg.seed);
    let timer = TimerEngine::new();
    let mut rng = RandomGenerator::new(cfg.seed);
    let mut scheduled = Vec::new();
    let mut cancelled = 0u64;

    for _ in 0..cfg.iteration_count {
        let op = rng.range(0, 1);
        if op == 0 {
            let delay_ms = rng.range(100, 60_000);
            let tid = timer.schedule(rng.random_workflow_id(), Duration::from_millis(delay_ms));
            scheduled.push(tid);
        } else if !scheduled.is_empty() {
            let idx = rng.range(0, scheduled.len() as u64 - 1) as usize;
            let tid = scheduled.remove(idx);
            timer.cancel(tid);
            cancelled += 1;
        }
    }
    println!(
        "  pending={}, cancelled={}",
        timer.pending_count(),
        cancelled
    );
    timer.shutdown();
}

#[test]
fn fuzz_search_index_mutations() {
    let cfg = FuzzConfig::quick();
    println!("fuzz_search_index_mutations: seed={}", cfg.seed);
    let index = SearchAttributeIndex::new();
    let mut rng = RandomGenerator::new(cfg.seed);
    let mut indexed_keys: Vec<u64> = Vec::new();

    for _ in 0..cfg.iteration_count {
        let op = rng.range(0, 2);
        match op {
            0 => {
                // index
                let wf_key = rng.random_workflow_id();
                let attr_val = SearchAttributeValue::Integer(rng.range(0, 1000) as i64);
                index.index_attribute(wf_key, "fuzz_attr", &attr_val);
                indexed_keys.push(wf_key);
            }
            1 => {
                // query
                let val = rng.range(0, 1000) as i64;
                let results = index.exact_match("fuzz_attr", &SearchAttributeValue::Integer(val));
                // Just verify no panic
                let _ = results.len();
            }
            2 if !indexed_keys.is_empty() => {
                // delete
                let idx = rng.range(0, indexed_keys.len() as u64 - 1) as usize;
                let key = indexed_keys.remove(idx);
                index.remove_workflow(key);
            }
            _ => {}
        }
    }
    let stats = index.stats();
    println!(
        "  indexed_workflows={}, total_entries={}",
        stats.indexed_workflows, stats.total_entries
    );
}

#[test]
fn fuzz_hot_swap_races() {
    let cfg = FuzzConfig::quick();
    println!("fuzz_hot_swap_races: seed={}", cfg.seed);
    let registry = Arc::new(HotSwapRegistry::new());
    let mut handles = Vec::new();

    for t in 0..4u64 {
        let reg = Arc::clone(&registry);
        let seed = cfg.seed + t;
        handles.push(thread::spawn(move || {
            let mut rng = RandomGenerator::new(seed);
            for _ in 0..50 {
                let patch_id = reg.register_patch(
                    rng.range(1, 5),
                    &format!("patch-{}", t),
                    vec![(0, rng.next_u64())],
                );
                let wf_key = rng.random_workflow_id();
                let _ = reg.apply_patch(patch_id, wf_key);
                let _ = reg.rollback(wf_key);
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    println!("  patches registered: {}", registry.patch_count());
}

#[test]
fn fuzz_db_adapter_stress() {
    let cfg = FuzzConfig::quick();
    println!("fuzz_db_adapter_stress: seed={}", cfg.seed);
    let adapter = Arc::new(InMemoryAdapter::new());
    let mut handles = Vec::new();

    for t in 0..4u64 {
        let adp = Arc::clone(&adapter);
        let seed = cfg.seed + t * 100;
        handles.push(thread::spawn(move || {
            let mut rng = RandomGenerator::new(seed);
            for i in 0..50u64 {
                let wf_key = t * 1000 + i;
                let record = velocity_workflow_engine::db_adapter::WorkflowRecord {
                    workflow_key: wf_key,
                    workflow_id: rng.random_workflow_id(),
                    run_id: rng.next_u64(),
                    workflow_type_id: rng.range(1, 10),
                    namespace_id: rng.range(0, 3),
                    namespace_name: format!("ns-{}", rng.range(0, 3)),
                    task_queue_hash: 42,
                    current_step: 0,
                    total_steps: 3,
                    merkle_root: vec![0u8; 32],
                    step_bitmask: vec![0u8; 32],
                    status: velocity_workflow_engine::engine::WorkflowStatus::Running,
                    step_results: Default::default(),
                    signal_buffer: Default::default(),
                    update_buffer: Default::default(),
                    input_data: None,
                    result_data: None,
                    parent_key: None,
                    child_keys: Vec::new(),
                    event_sequence: 0,
                };
                let _ = adp.save_workflow(wf_key, &record);
                let _ = adp.load_workflow(wf_key);
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    println!("  stored workflows: {}", adapter.workflow_count());
}

#[test]
fn fuzz_payload_sizes() {
    println!("fuzz_payload_sizes: testing 0 to 1MB payloads");
    let engine = WorkflowEngine::new();
    let sizes = [0, 1, 64, 1024, 65536, 1_048_576];
    for (i, &sz) in sizes.iter().enumerate() {
        let payload = vec![0xABu8; sz];
        let key = engine.start_workflow(i as u64 + 1, 1, 0, 42, 1, Some(payload.clone()));
        engine.complete_step(key, 0, payload);
        engine.complete_workflow(key, Some(vec![0xFF; sz]));
        assert_eq!(engine.get_status(key), WorkflowStatus::Completed);
    }
    println!("  all payload sizes handled successfully");
    engine.shutdown();
}

#[test]
fn fuzz_workflow_depth() {
    println!("fuzz_workflow_depth: testing 1 to 5000 step workflows");
    let engine = WorkflowEngine::new();
    let depths = [1, 10, 100, 500, 1000, 5000];
    for (i, &depth) in depths.iter().enumerate() {
        let key = engine.start_workflow(i as u64 + 100, 1, 0, 42, depth, None);
        for step in 0..depth {
            engine.complete_step(key, step, format!("step-{}", step).into_bytes());
        }
        engine.complete_workflow(key, Some(b"deep-done".to_vec()));
        assert_eq!(engine.get_status(key), WorkflowStatus::Completed);
    }
    println!("  all depths completed successfully");
    engine.shutdown();
}

#[test]
fn fuzz_workflow_breadth() {
    println!("fuzz_workflow_breadth: 5000 concurrent workflows");
    let engine = WorkflowEngine::new();
    let count = 5000u64;
    let mut keys = Vec::with_capacity(count as usize);
    for i in 0..count {
        let key = engine.start_workflow(i + 1, 1, 0, 42, 1, None);
        keys.push(key);
    }
    assert_eq!(engine.workflow_count(), count as usize);
    for key in &keys {
        engine.complete_step(*key, 0, b"done".to_vec());
        engine.complete_workflow(*key, None);
    }
    println!("  {} workflows created and completed", count);
    engine.shutdown();
}

#[test]
fn fuzz_child_workflow_trees() {
    let cfg = FuzzConfig::quick();
    println!("fuzz_child_workflow_trees: seed={}", cfg.seed);
    let engine = WorkflowEngine::new();
    let mut rng = RandomGenerator::new(cfg.seed);

    for _ in 0..50 {
        let root = engine.start_workflow(rng.random_workflow_id(), 1, 0, 42, 2, None);
        let depth = rng.range_u32(1, 4);
        let mut parents = vec![root];
        for _ in 0..depth {
            let mut children = Vec::new();
            for &p in &parents {
                let child_count = rng.range(0, 2);
                for _ in 0..child_count {
                    let child =
                        engine.start_child_workflow(p, rng.random_workflow_id(), 2, 42, 1, None);
                    children.push(child);
                }
            }
            parents = children;
        }
        // Complete all in reverse
        engine.complete_workflow(root, Some(b"root-done".to_vec()));
    }
    println!("  workflow count: {}", engine.workflow_count());
    engine.shutdown();
}

#[test]
fn fuzz_cron_expression_parsing() {
    let cfg = FuzzConfig::standard();
    println!(
        "fuzz_cron_expression_parsing: {} iterations",
        cfg.iteration_count
    );
    let mut rng = RandomGenerator::new(cfg.seed);
    let mut valid = 0u64;
    let mut invalid = 0u64;

    let chars = b"*0123456789/-, ";
    for _ in 0..cfg.iteration_count {
        let len = rng.range(5, 30) as usize;
        let expr: String = (0..len)
            .map(|_| chars[rng.range(0, chars.len() as u64 - 1) as usize] as char)
            .collect();
        if no_panic(std::panic::AssertUnwindSafe(|| {
            let _ = CronExpression::parse(&expr);
        })) {
            match CronExpression::parse(&expr) {
                Ok(_) => valid += 1,
                Err(_) => invalid += 1,
            }
        } else {
            panic!("CronExpression::parse panicked on: {:?}", expr);
        }
    }
    println!("  valid={}, invalid={}, no panics", valid, invalid);
}

#[test]
fn fuzz_retry_policy() {
    let cfg = FuzzConfig::standard();
    println!("fuzz_retry_policy: {} iterations", cfg.iteration_count);
    let mut rng = RandomGenerator::new(cfg.seed);

    for _ in 0..cfg.iteration_count {
        let max_attempts = rng.range_u32(1, 20);
        let initial_ms = rng.range(0, 60_000);
        let coeff = (rng.range(100, 500) as f64) / 100.0;
        let max_interval = if rng.range(0, 1) == 0 {
            Some(rng.range(100, 120_000))
        } else {
            None
        };

        let policy = RetryPolicy::defaults()
            .with_max_attempts(max_attempts)
            .with_initial_interval_ms(initial_ms)
            .with_backoff_coefficient(coeff);
        let policy = match max_interval {
            Some(ms) => policy.with_max_interval_ms(ms),
            None => policy,
        };

        for attempt in 0..max_attempts {
            let delay = policy.compute_delay(attempt);
            // Delay should always be non-negative (it's Duration, so always true)
            let _ = delay.as_millis();
        }
    }
    println!("  all retry policies computed without panic");
}

#[test]
fn fuzz_circuit_breaker() {
    let cfg = FuzzConfig::standard();
    println!("fuzz_circuit_breaker: {} iterations", cfg.seed);
    let mut rng = RandomGenerator::new(cfg.seed);
    let threshold = rng.range_u32(3, 10);
    let cb = CircuitBreaker::new(CircuitBreakerConfig {
        failure_threshold: threshold,
        recovery_timeout_ms: 1,
        half_open_max_calls: 3,
    });

    for _ in 0..cfg.iteration_count {
        let is_success = rng.range(0, 1) == 0;
        if is_success {
            cb.record_success();
        } else {
            cb.record_failure();
        }
        let _ = cb.allow_call();
        let _ = cb.state();
    }
    println!("  final state: {:?}", cb.state());
}

#[test]
fn fuzz_observability() {
    let cfg = FuzzConfig::quick();
    println!("fuzz_observability: seed={}", cfg.seed);
    let logger = StructuredLogger::new(LogLevel::Trace, "fuzz-test");
    let metrics = MetricsExporter::new();
    let tracer = SpanTracker::new();
    let mut rng = RandomGenerator::new(cfg.seed);

    for _ in 0..cfg.iteration_count {
        let op = rng.range(0, 2);
        match op {
            0 => {
                // log
                let level = match rng.range(0, 4) {
                    0 => LogLevel::Trace,
                    1 => LogLevel::Debug,
                    2 => LogLevel::Info,
                    3 => LogLevel::Warn,
                    _ => LogLevel::Error,
                };
                logger.log_event(level, "fuzz_event", &[("key", "val")]);
            }
            1 => {
                // metrics
                metrics.inc_counter("workflow_started_total");
                metrics.set_gauge("workflow_started_total", rng.range(0, 1000) as i64);
            }
            2 => {
                // spans
                let span_id = tracer.start_span("fuzz-span", None);
                tracer.end_span(span_id);
            }
            _ => {}
        }
    }
    println!(
        "  logger events: {}, metrics exported",
        logger.total_events()
    );
}

#[test]
fn fuzz_rate_limiter() {
    let cfg = FuzzConfig::quick();
    println!("fuzz_rate_limiter: seed={}", cfg.seed);
    let limiter = RateLimiter::new(1000.0, 100, 500.0);
    let mut rng = RandomGenerator::new(cfg.seed);
    let mut acquired = 0u64;
    let mut denied = 0u64;

    for _ in 0..cfg.iteration_count {
        let count = rng.range(1, 20);
        if limiter.try_acquire(0, count) {
            acquired += count;
        } else {
            denied += 1;
        }
    }
    println!("  acquired={}, denied={}", acquired, denied);
}

#[test]
fn fuzz_merkle_verification() {
    let cfg = FuzzConfig::standard();
    println!(
        "fuzz_merkle_verification: {} iterations",
        cfg.iteration_count
    );
    let mut rng = RandomGenerator::new(cfg.seed);

    for _ in 0..cfg.iteration_count {
        let data = rng.random_payload(1024);
        let root1 = compute_simple_merkle_root(&data);
        let root2 = compute_simple_merkle_root(&data);
        // Same input must always produce same root
        assert_eq!(root1, root2, "Merkle root not deterministic");
        // Different input should (almost certainly) produce different root
        let data2 = rng.random_payload(1024);
        let root3 = compute_simple_merkle_root(&data2);
        if data != data2 {
            assert_ne!(root1, root3, "Merkle collision detected");
        }
    }
    println!("  Merkle root consistency verified");
}

#[test]
fn fuzz_bitmask_operations() {
    use velocity_workflow_core::Bitmask256;
    let cfg = FuzzConfig::standard();
    println!(
        "fuzz_bitmask_operations: {} iterations",
        cfg.iteration_count
    );
    let mut rng = RandomGenerator::new(cfg.seed);

    for _ in 0..cfg.iteration_count {
        let mut bitmask = Bitmask256::new();
        let mut expected_set = std::collections::HashSet::new();

        for _ in 0..100 {
            let op = rng.range(0, 2);
            let step = rng.range(0, 255) as usize;
            match op {
                0 => {
                    bitmask.set_step(step);
                    expected_set.insert(step);
                }
                1 => {
                    bitmask.clear_step(step);
                    expected_set.remove(&step);
                }
                2 => {
                    let is_set = bitmask.is_step_set(step);
                    assert_eq!(is_set, expected_set.contains(&step));
                }
                _ => {}
            }
        }
        let count = bitmask.count_completed();
        assert_eq!(count, expected_set.len() as u32);
    }
    println!("  bitmask operations verified");
}
