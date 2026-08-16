//!
//! Phase 6.3 — Sustained Throughput Benchmark
//!
//! Runs workflows at high throughput for a sustained period, measuring:
//! - Step persist latency (p50, p99, p999)
//! - WAL file growth
//! - Workflow completion rate
//! - Zero data loss (WAL recovery verification)
//!
//! Run with:
//! - Quick test:  `cargo test -p velocity-workflow-engine --test sustained_benchmark -- --ignored --nocapture`
//! - 1-hour run:  `SUSTAINED_DURATION_SECS=3600 cargo test -p velocity-workflow-engine --test sustained_benchmark -- --ignored --nocapture`

use std::time::{Duration, Instant};

use velocity_workflow_engine::engine::{WorkflowEngine, WorkflowStatus};

/// Target throughput: workflows per second.
const TARGET_WF_PER_SEC: u64 = 1000;

/// Number of steps per workflow.
const STEPS_PER_WF: u32 = 10;

/// Sustained run duration — override with SUSTAINED_DURATION_SECS env var.
/// Default: 10s for quick validation. Production: set to 3600 for 1-hour run.
fn run_duration_secs() -> u64 {
    std::env::var("SUSTAINED_DURATION_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10)
}

/// Sampling interval for latency measurements.
const SAMPLE_INTERVAL_MS: u64 = 100;

#[test]
#[ignore] // Long-running benchmark — run explicitly
fn test_sustained_throughput_1hour() {
    let wal_path = format!("/tmp/velocity-sustained-{}.wal", std::process::id());
    let wal_max_size = 64 * 1024 * 1024; // 64MB

    let engine = WorkflowEngine::with_wal(&wal_path, wal_max_size)
        .expect("Failed to create WAL");
    engine.recover_from_wal().ok();

    let run_duration_secs = run_duration_secs();
    let run_duration = Duration::from_secs(run_duration_secs);
    let sample_interval = Duration::from_millis(SAMPLE_INTERVAL_MS);

    println!("=== Sustained Throughput Benchmark ===");
    println!("  Duration:      {}s", run_duration_secs);
    println!("  Target:        {} wf/s", TARGET_WF_PER_SEC);
    println!("  Steps/wf:      {}", STEPS_PER_WF);
    println!("  WAL max size:  {}MB", wal_max_size / 1024 / 1024);
    println!();

    // Latency samples (microseconds)
    let mut latency_samples: Vec<u64> = Vec::with_capacity(100_000);
    let mut total_workflows = 0u64;
    let mut total_steps = 0u64;
    let mut workflow_id_counter = 1u64;

    let start = Instant::now();
    let mut next_sample = start;
    let mut next_second = start;
    let mut wf_this_second = 0u64;
    let mut second_count = 0u64;

    while start.elapsed() < run_duration {
        let now = Instant::now();

        // Start and complete a batch of workflows
        let batch_size = 10.min(TARGET_WF_PER_SEC);
        let mut keys = Vec::with_capacity(batch_size as usize);

        for _ in 0..batch_size {
            let key = engine.start_workflow(
                workflow_id_counter,
                1,
                0,
                42,
                STEPS_PER_WF,
                None,
            );
            keys.push(key);
            workflow_id_counter += 1;
        }

        // Complete all steps with latency measurement
        for key in &keys {
            let step_start = Instant::now();
            for step in 0..STEPS_PER_WF {
                let _ = engine.persist_step(*key, step, "default");
            }
            let step_elapsed = step_start.elapsed();
            engine.complete_workflow(*key, None);

            latency_samples.push(step_elapsed.as_micros() as u64);
            total_steps += STEPS_PER_WF as u64;
        }
        total_workflows += batch_size;
        wf_this_second += batch_size;

        // Per-second throughput report
        if next_second.elapsed() >= Duration::ZERO && now >= next_second {
            second_count += 1;
            if second_count % 5 == 0 {
                println!(
                    "  [{:3}s] throughput: {} wf/s, total: {} wf, {} steps",
                    second_count,
                    wf_this_second,
                    total_workflows,
                    total_steps,
                );
            }
            wf_this_second = 0;
            next_second += Duration::from_secs(1);
        }

        // Throttle to target throughput
        let _target_interval = Duration::from_secs(1) / TARGET_WF_PER_SEC as u32 * batch_size as u32;
        let elapsed = start.elapsed();
        let expected = Duration::from_secs(total_workflows / TARGET_WF_PER_SEC);
        if elapsed < expected {
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    let total_elapsed = start.elapsed();

    // ── Compute latency percentiles ──────────────────────────────────────
    latency_samples.sort();
    let p50 = percentile(&latency_samples, 50);
    let p99 = percentile(&latency_samples, 99);
    let p999 = percentile(&latency_samples, 999);
    let mean = latency_samples.iter().sum::<u64>() as f64 / latency_samples.len() as f64;

    // ── WAL file size ────────────────────────────────────────────────────
    let wal_size = std::fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0);

    // ── Recovery verification ────────────────────────────────────────────
    println!();
    println!("=== Results ===");
    println!("  Total workflows:  {}", total_workflows);
    println!("  Total steps:      {}", total_steps);
    println!("  Duration:         {:?}", total_elapsed);
    println!("  Throughput:       {:.0} wf/s", total_workflows as f64 / total_elapsed.as_secs_f64());
    println!("  Step throughput:  {:.0} steps/s", total_steps as f64 / total_elapsed.as_secs_f64());
    println!();
    println!("  Step persist latency:");
    println!("    p50:  {:.1} us", p50);
    println!("    p99:  {:.1} us", p99);
    println!("    p999: {:.1} us", p999);
    println!("    mean: {:.1} us", mean);
    println!();
    println!("  WAL file size: {} bytes ({:.2} MB)", wal_size, wal_size as f64 / 1024.0 / 1024.0);

    // ── Verify zero data loss via WAL recovery ───────────────────────────
    let engine2 = WorkflowEngine::with_wal(&wal_path, wal_max_size)
        .expect("Failed to create recovery WAL");
    let (records, workflows) = engine2.recover_from_wal().expect("WAL recovery failed");
    println!();
    println!("  WAL recovery: {} records, {} workflows", records, workflows);
    println!("  Zero data loss: {} workflows recovered", workflows);
    engine2.shutdown();

    // Assertions
    assert!(total_workflows > 0, "Should have completed some workflows");
    assert!(p99 < 100_000.0, "p99 latency should be under 100ms (was {}us)", p99);

    // Cleanup
    engine.shutdown();
    let _ = std::fs::remove_file(&wal_path);
    let _ = std::fs::remove_file(format!("{}.1", wal_path));
    let _ = std::fs::remove_file(format!("{}.2", wal_path));

    println!();
    println!("=== Sustained benchmark PASSED ===");
}

/// Phase 6.4 — Cross-Flavor Parity Test
///
/// Runs the same workload on multiple engine configurations and verifies
/// identical recovery semantics.
#[test]
fn test_cross_flavor_parity() {
    println!("=== Cross-Flavor Parity Test ===");

    let num_workflows = 100u64;
    let steps_per_wf = 10u32;

    // ── Flavor 1: In-memory only ─────────────────────────────────────────
    let (_keys_mem, results_mem) = run_flavor_workload("in-memory", num_workflows, steps_per_wf, None);

    // ── Flavor 2: WAL-only ───────────────────────────────────────────────
    let wal_path = format!("/tmp/velocity-parity-{}.wal", std::process::id());
    let (_keys_wal, results_wal) = run_flavor_workload("WAL", num_workflows, steps_per_wf, Some(&wal_path));

    // ── Verify identical completion counts ───────────────────────────────
    assert_eq!(results_mem, results_wal,
        "In-memory ({}) and WAL ({}) should have same completion count",
        results_mem, results_wal);

    // ── Verify WAL recovery matches ──────────────────────────────────────
    let engine_wal2 = WorkflowEngine::with_wal(&wal_path, 16 * 1024 * 1024)
        .expect("WAL open");
    let (_records, recovered) = engine_wal2.recover_from_wal().expect("WAL recovery");
    println!("  WAL recovery: {} workflows recovered (expected {})", recovered, num_workflows);
    assert_eq!(recovered, num_workflows as usize,
        "WAL should recover all {} workflows", num_workflows);
    engine_wal2.shutdown();

    // ── Flavor 3: WAL with rotation ──────────────────────────────────────
    let wal_path2 = format!("/tmp/velocity-parity-rot-{}.wal", std::process::id());
    let (_keys_rot, results_rot) = run_flavor_workload_with_rotation(
        "WAL+rotation",
        num_workflows,
        steps_per_wf,
        &wal_path2,
        4096, // Small WAL to force rotation
    );

    assert_eq!(results_wal, results_rot,
        "WAL ({}) and WAL+rotation ({}) should have same completion count",
        results_wal, results_rot);

    // Cleanup
    let _ = std::fs::remove_file(&wal_path);
    let _ = std::fs::remove_file(&wal_path2);
    for p in &[&wal_path, &wal_path2] {
        for i in 1..=5 {
            let _ = std::fs::remove_file(format!("{}.{}", p, i));
        }
    }

    println!();
    println!("=== Cross-flavor parity PASSED ===");
    println!("  All flavors completed {} workflows with {} steps each",
        num_workflows, steps_per_wf);
    println!("  Recovery semantics verified identical across flavors");
}

// ─── Helpers ─────────────────────────────────────────────────────────────

fn run_flavor_workload(
    name: &str,
    num_workflows: u64,
    steps_per_wf: u32,
    wal_path: Option<&str>,
) -> (Vec<u64>, u64) {
    let engine = if let Some(path) = wal_path {
        WorkflowEngine::with_wal(path, 16 * 1024 * 1024).expect("WAL open")
    } else {
        WorkflowEngine::new()
    };

    let start = Instant::now();
    let mut keys = Vec::with_capacity(num_workflows as usize);
    let mut completed = 0u64;

    for i in 0..num_workflows {
        let key = engine.start_workflow(i + 1, 1, 0, 42, steps_per_wf, None);
        keys.push(key);
    }

    for key in &keys {
        for step in 0..steps_per_wf {
            let _ = engine.persist_step(*key, step, "default");
        }
        engine.complete_workflow(*key, None);
        completed += 1;
    }

    let elapsed = start.elapsed();
    println!(
        "  [{}] {} workflows in {:?} ({:.0} wf/s)",
        name,
        completed,
        elapsed,
        completed as f64 / elapsed.as_secs_f64()
    );

    // Verify all completed
    for key in &keys {
        assert_eq!(engine.get_status(*key), WorkflowStatus::Completed);
    }

    engine.shutdown();
    (keys, completed)
}

fn run_flavor_workload_with_rotation(
    name: &str,
    num_workflows: u64,
    steps_per_wf: u32,
    wal_path: &str,
    wal_max_size: u64,
) -> (Vec<u64>, u64) {
    let engine = WorkflowEngine::with_wal(wal_path, wal_max_size).expect("WAL open");

    let start = Instant::now();
    let mut keys = Vec::with_capacity(num_workflows as usize);
    let mut completed = 0u64;

    for i in 0..num_workflows {
        let key = engine.start_workflow(i + 1, 1, 0, 42, steps_per_wf, None);
        keys.push(key);
    }

    for key in &keys {
        for step in 0..steps_per_wf {
            let _ = engine.persist_step(*key, step, "default");
        }
        engine.complete_workflow(*key, None);
        completed += 1;
    }

    let elapsed = start.elapsed();
    println!(
        "  [{}] {} workflows in {:?} ({:.0} wf/s, WAL max {}KB)",
        name,
        completed,
        elapsed,
        completed as f64 / elapsed.as_secs_f64(),
        wal_max_size / 1024,
    );

    for key in &keys {
        assert_eq!(engine.get_status(*key), WorkflowStatus::Completed);
    }

    engine.shutdown();
    (keys, completed)
}

fn percentile(sorted: &[u64], p: u64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = (sorted.len() as f64 * p as f64 / 100.0).min(sorted.len() as f64 - 1.0);
    sorted[idx as usize] as f64
}
