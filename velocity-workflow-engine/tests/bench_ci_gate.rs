//! Benchmark CI Gate — Regression detection for core engine operations.
//!
//! This test measures key engine operations and compares against committed baseline
//! thresholds. It runs as part of CI to catch performance regressions before merge.
//!
//! Thresholds are set conservatively (~30% of observed performance) to tolerate
//! CI runner variance while still catching major regressions (>200% slowdown).
//!
//! Run locally: `cargo test --release -p velocity-workflow-engine --test bench_ci_gate -- --nocapture`

use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

use velocity_workflow_engine::engine::WorkflowEngine;
use velocity_workflow_engine::namespace::NamespaceConfig;
use velocity_workflow_engine::payload_codec::{CodecChain, XorCodec};
use velocity_workflow_engine::query_handler::QueryRegistry;
use velocity_workflow_engine::search_index::SearchAttributeIndex;
use velocity_workflow_engine::task_queue::{TaskItem, TaskKind, TaskQueue};
use velocity_workflow_engine::timer_engine::TimerEngine;
use velocity_workflow_engine::visibility::SearchAttributeValue;
use velocity_workflow_engine::wal::{WalRecord, WalWriter};

// ─── Baseline Thresholds ─────────────────────────────────────────────────────
// MINIMUM acceptable ops/sec and MAXIMUM acceptable p99 latency (µs).
// Set at ~30% of observed local release-build performance.

#[allow(dead_code)]
struct Baseline {
    name: &'static str,
    min_ops_per_sec: f64,
    max_p99_us: u64,
}

const BASELINES: &[Baseline] = &[
    Baseline {
        name: "task_queue_enqueue",
        min_ops_per_sec: 500_000.0,
        max_p99_us: 10,
    },
    Baseline {
        name: "task_queue_dequeue",
        min_ops_per_sec: 500_000.0,
        max_p99_us: 10,
    },
    Baseline {
        name: "timer_schedule",
        min_ops_per_sec: 500_000.0,
        max_p99_us: 10,
    },
    Baseline {
        name: "wal_append",
        min_ops_per_sec: 100_000.0,
        max_p99_us: 20,
    },
    Baseline {
        name: "wal_read",
        min_ops_per_sec: 5_000.0,
        max_p99_us: 500,
    },
    Baseline {
        name: "workflow_create",
        min_ops_per_sec: 100_000.0,
        max_p99_us: 20,
    },
    Baseline {
        name: "workflow_complete",
        min_ops_per_sec: 100_000.0,
        max_p99_us: 20,
    },
    Baseline {
        name: "search_index_upsert",
        min_ops_per_sec: 200_000.0,
        max_p99_us: 10,
    },
    Baseline {
        name: "search_index_query",
        min_ops_per_sec: 200_000.0,
        max_p99_us: 10,
    },
    Baseline {
        name: "query_handler_register",
        min_ops_per_sec: 500_000.0,
        max_p99_us: 10,
    },
    Baseline {
        name: "codec_encode_decode",
        min_ops_per_sec: 500_000.0,
        max_p99_us: 10,
    },
];

// ─── Benchmark Harness ───────────────────────────────────────────────────────

struct BenchResult {
    ops_per_sec: f64,
    p50_us: u64,
    p99_us: u64,
}

fn measure(min_iterations: usize, min_duration: Duration, mut f: impl FnMut()) -> BenchResult {
    // Warm-up
    for _ in 0..min_iterations.min(100) {
        f();
        black_box(());
    }

    let mut latencies = Vec::with_capacity(min_iterations.max(10_000));
    let start = Instant::now();

    for _ in 0..min_iterations {
        let iter_start = Instant::now();
        f();
        black_box(());
        latencies.push(iter_start.elapsed());
    }

    while start.elapsed() < min_duration {
        let iter_start = Instant::now();
        f();
        black_box(());
        latencies.push(iter_start.elapsed());
    }

    let total = start.elapsed();
    let n = latencies.len();
    latencies.sort();

    let ops_per_sec = n as f64 / total.as_secs_f64();
    let p50_idx = (n as f64 * 0.50) as usize;
    let p99_idx = (n as f64 * 0.99) as usize;

    BenchResult {
        ops_per_sec,
        p50_us: latencies[p50_idx.min(n - 1)].as_micros() as u64,
        p99_us: latencies[p99_idx.min(n - 1)].as_micros() as u64,
    }
}

// ─── Test ────────────────────────────────────────────────────────────────────

#[test]
fn benchmark_regression_gate() {
    println!();
    println!("═══════════════════════════════════════════════════════════");
    println!("  Benchmark CI Gate — Regression Detection");
    println!("═══════════════════════════════════════════════════════════");
    println!();
    println!(
        "  {:<30} | {:>12} | {:>10} | {:>10} | {}",
        "Benchmark", "Ops/sec", "p50 (µs)", "p99 (µs)", "Status"
    );
    println!(
        "  {:-<30}-+-{:-<12}-+-{:-<10}-+-{:-<10}-+-{:-<8}",
        "", "", "", "", ""
    );

    let mut failures = Vec::new();

    // ── Task Queue ──────────────────────────────────────────────────────
    {
        let queue = TaskQueue::new();
        let tq_hash: u64 = 42;

        let result = measure(10_000, Duration::from_millis(200), || {
            let task = TaskItem {
                task_id: 0,
                kind: TaskKind::ActivityTask,
                workflow_key: black_box(1u64),
                task_queue_hash: tq_hash,
                step_index: 0,
                activity_name_id: 100,
                attempt: 1,
                priority: 0,
                deadline_ms: 0,
            };
            queue.enqueue(tq_hash, task);
        });
        print_result("task_queue_enqueue", &result, &BASELINES[0], &mut failures);

        // Pre-fill queue for dequeue benchmark
        let queue2 = TaskQueue::new();
        for i in 0..1000u64 {
            queue2.enqueue(
                tq_hash,
                TaskItem {
                    task_id: 0,
                    kind: TaskKind::ActivityTask,
                    workflow_key: i,
                    task_queue_hash: tq_hash,
                    step_index: 0,
                    activity_name_id: 100,
                    attempt: 1,
                    priority: 0,
                    deadline_ms: 0,
                },
            );
        }

        let result = measure(10_000, Duration::from_millis(200), || {
            black_box(queue2.try_poll(tq_hash));
        });
        print_result("task_queue_dequeue", &result, &BASELINES[1], &mut failures);
    }

    // ── Timer Engine ────────────────────────────────────────────────────
    {
        let timer = TimerEngine::new();

        let result = measure(10_000, Duration::from_millis(200), || {
            black_box(timer.schedule(black_box(1u64), Duration::from_secs(3600)));
        });
        print_result("timer_schedule", &result, &BASELINES[2], &mut failures);
    }

    // ── WAL ─────────────────────────────────────────────────────────────
    {
        let dir = std::env::temp_dir().join(format!(
            "bench_gate_wal_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
        ));
        let _ = std::fs::create_dir_all(&dir);

        // WAL append: open file once, measure repeated appends
        let wal_path = dir.join("bench_append.wal");
        let mut writer = WalWriter::open(&wal_path).unwrap();
        let record = WalRecord::new(
            velocity_workflow_engine::wal::WalEventType::WorkflowStarted,
            1,
            vec![1, 2, 3, 4, 5, 6, 7, 8],
        );

        let result = measure(5_000, Duration::from_millis(200), || {
            black_box(writer.append(&record)).unwrap();
        });
        print_result("wal_append", &result, &BASELINES[3], &mut failures);
        drop(writer);

        // Write a WAL file then read it back
        let read_dir = dir.join("read_bench");
        let _ = std::fs::create_dir_all(&read_dir);
        {
            let mut writer = WalWriter::open(read_dir.join("read.wal")).unwrap();
            for i in 0..100u64 {
                let record = WalRecord::new(
                    velocity_workflow_engine::wal::WalEventType::ActivityScheduled,
                    i,
                    vec![1, 2, 3, 4, 5, 6, 7, 8],
                );
                writer.append(&record).unwrap();
            }
        }

        // WAL read: measure reading records from a file
        let read_path = read_dir.join("read.wal");
        let result = measure(5_000, Duration::from_millis(200), || {
            let records =
                velocity_workflow_engine::wal::read_wal_records(black_box(&read_path)).unwrap();
            black_box(records.len());
        });
        print_result("wal_read", &result, &BASELINES[4], &mut failures);

        let _ = std::fs::remove_dir_all(&dir);
    }

    // ── Workflow Engine ─────────────────────────────────────────────────
    {
        let engine = WorkflowEngine::new();
        // Register a namespace
        let _ = engine
            .namespaces()
            .register(NamespaceConfig::new(1, "bench-ns"));

        let result = measure(5_000, Duration::from_millis(200), || {
            black_box(engine.start_workflow(
                black_box(42u64), // workflow_id
                1,                // workflow_type_id
                1,                // namespace_id
                100,              // task_queue_hash
                5,                // total_steps
                Some(b"input".to_vec()),
            ));
        });
        print_result("workflow_create", &result, &BASELINES[5], &mut failures);

        // Create a workflow then complete a step
        let wf_key = engine.start_workflow(100, 1, 1, 100, 5, Some(b"input".to_vec()));

        let result = measure(5_000, Duration::from_millis(200), || {
            engine.complete_step(black_box(wf_key), black_box(0u32), b"result".to_vec());
        });
        print_result("workflow_complete", &result, &BASELINES[6], &mut failures);
    }

    // ── Search Index ────────────────────────────────────────────────────
    {
        let index = SearchAttributeIndex::new();

        let result = measure(10_000, Duration::from_millis(200), || {
            index.index_attribute(
                black_box(1u64),
                "key",
                &SearchAttributeValue::Keyword("value".to_string()),
            );
        });
        print_result("search_index_upsert", &result, &BASELINES[7], &mut failures);

        // Pre-fill for query
        let index2 = SearchAttributeIndex::new();
        for i in 0..100u64 {
            index2.index_attribute(
                i,
                "status",
                &SearchAttributeValue::Keyword(format!("val-{}", i % 10)),
            );
        }

        let query_val = SearchAttributeValue::Keyword("val-5".to_string());
        let result = measure(10_000, Duration::from_millis(200), || {
            black_box(index2.exact_match("status", &query_val));
        });
        print_result("search_index_query", &result, &BASELINES[8], &mut failures);
    }

    // ── Query Handler ───────────────────────────────────────────────────
    {
        let registry = QueryRegistry::new();

        let result = measure(10_000, Duration::from_millis(200), || {
            let handler: Box<dyn Fn(&[u8]) -> Vec<u8> + Send + Sync> =
                Box::new(|input: &[u8]| input.to_vec());
            registry.register_handler(black_box(1u64), black_box(100u64), handler);
        });
        print_result(
            "query_handler_register",
            &result,
            &BASELINES[9],
            &mut failures,
        );
    }

    // ── Codec ───────────────────────────────────────────────────────────
    {
        let mut chain = CodecChain::new();
        chain.add(Arc::new(XorCodec { key: 0x42 }));
        let data = vec![1u8; 256];

        let result = measure(10_000, Duration::from_millis(200), || {
            let encoded = chain.encode(black_box(&data)).unwrap();
            black_box(chain.decode(black_box(&encoded)).unwrap());
        });
        print_result(
            "codec_encode_decode",
            &result,
            &BASELINES[10],
            &mut failures,
        );
    }

    // ── Summary ─────────────────────────────────────────────────────────
    println!();
    println!("═══════════════════════════════════════════════════════════");

    if failures.is_empty() {
        println!(
            "  ✓ ALL {} BENCHMARKS PASSED — no regression detected",
            BASELINES.len()
        );
        println!("═══════════════════════════════════════════════════════════");
    } else {
        println!(
            "  ✗ {} of {} BENCHMARKS REGRESSED:",
            failures.len(),
            BASELINES.len()
        );
        for f in &failures {
            println!("    - {}", f);
        }
        println!("═══════════════════════════════════════════════════════════");
        println!();
        println!("  To update baselines, run locally with --nocapture and");
        println!("  copy the measured values to the BASELINES array in");
        println!("  velocity-workflow-engine/tests/bench_ci_gate.rs");
        panic!(
            "Benchmark regression detected: {} benchmarks below threshold",
            failures.len()
        );
    }
}

fn print_result(name: &str, result: &BenchResult, baseline: &Baseline, failures: &mut Vec<String>) {
    let throughput_ok = result.ops_per_sec >= baseline.min_ops_per_sec;
    let latency_ok = result.p99_us <= baseline.max_p99_us;
    let pass = throughput_ok && latency_ok;

    let status = if pass { "✓" } else { "✗ REGRESSED" };

    println!(
        "  {:<30} | {:>12.0} | {:>10} | {:>10} | {}",
        name, result.ops_per_sec, result.p50_us, result.p99_us, status
    );

    if !pass {
        let mut reason = format!("{}: ", name);
        if !throughput_ok {
            reason.push_str(&format!(
                "throughput {:.0} ops/s < threshold {:.0} ops/s",
                result.ops_per_sec, baseline.min_ops_per_sec
            ));
        }
        if !latency_ok {
            if !throughput_ok {
                reason.push_str(", ");
            }
            reason.push_str(&format!(
                "p99 latency {}µs > threshold {}µs",
                result.p99_us, baseline.max_p99_us
            ));
        }
        failures.push(reason);
    }
}
