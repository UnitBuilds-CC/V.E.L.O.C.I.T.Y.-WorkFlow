//! Comprehensive benchmark suite for the velocity-workflow-engine.
//!
//! Uses `std::hint::black_box` and `Instant` — no external benchmark framework required.
//! Run with: `cargo bench --bench engine_benchmarks`

use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

use velocity_workflow_engine::engine::WorkflowEngine;
use velocity_workflow_engine::namespace::NamespaceConfig;
use velocity_workflow_engine::namespace::NamespaceRegistry;
use velocity_workflow_engine::payload_codec::{CodecChain, XorCodec};
use velocity_workflow_engine::query_handler::QueryRegistry;
use velocity_workflow_engine::search_index::SearchAttributeIndex;
use velocity_workflow_engine::task_queue::{TaskItem, TaskKind, TaskQueue};
use velocity_workflow_engine::timer_engine::TimerEngine;
use velocity_workflow_engine::visibility::SearchAttributeValue;

use velocity_workflow_core::Bitmask256;
use velocity_workflow_core::SlabHeader;

// ─── Benchmark Harness ────────────────────────────────────────────────────────

/// Result of a single benchmark run.
struct BenchResult {
    name: String,
    iterations: usize,
    total_time: Duration,
    ops_per_sec: f64,
    p50_latency: Duration,
    p99_latency: Duration,
}

impl BenchResult {
    fn print_row(&self) {
        println!(
            "  {:<42} | {:>10} | {:>12.0} | {:>10.1?} | {:>10.1?} | {:>10.1?}",
            self.name,
            self.iterations,
            self.ops_per_sec,
            self.p50_latency,
            self.p99_latency,
            self.total_time,
        );
    }
}

/// Run a benchmark: executes `f` for at least `min_duration` or `min_iterations`,
/// collecting per-iteration latencies for percentile calculation.
fn bench(
    name: &str,
    min_iterations: usize,
    min_duration: Duration,
    mut f: impl FnMut(),
) -> BenchResult {
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

    // Continue until min_duration is reached
    while start.elapsed() < min_duration {
        let iter_start = Instant::now();
        f();
        black_box(());
        latencies.push(iter_start.elapsed());
    }

    let total_time = start.elapsed();
    let iterations = latencies.len();

    latencies.sort();

    let p50_idx = (iterations as f64 * 0.50) as usize;
    let p99_idx = (iterations as f64 * 0.99) as usize;

    let ops_per_sec = iterations as f64 / total_time.as_secs_f64();

    BenchResult {
        name: name.to_string(),
        iterations,
        total_time,
        ops_per_sec,
        p50_latency: latencies[p50_idx.min(iterations - 1)],
        p99_latency: latencies[p99_idx.min(iterations - 1)],
    }
}

/// Run a benchmark that takes an iteration index (for varying inputs).
fn bench_indexed(
    name: &str,
    min_iterations: usize,
    min_duration: Duration,
    mut f: impl FnMut(usize),
) -> BenchResult {
    // Warm-up
    for i in 0..min_iterations.min(100) {
        f(i);
        black_box(());
    }

    let mut latencies = Vec::with_capacity(min_iterations.max(10_000));
    let start = Instant::now();

    for i in 0..min_iterations {
        let iter_start = Instant::now();
        f(i);
        black_box(());
        latencies.push(iter_start.elapsed());
    }

    while start.elapsed() < min_duration {
        let i = latencies.len();
        let iter_start = Instant::now();
        f(i);
        black_box(());
        latencies.push(iter_start.elapsed());
    }

    let total_time = start.elapsed();
    let iterations = latencies.len();
    latencies.sort();

    let p50_idx = (iterations as f64 * 0.50) as usize;
    let p99_idx = (iterations as f64 * 0.99) as usize;
    let ops_per_sec = iterations as f64 / total_time.as_secs_f64();

    BenchResult {
        name: name.to_string(),
        iterations,
        total_time,
        ops_per_sec,
        p50_latency: latencies[p50_idx.min(iterations - 1)],
        p99_latency: latencies[p99_idx.min(iterations - 1)],
    }
}

// ─── Individual Benchmarks ────────────────────────────────────────────────────

fn bench_workflow_creation() -> BenchResult {
    let engine = WorkflowEngine::new();
    let mut counter = 0u64;
    bench(
        "workflow_creation",
        5_000,
        Duration::from_millis(100),
        || {
            counter += 1;
            engine.start_workflow(counter, 1, 0, 100, 10, None);
        },
    )
}

fn bench_step_completion() -> BenchResult {
    let engine = WorkflowEngine::new();
    let key = engine.start_workflow(1, 1, 0, 100, 200, None);
    let mut step = 0u32;
    bench(
        "step_completion",
        10_000,
        Duration::from_millis(100),
        || {
            let s = step % 200;
            engine.complete_step(black_box(key), black_box(s), vec![0u8; 64]);
            step += 1;
        },
    )
}

fn bench_signal_delivery() -> BenchResult {
    let engine = WorkflowEngine::new();
    let key = engine.start_workflow(1, 1, 0, 100, 10, None);
    let mut sig = 0u64;
    bench(
        "signal_delivery",
        10_000,
        Duration::from_millis(100),
        || {
            engine.signal_workflow(black_box(key), sig % 100, vec![0u8; 32]);
            sig += 1;
        },
    )
}

fn bench_query_execution() -> BenchResult {
    let registry = QueryRegistry::new();
    // Register handlers for 100 workflows
    for wk in 1..=100u64 {
        registry.register_handler(
            wk,
            1,
            Box::new(|input| {
                let mut out = input.to_vec();
                out.push(0xFF);
                out
            }),
        );
    }
    let mut wk = 1u64;
    bench(
        "query_execution",
        10_000,
        Duration::from_millis(100),
        || {
            let _ = registry.execute_query(black_box(wk), 1, black_box(&[1, 2, 3, 4]));
            wk = (wk % 100) + 1;
        },
    )
}

fn bench_task_queue_enqueue() -> BenchResult {
    let tq = TaskQueue::new();
    let mut counter = 0u64;
    bench(
        "task_queue_enqueue",
        10_000,
        Duration::from_millis(100),
        || {
            let task = TaskItem {
                task_id: 0,
                kind: TaskKind::WorkflowTask,
                workflow_key: counter,
                task_queue_hash: 42,
                step_index: 0,
                activity_name_id: 0,
                attempt: 1,
                priority: 0,
                deadline_ms: 0,
            };
            tq.enqueue(black_box(42), task);
            counter += 1;
        },
    )
}

fn bench_task_queue_poll() -> BenchResult {
    let tq = TaskQueue::new();
    // Pre-fill the queue
    for i in 0..10_000u64 {
        tq.enqueue(
            42,
            TaskItem {
                task_id: 0,
                kind: TaskKind::WorkflowTask,
                workflow_key: i,
                task_queue_hash: 42,
                step_index: 0,
                activity_name_id: 0,
                attempt: 1,
                priority: 0,
                deadline_ms: 0,
            },
        );
    }
    bench(
        "task_queue_poll",
        10_000,
        Duration::from_millis(100),
        || {
            let _ = black_box(tq.try_poll(42));
        },
    )
}

fn bench_timer_schedule() -> BenchResult {
    let te = TimerEngine::new();
    let mut wk = 0u64;
    bench("timer_schedule", 10_000, Duration::from_millis(100), || {
        let _ = te.schedule(black_box(wk), Duration::from_secs(60));
        wk += 1;
    })
}

fn bench_namespace_register() -> BenchResult {
    let registry = NamespaceRegistry::new();
    let mut id = 1000u64;
    bench(
        "namespace_register",
        5_000,
        Duration::from_millis(100),
        || {
            id += 1;
            let config = NamespaceConfig::new(id, format!("bench-ns-{}", id));
            let _ = registry.register(config);
        },
    )
}

fn bench_search_index_write() -> BenchResult {
    let index = SearchAttributeIndex::new();
    let mut wk = 0u64;
    let val = SearchAttributeValue::Keyword("benchmark".to_string());
    bench(
        "search_index_write",
        10_000,
        Duration::from_millis(100),
        || {
            index.index_attribute(black_box(wk), "bench_attr", &val);
            wk += 1;
        },
    )
}

fn bench_search_index_query() -> BenchResult {
    let index = SearchAttributeIndex::new();
    // Pre-populate with data
    for i in 0..5_000u64 {
        let val = SearchAttributeValue::Integer((i % 100) as i64);
        index.index_attribute(i, "query_attr", &val);
    }
    let query_val = SearchAttributeValue::Integer(42);
    bench(
        "search_index_query",
        5_000,
        Duration::from_millis(100),
        || {
            let _ = index.exact_match("query_attr", &query_val);
        },
    )
}

fn bench_merkle_compute() -> BenchResult {
    bench_indexed("merkle_compute", 5_000, Duration::from_millis(100), |i| {
        let mut slab = SlabHeader::new(1, 1, 256);
        // Complete a varying number of steps to exercise Merkle recomputation
        for step in 0..(i % 64) {
            slab.mark_step_completed(step);
        }
        black_box(slab.merkle_root);
    })
}

fn bench_bitmask_operations() -> BenchResult {
    bench_indexed(
        "bitmask_operations",
        10_000,
        Duration::from_millis(100),
        |i| {
            let mut mask = Bitmask256::new();
            // Set 32 bits
            for b in 0..32 {
                mask.set_step((i.wrapping_add(b)) % 256);
            }
            black_box(mask.count_completed());
            // Clear half
            for b in 0..16 {
                mask.clear_step((i.wrapping_add(b)) % 256);
            }
            black_box(mask.count_completed());
            // Check membership
            for b in 0..32 {
                black_box(mask.is_step_set((i.wrapping_add(b)) % 256));
            }
        },
    )
}

fn bench_slab_creation() -> BenchResult {
    bench_indexed("slab_creation", 5_000, Duration::from_millis(100), |i| {
        let slab = SlabHeader::new(i as u64, i as u64 + 1, 128);
        black_box(slab);
    })
}

fn bench_payload_encode_decode() -> BenchResult {
    let mut chain = CodecChain::new();
    chain.add(Arc::new(XorCodec { key: 0xAB }));
    chain.add(Arc::new(XorCodec { key: 0xCD }));
    let payload = vec![0x42u8; 1024]; // 1 KB payload
    bench(
        "payload_encode_decode",
        5_000,
        Duration::from_millis(100),
        || {
            let encoded = chain.encode(black_box(&payload)).unwrap();
            let decoded = chain.decode(black_box(&encoded)).unwrap();
            black_box(decoded);
        },
    )
}

fn bench_concurrent_workflow_creation() -> BenchResult {
    let engine = Arc::new(WorkflowEngine::new());
    let num_threads = 4;
    let iters_per_thread = 2_000;

    let start = Instant::now();
    let mut handles = Vec::new();

    for t in 0..num_threads {
        let eng = Arc::clone(&engine);
        handles.push(std::thread::spawn(move || {
            for i in 0..iters_per_thread {
                let wf_id = (t * iters_per_thread + i) as u64 + 1;
                eng.start_workflow(wf_id, 1, 0, 100, 10, None);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }
    let total_time = start.elapsed();
    let total_iters = num_threads * iters_per_thread;
    let ops_per_sec = total_iters as f64 / total_time.as_secs_f64();

    BenchResult {
        name: "concurrent_workflow_creation".to_string(),
        iterations: total_iters,
        total_time,
        ops_per_sec,
        p50_latency: total_time / total_iters as u32,
        p99_latency: total_time / total_iters as u32,
    }
}

// ─── Main ─────────────────────────────────────────────────────────────────────

fn main() {
    println!();
    println!("╔══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗");
    println!("║                                         Velocity Workflow Engine — Benchmark Suite                                                     ║");
    println!("╠══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣");
    println!(
        "║ {:<42} │ {:>10} │ {:>12} │ {:>10} │ {:>10} │ {:>10} ║",
        "Benchmark", "Iterations", "Ops/sec", "p50", "p99", "Total"
    );
    println!("╠══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣");

    let results: Vec<BenchResult> = vec![
        bench_workflow_creation(),
        bench_step_completion(),
        bench_signal_delivery(),
        bench_query_execution(),
        bench_task_queue_enqueue(),
        bench_task_queue_poll(),
        bench_timer_schedule(),
        bench_namespace_register(),
        bench_search_index_write(),
        bench_search_index_query(),
        bench_merkle_compute(),
        bench_bitmask_operations(),
        bench_slab_creation(),
        bench_payload_encode_decode(),
        bench_concurrent_workflow_creation(),
    ];

    for r in &results {
        r.print_row();
    }

    println!("╚══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╝");
    println!();
    println!(
        "All benchmarks completed. {} benchmarks executed.",
        results.len()
    );
    println!();
}
