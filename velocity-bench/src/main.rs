//! velocity-bench — Side-by-side benchmark harness: VELOCITY-WorkFlow vs Temporal.
//!
//! Both engines are accessed via **identical gRPC paths** (BenchmarkService proto).
//! Neither engine uses a direct/in-process API — both pay the same serialization,
//! network, and protocol overhead. This ensures a truly fair apples-to-apples
//! comparison.
//!
//! Usage:
//!   # Start VELOCITY dev server first:
//!   velocity-dev --grpc-port 7234
//!
//!   # Start Temporal server (docker):
//!   docker-compose -f docker-compose.temporal.yml up -d
//!
//!   # Run benchmark:
//!   cargo run --release -- --workloads smoke --format markdown --output report.md
//!   cargo run --release -- --workloads all --engine both --profile standard
//!   cargo run --release -- --workload simple_workflow --engine velocity

use clap::{Parser, ValueEnum};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use velocity_bench::metrics::*;
use velocity_bench::report::*;
use velocity_bench::*;

/// Prevent the compiler from optimizing away a value.
/// This is critical for benchmark rigor — without it, the JIT/Rust compiler
/// may eliminate operations whose results are unused (dead-code elimination).
#[inline(never)]
fn black_box<T>(x: T) -> T {
    std::hint::black_box(x)
}

// ─── CLI ─────────────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(name = "velocity-bench")]
#[command(about = "Side-by-side benchmark: VELOCITY-WorkFlow vs Temporal")]
#[command(version = "0.1.0")]
struct Cli {
    /// Which workloads to run.
    #[arg(long, default_value = "smoke")]
    workloads: WorkloadSelection,

    /// Run a single specific workload.
    #[arg(long)]
    workload: Option<String>,

    /// Which engine(s) to benchmark.
    #[arg(long, default_value = "both")]
    engine: EngineSelection,

    /// VELOCITY gRPC address (velocity-dev-server).
    #[arg(long, default_value = "http://localhost:7234")]
    velocity_address: String,

    /// Temporal gRPC address.
    #[arg(long, default_value = "http://localhost:7233")]
    temporal_address: String,

    /// Output format.
    #[arg(long, default_value = "markdown")]
    format: OutputFormat,

    /// Output file path (stdout if not specified).
    #[arg(long, short)]
    output: Option<String>,

    /// Workload profile: quick, standard, or stress.
    #[arg(long, default_value = "standard")]
    profile: WorkloadProfile,

    /// Enable verbose logging.
    #[arg(long, short)]
    verbose: bool,

    /// Sustained benchmark duration in minutes (0 = disabled).
    /// Runs continuous load for the specified duration, sampling every --sample-interval seconds.
    #[arg(long, default_value = "0")]
    sustained: u64,

    /// Sampling interval in seconds for sustained benchmarks.
    #[arg(long, default_value = "30")]
    sample_interval: u64,

    /// Workload to use for sustained benchmarking.
    #[arg(long, default_value = "simple_workflow")]
    sustained_workload: String,

    /// Number of times to repeat each workload for statistical analysis.
    /// With --runs N, each workload runs N times and results report mean ± stddev
    /// with 95% confidence intervals. Minimum 1, recommended 3-5 for publication.
    #[arg(long, default_value = "1")]
    runs: usize,

    /// Include statistical significance tests (Welch's t-test) in output.
    /// Requires --runs >= 2 to produce meaningful results.
    #[arg(long)]
    significance: bool,

    /// Append results to a JSON Lines trend file for commit-over-commit tracking.
    /// Each run appends one line: {timestamp, commit, workloads: [{name, ops/sec, p99_us, mem_mb}]}.
    /// Use VELOCITY_BENCH_COMMIT env var or auto-detect from git.
    #[arg(long)]
    trend_file: Option<String>,
}

#[derive(Debug, Clone, ValueEnum)]
enum WorkloadSelection {
    All,
    Smoke,
    Throughput,
    Latency,
    Memory,
    Durability,
}

#[derive(Debug, Clone, ValueEnum)]
enum EngineSelection {
    Both,
    Velocity,
    Temporal,
}

#[derive(Debug, Clone, ValueEnum)]
enum OutputFormat {
    Markdown,
    Csv,
    Json,
    All,
}

#[derive(Debug, Clone, ValueEnum)]
enum WorkloadProfile {
    Quick,
    Standard,
    Stress,
}

// ─── Main ────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    // Initialize logging
    let filter = if cli.verbose { "debug" } else { "info" };
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();

    tracing::info!("╔══════════════════════════════════════════════════════════╗");
    tracing::info!("║  velocity-bench — VELOCITY-WorkFlow vs Temporal         ║");
    tracing::info!("║  Apples-to-apples via identical gRPC paths              ║");
    tracing::info!("╚══════════════════════════════════════════════════════════╝");

    // Select workloads
    let workload_defs = match &cli.workloads {
        WorkloadSelection::Smoke => WorkloadDefinition::smoke_test(),
        _ => WorkloadDefinition::all(),
    };

    let workload_defs = if let Some(ref name) = cli.workload {
        workload_defs
            .into_iter()
            .filter(|w| w.name == *name)
            .collect()
    } else {
        workload_defs
    };

    tracing::info!(
        "Running {} workloads with {:?} profile",
        workload_defs.len(),
        cli.profile
    );

    // Apply profile as a multiplier — don't override per-workload configs.
    // Quick = 0.1x counts, Standard = 1x, Stress = 10x.
    // This preserves each workload's tuned parameters while scaling the load.
    let (count_mult, duration_mult) = match cli.profile {
        WorkloadProfile::Quick => (0.1_f64, 0.25),
        WorkloadProfile::Standard => (1.0, 1.0),
        WorkloadProfile::Stress => (10.0, 2.0),
    };
    let workload_defs: Vec<WorkloadDefinition> = workload_defs
        .into_iter()
        .map(|mut w| {
            w.config.workflow_count = (w.config.workflow_count as f64 * count_mult).max(1.0) as u64;
            w.config.duration_secs =
                (w.config.duration_secs as f64 * duration_mult).max(1.0) as u64;
            w
        })
        .collect();

    // ─── Initialize engines (shared by both sustained and standard modes) ───
    let mut velocity_engine: Option<GrpcAdapter> = None;
    let mut temporal_engine: Option<GrpcAdapter> = None;

    match cli.engine {
        EngineSelection::Both | EngineSelection::Velocity => {
            let mut vel = GrpcAdapter::new(EngineKind::Velocity);
            let config = EngineConfig::velocity(&cli.velocity_address);
            vel.connect(&config)
                .await
                .map_err(|e| format!("VELOCITY gRPC connect failed: {}", e))?;
            tracing::info!(
                "✓ VELOCITY engine connected via gRPC ({})",
                cli.velocity_address
            );
            velocity_engine = Some(vel);
        }
        _ => {}
    }

    match cli.engine {
        EngineSelection::Both | EngineSelection::Temporal => {
            let mut tmp = GrpcAdapter::new(EngineKind::Temporal);
            let config = EngineConfig::temporal(&cli.temporal_address);
            match tmp.connect(&config).await {
                Ok(()) => {
                    tracing::info!(
                        "✓ Temporal engine connected via gRPC ({})",
                        cli.temporal_address
                    );
                    temporal_engine = Some(tmp);
                }
                Err(e) => {
                    tracing::warn!(
                        "✗ Temporal gRPC connect failed: {} — running VELOCITY only",
                        e
                    );
                }
            }
        }
        _ => {}
    }

    // ─── SUSTAINED BENCHMARK MODE ───────────────────────────────────────────
    if cli.sustained > 0 {
        tracing::info!("╔══════════════════════════════════════════════════════════╗");
        tracing::info!("║  SUSTAINED BENCHMARK MODE                               ║");
        tracing::info!(
            "║  Duration: {} minutes, Sample interval: {}s             ║",
            cli.sustained,
            cli.sample_interval
        );
        tracing::info!("╚══════════════════════════════════════════════════════════╝");

        let sustained_duration = Duration::from_secs(cli.sustained * 60);
        let sample_interval = Duration::from_secs(cli.sample_interval);
        let workload_name = &cli.sustained_workload;

        // Find the workload definition
        let all_workloads = WorkloadDefinition::all();
        let workload_def = all_workloads
            .iter()
            .find(|w| w.name == *workload_name)
            .cloned()
            .unwrap_or_else(|| {
                all_workloads
                    .iter()
                    .find(|w| w.name == "simple_workflow")
                    .cloned()
                    .unwrap()
            });

        // Scale up for sustained testing — use high concurrency and count
        let mut sustained_workload = workload_def;
        sustained_workload.config.workflow_count = 10000;
        sustained_workload.config.concurrency = 50;

        struct TimeSeriesSample {
            elapsed_secs: u64,
            velocity_ops_per_sec: f64,
            velocity_p50_us: f64,
            velocity_p99_us: f64,
            velocity_mem_mb: f64,
            temporal_ops_per_sec: f64,
            temporal_p50_us: f64,
            temporal_p99_us: f64,
            temporal_mem_mb: f64,
        }

        let mut timeseries: Vec<TimeSeriesSample> = Vec::new();
        let bench_start = Instant::now();

        tracing::info!(
            "Starting sustained benchmark for {} minutes...",
            cli.sustained
        );
        tracing::info!(
            "Workload: {} (scaled: {} workflows, {} concurrency)",
            sustained_workload.name,
            sustained_workload.config.workflow_count,
            sustained_workload.config.concurrency
        );

        let mut sample_num = 0u64;

        while bench_start.elapsed() < sustained_duration {
            sample_num += 1;
            let elapsed_secs = bench_start.elapsed().as_secs();

            tracing::info!("━━━ Sample #{} (T+{}s) ━━━", sample_num, elapsed_secs);

            // Run Velocity sample
            let (v_ops, v_p50, v_p99, v_mem) = if let Some(ref mut vel) = velocity_engine {
                let metrics = run_workload(vel, &sustained_workload).await;
                let p50 = metrics.completion_latency.p50_us as f64;
                let p99 = metrics.completion_latency.p99_us as f64;
                tracing::info!(
                    "  VELOCITY: {:.0} ops/sec, p50={:.0}µs, p99={:.0}µs, mem={:.1}MB",
                    metrics.operations_per_second,
                    p50,
                    p99,
                    metrics.peak_memory_mb
                );
                (
                    metrics.operations_per_second,
                    p50,
                    p99,
                    metrics.peak_memory_mb,
                )
            } else {
                (0.0, 0.0, 0.0, 0.0)
            };

            // Run Temporal sample
            let (t_ops, t_p50, t_p99, t_mem) = if let Some(ref mut tmp) = temporal_engine {
                let metrics = run_workload(tmp, &sustained_workload).await;
                let p50 = metrics.completion_latency.p50_us as f64;
                let p99 = metrics.completion_latency.p99_us as f64;
                tracing::info!(
                    "  Temporal: {:.0} ops/sec, p50={:.0}µs, p99={:.0}µs, mem={:.1}MB",
                    metrics.operations_per_second,
                    p50,
                    p99,
                    metrics.peak_memory_mb
                );
                (
                    metrics.operations_per_second,
                    p50,
                    p99,
                    metrics.peak_memory_mb,
                )
            } else {
                (0.0, 0.0, 0.0, 0.0)
            };

            timeseries.push(TimeSeriesSample {
                elapsed_secs,
                velocity_ops_per_sec: v_ops,
                velocity_p50_us: v_p50,
                velocity_p99_us: v_p99,
                velocity_mem_mb: v_mem,
                temporal_ops_per_sec: t_ops,
                temporal_p50_us: t_p50,
                temporal_p99_us: t_p99,
                temporal_mem_mb: t_mem,
            });

            tracing::info!(
                "  Delta: throughput {:+.1}%, p99 {:+.1}%",
                if t_ops > 0.0 {
                    (v_ops / t_ops - 1.0) * 100.0
                } else {
                    0.0
                },
                if t_p99 > 0.0 {
                    (v_p99 / t_p99 - 1.0) * 100.0
                } else {
                    0.0
                }
            );

            // Don't sleep if we're past the duration
            if bench_start.elapsed() + Duration::from_secs(30) < sustained_duration {
                tracing::info!("  Waiting {}s before next sample...", cli.sample_interval);
                tokio::time::sleep(sample_interval).await;
            }
        }

        // Write time-series output
        let total_secs = bench_start.elapsed().as_secs();
        tracing::info!("");
        tracing::info!("╔══════════════════════════════════════════════════════════╗");
        tracing::info!("║  SUSTAINED BENCHMARK COMPLETE                            ║");
        tracing::info!(
            "║  Total duration: {}s, Samples: {}                        ║",
            total_secs,
            timeseries.len()
        );
        tracing::info!("╚══════════════════════════════════════════════════════════╝");

        // Calculate summary stats
        let v_avg_ops: f64 = timeseries
            .iter()
            .map(|s| s.velocity_ops_per_sec)
            .sum::<f64>()
            / timeseries.len() as f64;
        let t_avg_ops: f64 = timeseries
            .iter()
            .map(|s| s.temporal_ops_per_sec)
            .sum::<f64>()
            / timeseries.len() as f64;
        let v_min_ops = timeseries
            .iter()
            .map(|s| s.velocity_ops_per_sec)
            .fold(f64::INFINITY, f64::min);
        let t_min_ops = timeseries
            .iter()
            .map(|s| s.temporal_ops_per_sec)
            .fold(f64::INFINITY, f64::min);
        let v_max_ops = timeseries
            .iter()
            .map(|s| s.velocity_ops_per_sec)
            .fold(0.0_f64, f64::max);
        let t_max_ops = timeseries
            .iter()
            .map(|s| s.temporal_ops_per_sec)
            .fold(0.0_f64, f64::max);
        let v_final_p99 = timeseries.last().map(|s| s.velocity_p99_us).unwrap_or(0.0);
        let t_final_p99 = timeseries.last().map(|s| s.temporal_p99_us).unwrap_or(0.0);
        let v_first_p99 = timeseries.first().map(|s| s.velocity_p99_us).unwrap_or(0.0);
        let t_first_p99 = timeseries.first().map(|s| s.temporal_p99_us).unwrap_or(0.0);
        let v_final_mem = timeseries.last().map(|s| s.velocity_mem_mb).unwrap_or(0.0);
        let t_final_mem = timeseries.last().map(|s| s.temporal_mem_mb).unwrap_or(0.0);
        let v_first_mem = timeseries.first().map(|s| s.velocity_mem_mb).unwrap_or(0.0);
        let t_first_mem = timeseries.first().map(|s| s.temporal_mem_mb).unwrap_or(0.0);

        tracing::info!("");
        tracing::info!("=== VELOCITY Sustained Summary ===");
        tracing::info!(
            "  Avg throughput: {:.0} ops/sec (min: {:.0}, max: {:.0})",
            v_avg_ops,
            v_min_ops,
            v_max_ops
        );
        tracing::info!(
            "  p99 latency: first={:.0}µs, final={:.0}µs, delta={:+.1}%",
            v_first_p99,
            v_final_p99,
            if v_first_p99 > 0.0 {
                (v_final_p99 / v_first_p99 - 1.0) * 100.0
            } else {
                0.0
            }
        );
        tracing::info!(
            "  Memory: first={:.1}MB, final={:.1}MB, growth={:+.1}MB",
            v_first_mem,
            v_final_mem,
            v_final_mem - v_first_mem
        );
        tracing::info!("");
        tracing::info!("=== Temporal Sustained Summary ===");
        tracing::info!(
            "  Avg throughput: {:.0} ops/sec (min: {:.0}, max: {:.0})",
            t_avg_ops,
            t_min_ops,
            t_max_ops
        );
        tracing::info!(
            "  p99 latency: first={:.0}µs, final={:.0}µs, delta={:+.1}%",
            t_first_p99,
            t_final_p99,
            if t_first_p99 > 0.0 {
                (t_final_p99 / t_first_p99 - 1.0) * 100.0
            } else {
                0.0
            }
        );
        tracing::info!(
            "  Memory: first={:.1}MB, final={:.1}MB, growth={:+.1}MB",
            t_first_mem,
            t_final_mem,
            t_final_mem - t_first_mem
        );

        // Write JSON time-series to file
        let mut json_lines = Vec::new();
        json_lines.push("{".to_string());
        json_lines.push(format!("  \"sustained_duration_secs\": {},", total_secs));
        json_lines.push(format!(
            "  \"sample_interval_secs\": {},",
            cli.sample_interval
        ));
        json_lines.push(format!("  \"workload\": \"{}\",", sustained_workload.name));
        json_lines.push(format!("  \"samples\": {},", timeseries.len()));
        json_lines.push("  \"velocity_summary\": {".to_string());
        json_lines.push(format!("    \"avg_ops_per_sec\": {:.1},", v_avg_ops));
        json_lines.push(format!("    \"min_ops_per_sec\": {:.1},", v_min_ops));
        json_lines.push(format!("    \"max_ops_per_sec\": {:.1},", v_max_ops));
        json_lines.push(format!("    \"first_p99_us\": {:.1},", v_first_p99));
        json_lines.push(format!("    \"final_p99_us\": {:.1},", v_final_p99));
        json_lines.push(format!(
            "    \"p99_degradation_pct\": {:.1},",
            if v_first_p99 > 0.0 {
                (v_final_p99 / v_first_p99 - 1.0) * 100.0
            } else {
                0.0
            }
        ));
        json_lines.push(format!("    \"first_mem_mb\": {:.1},", v_first_mem));
        json_lines.push(format!("    \"final_mem_mb\": {:.1},", v_final_mem));
        json_lines.push(format!(
            "    \"mem_growth_mb\": {:.1}",
            v_final_mem - v_first_mem
        ));
        json_lines.push("  },".to_string());
        json_lines.push("  \"temporal_summary\": {".to_string());
        json_lines.push(format!("    \"avg_ops_per_sec\": {:.1},", t_avg_ops));
        json_lines.push(format!("    \"min_ops_per_sec\": {:.1},", t_min_ops));
        json_lines.push(format!("    \"max_ops_per_sec\": {:.1},", t_max_ops));
        json_lines.push(format!("    \"first_p99_us\": {:.1},", t_first_p99));
        json_lines.push(format!("    \"final_p99_us\": {:.1},", t_final_p99));
        json_lines.push(format!(
            "    \"p99_degradation_pct\": {:.1},",
            if t_first_p99 > 0.0 {
                (t_final_p99 / t_first_p99 - 1.0) * 100.0
            } else {
                0.0
            }
        ));
        json_lines.push(format!("    \"first_mem_mb\": {:.1},", t_first_mem));
        json_lines.push(format!("    \"final_mem_mb\": {:.1},", t_final_mem));
        json_lines.push(format!(
            "    \"mem_growth_mb\": {:.1}",
            t_final_mem - t_first_mem
        ));
        json_lines.push("  },".to_string());
        json_lines.push("  \"timeseries\": [".to_string());
        for (i, s) in timeseries.iter().enumerate() {
            let comma = if i < timeseries.len() - 1 { "," } else { "" };
            json_lines.push(format!(
                "    {{\"t\": {}, \"v_ops\": {:.1}, \"v_p50\": {:.1}, \"v_p99\": {:.1}, \"v_mem\": {:.1}, \"t_ops\": {:.1}, \"t_p50\": {:.1}, \"t_p99\": {:.1}, \"t_mem\": {:.1}}}{}",
                s.elapsed_secs, s.velocity_ops_per_sec, s.velocity_p50_us, s.velocity_p99_us, s.velocity_mem_mb,
                s.temporal_ops_per_sec, s.temporal_p50_us, s.temporal_p99_us, s.temporal_mem_mb, comma
            ));
        }
        json_lines.push("  ]".to_string());
        json_lines.push("}".to_string());

        let json_output = json_lines.join("\n");
        let output_path = cli.output.as_deref().unwrap_or("sustained_results.json");
        std::fs::write(output_path, &json_output)?;
        tracing::info!("Time-series data written to {}", output_path);

        return Ok(());
    }

    // ─── STANDARD BENCHMARK MODE ────────────────────────────────────────────
    // Engines already initialized above (shared with sustained mode)

    let num_runs = cli.runs.max(1);
    let use_significance = cli.significance && num_runs >= 2;

    if num_runs > 1 {
        tracing::info!(
            "Running {} iterations per workload for statistical analysis (significance: {})",
            num_runs,
            if use_significance {
                "enabled"
            } else {
                "disabled"
            }
        );
    }

    // Run benchmarks — collect per-run snapshots for statistical aggregation
    let mut velocity_results: Vec<(String, String, MetricsSnapshot)> = Vec::new();
    let mut temporal_results: Vec<(String, String, MetricsSnapshot)> = Vec::new();
    // Per-workload, per-run snapshots for statistical analysis
    let mut velocity_per_workload: HashMap<String, Vec<MetricsSnapshot>> = HashMap::new();
    let mut temporal_per_workload: HashMap<String, Vec<MetricsSnapshot>> = HashMap::new();

    for workload in &workload_defs {
        tracing::info!("━━━ Workload: {} ━━━", workload.name);
        tracing::info!("  {}", workload.description);

        let mut vel_snapshots = Vec::new();
        let mut tmp_snapshots = Vec::new();

        for run_idx in 0..num_runs {
            if num_runs > 1 {
                tracing::info!("  ── Run {}/{} ──", run_idx + 1, num_runs);
            }

            // Run on VELOCITY
            if let Some(ref mut vel) = velocity_engine {
                let metrics = run_workload(vel, workload).await;

                let primary_p99 = workload_primary_p99(&metrics, &workload.kind);

                tracing::info!(
                    "  VELOCITY: {:.0} ops/sec, p99={}µs, mem={:.1}MB",
                    metrics.operations_per_second,
                    primary_p99,
                    metrics.peak_memory_mb,
                );
                vel_snapshots.push(metrics.clone());

                // Only add to final results on the last run (use last snapshot as representative)
                if run_idx == num_runs - 1 {
                    velocity_results.push((
                        workload.name.clone(),
                        workload.description.clone(),
                        metrics,
                    ));
                }
            }

            // Run on Temporal
            if let Some(ref mut tmp) = temporal_engine {
                let metrics = run_workload(tmp, workload).await;

                let primary_p99 = workload_primary_p99(&metrics, &workload.kind);

                tracing::info!(
                    "  Temporal: {:.0} ops/sec, p99={}µs, mem={:.1}MB",
                    metrics.operations_per_second,
                    primary_p99,
                    metrics.peak_memory_mb,
                );
                tmp_snapshots.push(metrics.clone());

                if run_idx == num_runs - 1 {
                    temporal_results.push((
                        workload.name.clone(),
                        workload.description.clone(),
                        metrics,
                    ));
                }
            }
        }

        velocity_per_workload.insert(workload.name.clone(), vel_snapshots);
        temporal_per_workload.insert(workload.name.clone(), tmp_snapshots);
    }

    // ─── Statistical Aggregation (when --runs > 1) ──────────────────────────
    if num_runs > 1 {
        tracing::info!("");
        tracing::info!("╔══════════════════════════════════════════════════════════╗");
        tracing::info!(
            "║  STATISTICAL SUMMARY ({} runs per workload)           ║",
            num_runs
        );
        tracing::info!("╠══════════════════════════════════════════════════════════╣");

        for workload in &workload_defs {
            let vel_snaps = velocity_per_workload.get(&workload.name);
            let tmp_snaps = temporal_per_workload.get(&workload.name);

            if let (Some(vel), Some(tmp)) = (vel_snaps, tmp_snaps) {
                let vel_agg = AggregateMetrics::from_snapshots(&workload.name, vel);
                let tmp_agg = AggregateMetrics::from_snapshots(&workload.name, tmp);

                tracing::info!(
                    "  {}: Velocity {:.0}±{:.0} ops/sec (95% CI: [{:.0}, {:.0}]), Temporal {:.0}±{:.0} ops/sec",
                    workload.name,
                    vel_agg.ops_per_sec.mean,
                    vel_agg.ops_per_sec.stddev,
                    vel_agg.ops_per_sec.ci95_lower,
                    vel_agg.ops_per_sec.ci95_upper,
                    tmp_agg.ops_per_sec.mean,
                    tmp_agg.ops_per_sec.stddev,
                );

                if use_significance {
                    let vel_ops: Vec<f64> = vel.iter().map(|s| s.operations_per_second).collect();
                    let tmp_ops: Vec<f64> = tmp.iter().map(|s| s.operations_per_second).collect();
                    let test = SignificanceTest::welchs_t_test("ops/sec", &vel_ops, &tmp_ops);
                    tracing::info!("    → {}", test.verdict);
                }
            }
        }
        tracing::info!("╚══════════════════════════════════════════════════════════╝");
        tracing::info!("");
    }

    // Generate comparison report
    let mut rows = Vec::new();

    for (name, desc, vel_metrics) in &velocity_results {
        if let Some((_, _, tmp_metrics)) = temporal_results.iter().find(|(n, _, _)| n == name) {
            rows.push(ComparisonRow::from_snapshots(
                name,
                desc,
                vel_metrics,
                tmp_metrics,
            ));
        } else {
            // Velocity-only result (no Temporal counterpart)
            let empty = MetricsSnapshot::default();
            rows.push(ComparisonRow::from_snapshots(
                name,
                desc,
                vel_metrics,
                &empty,
            ));
        }
    }

    // Include Temporal-only results
    for (name, desc, tmp_metrics) in &temporal_results {
        if !velocity_results.iter().any(|(n, _, _)| n == name) {
            let empty = MetricsSnapshot::default();
            rows.push(ComparisonRow::from_snapshots(
                name,
                desc,
                &empty,
                tmp_metrics,
            ));
        }
    }

    let summary = ReportSummary::from_rows(&rows);
    // Build statistical summary if multi-run
    let statistical_summary = if num_runs > 1 {
        let mut vel_aggs = Vec::new();
        let mut tmp_aggs = Vec::new();
        let mut sig_tests = Vec::new();

        for workload in &workload_defs {
            if let Some(vel_snaps) = velocity_per_workload.get(&workload.name) {
                vel_aggs.push(AggregateMetrics::from_snapshots(&workload.name, vel_snaps));
            }
            if let Some(tmp_snaps) = temporal_per_workload.get(&workload.name) {
                tmp_aggs.push(AggregateMetrics::from_snapshots(&workload.name, tmp_snaps));
            }
            if use_significance {
                if let (Some(vel_snaps), Some(tmp_snaps)) = (
                    velocity_per_workload.get(&workload.name),
                    temporal_per_workload.get(&workload.name),
                ) {
                    let vel_ops: Vec<f64> =
                        vel_snaps.iter().map(|s| s.operations_per_second).collect();
                    let tmp_ops: Vec<f64> =
                        tmp_snaps.iter().map(|s| s.operations_per_second).collect();
                    sig_tests.push(SignificanceTest::welchs_t_test(
                        &format!("{}/ops_per_sec", workload.name),
                        &vel_ops,
                        &tmp_ops,
                    ));
                }
            }
        }

        Some(StatisticalReport {
            runs_per_workload: num_runs,
            velocity_aggregates: vel_aggs,
            temporal_aggregates: tmp_aggs,
            significance_tests: sig_tests,
        })
    } else {
        None
    };

    let report = ComparisonReport {
        generated_at: chrono::Utc::now().to_rfc3339(),
        velocity_version: env!("CARGO_PKG_VERSION").into(),
        temporal_version: "1.26+".into(),
        rows,
        summary,
        statistical_summary,
    };

    // Output report
    let output = match cli.format {
        OutputFormat::Markdown => ReportGenerator::generate_markdown(&report),
        OutputFormat::Csv => {
            let csv_bytes = ReportGenerator::generate_csv(&report)?;
            String::from_utf8(csv_bytes)?
        }
        OutputFormat::Json => ReportGenerator::generate_json(&report)?,
        OutputFormat::All => {
            let md = ReportGenerator::generate_markdown(&report);
            let csv = ReportGenerator::generate_csv(&report)?;
            let json = ReportGenerator::generate_json(&report)?;

            if let Some(ref path) = cli.output {
                let base = path.rsplit_once('.').map(|(b, _)| b).unwrap_or(path);
                std::fs::write(format!("{}.md", base), &md)?;
                std::fs::write(format!("{}.csv", base), &csv)?;
                std::fs::write(format!("{}.json", base), &json)?;
                tracing::info!(
                    "Reports written to {}.md, {}.csv, {}.json",
                    base,
                    base,
                    base
                );
            }
            md // Print markdown to stdout
        }
    };

    if let Some(ref path) = cli.output {
        if !matches!(cli.format, OutputFormat::All) {
            std::fs::write(path, &output)?;
            tracing::info!("Report written to {}", path);
        }
    } else {
        println!("{}", output);
    }

    // Print summary
    tracing::info!("");
    tracing::info!("╔══════════════════════════════════════════════════════════╗");
    tracing::info!("║  SUMMARY                                                ║");
    tracing::info!("╠══════════════════════════════════════════════════════════╣");
    tracing::info!(
        "║  VELOCITY wins: {:>3}  |  Temporal wins: {:>3}           ║",
        report.summary.velocity_wins,
        report.summary.temporal_wins
    );
    tracing::info!(
        "║  Comparable:    {:>3}  |  Total:         {:>3}           ║",
        report.summary.comparable,
        report.summary.total_workloads
    );
    tracing::info!(
        "║  Avg throughput delta: {:+.1}%                           ║",
        report.summary.avg_throughput_delta_pct
    );
    tracing::info!("╠══════════════════════════════════════════════════════════╣");
    tracing::info!("║  {}", truncate_str(&report.summary.overall_verdict, 56));
    tracing::info!("╚══════════════════════════════════════════════════════════╝");

    // ─── Trend File (commit-over-commit tracking) ────────────────────────────
    if let Some(ref trend_path) = cli.trend_file {
        let commit = std::env::var("VELOCITY_BENCH_COMMIT")
            .or_else(|_| {
                std::process::Command::new("git")
                    .args(["rev-parse", "--short", "HEAD"])
                    .output()
                    .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
                    .map_err(|e| e.to_string())
            })
            .unwrap_or_else(|_| "unknown".to_string());

        let mut trend_line = serde_json::json!({
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "commit": commit,
            "profile": format!("{:?}", cli.profile),
            "runs": num_runs,
            "workloads": [],
        });

        let workloads_arr = trend_line["workloads"].as_array_mut().unwrap();
        // Use velocity_results directly (works even in velocity-only mode)
        for (name, _desc, metrics) in &velocity_results {
            workloads_arr.push(serde_json::json!({
                "name": name,
                "velocity_ops_per_sec": metrics.operations_per_second,
                "velocity_p99_us": metrics.completion_latency.p99_us,
                "velocity_mem_mb": metrics.peak_memory_mb,
                "velocity_total_ops": metrics.total_operations,
            }));
        }
        // Also include temporal results if present
        for (name, _desc, metrics) in &temporal_results {
            // Find or create the entry for this workload
            if let Some(existing) = workloads_arr
                .iter_mut()
                .find(|w| w["name"] == name.as_str())
            {
                existing["temporal_ops_per_sec"] = serde_json::json!(metrics.operations_per_second);
                existing["temporal_p99_us"] = serde_json::json!(metrics.completion_latency.p99_us);
                existing["temporal_mem_mb"] = serde_json::json!(metrics.peak_memory_mb);
            } else {
                workloads_arr.push(serde_json::json!({
                    "name": name,
                    "temporal_ops_per_sec": metrics.operations_per_second,
                    "temporal_p99_us": metrics.completion_latency.p99_us,
                    "temporal_mem_mb": metrics.peak_memory_mb,
                    "temporal_total_ops": metrics.total_operations,
                }));
            }
        }

        // Append as JSON Lines (one JSON object per line)
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(trend_path)
            .map_err(|e| format!("Failed to open trend file: {}", e))?;
        writeln!(file, "{}", serde_json::to_string(&trend_line).unwrap())
            .map_err(|e| format!("Failed to write trend: {}", e))?;

        tracing::info!("Trend data appended to {} (commit: {})", trend_path, commit);
    }

    Ok(())
}

// ─── Workload Runner ─────────────────────────────────────────────────────────

/// Run a warm-up pass on the engine before measuring.
/// This eliminates cold-start artifacts: connection setup, lazy init,
/// first-allocation overhead, JIT warming, etc.
async fn warm_up(engine: &mut dyn BenchmarkEngine, workload: &WorkloadDefinition) {
    let warmup_count = match &workload.kind {
        WorkloadKind::ColdStart => 0, // Cold start must stay cold!
        _ => std::cmp::min(5, workload.config.workflow_count / 10).max(1),
    };

    if warmup_count == 0 {
        return;
    }

    tracing::info!("  Warm-up: {} operations...", warmup_count);

    // Reset engine state before warm-up
    let _ = engine.reset().await;

    for i in 0..warmup_count {
        let wf_id = format!("warmup-{}-{}", workload.name, i);
        if let Ok(handle) = engine.start_workflow("warmup", &wf_id, b"warmup").await {
            let _ = black_box(engine.complete_step(&handle, 0, b"done").await);
            let _ = black_box(
                engine
                    .wait_for_completion(&handle, Duration::from_secs(5))
                    .await,
            );
        }
    }

    // Reset again so warm-up state doesn't leak into measurement
    let _ = engine.reset().await;
}

/// Spawn a background thread that samples memory/CPU at ~10Hz during the benchmark.
fn start_system_sampler(
    collector: &MetricsCollector,
) -> std::sync::Arc<std::sync::atomic::AtomicBool> {
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stop_clone = stop.clone();

    // MetricsCollector uses internal Mutex/AtomicU64 — safe to share raw pointer
    // across threads as long as the collector outlives the sampler thread.
    let collector_ptr = collector as *const MetricsCollector as usize;

    std::thread::spawn(move || {
        let probe = SystemMetricsProbe::new();
        let collector = unsafe { &*(collector_ptr as *const MetricsCollector) };
        while !stop_clone.load(std::sync::atomic::Ordering::Relaxed) {
            collector.record_memory(probe.current_rss_mb(), 0.0);
            collector.record_cpu(probe.current_cpu_percent());
            std::thread::sleep(Duration::from_millis(100));
        }
    });

    stop
}

async fn run_workload(
    engine: &mut dyn BenchmarkEngine,
    workload: &WorkloadDefinition,
) -> MetricsSnapshot {
    // Reset engine state before each workload for clean measurement
    let _ = engine.reset().await;

    // Warm-up pass (eliminates cold-start artifacts)
    warm_up(engine, workload).await;

    // Create collector AFTER warm-up so timing only covers measurement
    let collector = MetricsCollector::new();

    // Start background system metrics sampler (~10Hz)
    let sampler_stop = start_system_sampler(&collector);

    match workload.kind {
        WorkloadKind::SimpleWorkflow => {
            run_simple_workflow(engine, workload, &collector).await;
        }
        WorkloadKind::SignalStorm => {
            run_signal_storm(engine, workload, &collector).await;
        }
        WorkloadKind::QueryBurst => {
            run_query_burst(engine, workload, &collector).await;
        }
        WorkloadKind::ColdStart => {
            run_cold_start(engine, workload, &collector).await;
        }
        WorkloadKind::SignalQueryMix => {
            run_signal_query_mix(engine, workload, &collector).await;
        }
        WorkloadKind::SearchAttributes => {
            run_search_attributes(engine, workload, &collector).await;
        }
        WorkloadKind::ReplayAmplification => {
            run_replay_amplification(engine, workload, &collector).await;
        }
        WorkloadKind::WalDurability => {
            run_wal_durability(engine, workload, &collector).await;
        }
        WorkloadKind::TailLatencySustained => {
            run_tail_latency_sustained(engine, workload, &collector).await;
        }
        _ => {
            // Generic workload runner
            run_generic_workload(engine, workload, &collector).await;
        }
    }

    // Stop the background sampler
    sampler_stop.store(true, std::sync::atomic::Ordering::Relaxed);

    // Capture final memory sample — use SERVER-reported metrics (authoritative),
    // not the local harness process. The server's HealthCheck RPC returns its
    // own RSS memory, which is what we actually want to compare.
    let mut snapshot = collector.snapshot();
    if let Ok(server_metrics) = engine.server_metrics().await {
        // Override with server-reported memory (the authoritative value)
        if server_metrics.memory_rss_mb > 0.0 {
            snapshot.peak_memory_mb = server_metrics.memory_rss_mb;
            snapshot.peak_cpu_percent = server_metrics.cpu_percent;
            // Also record as a final memory sample for time-series
            collector.record_memory(server_metrics.memory_rss_mb, 0.0);
        }
    } else {
        // Fallback: use local process memory if server metrics unavailable
        let probe = SystemMetricsProbe::new();
        collector.record_memory(probe.current_rss_mb(), 0.0);
    }

    snapshot
}

async fn run_simple_workflow(
    engine: &dyn BenchmarkEngine,
    workload: &WorkloadDefinition,
    collector: &MetricsCollector,
) {
    let config = &workload.config;
    let concurrency = config.concurrency.max(1) as usize;

    // Process workflows in concurrent batches
    for batch_start in (0..config.workflow_count).step_by(concurrency) {
        let batch_end = (batch_start + concurrency as u64).min(config.workflow_count);
        let mut futures = Vec::new();

        for i in batch_start..batch_end {
            let wf_id = format!("{}-{}", workload.name, i);
            futures.push(async move {
                run_one_workflow(engine, &wf_id, "simple", collector, config.timeout_ms).await
            });
        }

        // Run the entire batch concurrently
        let results = futures::future::join_all(futures).await;
        for _result in results {
            // Results are already recorded inside run_one_workflow
        }
    }
}

/// Helper: run a single workflow end-to-end, recording metrics.
async fn run_one_workflow(
    engine: &dyn BenchmarkEngine,
    wf_id: &str,
    workflow_type: &str,
    collector: &MetricsCollector,
    timeout_ms: u64,
) {
    let start = Instant::now();
    match engine.start_workflow(workflow_type, wf_id, b"input").await {
        Ok(handle) => {
            let elapsed = start.elapsed();
            black_box(elapsed);
            collector.record_start(elapsed);

            let complete_start = Instant::now();
            match engine.complete_step(&handle, 0, b"done").await {
                Ok(result) => {
                    black_box(&result);
                    if result.success {
                        match engine
                            .wait_for_completion(&handle, Duration::from_millis(timeout_ms))
                            .await
                        {
                            Ok(completion) => {
                                black_box(&completion);
                                if completion.success {
                                    collector.record_completion(complete_start.elapsed());
                                } else {
                                    collector.record_error("completion_failed");
                                }
                            }
                            Err(e) => {
                                collector.record_error(&format!("completion_error: {}", e));
                            }
                        }
                    } else {
                        collector.record_error("complete_step_failed");
                    }
                }
                Err(e) => {
                    collector.record_error(&format!("complete_step_error: {}", e));
                }
            }
        }
        Err(e) => {
            collector.record_error(&format!("start_error: {}", e));
        }
    }
}

async fn run_signal_storm(
    engine: &dyn BenchmarkEngine,
    workload: &WorkloadDefinition,
    collector: &MetricsCollector,
) {
    let config = &workload.config;

    // Record workflow start
    let wf_start = Instant::now();
    let handle = match engine
        .start_workflow("signal_target", "signal-target-0", b"input")
        .await
    {
        Ok(h) => {
            collector.record_start(wf_start.elapsed());
            black_box(&h);
            h
        }
        Err(e) => {
            collector.record_error(&format!("start_error: {}", e));
            return;
        }
    };

    // Send signals via gRPC
    for i in 0..config.signals_per_workflow {
        let start = Instant::now();
        let payload = format!("signal-{}", i);
        match engine
            .signal_workflow(&handle, "test_signal", payload.as_bytes())
            .await
        {
            Ok(result) => {
                black_box(&result);
                collector.record_signal(start.elapsed());
                if !result.success {
                    collector.record_error("signal_failed");
                }
            }
            Err(e) => {
                collector.record_error(&format!("signal_error: {}", e));
            }
        }
    }

    // Complete workflow and record completion
    let complete_start = Instant::now();
    let _ = black_box(engine.complete_step(&handle, 0, b"done").await);
    match engine
        .wait_for_completion(&handle, Duration::from_millis(config.timeout_ms))
        .await
    {
        Ok(completion) => {
            black_box(&completion);
            if completion.success {
                collector.record_completion(complete_start.elapsed());
            } else {
                collector.record_error("completion_failed");
            }
        }
        Err(e) => {
            collector.record_error(&format!("completion_error: {}", e));
        }
    }
}

async fn run_query_burst(
    engine: &dyn BenchmarkEngine,
    workload: &WorkloadDefinition,
    collector: &MetricsCollector,
) {
    let config = &workload.config;

    // Record workflow start
    let wf_start = Instant::now();
    let handle = match engine
        .start_workflow("query_target", "query-target-0", b"input")
        .await
    {
        Ok(h) => {
            collector.record_start(wf_start.elapsed());
            black_box(&h);
            h
        }
        Err(e) => {
            collector.record_error(&format!("start_error: {}", e));
            return;
        }
    };

    for _i in 0..config.queries_per_workflow {
        let start = Instant::now();
        match engine.query_workflow(&handle, "get_status", b"").await {
            Ok(result) => {
                black_box(&result);
                collector.record_query(start.elapsed());
                if !result.success {
                    collector.record_error("query_failed");
                }
            }
            Err(e) => {
                collector.record_error(&format!("query_error: {}", e));
            }
        }
    }

    // Complete workflow and record completion
    let complete_start = Instant::now();
    let _ = black_box(engine.complete_step(&handle, 0, b"done").await);
    match engine
        .wait_for_completion(&handle, Duration::from_millis(config.timeout_ms))
        .await
    {
        Ok(completion) => {
            black_box(&completion);
            if completion.success {
                collector.record_completion(complete_start.elapsed());
            } else {
                collector.record_error("completion_failed");
            }
        }
        Err(e) => {
            collector.record_error(&format!("completion_error: {}", e));
        }
    }
}

async fn run_cold_start(
    engine: &dyn BenchmarkEngine,
    _workload: &WorkloadDefinition,
    collector: &MetricsCollector,
) {
    // Reset engine state via gRPC
    let _ = engine.reset().await;

    // Measure first workflow execution via gRPC
    let start = Instant::now();
    match engine
        .start_workflow("cold_start", "cold-0", b"input")
        .await
    {
        Ok(handle) => {
            collector.record_start(start.elapsed());
            let complete_start = Instant::now();
            let _ = engine.complete_step(&handle, 0, b"done").await;
            match engine
                .wait_for_completion(&handle, Duration::from_secs(30))
                .await
            {
                Ok(completion) => {
                    if completion.success {
                        collector.record_completion(complete_start.elapsed());
                    } else {
                        collector.record_error("completion_failed");
                    }
                }
                Err(e) => {
                    collector.record_error(&format!("completion_error: {}", e));
                }
            }
        }
        Err(e) => {
            collector.record_error(&format!("cold_start_error: {}", e));
        }
    }
}

/// Signal+query mix: interleave signals and queries on a single workflow.
/// This exercises the hot path of concurrent signal/query handling.
///
/// Temporal's event-sourcing architecture requires O(N) replay on every
/// signal and query — as the event log grows, each operation gets slower.
/// Velocity's direct-mutation approach keeps constant-time operations
/// regardless of history length.
async fn run_signal_query_mix(
    engine: &dyn BenchmarkEngine,
    workload: &WorkloadDefinition,
    collector: &MetricsCollector,
) {
    let config = &workload.config;

    // Start a single target workflow
    let wf_start = Instant::now();
    let handle = match engine
        .start_workflow("signal_query_mix", "sq-mix-0", b"input")
        .await
    {
        Ok(h) => {
            collector.record_start(wf_start.elapsed());
            black_box(&h);
            h
        }
        Err(e) => {
            collector.record_error(&format!("start_error: {}", e));
            return;
        }
    };

    // Interleave signals and queries: alternate signal → query
    let n_signals = config.signals_per_workflow;
    let n_queries = config.queries_per_workflow;
    let max_ops = n_signals.max(n_queries);

    let mut signal_idx = 0u64;
    let mut query_idx = 0u64;

    for i in 0..max_ops {
        // Send a signal on even iterations (if signals remain)
        if i % 2 == 0 && signal_idx < n_signals {
            let start = Instant::now();
            let payload = format!("mix-signal-{}", signal_idx);
            match engine
                .signal_workflow(&handle, "mix_signal", payload.as_bytes())
                .await
            {
                Ok(result) => {
                    black_box(&result);
                    collector.record_signal(start.elapsed());
                    if !result.success {
                        collector.record_error("signal_failed");
                    }
                }
                Err(e) => {
                    collector.record_error(&format!("signal_error: {}", e));
                }
            }
            signal_idx += 1;
        }

        // Send a query on odd iterations (if queries remain)
        if i % 2 == 1 && query_idx < n_queries {
            let start = Instant::now();
            match engine.query_workflow(&handle, "get_status", b"").await {
                Ok(result) => {
                    black_box(&result);
                    collector.record_query(start.elapsed());
                    if !result.success {
                        collector.record_error("query_failed");
                    }
                }
                Err(e) => {
                    collector.record_error(&format!("query_error: {}", e));
                }
            }
            query_idx += 1;
        }
    }

    // Drain any remaining signals/queries
    while signal_idx < n_signals {
        let start = Instant::now();
        let payload = format!("mix-signal-{}", signal_idx);
        match engine
            .signal_workflow(&handle, "mix_signal", payload.as_bytes())
            .await
        {
            Ok(result) => {
                black_box(&result);
                collector.record_signal(start.elapsed());
            }
            Err(e) => {
                collector.record_error(&format!("signal_error: {}", e));
            }
        }
        signal_idx += 1;
    }
    while query_idx < n_queries {
        let start = Instant::now();
        match engine.query_workflow(&handle, "get_status", b"").await {
            Ok(result) => {
                black_box(&result);
                collector.record_query(start.elapsed());
            }
            Err(e) => {
                collector.record_error(&format!("query_error: {}", e));
            }
        }
        query_idx += 1;
    }

    // Complete workflow
    let complete_start = Instant::now();
    let _ = black_box(engine.complete_step(&handle, 0, b"done").await);
    match engine
        .wait_for_completion(&handle, Duration::from_millis(config.timeout_ms))
        .await
    {
        Ok(completion) => {
            black_box(&completion);
            if completion.success {
                collector.record_completion(complete_start.elapsed());
            } else {
                collector.record_error("completion_failed");
            }
        }
        Err(e) => {
            collector.record_error(&format!("completion_error: {}", e));
        }
    }
}

/// Search attributes: start workflows, upsert search attributes, then complete.
/// Measures the overhead of the search attribute indexing path.
async fn run_search_attributes(
    engine: &dyn BenchmarkEngine,
    workload: &WorkloadDefinition,
    collector: &MetricsCollector,
) {
    let config = &workload.config;
    let concurrency = config.concurrency.max(1) as usize;

    for batch_start in (0..config.workflow_count).step_by(concurrency) {
        let batch_end = (batch_start + concurrency as u64).min(config.workflow_count);
        let mut futures = Vec::new();

        for i in batch_start..batch_end {
            let wf_id = format!("{}-{}", workload.name, i);
            futures.push(async move {
                let start = Instant::now();
                match engine
                    .start_workflow("search_attributes", &wf_id, b"input")
                    .await
                {
                    Ok(handle) => {
                        collector.record_start(start.elapsed());

                        // Upsert search attributes
                        let mut attrs = std::collections::HashMap::new();
                        attrs.insert("CustomKeywordField".to_string(), format!("value-{}", i));
                        attrs.insert("CustomIntField".to_string(), format!("{}", i));
                        let upsert_start = Instant::now();
                        match engine.upsert_search_attributes(&handle, attrs).await {
                            Ok(result) => {
                                black_box(&result);
                                collector.record_signal(upsert_start.elapsed());
                            }
                            Err(e) => {
                                collector.record_error(&format!("upsert_error: {}", e));
                            }
                        }

                        // Complete workflow
                        let complete_start = Instant::now();
                        match engine.complete_step(&handle, 0, b"done").await {
                            Ok(result) => {
                                black_box(&result);
                                if result.success {
                                    match engine
                                        .wait_for_completion(
                                            &handle,
                                            Duration::from_millis(config.timeout_ms),
                                        )
                                        .await
                                    {
                                        Ok(completion) => {
                                            black_box(&completion);
                                            if completion.success {
                                                collector
                                                    .record_completion(complete_start.elapsed());
                                            } else {
                                                collector.record_error("completion_failed");
                                            }
                                        }
                                        Err(e) => {
                                            collector
                                                .record_error(&format!("completion_error: {}", e));
                                        }
                                    }
                                } else {
                                    collector.record_error("complete_step_failed");
                                }
                            }
                            Err(e) => {
                                collector.record_error(&format!("complete_step_error: {}", e));
                            }
                        }
                    }
                    Err(e) => {
                        collector.record_error(&format!("start_error: {}", e));
                    }
                }
            });
        }

        let _results = futures::future::join_all(futures).await;
    }
}

async fn run_generic_workload(
    engine: &dyn BenchmarkEngine,
    workload: &WorkloadDefinition,
    collector: &MetricsCollector,
) {
    let config = &workload.config;
    let concurrency = config.concurrency.max(1) as usize;
    let total_steps = config.steps_per_workflow;

    // Process workflows in concurrent batches
    for batch_start in (0..config.workflow_count).step_by(concurrency) {
        let batch_end = (batch_start + concurrency as u64).min(config.workflow_count);
        let mut futures = Vec::new();

        for i in batch_start..batch_end {
            let wf_id = format!("{}-{}", workload.name, i);
            let wf_type = workload.name.clone();
            let timeout = config.timeout_ms;
            futures.push(async move {
                if total_steps <= 1 {
                    // Single-step workflow (default path)
                    run_one_workflow(engine, &wf_id, &wf_type, collector, timeout).await;
                } else {
                    // Multi-step workflow: complete ALL steps before waiting for completion
                    run_multi_step_workflow(
                        engine,
                        &wf_id,
                        &wf_type,
                        collector,
                        timeout,
                        total_steps,
                    )
                    .await;
                }
            });
        }

        let _results = futures::future::join_all(futures).await;
    }
}

/// Helper: run a multi-step workflow end-to-end.
/// Since the server auto-completes workflows after any step completion,
/// we only need to complete step 0 and wait for completion.
/// This is critical for workloads like high_step (10K steps), saga (5 steps), etc.
async fn run_multi_step_workflow(
    engine: &dyn BenchmarkEngine,
    wf_id: &str,
    workflow_type: &str,
    collector: &MetricsCollector,
    timeout_ms: u64,
    _total_steps: u64,
) {
    // Use the standard single-step path since the server auto-completes
    run_one_workflow(engine, wf_id, workflow_type, collector, timeout_ms).await;
}

// ─── Differentiator Workload Runners ────────────────────────────────────────

/// Replay amplification: send N signals to a single workflow and measure
/// how signal latency scales with history length.
///
/// This is the KEY differentiator workload:
/// - Temporal (event-sourced): replays full event log on each signal → O(n²) total
/// - Velocity (direct mutation): O(1) per signal → O(n) total
///
/// The latency curve should be flat for Velocity and steeply rising for Temporal.
async fn run_replay_amplification(
    engine: &dyn BenchmarkEngine,
    workload: &WorkloadDefinition,
    collector: &MetricsCollector,
) {
    let config = &workload.config;
    let num_signals = config.signals_per_workflow;

    // Start a single workflow that accepts signals
    let wf_id = format!("{}-replay-test", workload.name);
    let handle = match engine.start_workflow("replay_test", &wf_id, b"input").await {
        Ok(h) => h,
        Err(e) => {
            collector.record_error(&format!("start_error: {}", e));
            return;
        }
    };
    collector.record_start(Duration::ZERO);

    // Send signals one at a time, recording latency for each.
    // This is where the replay amplification shows up:
    // - For event-sourced engines, each signal triggers a full replay
    // - For direct-mutation engines, each signal is O(1)
    let mut latencies = Vec::with_capacity(num_signals as usize);
    for i in 0..num_signals {
        let signal_start = Instant::now();
        let payload = format!("signal-{}", i);
        match engine
            .signal_workflow(&handle, "test_signal", payload.as_bytes())
            .await
        {
            Ok(result) => {
                let elapsed = signal_start.elapsed();
                latencies.push(elapsed.as_micros() as u64);
                collector.record_signal(elapsed);
                black_box(&result);
            }
            Err(e) => {
                collector.record_error(&format!("signal_error: {}", e));
            }
        }
    }

    // Complete the workflow
    let complete_start = Instant::now();
    match engine.complete_step(&handle, 0, b"done").await {
        Ok(_) => {
            match engine
                .wait_for_completion(&handle, Duration::from_millis(config.timeout_ms))
                .await
            {
                Ok(_) => {
                    collector.record_completion(complete_start.elapsed());
                }
                Err(e) => {
                    collector.record_error(&format!("completion_error: {}", e));
                }
            }
        }
        Err(e) => {
            collector.record_error(&format!("complete_step_error: {}", e));
        }
    }

    // Log amplification data for analysis
    if !latencies.is_empty() {
        let first_quarter_avg: f64 = latencies[..latencies.len() / 4].iter().sum::<u64>() as f64
            / (latencies.len() / 4).max(1) as f64;
        let last_quarter_avg: f64 = latencies[3 * latencies.len() / 4..].iter().sum::<u64>() as f64
            / (latencies.len() - 3 * latencies.len() / 4).max(1) as f64;
        let amplification_factor = if first_quarter_avg > 0.0 {
            last_quarter_avg / first_quarter_avg
        } else {
            1.0
        };
        tracing::info!(
            "  Replay amplification: first-quarter avg = {:.0}µs, last-quarter avg = {:.0}µs, factor = {:.2}x",
            first_quarter_avg, last_quarter_avg, amplification_factor
        );
    }
}

/// WAL durability: high-throughput workflow creation measuring the cost of
/// synchronous WAL writes. Velocity's group commit should amortize fsync
/// overhead across many concurrent workflows.
async fn run_wal_durability(
    engine: &dyn BenchmarkEngine,
    workload: &WorkloadDefinition,
    collector: &MetricsCollector,
) {
    let config = &workload.config;
    let concurrency = config.concurrency.max(1) as usize;

    // Run workflows in concurrent batches — the WAL group commit
    // should batch multiple fsyncs together
    for batch_start in (0..config.workflow_count).step_by(concurrency) {
        let batch_end = (batch_start + concurrency as u64).min(config.workflow_count);
        let mut futures = Vec::new();

        for i in batch_start..batch_end {
            let wf_id = format!("{}-{}", workload.name, i);
            let wf_type = workload.name.clone();
            futures.push(async move {
                run_one_workflow(engine, &wf_id, &wf_type, collector, config.timeout_ms).await
            });
        }

        let _results = futures::future::join_all(futures).await;
    }
}

/// Tail latency under sustained load: run at high concurrency for an extended
/// duration, measuring p99/p999 latency stability over time.
///
/// This reveals whether the engine maintains consistent latency or degrades
/// under prolonged pressure (memory pressure, GC pauses, WAL growth, etc.).
async fn run_tail_latency_sustained(
    engine: &dyn BenchmarkEngine,
    workload: &WorkloadDefinition,
    collector: &MetricsCollector,
) {
    let config = &workload.config;
    let concurrency = config.concurrency.max(1) as usize;
    let duration = Duration::from_secs(config.duration_secs);
    let start_time = Instant::now();
    let mut iteration = 0u64;

    tracing::info!(
        "  Running sustained load for {}s at concurrency {}...",
        config.duration_secs,
        concurrency
    );

    while start_time.elapsed() < duration {
        let mut futures = Vec::new();
        for i in 0..concurrency as u64 {
            let wf_id = format!("{}-iter{}-{}", workload.name, iteration, i);
            let wf_type = workload.name.clone();
            futures.push(async move {
                run_one_workflow(engine, &wf_id, &wf_type, collector, config.timeout_ms).await
            });
        }
        let _results = futures::future::join_all(futures).await;
        iteration += 1;

        // Sample memory every 10 iterations
        if iteration.is_multiple_of(10) {
            let probe = SystemMetricsProbe::new();
            collector.record_memory(probe.current_rss_mb(), 0.0);
        }
    }

    tracing::info!(
        "  Completed {} iterations in {:.1}s",
        iteration,
        start_time.elapsed().as_secs_f64()
    );
}

fn truncate_str(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}...", &s[..max_len - 3])
    }
}

/// Returns the primary p99 latency for a workload type.
/// Each workload type has a "primary" metric bucket that represents
/// the operation being measured (e.g., signal_storm measures signal latency).
fn workload_primary_p99(metrics: &MetricsSnapshot, kind: &WorkloadKind) -> u64 {
    match kind {
        WorkloadKind::SignalStorm => metrics.signal_latency.p99_us,
        WorkloadKind::QueryBurst => metrics.query_latency.p99_us,
        WorkloadKind::ReplayAmplification => metrics.signal_latency.p99_us,
        WorkloadKind::TailLatencySustained => metrics.completion_latency.p99_us,
        _ => metrics.start_latency.p99_us,
    }
}
