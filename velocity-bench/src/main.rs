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
use std::time::{Duration, Instant};
use velocity_bench::metrics::*;
use velocity_bench::report::*;
use velocity_bench::*;

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

    // Apply profile
    let workload_defs: Vec<WorkloadDefinition> = workload_defs
        .into_iter()
        .map(|mut w| {
            w.config = match cli.profile {
                WorkloadProfile::Quick => WorkloadConfig::quick(),
                WorkloadProfile::Standard => WorkloadConfig::standard(),
                WorkloadProfile::Stress => WorkloadConfig::stress(),
            };
            w
        })
        .collect();

    // Initialize engines — both via gRPC
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

    // Run benchmarks
    let mut velocity_results: Vec<(String, String, MetricsSnapshot)> = Vec::new();
    let mut temporal_results: Vec<(String, String, MetricsSnapshot)> = Vec::new();

    for workload in &workload_defs {
        tracing::info!("━━━ Workload: {} ━━━", workload.name);
        tracing::info!("  {}", workload.description);

        // Run on VELOCITY
        if let Some(ref mut vel) = velocity_engine {
            let metrics = run_workload(vel, workload).await;
            tracing::info!(
                "  VELOCITY: {:.0} ops/sec, p99={}µs, mem={:.1}MB",
                metrics.operations_per_second,
                metrics.start_latency.p99_us,
                metrics.peak_memory_mb,
            );
            velocity_results.push((workload.name.clone(), workload.description.clone(), metrics));
        }

        // Run on Temporal
        if let Some(ref mut tmp) = temporal_engine {
            let metrics = run_workload(tmp, workload).await;
            tracing::info!(
                "  Temporal: {:.0} ops/sec, p99={}µs, mem={:.1}MB",
                metrics.operations_per_second,
                metrics.start_latency.p99_us,
                metrics.peak_memory_mb,
            );
            temporal_results.push((workload.name.clone(), workload.description.clone(), metrics));
        }
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
        }
    }

    let summary = ReportSummary::from_rows(&rows);
    let report = ComparisonReport {
        generated_at: chrono::Utc::now().to_rfc3339(),
        velocity_version: env!("CARGO_PKG_VERSION").into(),
        temporal_version: "1.26+".into(),
        rows,
        summary,
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

    Ok(())
}

// ─── Workload Runner ─────────────────────────────────────────────────────────

async fn run_workload(
    engine: &mut dyn BenchmarkEngine,
    workload: &WorkloadDefinition,
) -> MetricsSnapshot {
    let collector = MetricsCollector::new();
    let probe = SystemMetricsProbe::new();

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
        _ => {
            // Generic workload runner
            run_generic_workload(engine, workload, &collector).await;
        }
    }

    // Capture final memory sample
    collector.record_memory(probe.current_rss_mb(), 0.0);

    collector.snapshot()
}

async fn run_simple_workflow(
    engine: &dyn BenchmarkEngine,
    workload: &WorkloadDefinition,
    collector: &MetricsCollector,
) {
    let config = &workload.config;

    for i in 0..config.workflow_count {
        let start = Instant::now();
        let wf_id = format!("{}-{}", workload.name, i);

        match engine.start_workflow("simple", &wf_id, b"input").await {
            Ok(handle) => {
                collector.record_start(start.elapsed());

                // Drive the workflow to completion via gRPC (same path for both engines).
                let complete_start = Instant::now();
                match engine.complete_step(&handle, 0, b"done").await {
                    Ok(result) if result.success => {
                        // Now wait for the server to confirm completion.
                        match engine
                            .wait_for_completion(&handle, Duration::from_millis(config.timeout_ms))
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
                    Ok(_) => {
                        collector.record_error("complete_step_failed");
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
}

async fn run_signal_storm(
    engine: &dyn BenchmarkEngine,
    workload: &WorkloadDefinition,
    collector: &MetricsCollector,
) {
    let config = &workload.config;

    // Start workflow via gRPC
    let handle = engine
        .start_workflow("signal_target", "signal-target-0", b"input")
        .await
        .expect("Failed to start signal target workflow");

    // Send signals via gRPC
    for i in 0..config.signals_per_workflow {
        let start = Instant::now();
        let payload = format!("signal-{}", i);
        match engine
            .signal_workflow(&handle, "test_signal", payload.as_bytes())
            .await
        {
            Ok(result) => {
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

    // Complete via gRPC
    let _ = engine.complete_step(&handle, 0, b"done").await;
    let _ = engine
        .wait_for_completion(&handle, Duration::from_millis(config.timeout_ms))
        .await;
}

async fn run_query_burst(
    engine: &dyn BenchmarkEngine,
    workload: &WorkloadDefinition,
    collector: &MetricsCollector,
) {
    let config = &workload.config;

    let handle = engine
        .start_workflow("query_target", "query-target-0", b"input")
        .await
        .expect("Failed to start query target workflow");

    for _i in 0..config.queries_per_workflow {
        let start = Instant::now();
        match engine.query_workflow(&handle, "get_status", b"").await {
            Ok(result) => {
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

    let _ = engine.complete_step(&handle, 0, b"done").await;
    let _ = engine
        .wait_for_completion(&handle, Duration::from_millis(config.timeout_ms))
        .await;
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
            let _ = engine.complete_step(&handle, 0, b"done").await;
            let _ = engine
                .wait_for_completion(&handle, Duration::from_secs(30))
                .await;
        }
        Err(e) => {
            collector.record_error(&format!("cold_start_error: {}", e));
        }
    }
}

async fn run_generic_workload(
    engine: &dyn BenchmarkEngine,
    workload: &WorkloadDefinition,
    collector: &MetricsCollector,
) {
    let config = &workload.config;

    for i in 0..config.workflow_count {
        let start = Instant::now();
        let wf_id = format!("{}-{}", workload.name, i);

        match engine
            .start_workflow(&workload.name, &wf_id, b"input")
            .await
        {
            Ok(handle) => {
                collector.record_start(start.elapsed());
                let _ = engine.complete_step(&handle, 0, b"done").await;
                let _ = engine
                    .wait_for_completion(&handle, Duration::from_millis(config.timeout_ms))
                    .await;
            }
            Err(e) => {
                collector.record_error(&format!("error: {}", e));
            }
        }
    }
}

fn truncate_str(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}...", &s[..max_len - 3])
    }
}
