//! Production Benchmark Suite — Velocity vs DBOS vs Restate
//!
//! Benchmarks each engine through its REAL HTTP API with production deployments.
//! No mocks. Each engine runs with its actual persistence layer.
//!
//! Usage:
//!   prod-bench --engines all --profile standard
//!   prod-bench --engines velocity,dbos --profile quick
//!   prod-bench --engine velocity --workload simple_workflow

use clap::Parser;
use serde::{Deserialize, Serialize};
use std::time::Instant;
use tracing::{error, info, warn};

mod velocity_client;
mod velocity_embedded_client;
mod velocity_classic_client;
mod dbos_client;
mod restate_client;
mod workloads;

use velocity_client::VelocityClient;
use velocity_embedded_client::VelocityEmbeddedClient;
use velocity_classic_client::VelocityClassicClient;
use dbos_client::DbosClient;
use restate_client::RestateClient;
use workloads::WorkloadDef;

// ─── CLI ─────────────────────────────────────────────────────────────────────

#[derive(Parser)]
#[command(name = "prod-bench", about = "Production benchmark: Velocity vs DBOS vs Restate")]
struct Cli {
    /// Comma-separated engines to benchmark: velocity,velocity-embedded,velocity-classic,dbos,restate,all
    #[arg(long, default_value = "all")]
    engines: String,

    /// Single workload to run (runs all if omitted).
    #[arg(long)]
    workload: Option<String>,

    /// Profile: quick, standard, stress
    #[arg(long, default_value = "standard")]
    profile: String,

    /// Velocity Server gRPC address (production server with WAL)
    #[arg(long, env = "VELOCITY_URL", default_value = "http://localhost:7234")]
    velocity_url: String,

    /// Velocity Embedded HTTP address (PostgreSQL-backed)
    #[arg(long, env = "VELOCITY_EMBEDDED_URL", default_value = "http://localhost:8082")]
    velocity_embedded_url: String,

    /// Velocity Classic HTTP address (Temporal-compatible)
    #[arg(long, env = "VELOCITY_CLASSIC_URL", default_value = "http://localhost:8083")]
    velocity_classic_url: String,

    /// DBOS HTTP address
    #[arg(long, env = "DBOS_URL", default_value = "http://localhost:8081")]
    dbos_url: String,

    /// Restate HTTP address
    #[arg(long, env = "RESTATE_URL", default_value = "http://localhost:9070")]
    restate_url: String,

    /// Output format: json, markdown, csv
    #[arg(long, default_value = "markdown")]
    format: String,

    /// Output file path (stdout if omitted)
    #[arg(long, short)]
    output: Option<String>,
}

// ─── Result Types ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineResult {
    pub engine: String,
    pub engine_version: String,
    pub workloads: Vec<WorkloadResult>,
    pub timestamp: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkloadResult {
    pub name: String,
    pub description: String,
    pub total_operations: u64,
    pub successful_operations: u64,
    pub failed_operations: u64,
    pub ops_per_second: f64,
    pub latency_p50_us: f64,
    pub latency_p99_us: f64,
    pub latency_p999_us: f64,
    pub latency_mean_us: f64,
    pub peak_memory_mb: f64,
    pub error_rate_pct: f64,
}

// ─── Main ────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_target(false)
        .init();

    info!("╔══════════════════════════════════════════════════════════╗");
    info!("║  prod-bench — Production Workflow Engine Benchmark       ║");
    info!("║  Real engines. Real APIs. Real persistence.              ║");
    info!("╚══════════════════════════════════════════════════════════╝");

    let profile = match cli.profile.as_str() {
        "quick" => workloads::PROFILE_QUICK,
        "stress" => workloads::PROFILE_STRESS,
        _ => workloads::PROFILE_STANDARD,
    };

    let count_mult = profile.count_multiplier;
    let workloads = workloads::all_workloads()
        .into_iter()
        .filter(|w| cli.workload.is_none() || w.name == cli.workload.as_deref().unwrap())
        .map(|mut w| {
            w.config.workflow_count = (w.config.workflow_count as f64 * count_mult).max(1.0) as u64;
            w
        })
        .collect::<Vec<_>>();

    let engines: Vec<&str> = if cli.engines == "all" {
        vec!["velocity", "velocity-embedded", "velocity-classic", "dbos", "restate"]
    } else {
        cli.engines.split(',').map(|s| s.trim()).collect()
    };

    info!("Engines: {:?}", engines);
    info!("Workloads: {} (profile: {})", workloads.len(), cli.profile);
    info!("");

    let mut all_results: Vec<EngineResult> = Vec::new();

    // ─── Velocity Server (gRPC + WAL) ──────────────────────────────────
    if engines.contains(&"velocity") {
        info!("━━━ VELOCITY SERVER (Real gRPC + WAL persistence) ━━━");
        info!("Target: {}", cli.velocity_url);

        match VelocityClient::new(&cli.velocity_url).await {
            Ok(client) => {
                let mut results = Vec::new();
                for w in &workloads {
                    info!("  Running {} ({} ops)...", w.name, w.config.workflow_count);
                    let r = run_velocity_workload(&client, w).await;
                    info!(
                        "    -> {:.1} ops/sec, p99={:.0}µs, errors={:.1}%",
                        r.ops_per_second, r.latency_p99_us, r.error_rate_pct
                    );
                    results.push(r);
                }
                all_results.push(EngineResult {
                    engine: "Velocity-Server".into(),
                    engine_version: "0.1.0".into(),
                    workloads: results,
                    timestamp: chrono::Utc::now().to_rfc3339(),
                });
            }
            Err(e) => {
                error!("Velocity Server connection failed: {}", e);
                warn!("Skipping Velocity Server — is the server running at {}?", cli.velocity_url);
            }
        }
        info!("");
    }

    // ─── Velocity Embedded (PostgreSQL-backed) ─────────────────────────
    if engines.contains(&"velocity-embedded") {
        info!("━━━ VELOCITY EMBEDDED (PostgreSQL-backed) ━━━");
        info!("Target: {}", cli.velocity_embedded_url);

        match VelocityEmbeddedClient::new(&cli.velocity_embedded_url).await {
            Ok(client) => {
                let mut results = Vec::new();
                for w in &workloads {
                    info!("  Running {} ({} ops)...", w.name, w.config.workflow_count);
                    let r = run_velocity_embedded_workload(&client, w).await;
                    info!(
                        "    -> {:.1} ops/sec, p99={:.0}µs, errors={:.1}%",
                        r.ops_per_second, r.latency_p99_us, r.error_rate_pct
                    );
                    results.push(r);
                }
                all_results.push(EngineResult {
                    engine: "Velocity-Embedded".into(),
                    engine_version: "0.1.0".into(),
                    workloads: results,
                    timestamp: chrono::Utc::now().to_rfc3339(),
                });
            }
            Err(e) => {
                error!("Velocity Embedded connection failed: {}", e);
                warn!("Skipping Velocity Embedded — is the server running at {}?", cli.velocity_embedded_url);
            }
        }
        info!("");
    }

    // ─── Velocity Classic (Temporal-compatible) ────────────────────────
    if engines.contains(&"velocity-classic") {
        info!("━━━ VELOCITY CLASSIC (Temporal-compatible) ━━━");
        info!("Target: {}", cli.velocity_classic_url);

        match VelocityClassicClient::new(&cli.velocity_classic_url).await {
            Ok(client) => {
                let mut results = Vec::new();
                for w in &workloads {
                    info!("  Running {} ({} ops)...", w.name, w.config.workflow_count);
                    let r = run_velocity_classic_workload(&client, w).await;
                    info!(
                        "    -> {:.1} ops/sec, p99={:.0}µs, errors={:.1}%",
                        r.ops_per_second, r.latency_p99_us, r.error_rate_pct
                    );
                    results.push(r);
                }
                all_results.push(EngineResult {
                    engine: "Velocity-Classic".into(),
                    engine_version: "0.1.0".into(),
                    workloads: results,
                    timestamp: chrono::Utc::now().to_rfc3339(),
                });
            }
            Err(e) => {
                error!("Velocity Classic connection failed: {}", e);
                warn!("Skipping Velocity Classic — is the server running at {}?", cli.velocity_classic_url);
            }
        }
        info!("");
    }

    // ─── DBOS ──────────────────────────────────────────────────────────
    if engines.contains(&"dbos") {
        info!("━━━ DBOS (Real HTTP API + PostgreSQL) ━━━");
        info!("Target: {}", cli.dbos_url);

        match DbosClient::new(&cli.dbos_url).await {
            Ok(client) => {
                let mut results = Vec::new();
                for w in &workloads {
                    info!("  Running {} ({} ops)...", w.name, w.config.workflow_count);
                    let r = run_dbos_workload(&client, w).await;
                    info!(
                        "    -> {:.1} ops/sec, p99={:.0}µs, errors={:.1}%",
                        r.ops_per_second, r.latency_p99_us, r.error_rate_pct
                    );
                    results.push(r);
                }
                all_results.push(EngineResult {
                    engine: "DBOS".into(),
                    engine_version: "2.29.0".into(),
                    workloads: results,
                    timestamp: chrono::Utc::now().to_rfc3339(),
                });
            }
            Err(e) => {
                error!("DBOS connection failed: {}", e);
                warn!("Skipping DBOS — is the DBOS server running at {}?", cli.dbos_url);
            }
        }
        info!("");
    }

    // ─── Restate ───────────────────────────────────────────────────────
    if engines.contains(&"restate") {
        info!("━━━ Restate (Real HTTP API) ━━━");
        info!("Target: {}", cli.restate_url);

        match RestateClient::new(&cli.restate_url).await {
            Ok(client) => {
                let mut results = Vec::new();
                for w in &workloads {
                    info!("  Running {} ({} ops)...", w.name, w.config.workflow_count);
                    let r = run_restate_workload(&client, w).await;
                    info!(
                        "    -> {:.1} ops/sec, p99={:.0}µs, errors={:.1}%",
                        r.ops_per_second, r.latency_p99_us, r.error_rate_pct
                    );
                    results.push(r);
                }
                all_results.push(EngineResult {
                    engine: "Restate".into(),
                    engine_version: "1.1".into(),
                    workloads: results,
                    timestamp: chrono::Utc::now().to_rfc3339(),
                });
            }
            Err(e) => {
                error!("Restate connection failed: {}", e);
                warn!("Skipping Restate — is the Restate server running at {}?", cli.restate_url);
            }
        }
        info!("");
    }

    // ─── Output Results ────────────────────────────────────────────────
    if all_results.is_empty() {
        error!("No engines produced results. Check connectivity.");
        std::process::exit(1);
    }

    let output = match cli.format.as_str() {
        "json" => format_json(&all_results),
        "csv" => format_csv(&all_results),
        _ => format_markdown(&all_results),
    };

    if let Some(path) = &cli.output {
        std::fs::write(path, &output)?;
        info!("Results written to {}", path);
    } else {
        println!("\n{}", output);
    }

    Ok(())
}

// ─── Workload Runners ────────────────────────────────────────────────────────

async fn run_velocity_workload(client: &VelocityClient, w: &WorkloadDef) -> WorkloadResult {
    let count = w.config.workflow_count;
    let concurrency = w.config.concurrency.max(1) as usize;
    let mut latencies: Vec<f64> = Vec::new();
    let mut success = 0u64;
    let mut fail = 0u64;
    let bench_start = Instant::now();

    for batch_start in (0..count).step_by(concurrency) {
        let batch_end = (batch_start + concurrency as u64).min(count);
        let wf_ids: Vec<String> = (batch_start..batch_end)
            .map(|i| format!("{}-{}", w.name, i))
            .collect();
        let mut futs = Vec::new();
        for wf_id in &wf_ids {
            futs.push(client.run_workflow(wf_id, &w.name, &w.kind));
        }
        let results = futures::future::join_all(futs).await;
        for r in results {
            match r {
                Ok(latency_us) => {
                    success += 1;
                    latencies.push(latency_us);
                }
                Err(_) => fail += 1,
            }
        }
    }

    let wall = bench_start.elapsed().as_secs_f64();
    let ops_sec = success as f64 / wall;
    latencies.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let n = latencies.len();
    let p50 = if n > 0 { latencies[n * 50 / 100] } else { 0.0 };
    let p99 = if n > 0 { latencies[n * 99 / 100] } else { 0.0 };
    let p999 = if n > 0 { latencies[n * 999 / 1000.min(n)] } else { 0.0 };
    let mean = if n > 0 { latencies.iter().sum::<f64>() / n as f64 } else { 0.0 };
    let err_rate = if (success + fail) > 0 {
        fail as f64 / (success + fail) as f64 * 100.0
    } else {
        0.0
    };

    WorkloadResult {
        name: w.name.to_string(),
        description: w.description.to_string(),
        total_operations: count,
        successful_operations: success,
        failed_operations: fail,
        ops_per_second: ops_sec,
        latency_p50_us: p50,
        latency_p99_us: p99,
        latency_p999_us: p999,
        latency_mean_us: mean,
        peak_memory_mb: 0.0,
        error_rate_pct: err_rate,
    }
}

async fn run_velocity_embedded_workload(client: &VelocityEmbeddedClient, w: &WorkloadDef) -> WorkloadResult {
    let count = w.config.workflow_count;
    let concurrency = w.config.concurrency.max(1) as usize;
    let mut latencies: Vec<f64> = Vec::new();
    let mut success = 0u64;
    let mut fail = 0u64;
    let bench_start = Instant::now();

    for batch_start in (0..count).step_by(concurrency) {
        let batch_end = (batch_start + concurrency as u64).min(count);
        let wf_ids: Vec<String> = (batch_start..batch_end)
            .map(|i| format!("{}-{}", w.name, i))
            .collect();
        let mut futs = Vec::new();
        for wf_id in &wf_ids {
            futs.push(client.run_workflow(wf_id, &w.name, &w.kind));
        }
        let results = futures::future::join_all(futs).await;
        for r in results {
            match r {
                Ok(latency_us) => {
                    success += 1;
                    latencies.push(latency_us);
                }
                Err(_) => fail += 1,
            }
        }
    }

    let wall = bench_start.elapsed().as_secs_f64();
    let ops_sec = success as f64 / wall;
    latencies.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let n = latencies.len();
    let p50 = if n > 0 { latencies[n * 50 / 100] } else { 0.0 };
    let p99 = if n > 0 { latencies[n * 99 / 100] } else { 0.0 };
    let p999 = if n > 0 { latencies[n * 999 / 1000.min(n)] } else { 0.0 };
    let mean = if n > 0 { latencies.iter().sum::<f64>() / n as f64 } else { 0.0 };
    let err_rate = if (success + fail) > 0 {
        fail as f64 / (success + fail) as f64 * 100.0
    } else {
        0.0
    };

    WorkloadResult {
        name: w.name.to_string(),
        description: w.description.to_string(),
        total_operations: count,
        successful_operations: success,
        failed_operations: fail,
        ops_per_second: ops_sec,
        latency_p50_us: p50,
        latency_p99_us: p99,
        latency_p999_us: p999,
        latency_mean_us: mean,
        peak_memory_mb: 0.0,
        error_rate_pct: err_rate,
    }
}

async fn run_velocity_classic_workload(client: &VelocityClassicClient, w: &WorkloadDef) -> WorkloadResult {
    let count = w.config.workflow_count;
    let concurrency = w.config.concurrency.max(1) as usize;
    let mut latencies: Vec<f64> = Vec::new();
    let mut success = 0u64;
    let mut fail = 0u64;
    let bench_start = Instant::now();

    for batch_start in (0..count).step_by(concurrency) {
        let batch_end = (batch_start + concurrency as u64).min(count);
        let wf_ids: Vec<String> = (batch_start..batch_end)
            .map(|i| format!("{}-{}", w.name, i))
            .collect();
        let mut futs = Vec::new();
        for wf_id in &wf_ids {
            futs.push(client.run_workflow(wf_id, &w.name, &w.kind));
        }
        let results = futures::future::join_all(futs).await;
        for r in results {
            match r {
                Ok(latency_us) => {
                    success += 1;
                    latencies.push(latency_us);
                }
                Err(_) => fail += 1,
            }
        }
    }

    let wall = bench_start.elapsed().as_secs_f64();
    let ops_sec = success as f64 / wall;
    latencies.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let n = latencies.len();
    let p50 = if n > 0 { latencies[n * 50 / 100] } else { 0.0 };
    let p99 = if n > 0 { latencies[n * 99 / 100] } else { 0.0 };
    let p999 = if n > 0 { latencies[n * 999 / 1000.min(n)] } else { 0.0 };
    let mean = if n > 0 { latencies.iter().sum::<f64>() / n as f64 } else { 0.0 };
    let err_rate = if (success + fail) > 0 {
        fail as f64 / (success + fail) as f64 * 100.0
    } else {
        0.0
    };

    WorkloadResult {
        name: w.name.to_string(),
        description: w.description.to_string(),
        total_operations: count,
        successful_operations: success,
        failed_operations: fail,
        ops_per_second: ops_sec,
        latency_p50_us: p50,
        latency_p99_us: p99,
        latency_p999_us: p999,
        latency_mean_us: mean,
        peak_memory_mb: 0.0,
        error_rate_pct: err_rate,
    }
}

async fn run_dbos_workload(client: &DbosClient, w: &WorkloadDef) -> WorkloadResult {
    let count = w.config.workflow_count;
    let concurrency = w.config.concurrency.max(1) as usize;
    let mut latencies: Vec<f64> = Vec::new();
    let mut success = 0u64;
    let mut fail = 0u64;
    let bench_start = Instant::now();

    for batch_start in (0..count).step_by(concurrency) {
        let batch_end = (batch_start + concurrency as u64).min(count);
        let mut futs = Vec::new();
        for _ in batch_start..batch_end {
            futs.push(client.run_workload(&w.name, &w.kind));
        }
        let results = futures::future::join_all(futs).await;
        for r in results {
            match r {
                Ok(latency_us) => {
                    success += 1;
                    latencies.push(latency_us);
                }
                Err(_) => fail += 1,
            }
        }
    }

    let wall = bench_start.elapsed().as_secs_f64();
    let ops_sec = success as f64 / wall;
    latencies.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let n = latencies.len();
    let p50 = if n > 0 { latencies[n * 50 / 100] } else { 0.0 };
    let p99 = if n > 0 { latencies[n * 99 / 100] } else { 0.0 };
    let p999 = if n > 0 { latencies[n * 999 / 1000.min(n)] } else { 0.0 };
    let mean = if n > 0 { latencies.iter().sum::<f64>() / n as f64 } else { 0.0 };
    let err_rate = if (success + fail) > 0 {
        fail as f64 / (success + fail) as f64 * 100.0
    } else {
        0.0
    };

    WorkloadResult {
        name: w.name.to_string(),
        description: w.description.to_string(),
        total_operations: count,
        successful_operations: success,
        failed_operations: fail,
        ops_per_second: ops_sec,
        latency_p50_us: p50,
        latency_p99_us: p99,
        latency_p999_us: p999,
        latency_mean_us: mean,
        peak_memory_mb: 0.0,
        error_rate_pct: err_rate,
    }
}

async fn run_restate_workload(client: &RestateClient, w: &WorkloadDef) -> WorkloadResult {
    let count = w.config.workflow_count;
    let concurrency = w.config.concurrency.max(1) as usize;
    let mut latencies: Vec<f64> = Vec::new();
    let mut success = 0u64;
    let mut fail = 0u64;
    let bench_start = Instant::now();

    for batch_start in (0..count).step_by(concurrency) {
        let batch_end = (batch_start + concurrency as u64).min(count);
        let mut futs = Vec::new();
        for _ in batch_start..batch_end {
            futs.push(client.run_workload(&w.name, &w.kind));
        }
        let results = futures::future::join_all(futs).await;
        for r in results {
            match r {
                Ok(latency_us) => {
                    success += 1;
                    latencies.push(latency_us);
                }
                Err(_) => fail += 1,
            }
        }
    }

    let wall = bench_start.elapsed().as_secs_f64();
    let ops_sec = success as f64 / wall;
    latencies.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let n = latencies.len();
    let p50 = if n > 0 { latencies[n * 50 / 100] } else { 0.0 };
    let p99 = if n > 0 { latencies[n * 99 / 100] } else { 0.0 };
    let p999 = if n > 0 { latencies[n * 999 / 1000.min(n)] } else { 0.0 };
    let mean = if n > 0 { latencies.iter().sum::<f64>() / n as f64 } else { 0.0 };
    let err_rate = if (success + fail) > 0 {
        fail as f64 / (success + fail) as f64 * 100.0
    } else {
        0.0
    };

    WorkloadResult {
        name: w.name.to_string(),
        description: w.description.to_string(),
        total_operations: count,
        successful_operations: success,
        failed_operations: fail,
        ops_per_second: ops_sec,
        latency_p50_us: p50,
        latency_p99_us: p99,
        latency_p999_us: p999,
        latency_mean_us: mean,
        peak_memory_mb: 0.0,
        error_rate_pct: err_rate,
    }
}

// ─── Formatters ──────────────────────────────────────────────────────────────

fn format_markdown(results: &[EngineResult]) -> String {
    let mut out = String::new();
    out.push_str("# Production Workflow Engine Benchmark\n\n");
    out.push_str("**Generated:** ");
    out.push_str(&chrono::Utc::now().to_rfc3339());
    out.push_str("  \n");
    out.push_str("**Engines:** ");
    out.push_str(&results.iter().map(|r| r.engine.as_str()).collect::<Vec<_>>().join(", "));
    out.push_str("  \n**Mode:** Production (real APIs, real persistence)\n\n");

    // Summary table
    out.push_str("## Summary\n\n");
    out.push_str("| Engine | Avg ops/sec | p99 µs | Error Rate | Workloads |\n");
    out.push_str("|--------|------------|--------|------------|----------|\n");
    for r in results {
        let avg_ops: f64 = r.workloads.iter().map(|w| w.ops_per_second).sum::<f64>()
            / r.workloads.len().max(1) as f64;
        let avg_p99: f64 = r.workloads.iter().map(|w| w.latency_p99_us).sum::<f64>()
            / r.workloads.len().max(1) as f64;
        let avg_err: f64 = r.workloads.iter().map(|w| w.error_rate_pct).sum::<f64>()
            / r.workloads.len().max(1) as f64;
        let ok_count = r.workloads.iter().filter(|w| w.error_rate_pct < 1.0).count();
        out.push_str(&format!(
            "| {} | {:.1} | {:.0} | {:.1}% | {}/{} |\n",
            r.engine,
            avg_ops,
            avg_p99,
            avg_err,
            ok_count,
            r.workloads.len()
        ));
    }

    // Per-workload comparison
    out.push_str("\n## Per-Workload Comparison\n\n");
    let all_names: Vec<&str> = results[0].workloads.iter().map(|w| w.name.as_str()).collect();

    for name in &all_names {
        out.push_str(&format!("### {}\n\n", name));
        out.push_str("| Engine | ops/sec | p50 µs | p99 µs | p999 µs | Errors |\n");
        out.push_str("|--------|---------|--------|--------|---------|--------|\n");
        for r in results {
            if let Some(w) = r.workloads.iter().find(|w| w.name == *name) {
                out.push_str(&format!(
                    "| {} | {:.1} | {:.0} | {:.0} | {:.0} | {}/{} |\n",
                    r.engine,
                    w.ops_per_second,
                    w.latency_p50_us,
                    w.latency_p99_us,
                    w.latency_p999_us,
                    w.failed_operations,
                    w.total_operations
                ));
            }
        }
        out.push_str("\n");
    }

    out
}

fn format_json(results: &[EngineResult]) -> String {
    serde_json::to_string_pretty(results).unwrap_or_default()
}

fn format_csv(results: &[EngineResult]) -> String {
    let mut out = String::from("engine,workload,ops_per_sec,p50_us,p99_us,p999_us,mean_us,success,fail,error_rate_pct\n");
    for r in results {
        for w in &r.workloads {
            out.push_str(&format!(
                "{},{},{:.1},{:.0},{:.0},{:.0},{:.0},{},{},{:.2}\n",
                r.engine, w.name, w.ops_per_second, w.latency_p50_us,
                w.latency_p99_us, w.latency_p999_us, w.latency_mean_us,
                w.successful_operations, w.failed_operations, w.error_rate_pct
            ));
        }
    }
    out
}
