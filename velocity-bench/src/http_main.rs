//! velocity-bench-http — HTTP benchmark harness: Velocity Runtime vs Restate.
//!
//! Measures handler invocation throughput, stateful operations, concurrent
//! handler performance, and payload handling over HTTP.
//!
//! Architecture:
//!   [velocity-bench-http] ──HTTP──► [Velocity Runtime]  (handler invocation)
//!   [velocity-bench-http] ──HTTP──► [Restate Ingress]   (service handler)

use clap::Parser;
use velocity_bench::http_adapter::{HttpAdapter, HttpBenchmarkResult, HttpEngineConfig, HttpEngineKind};
use velocity_bench::http_workloads::{HttpWorkloadDefinition, HttpWorkloadKind};
use std::time::{Duration, Instant};
use tracing_subscriber::EnvFilter;

// ─── CLI ────────────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(name = "velocity-bench-http")]
#[command(about = "HTTP benchmark: Velocity Runtime vs Restate")]
struct Cli {
    /// Which workloads to run.
    #[arg(long, default_value = "all")]
    workloads: String,

    /// Run only a specific workload by name.
    #[arg(long)]
    workload: Option<String>,

    /// Which engine(s) to benchmark.
    #[arg(long, default_value = "both")]
    engine: String,

    /// Velocity Runtime address.
    #[arg(long, default_value = "http://localhost:8080")]
    velocity_address: String,

    /// Restate address.
    #[arg(long, default_value = "http://localhost:8081")]
    restate_address: String,

    /// Number of runs per workload (for statistical analysis).
    #[arg(long, default_value = "1")]
    runs: usize,

    /// Output format: json, csv, md, all.
    #[arg(long, default_value = "all")]
    format: String,

    /// Output file path (without extension).
    #[arg(long, default_value = "http_bench_results")]
    output: String,

    /// Benchmark profile: quick, standard, stress.
    #[arg(long, default_value = "standard")]
    profile: String,
}

// ─── Main ───────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive(tracing::Level::INFO.into()))
        .with_target(false)
        .init();

    let cli = Cli::parse();

    print_banner();

    // Select workloads
    let mut workload_defs = match cli.workloads.as_str() {
        "smoke" => HttpWorkloadDefinition::smoke(),
        _ => HttpWorkloadDefinition::all(),
    };

    // Filter to specific workload if requested
    if let Some(ref name) = cli.workload {
        workload_defs.retain(|w| w.name == *name);
    }

    // Apply profile multipliers
    let profile_mult = match cli.profile.as_str() {
        "quick" => 0.1,
        "stress" => 10.0,
        _ => 1.0,
    };
    for w in workload_defs.iter_mut() {
        w.operation_count = ((w.operation_count as f64) * profile_mult) as u64;
        if w.duration_secs > 0 {
            w.duration_secs = ((w.duration_secs as f64) * profile_mult.min(2.0)) as u64;
        }
    }

    tracing::info!(
        "Running {} workloads with {} profile (multiplier: {}x)",
        workload_defs.len(),
        cli.profile,
        profile_mult
    );

    // Determine which engines to run
    let run_velocity = matches!(cli.engine.as_str(), "velocity" | "both");
    let run_restate = matches!(cli.engine.as_str(), "restate" | "both");

    // Connect to engines
    let velocity_adapter = if run_velocity {
        let mut adapter = HttpAdapter::new(HttpEngineKind::VelocityRuntime);
        let config = HttpEngineConfig::velocity_runtime(&cli.velocity_address);
        adapter.connect(&config).await.map_err(|e| {
            tracing::error!("Failed to connect to Velocity Runtime: {}", e);
            e
        })?;
        Some(adapter)
    } else {
        None
    };

    let restate_adapter = if run_restate {
        let mut adapter = HttpAdapter::new(HttpEngineKind::Restate);
        let config = HttpEngineConfig::restate(&cli.restate_address);
        adapter.connect(&config).await.map_err(|e| {
            tracing::error!("Failed to connect to Restate: {}", e);
            e
        })?;
        Some(adapter)
    } else {
        None
    };

    // Run benchmarks
    let mut velocity_results: Vec<HttpBenchmarkResult> = Vec::new();
    let mut restate_results: Vec<HttpBenchmarkResult> = Vec::new();

    for workload in &workload_defs {
        tracing::info!("━━━ Workload: {} ━━━", workload.name);
        tracing::info!("  {}", workload.description);

        // Run Velocity
        if let Some(ref adapter) = velocity_adapter {
            for run_idx in 0..cli.runs {
                if cli.runs > 1 {
                    tracing::info!("  ── Run {}/{} ──", run_idx + 1, cli.runs);
                }
                let result = run_http_workload(adapter, workload).await;
                tracing::info!(
                    "  VELOCITY RUNTIME: {:.0} ops/sec, p99={:.0}µs, mem={:.1}MB",
                    result.operations_per_second,
                    result.latency_p99_us,
                    result.peak_memory_mb
                );
                velocity_results.push(result);
            }
        }

        // Run Restate
        if let Some(ref adapter) = restate_adapter {
            for run_idx in 0..cli.runs {
                if cli.runs > 1 {
                    tracing::info!("  ── Run {}/{} ──", run_idx + 1, cli.runs);
                }
                let result = run_http_workload(adapter, workload).await;
                tracing::info!(
                    "  RESTATE:          {:.0} ops/sec, p99={:.0}µs, mem={:.1}MB",
                    result.operations_per_second,
                    result.latency_p99_us,
                    result.peak_memory_mb
                );
                restate_results.push(result);
            }
        }
    }

    // Generate report
    let report = generate_report(&velocity_results, &restate_results);

    // Write output
    write_output(&cli, &report)?;

    // Print summary
    print_summary(&report);

    Ok(())
}

// ─── Workload Runner ────────────────────────────────────────────────────────

async fn run_http_workload(
    adapter: &HttpAdapter,
    workload: &HttpWorkloadDefinition,
) -> HttpBenchmarkResult {
    let mut latencies: Vec<u64> = Vec::new();
    let mut success_count: u64 = 0;
    let mut fail_count: u64 = 0;
    let mut total_bytes: u64 = 0;

    let start = Instant::now();

    match workload.kind {
        HttpWorkloadKind::HandlerInvocation => {
            // Sequential handler invocations
            let payload = vec![b'x'; workload.payload_size];
            for _ in 0..workload.operation_count {
                let result = adapter
                    .invoke_handler(&workload.service, &workload.handler, &payload)
                    .await;
                if result.success {
                    success_count += 1;
                    latencies.push(result.latency_us);
                    total_bytes += result.response_bytes;
                } else {
                    fail_count += 1;
                }
            }
        }

        HttpWorkloadKind::StatefulHandler => {
            // Keyed handler invocations with state
            let payload = vec![b'x'; workload.payload_size];
            for i in 0..workload.operation_count {
                let key = format!("bench-key-{}", i % 10);
                let result = adapter
                    .invoke_keyed_handler(&workload.service, &key, &workload.handler, &payload)
                    .await;
                if result.success {
                    success_count += 1;
                    latencies.push(result.latency_us);
                    total_bytes += result.response_bytes;
                } else {
                    fail_count += 1;
                }
            }
        }

        HttpWorkloadKind::ConcurrentHandlers => {
            // Concurrent handler invocations
            let payload = vec![b'x'; workload.payload_size];
            let mut handles = Vec::new();
            let base = adapter.base_url().to_string();

            for _ in 0..workload.concurrency {
                let p = payload.clone();
                let svc = workload.service.clone();
                let hdl = workload.handler.clone();
                let base_url = base.clone();
                // Clone adapter state for concurrent use
                handles.push(tokio::spawn(async move {
                    // We'll use a simple HTTP request per task
                    let client = reqwest::Client::new();
                    let url = format!("{}/{}/{}", base_url, svc, hdl);
                    let s = Instant::now();
                    match client.post(&url).body(p).send().await {
                        Ok(resp) => {
                            let status = resp.status().as_u16();
                            let bytes = resp.content_length().unwrap_or(0);
                            (s.elapsed().as_micros() as u64, status >= 200 && status < 300, bytes)
                        }
                        Err(_) => (s.elapsed().as_micros() as u64, false, 0),
                    }
                }));
            }

            for handle in handles {
                if let Ok((latency, success, bytes)) = handle.await {
                    if success {
                        success_count += 1;
                        latencies.push(latency);
                        total_bytes += bytes;
                    } else {
                        fail_count += 1;
                    }
                }
            }
        }

        HttpWorkloadKind::PayloadRoundtrip => {
            let payload = vec![b'x'; workload.payload_size];
            for _ in 0..workload.operation_count {
                let result = adapter
                    .invoke_handler(&workload.service, &workload.handler, &payload)
                    .await;
                if result.success {
                    success_count += 1;
                    latencies.push(result.latency_us);
                    total_bytes += result.response_bytes;
                } else {
                    fail_count += 1;
                }
            }
        }

        HttpWorkloadKind::SustainedLoad => {
            let duration = Duration::from_secs(workload.duration_secs);
            let payload = vec![b'x'; workload.payload_size];
            let mut iteration: u64 = 0;
            let base = adapter.base_url().to_string();

            tracing::info!(
                "  Running sustained load for {}s at concurrency {}...",
                workload.duration_secs,
                workload.concurrency
            );

            while start.elapsed() < duration {
                let mut handles = Vec::new();
                for _ in 0..workload.concurrency {
                    let p = payload.clone();
                    let svc = workload.service.clone();
                    let hdl = workload.handler.clone();
                    let base_url = base.clone();
                    handles.push(tokio::spawn(async move {
                        let client = reqwest::Client::new();
                        let url = format!("{}/{}/{}", base_url, svc, hdl);
                        let s = Instant::now();
                        match client.post(&url).body(p).send().await {
                            Ok(resp) => {
                                let status = resp.status().as_u16();
                                let bytes = resp.content_length().unwrap_or(0);
                                (s.elapsed().as_micros() as u64, status >= 200 && status < 300, bytes)
                            }
                            Err(_) => (s.elapsed().as_micros() as u64, false, 0),
                        }
                    }));
                }

                for handle in handles {
                    if let Ok((latency, success, bytes)) = handle.await {
                        if success {
                            success_count += 1;
                            latencies.push(latency);
                            total_bytes += bytes;
                        } else {
                            fail_count += 1;
                        }
                    }
                }
                iteration += 1;
            }

            tracing::info!(
                "  Completed {} iterations in {:.1}s",
                iteration,
                start.elapsed().as_secs_f64()
            );
        }

        HttpWorkloadKind::MixedOperations => {
            let payload = vec![b'x'; workload.payload_size];
            for i in 0..workload.operation_count {
                let pct = (i * 100) / workload.operation_count.max(1);
                let result = if pct < 70 {
                    // 70% invoke
                    adapter
                        .invoke_handler(&workload.service, &workload.handler, &payload)
                        .await
                } else if pct < 90 {
                    // 20% stateful
                    let key = format!("mixed-{}", i % 5);
                    adapter
                        .invoke_keyed_handler(&workload.service, &key, &workload.handler, &payload)
                        .await
                } else {
                    // 10% echo
                    adapter
                        .invoke_handler(&workload.service, "echo", &payload)
                        .await
                };

                if result.success {
                    success_count += 1;
                    latencies.push(result.latency_us);
                    total_bytes += result.response_bytes;
                } else {
                    fail_count += 1;
                }
            }
        }

        HttpWorkloadKind::ColdStart => {
            // Idle for 5 seconds, then measure first N invocations
            tokio::time::sleep(Duration::from_secs(5)).await;
            let payload = vec![b'x'; workload.payload_size];
            for _ in 0..workload.operation_count {
                let result = adapter
                    .invoke_handler(&workload.service, &workload.handler, &payload)
                    .await;
                if result.success {
                    success_count += 1;
                    latencies.push(result.latency_us);
                    total_bytes += result.response_bytes;
                } else {
                    fail_count += 1;
                }
            }
        }

        HttpWorkloadKind::DurablePromise => {
            let payload = vec![b'x'; workload.payload_size];
            for _ in 0..workload.operation_count {
                let result = adapter
                    .invoke_handler(&workload.service, &workload.handler, &payload)
                    .await;
                if result.success {
                    success_count += 1;
                    latencies.push(result.latency_us);
                    total_bytes += result.response_bytes;
                } else {
                    fail_count += 1;
                }
            }
        }
    }

    let total_duration = start.elapsed();

    // Get server memory
    let peak_memory_mb = adapter.server_memory_mb().await.unwrap_or(0.0);

    // Compute latency percentiles
    latencies.sort();
    let total_ops = success_count + fail_count;
    let ops_per_sec = if total_duration.as_secs_f64() > 0.0 {
        success_count as f64 / total_duration.as_secs_f64()
    } else {
        0.0
    };

    let percentile = |p: f64| -> u64 {
        if latencies.is_empty() {
            return 0;
        }
        let idx = ((latencies.len() as f64) * p / 100.0).min(latencies.len() as f64 - 1.0) as usize;
        latencies[idx]
    };

    let mean_latency = if latencies.is_empty() {
        0
    } else {
        (latencies.iter().map(|&l| l as f64).sum::<f64>() / latencies.len() as f64) as u64
    };

    HttpBenchmarkResult {
        engine: adapter.kind(),
        workload_name: workload.name.clone(),
        total_operations: total_ops,
        successful_operations: success_count,
        failed_operations: fail_count,
        total_duration_ms: total_duration.as_millis() as u64,
        operations_per_second: ops_per_sec,
        latency_p50_us: percentile(50.0),
        latency_p95_us: percentile(95.0),
        latency_p99_us: percentile(99.0),
        latency_p999_us: percentile(99.9),
        latency_min_us: latencies.first().copied().unwrap_or(0),
        latency_max_us: latencies.last().copied().unwrap_or(0),
        latency_mean_us: mean_latency,
        peak_memory_mb,
        total_bytes_transferred: total_bytes,
    }
}

// ─── Report Generation ──────────────────────────────────────────────────────

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct HttpComparisonReport {
    timestamp: String,
    profile: String,
    velocity_results: Vec<HttpBenchmarkResult>,
    restate_results: Vec<HttpBenchmarkResult>,
    summary: HttpSummary,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct HttpSummary {
    velocity_wins: usize,
    restate_wins: usize,
    comparable: usize,
    total: usize,
    avg_throughput_delta_pct: f64,
}

fn generate_report(
    velocity_results: &[HttpBenchmarkResult],
    restate_results: &[HttpBenchmarkResult],
) -> HttpComparisonReport {
    let mut velocity_wins = 0;
    let mut restate_wins = 0;
    let mut comparable = 0;
    let mut total_delta = 0.0;

    // Group results by workload name
    let workload_names: Vec<String> = velocity_results
        .iter()
        .map(|r| r.workload_name.clone())
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();

    for name in &workload_names {
        let vel: Vec<_> = velocity_results
            .iter()
            .filter(|r| r.workload_name == *name)
            .collect();
        let res: Vec<_> = restate_results
            .iter()
            .filter(|r| r.workload_name == *name)
            .collect();

        if vel.is_empty() || res.is_empty() {
            continue;
        }

        let vel_avg = vel.iter().map(|r| r.operations_per_second).sum::<f64>() / vel.len() as f64;
        let res_avg = res.iter().map(|r| r.operations_per_second).sum::<f64>() / res.len() as f64;

        if vel_avg > 0.0 && res_avg > 0.0 {
            comparable += 1;
            let delta_pct = ((vel_avg - res_avg) / res_avg) * 100.0;
            total_delta += delta_pct;

            if delta_pct > 5.0 {
                velocity_wins += 1;
            } else if delta_pct < -5.0 {
                restate_wins += 1;
            }
        }
    }

    let total = velocity_wins + restate_wins + comparable;
    let avg_delta = if comparable > 0 {
        total_delta / comparable as f64
    } else {
        0.0
    };

    HttpComparisonReport {
        timestamp: chrono::Utc::now().to_rfc3339(),
        profile: "standard".to_string(),
        velocity_results: velocity_results.to_vec(),
        restate_results: restate_results.to_vec(),
        summary: HttpSummary {
            velocity_wins,
            restate_wins,
            comparable,
            total,
            avg_throughput_delta_pct: avg_delta,
        },
    }
}

fn write_output(cli: &Cli, report: &HttpComparisonReport) -> Result<(), Box<dyn std::error::Error>> {
    // JSON output
    let json_path = format!("{}.json", cli.output);
    let json = serde_json::to_string_pretty(report)?;
    std::fs::write(&json_path, &json)?;
    tracing::info!("JSON report written to {}", json_path);

    // Markdown output
    if matches!(cli.format.as_str(), "md" | "all") {
        let md_path = format!("{}.md", cli.output);
        let md = generate_markdown(report);
        std::fs::write(&md_path, &md)?;
        tracing::info!("Markdown report written to {}", md_path);
    }

    // CSV output
    if matches!(cli.format.as_str(), "csv" | "all") {
        let csv_path = format!("{}.csv", cli.output);
        let mut wtr = csv::Writer::from_path(&csv_path)?;
        wtr.write_record([
            "workload",
            "engine",
            "ops_sec",
            "p50_us",
            "p95_us",
            "p99_us",
            "p999_us",
            "mean_us",
            "min_us",
            "max_us",
            "memory_mb",
            "total_ops",
            "success_ops",
            "failed_ops",
            "duration_ms",
        ])?;

        for r in &report.velocity_results {
            wtr.write_record(&[
                &r.workload_name,
                &r.engine.to_string(),
                &format!("{:.1}", r.operations_per_second),
                &r.latency_p50_us.to_string(),
                &r.latency_p95_us.to_string(),
                &r.latency_p99_us.to_string(),
                &r.latency_p999_us.to_string(),
                &r.latency_mean_us.to_string(),
                &r.latency_min_us.to_string(),
                &r.latency_max_us.to_string(),
                &format!("{:.1}", r.peak_memory_mb),
                &r.total_operations.to_string(),
                &r.successful_operations.to_string(),
                &r.failed_operations.to_string(),
                &r.total_duration_ms.to_string(),
            ])?;
        }
        for r in &report.restate_results {
            wtr.write_record(&[
                &r.workload_name,
                &r.engine.to_string(),
                &format!("{:.1}", r.operations_per_second),
                &r.latency_p50_us.to_string(),
                &r.latency_p95_us.to_string(),
                &r.latency_p99_us.to_string(),
                &r.latency_p999_us.to_string(),
                &r.latency_mean_us.to_string(),
                &r.latency_min_us.to_string(),
                &r.latency_max_us.to_string(),
                &format!("{:.1}", r.peak_memory_mb),
                &r.total_operations.to_string(),
                &r.successful_operations.to_string(),
                &r.failed_operations.to_string(),
                &r.total_duration_ms.to_string(),
            ])?;
        }
        wtr.flush()?;
        tracing::info!("CSV report written to {}", csv_path);
    }

    Ok(())
}

fn generate_markdown(report: &HttpComparisonReport) -> String {
    let mut md = String::new();
    md.push_str("# HTTP Benchmark: Velocity Runtime vs Restate\n\n");
    md.push_str(&format!("**Date:** {}\n\n", report.timestamp));
    md.push_str(&format!("**Profile:** {}\n\n", report.profile));

    // Summary
    md.push_str("## Summary\n\n");
    md.push_str(&format!(
        "| Metric | Value |\n|--------|-------|\n"
    ));
    md.push_str(&format!(
        "| Velocity Runtime wins | {} |\n",
        report.summary.velocity_wins
    ));
    md.push_str(&format!(
        "| Restate wins | {} |\n",
        report.summary.restate_wins
    ));
    md.push_str(&format!(
        "| Comparable | {} |\n",
        report.summary.comparable
    ));
    md.push_str(&format!(
        "| Avg throughput delta | +{:.1}% |\n\n",
        report.summary.avg_throughput_delta_pct
    ));

    // Detailed comparison
    md.push_str("## Detailed Comparison\n\n");
    md.push_str("| Workload | Engine | ops/sec | p50 (us) | p99 (us) | p999 (us) | Mem (MB) |\n");
    md.push_str("|----------|--------|---------|----------|----------|-----------|----------|\n");

    let workload_names: Vec<String> = report
        .velocity_results
        .iter()
        .map(|r| r.workload_name.clone())
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();

    for name in &workload_names {
        for r in report
            .velocity_results
            .iter()
            .filter(|r| r.workload_name == *name)
        {
            md.push_str(&format!(
                "| {} | {} | {:.0} | {} | {} | {} | {:.1} |\n",
                r.workload_name, r.engine, r.operations_per_second, r.latency_p50_us,
                r.latency_p99_us, r.latency_p999_us, r.peak_memory_mb
            ));
        }
        for r in report
            .restate_results
            .iter()
            .filter(|r| r.workload_name == *name)
        {
            md.push_str(&format!(
                "| {} | {} | {:.0} | {} | {} | {} | {:.1} |\n",
                r.workload_name, r.engine, r.operations_per_second, r.latency_p50_us,
                r.latency_p99_us, r.latency_p999_us, r.peak_memory_mb
            ));
        }
    }

    md
}

fn print_summary(report: &HttpComparisonReport) {
    tracing::info!("");
    tracing::info!("╔══════════════════════════════════════════════════════════╗");
    tracing::info!("║  HTTP BENCHMARK SUMMARY                                 ║");
    tracing::info!("╠══════════════════════════════════════════════════════════╣");
    tracing::info!(
        "║  Velocity Runtime wins: {:>3}  |  Restate wins: {:>3}     ║",
        report.summary.velocity_wins,
        report.summary.restate_wins
    );
    tracing::info!(
        "║  Comparable:          {:>3}  |  Total:        {:>3}     ║",
        report.summary.comparable,
        report.summary.total
    );
    tracing::info!(
        "║  Avg throughput delta: +{:.1}%                          ║",
        report.summary.avg_throughput_delta_pct
    );
    tracing::info!("╠══════════════════════════════════════════════════════════╣");
    if report.summary.avg_throughput_delta_pct > 5.0 {
        tracing::info!("║  Velocity Runtime is faster than Restate                   ║");
    } else if report.summary.avg_throughput_delta_pct < -5.0 {
        tracing::info!("║  Restate is faster than Velocity Runtime                   ║");
    } else {
        tracing::info!("║  Velocity Runtime and Restate are roughly comparable       ║");
    }
    tracing::info!("╚══════════════════════════════════════════════════════════╝");
}

fn print_banner() {
    tracing::info!("╔══════════════════════════════════════════════════════════╗");
    tracing::info!("║  velocity-bench-http — HTTP Benchmark Harness           ║");
    tracing::info!("║  Velocity Runtime vs Restate                            ║");
    tracing::info!("║  Apples-to-apples via identical HTTP paths              ║");
    tracing::info!("╚══════════════════════════════════════════════════════════╝");
}
