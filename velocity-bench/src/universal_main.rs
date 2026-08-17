//! velocity-bench-universal — Universal HTTP benchmark harness for all 6 engines.
//!
//! Supports: Velocity Runtime, Velocity Classic, Velocity Embedded (skip if NMCP-only),
//!           Restate, DBOS, Temporal.
//!
//! Each engine is hit via its native HTTP bench endpoints. The same logical workloads
//! are run against each engine, and results are compared side-by-side.
//!
//! Usage:
//!   velocity-bench-universal \
//!     --engines velocity-classic,temporal \
//!     --velocity-classic-address http://localhost:18083 \
//!     --temporal-address http://localhost:8083 \
//!     --runs 5 --profile standard \
//!     --output bench-suite/benchmark-results/classic_vs_temporal

use clap::Parser;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tracing_subscriber::EnvFilter;

// ─── CLI ────────────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(name = "velocity-bench-universal")]
#[command(about = "Universal HTTP benchmark: all 6 engines")]
struct Cli {
    /// Comma-separated list of engines to benchmark.
    /// Options: velocity-runtime, velocity-classic, velocity-embedded, restate, dbos, temporal
    #[arg(long, default_value = "velocity-runtime,restate")]
    engines: String,

    #[arg(long, default_value = "http://localhost:7234")]
    velocity_runtime_address: String,

    #[arg(long, default_value = "http://localhost:18083")]
    velocity_classic_address: String,

    #[arg(long, default_value = "http://localhost:18082")]
    velocity_embedded_address: String,

    #[arg(long, default_value = "http://localhost:8082")]
    restate_address: String,

    #[arg(long, default_value = "http://localhost:8081")]
    dbos_address: String,

    #[arg(long, default_value = "http://localhost:8083")]
    temporal_address: String,

    /// Number of runs per workload per engine.
    #[arg(long, default_value = "5")]
    runs: usize,

    /// Output format: json, csv, md, all.
    #[arg(long, default_value = "all")]
    format: String,

    /// Output file path (without extension).
    #[arg(long)]
    output: String,

    /// Benchmark profile: quick, standard, stress.
    #[arg(long, default_value = "standard")]
    profile: String,

    /// Run only specific workloads (comma-separated).
    #[arg(long)]
    workload: Option<String>,
}

// ─── Engine Kind ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum EngineKind {
    VelocityRuntime,
    VelocityClassic,
    VelocityEmbedded,
    Restate,
    Dbos,
    Temporal,
}

impl std::fmt::Display for EngineKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EngineKind::VelocityRuntime => write!(f, "velocity-runtime"),
            EngineKind::VelocityClassic => write!(f, "velocity-classic"),
            EngineKind::VelocityEmbedded => write!(f, "velocity-embedded"),
            EngineKind::Restate => write!(f, "restate"),
            EngineKind::Dbos => write!(f, "dbos"),
            EngineKind::Temporal => write!(f, "temporal"),
        }
    }
}

impl EngineKind {
    fn from_str(s: &str) -> Option<Self> {
        match s.trim().to_lowercase().as_str() {
            "velocity-runtime" | "velocity_runtime" | "vr" => Some(EngineKind::VelocityRuntime),
            "velocity-classic" | "velocity_classic" | "vc" => Some(EngineKind::VelocityClassic),
            "velocity-embedded" | "velocity_embedded" | "ve" => Some(EngineKind::VelocityEmbedded),
            "restate" => Some(EngineKind::Restate),
            "dbos" => Some(EngineKind::Dbos),
            "temporal" => Some(EngineKind::Temporal),
            _ => None,
        }
    }
}

// ─── Workload Definition ────────────────────────────────────────────────────

/// A universal workload that maps to engine-specific endpoints.
#[derive(Debug, Clone)]
struct UniversalWorkload {
    name: String,
    description: String,
    /// Number of sequential operations per run.
    operations: u64,
    /// Concurrency level (for concurrent workloads).
    concurrency: usize,
    /// Payload size in bytes.
    payload_size: usize,
    /// Duration in seconds (for sustained load).
    duration_secs: u64,
}

impl UniversalWorkload {
    fn all() -> Vec<Self> {
        vec![
            Self {
                name: "simple_workflow".into(),
                description: "Simple workflow: 10 compute steps".into(),
                operations: 50,
                concurrency: 1,
                payload_size: 0,
                duration_secs: 0,
            },
            Self {
                name: "multi_step".into(),
                description: "Multi-step workflow: 100 sequential steps".into(),
                operations: 20,
                concurrency: 1,
                payload_size: 0,
                duration_secs: 0,
            },
            Self {
                name: "stateful".into(),
                description: "Stateful workflow: read + write with durable state".into(),
                operations: 50,
                concurrency: 1,
                payload_size: 0,
                duration_secs: 0,
            },
            Self {
                name: "durable_promise".into(),
                description: "Durable promise: set + get with persistence".into(),
                operations: 50,
                concurrency: 1,
                payload_size: 0,
                duration_secs: 0,
            },
            Self {
                name: "payload".into(),
                description: "Payload roundtrip: 4KB data transfer".into(),
                operations: 50,
                concurrency: 1,
                payload_size: 4096,
                duration_secs: 0,
            },
            Self {
                name: "echo".into(),
                description: "Echo: return input as-is".into(),
                operations: 100,
                concurrency: 1,
                payload_size: 256,
                duration_secs: 0,
            },
            Self {
                name: "cold_start".into(),
                description: "Cold start: first invocation after idle".into(),
                operations: 3,
                concurrency: 1,
                payload_size: 0,
                duration_secs: 0,
            },
            Self {
                name: "concurrent".into(),
                description: "Concurrent workflows: parallel execution".into(),
                operations: 20,
                concurrency: 10,
                payload_size: 0,
                duration_secs: 0,
            },
        ]
    }

    fn smoke() -> Vec<Self> {
        vec![
            Self {
                name: "simple_workflow".into(),
                description: "Simple workflow (smoke)".into(),
                operations: 5,
                concurrency: 1,
                payload_size: 0,
                duration_secs: 0,
            },
            Self {
                name: "echo".into(),
                description: "Echo (smoke)".into(),
                operations: 5,
                concurrency: 1,
                payload_size: 64,
                duration_secs: 0,
            },
        ]
    }
}

// ─── Engine Endpoint Adapter ────────────────────────────────────────────────

/// Maps workload names to engine-specific HTTP endpoints and request bodies.
struct EngineAdapter {
    kind: EngineKind,
    base_url: String,
    client: reqwest::Client,
}

impl EngineAdapter {
    async fn new(kind: EngineKind, base_url: &str) -> Result<Self, String> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(60))
            .connect_timeout(Duration::from_secs(5))
            .pool_max_idle_per_host(100)
            .build()
            .map_err(|e| format!("Failed to build HTTP client: {}", e))?;

        let adapter = Self {
            kind,
            base_url: base_url.trim_end_matches('/').to_string(),
            client,
        };

        // Verify connectivity
        let health_ok = match kind {
            EngineKind::VelocityRuntime => adapter.check_health("/health").await,
            EngineKind::VelocityClassic => adapter.check_health("/api/health").await,
            EngineKind::VelocityEmbedded => adapter.check_health("/health").await,
            EngineKind::Restate => {
                // Restate doesn't have /health on ingress; check admin instead
                Ok(())
            }
            EngineKind::Dbos => adapter.check_health("/health").await,
            EngineKind::Temporal => adapter.check_health("/health").await,
        };

        if let Err(e) = health_ok {
            tracing::warn!(engine = %kind, error = %e, "Health check failed (continuing)");
        }

        Ok(adapter)
    }

    async fn check_health(&self, path: &str) -> Result<(), String> {
        let url = format!("{}{}", self.base_url, path);
        self.client
            .get(&url)
            .send()
            .await
            .map_err(|e| format!("Health check failed: {}", e))?;
        Ok(())
    }

    /// Get the endpoint URL and request body for a given workload.
    fn get_endpoint(&self, workload_name: &str, iteration: u64) -> (String, Vec<u8>) {
        match self.kind {
            EngineKind::VelocityRuntime => self.velocity_runtime_endpoint(workload_name, iteration),
            EngineKind::VelocityClassic => self.velocity_classic_endpoint(workload_name, iteration),
            EngineKind::VelocityEmbedded => self.velocity_embedded_endpoint(workload_name, iteration),
            EngineKind::Restate => self.restate_endpoint(workload_name, iteration),
            EngineKind::Dbos => self.dbos_endpoint(workload_name, iteration),
            EngineKind::Temporal => self.temporal_endpoint(workload_name, iteration),
        }
    }

    /// Velocity Runtime: /bench/* routes (original velocity-bench-server)
    fn velocity_runtime_endpoint(&self, workload: &str, iter: u64) -> (String, Vec<u8>) {
        let base = &self.base_url;
        match workload {
            "simple_workflow" => (format!("{}/bench/simple_workflow", base), b"{}".to_vec()),
            "multi_step" => (format!("{}/bench/multi_step", base), serde_json::to_vec(&serde_json::json!({"steps": 100})).unwrap()),
            "stateful" => (format!("{}/keyed_bench/key-{}/stateful", base, iter % 10), b"{}".to_vec()),
            "durable_promise" => (format!("{}/bench/durablePromise", base), b"{}".to_vec()),
            "payload" => (format!("{}/bench/payload", base), b"{}".to_vec()),
            "echo" => (format!("{}/bench/echo", base), b"{}".to_vec()),
            "cold_start" => (format!("{}/bench/cold_start", base), b"{}".to_vec()),
            "concurrent" => (format!("{}/bench/concurrent", base), serde_json::to_vec(&serde_json::json!({"id": iter})).unwrap()),
            _ => (format!("{}/bench/invoke", base), b"{}".to_vec()),
        }
    }

    /// Velocity Classic: /bench/* routes (Temporal-compatible API)
    fn velocity_classic_endpoint(&self, workload: &str, iter: u64) -> (String, Vec<u8>) {
        let base = &self.base_url;
        match workload {
            "simple_workflow" => (format!("{}/bench/simple_workflow", base), b"{}".to_vec()),
            "multi_step" => (format!("{}/bench/multi_step", base), serde_json::to_vec(&serde_json::json!({"steps": 100})).unwrap()),
            "stateful" => (format!("{}/bench/stateful", base), b"{}".to_vec()),
            "durable_promise" => (format!("{}/bench/durable_promise", base), b"{}".to_vec()),
            "payload" => (format!("{}/bench/payload", base), b"{}".to_vec()),
            "echo" => (format!("{}/bench/echo", base), b"{}".to_vec()),
            "cold_start" => (format!("{}/bench/cold_start", base), b"{}".to_vec()),
            "concurrent" => (format!("{}/bench/concurrent", base), serde_json::to_vec(&serde_json::json!({"id": iter})).unwrap()),
            _ => (format!("{}/bench/simple_workflow", base), b"{}".to_vec()),
        }
    }

    /// Velocity Embedded: NMCP transport — may not have HTTP bench routes
    fn velocity_embedded_endpoint(&self, workload: &str, _iter: u64) -> (String, Vec<u8>) {
        // Velocity Embedded uses NMCP (shmem + WebSocket), not plain HTTP.
        // If it exposes HTTP bench routes, they would follow the same pattern as Classic.
        let base = &self.base_url;
        match workload {
            "simple_workflow" => (format!("{}/bench/simple_workflow", base), b"{}".to_vec()),
            _ => (format!("{}/bench/simple_workflow", base), b"{}".to_vec()),
        }
    }

    /// Restate: /bench/default/* routes (Virtual Objects with key)
    fn restate_endpoint(&self, workload: &str, iter: u64) -> (String, Vec<u8>) {
        let base = &self.base_url;
        match workload {
            "simple_workflow" => (format!("{}/bench/default/simple", base), b"{}".to_vec()),
            "multi_step" => (format!("{}/bench/default/multiStep", base), serde_json::to_vec(&serde_json::json!({"steps": 100})).unwrap()),
            "stateful" => (format!("{}/keyed_bench/default/stateful", base), b"{}".to_vec()),
            "durable_promise" => (format!("{}/bench/default/durablePromise", base), b"{}".to_vec()),
            "payload" => (format!("{}/bench/default/payload", base), b"{}".to_vec()),
            "echo" => (format!("{}/bench/default/echo", base), b"{}".to_vec()),
            "cold_start" => (format!("{}/bench/default/coldStart", base), b"{}".to_vec()),
            "concurrent" => (format!("{}/concurrent_bench/default/execute", base), serde_json::to_vec(&serde_json::json!({"id": iter})).unwrap()),
            _ => (format!("{}/bench/default/invoke", base), b"{}".to_vec()),
        }
    }

    /// DBOS: /bench/* routes (FastAPI)
    fn dbos_endpoint(&self, workload: &str, iter: u64) -> (String, Vec<u8>) {
        let base = &self.base_url;
        match workload {
            "simple_workflow" => (format!("{}/bench/simple_workflow", base), b"{}".to_vec()),
            "multi_step" => (format!("{}/bench/multi_step", base), serde_json::to_vec(&serde_json::json!({"steps": 100})).unwrap()),
            "stateful" => (format!("{}/bench/stateful", base), b"{}".to_vec()),
            "durable_promise" => (format!("{}/bench/durable_promise", base), b"{}".to_vec()),
            "payload" => (format!("{}/bench/payload", base), b"{}".to_vec()),
            "echo" => (format!("{}/bench/echo", base), b"{}".to_vec()),
            "cold_start" => (format!("{}/bench/cold_start", base), b"{}".to_vec()),
            "concurrent" => (format!("{}/bench/concurrent", base), serde_json::to_vec(&serde_json::json!({"id": iter})).unwrap()),
            _ => (format!("{}/bench/simple_workflow", base), b"{}".to_vec()),
        }
    }

    /// Temporal: /bench/* routes (FastAPI)
    fn temporal_endpoint(&self, workload: &str, iter: u64) -> (String, Vec<u8>) {
        // Same as DBOS — both use FastAPI with identical route names
        self.dbos_endpoint(workload, iter)
    }

    /// Execute a single HTTP request for a workload.
    async fn execute_one(&self, workload_name: &str, iteration: u64, payload_override: Option<&[u8]>) -> (u64, bool, u64) {
        let (url, mut body) = self.get_endpoint(workload_name, iteration);

        if let Some(payload) = payload_override {
            body = payload.to_vec();
        }

        let start = Instant::now();
        match self.client.post(&url)
            .header("content-type", "application/json")
            .body(body)
            .send()
            .await
        {
            Ok(resp) => {
                let status = resp.status().as_u16();
                let bytes = resp.content_length().unwrap_or(0);
                let latency = start.elapsed().as_micros() as u64;
                (latency, status >= 200 && status < 300, bytes)
            }
            Err(_) => {
                let latency = start.elapsed().as_micros() as u64;
                (latency, false, 0)
            }
        }
    }

    /// Get server memory usage via health endpoint.
    async fn server_memory_mb(&self) -> f64 {
        let health_path = match self.kind {
            EngineKind::VelocityRuntime => "/health",
            EngineKind::VelocityClassic => "/api/health",
            EngineKind::VelocityEmbedded => "/health",
            EngineKind::Restate => return 0.0,
            EngineKind::Dbos => "/health",
            EngineKind::Temporal => "/health",
        };
        let url = format!("{}{}", self.base_url, health_path);
        if let Ok(resp) = self.client.get(&url).send().await {
            if let Ok(body) = resp.text().await {
                if let Ok(json) = serde_json::from_str::<serde_json::Value>(&body) {
                    if let Some(mem) = json.get("memory_rss_mb").and_then(|v| v.as_f64()) {
                        return mem;
                    }
                    if let Some(mem) = json.get("memory_usage_bytes").and_then(|v| v.as_u64()) {
                        return mem as f64 / 1_048_576.0;
                    }
                }
            }
        }
        0.0
    }
}

// ─── Benchmark Result ───────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchResult {
    engine: EngineKind,
    workload: String,
    run_index: usize,
    total_ops: u64,
    success_ops: u64,
    failed_ops: u64,
    duration_ms: u64,
    ops_per_sec: f64,
    p50_us: u64,
    p95_us: u64,
    p99_us: u64,
    p999_us: u64,
    min_us: u64,
    max_us: u64,
    mean_us: u64,
    memory_mb: f64,
}

// ─── Main ───────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive(tracing::Level::INFO.into()))
        .with_target(false)
        .init();

    let cli = Cli::parse();
    print_banner();

    // Parse engines
    let engines: Vec<EngineKind> = cli.engines.split(',')
        .filter_map(|s| EngineKind::from_str(s))
        .collect();

    if engines.is_empty() {
        tracing::error!("No valid engines specified. Use: velocity-runtime, velocity-classic, velocity-embedded, restate, dbos, temporal");
        return Err("No valid engines".into());
    }

    tracing::info!("Engines: {:?}", engines);

    // Select workloads
    let mut workloads = if cli.workload.is_some() {
        let names: Vec<String> = cli.workload.as_ref().unwrap().split(',').map(|s| s.trim().to_string()).collect();
        UniversalWorkload::all().into_iter().filter(|w| names.contains(&w.name)).collect()
    } else {
        UniversalWorkload::all()
    };

    // Apply profile
    let profile_mult = match cli.profile.as_str() {
        "quick" => 0.2,
        "stress" => 5.0,
        _ => 1.0,
    };
    for w in workloads.iter_mut() {
        w.operations = ((w.operations as f64) * profile_mult).max(1.0) as u64;
        if w.duration_secs > 0 {
            w.duration_secs = ((w.duration_secs as f64) * profile_mult.min(2.0)) as u64;
        }
    }

    tracing::info!("Running {} workloads × {} engines × {} runs", workloads.len(), engines.len(), cli.runs);

    // Connect to engines
    let mut adapters: HashMap<EngineKind, EngineAdapter> = HashMap::new();
    for engine in &engines {
        let addr = match engine {
            EngineKind::VelocityRuntime => &cli.velocity_runtime_address,
            EngineKind::VelocityClassic => &cli.velocity_classic_address,
            EngineKind::VelocityEmbedded => &cli.velocity_embedded_address,
            EngineKind::Restate => &cli.restate_address,
            EngineKind::Dbos => &cli.dbos_address,
            EngineKind::Temporal => &cli.temporal_address,
        };
        match EngineAdapter::new(*engine, addr).await {
            Ok(adapter) => {
                tracing::info!(engine = %engine, address = %addr, "Connected");
                adapters.insert(*engine, adapter);
            }
            Err(e) => {
                tracing::error!(engine = %engine, error = %e, "Failed to connect");
                return Err(format!("Failed to connect to {}: {}", engine, e).into());
            }
        }
    }

    // Run benchmarks
    let mut all_results: Vec<BenchResult> = Vec::new();

    for workload in &workloads {
        tracing::info!("━━━ Workload: {} ━━━", workload.name);
        tracing::info!("  {}", workload.description);

        for engine in &engines {
            let adapter = match adapters.get(engine) {
                Some(a) => a,
                None => continue,
            };

            for run_idx in 0..cli.runs {
                if cli.runs > 1 {
                    tracing::info!("  [{}/{}] Run {}/{}", engine, workload.name, run_idx + 1, cli.runs);
                }

                let result = run_workload(adapter, workload, run_idx).await;
                tracing::info!(
                    "  {}: {:.1} ops/s, p99={}µs, fail={}, mem={:.1}MB",
                    engine, result.ops_per_sec, result.p99_us, result.failed_ops, result.memory_mb
                );
                all_results.push(result);
            }
        }

        // Cold start: idle between runs
        if workload.name == "cold_start" {
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }

    // Generate report
    let report = generate_report(&all_results, &engines, &cli.profile);

    // Write output
    write_output(&cli, &report)?;

    // Print summary
    print_summary(&report, &engines);

    Ok(())
}

// ─── Workload Runner ────────────────────────────────────────────────────────

async fn run_workload(adapter: &EngineAdapter, workload: &UniversalWorkload, run_idx: usize) -> BenchResult {
    let mut latencies: Vec<u64> = Vec::new();
    let mut success_count: u64 = 0;
    let mut fail_count: u64 = 0;
    let mut total_bytes: u64 = 0;
    let start = Instant::now();

    if workload.concurrency > 1 {
        // Concurrent workload
        let mut iteration = 0u64;
        let duration = if workload.duration_secs > 0 {
            Duration::from_secs(workload.duration_secs)
        } else {
            Duration::from_secs(30) // default 30s for concurrent
        };

        while start.elapsed() < duration && iteration < workload.operations * workload.concurrency as u64 {
            let mut handles = Vec::new();
            for c in 0..workload.concurrency {
                let eng = adapter.kind;
                let wl_name = workload.name.clone();
                let iter = iteration + c as u64;
                let base = adapter.base_url.clone();
                let client = adapter.client.clone();
                handles.push(tokio::spawn(async move {
                    let temp_adapter = EngineAdapter {
                        kind: eng,
                        base_url: base,
                        client,
                    };
                    temp_adapter.execute_one(&wl_name, iter, None).await
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
            iteration += workload.concurrency as u64;
        }
    } else if workload.name == "cold_start" {
        // Cold start: idle then measure first N
        tokio::time::sleep(Duration::from_secs(5)).await;
        for i in 0..workload.operations {
            let (latency, success, bytes) = adapter.execute_one(&workload.name, i, None).await;
            if success {
                success_count += 1;
                latencies.push(latency);
                total_bytes += bytes;
            } else {
                fail_count += 1;
            }
        }
    } else {
        // Sequential workload
        for i in 0..workload.operations {
            let (latency, success, bytes) = adapter.execute_one(&workload.name, i, None).await;
            if success {
                success_count += 1;
                latencies.push(latency);
                total_bytes += bytes;
            } else {
                fail_count += 1;
            }
        }
    }

    let total_duration = start.elapsed();
    let memory_mb = adapter.server_memory_mb().await;

    // Compute percentiles
    latencies.sort();
    let total_ops = success_count + fail_count;
    let ops_per_sec = if total_duration.as_secs_f64() > 0.0 {
        success_count as f64 / total_duration.as_secs_f64()
    } else {
        0.0
    };

    let percentile = |p: f64| -> u64 {
        if latencies.is_empty() { return 0; }
        let idx = ((latencies.len() as f64) * p / 100.0).min(latencies.len() as f64 - 1.0) as usize;
        latencies[idx]
    };

    let mean_latency = if latencies.is_empty() { 0 } else {
        (latencies.iter().map(|&l| l as f64).sum::<f64>() / latencies.len() as f64) as u64
    };

    BenchResult {
        engine: adapter.kind,
        workload: workload.name.clone(),
        run_index: run_idx,
        total_ops,
        success_ops: success_count,
        failed_ops: fail_count,
        duration_ms: total_duration.as_millis() as u64,
        ops_per_sec,
        p50_us: percentile(50.0),
        p95_us: percentile(95.0),
        p99_us: percentile(99.0),
        p999_us: percentile(99.9),
        min_us: latencies.first().copied().unwrap_or(0),
        max_us: latencies.last().copied().unwrap_or(0),
        mean_us: mean_latency,
        memory_mb,
    }
}

// ─── Report Generation ──────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UniversalReport {
    timestamp: String,
    profile: String,
    engines: Vec<String>,
    results: Vec<BenchResult>,
    summary: UniversalSummary,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UniversalSummary {
    workload_count: usize,
    engine_count: usize,
    total_runs: usize,
    /// Per-workload averages: (workload_name, engine_name, avg_ops_sec, avg_p99_us)
    per_workload_avg: Vec<WorkloadAvg>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WorkloadAvg {
    workload: String,
    engine: String,
    avg_ops_sec: f64,
    avg_p50_us: u64,
    avg_p99_us: u64,
    avg_p999_us: u64,
    total_success: u64,
    total_fail: u64,
}

fn generate_report(results: &[BenchResult], engines: &[EngineKind], profile: &str) -> UniversalReport {
    let mut per_workload_avg = Vec::new();

    // Group by (workload, engine)
    let mut groups: HashMap<(String, String), Vec<&BenchResult>> = HashMap::new();
    for r in results {
        groups.entry((r.workload.clone(), r.engine.to_string()))
            .or_default()
            .push(r);
    }

    for ((workload, engine), group_results) in &groups {
        let avg_ops = group_results.iter().map(|r| r.ops_per_sec).sum::<f64>() / group_results.len() as f64;
        let avg_p50 = (group_results.iter().map(|r| r.p50_us as f64).sum::<f64>() / group_results.len() as f64) as u64;
        let avg_p99 = (group_results.iter().map(|r| r.p99_us as f64).sum::<f64>() / group_results.len() as f64) as u64;
        let avg_p999 = (group_results.iter().map(|r| r.p999_us as f64).sum::<f64>() / group_results.len() as f64) as u64;
        let total_success = group_results.iter().map(|r| r.success_ops).sum();
        let total_fail = group_results.iter().map(|r| r.failed_ops).sum();

        per_workload_avg.push(WorkloadAvg {
            workload: workload.clone(),
            engine: engine.clone(),
            avg_ops_sec: avg_ops,
            avg_p50_us: avg_p50,
            avg_p99_us: avg_p99,
            avg_p999_us: avg_p999,
            total_success,
            total_fail,
        });
    }

    // Sort by workload then engine
    per_workload_avg.sort_by(|a, b| {
        a.workload.cmp(&b.workload).then(a.engine.cmp(&b.engine))
    });

    let workload_names: Vec<String> = results.iter().map(|r| r.workload.clone()).collect::<std::collections::HashSet<_>>().into_iter().collect();

    UniversalReport {
        timestamp: chrono::Utc::now().to_rfc3339(),
        profile: profile.to_string(),
        engines: engines.iter().map(|e| e.to_string()).collect(),
        results: results.to_vec(),
        summary: UniversalSummary {
            workload_count: workload_names.len(),
            engine_count: engines.len(),
            total_runs: results.len(),
            per_workload_avg,
        },
    }
}

fn write_output(cli: &Cli, report: &UniversalReport) -> Result<(), Box<dyn std::error::Error>> {
    // JSON
    let json_path = format!("{}.json", cli.output);
    let json = serde_json::to_string_pretty(report)?;
    std::fs::write(&json_path, &json)?;
    tracing::info!("JSON report: {}", json_path);

    // Markdown
    if matches!(cli.format.as_str(), "md" | "all") {
        let md_path = format!("{}.md", cli.output);
        let md = generate_markdown(report);
        std::fs::write(&md_path, &md)?;
        tracing::info!("Markdown report: {}", md_path);
    }

    // CSV
    if matches!(cli.format.as_str(), "csv" | "all") {
        let csv_path = format!("{}.csv", cli.output);
        let mut wtr = csv::Writer::from_path(&csv_path)?;
        wtr.write_record([
            "workload", "engine", "run", "ops_sec", "p50_us", "p95_us", "p99_us", "p999_us",
            "mean_us", "min_us", "max_us", "memory_mb", "total_ops", "success_ops", "failed_ops", "duration_ms",
        ])?;
        for r in &report.results {
            wtr.write_record(&[
                &r.workload, &r.engine.to_string(), &r.run_index.to_string(),
                &format!("{:.1}", r.ops_per_sec), &r.p50_us.to_string(), &r.p95_us.to_string(),
                &r.p99_us.to_string(), &r.p999_us.to_string(), &r.mean_us.to_string(),
                &r.min_us.to_string(), &r.max_us.to_string(), &format!("{:.1}", r.memory_mb),
                &r.total_ops.to_string(), &r.success_ops.to_string(), &r.failed_ops.to_string(),
                &r.duration_ms.to_string(),
            ])?;
        }
        wtr.flush()?;
        tracing::info!("CSV report: {}", csv_path);
    }

    Ok(())
}

fn generate_markdown(report: &UniversalReport) -> String {
    let mut md = String::new();
    md.push_str(&format!("# Universal Benchmark: {} Engines\n\n", report.engines.join(" vs ")));
    md.push_str(&format!("**Date:** {}\n\n", report.timestamp));
    md.push_str(&format!("**Profile:** {} | **Workloads:** {} | **Engines:** {}\n\n",
        report.profile, report.summary.workload_count, report.summary.engine_count));

    // Summary table
    md.push_str("## Per-Workload Averages\n\n");
    md.push_str("| Workload | Engine | ops/s | p50 (µs) | p99 (µs) | p999 (µs) | Success | Fail |\n");
    md.push_str("|----------|--------|------:|---------:|---------:|----------:|--------:|-----:|\n");

    for avg in &report.summary.per_workload_avg {
        md.push_str(&format!(
            "| {} | {} | {:.1} | {} | {} | {} | {} | {} |\n",
            avg.workload, avg.engine, avg.avg_ops_sec, avg.avg_p50_us, avg.avg_p99_us, avg.avg_p999_us,
            avg.total_success, avg.total_fail
        ));
    }

    // Comparison section
    if report.engines.len() == 2 {
        md.push_str("\n## Head-to-Head Comparison\n\n");
        md.push_str("| Workload | Engine 1 (ops/s) | Engine 2 (ops/s) | Delta | Winner |\n");
        md.push_str("|----------|------------------:|------------------:|------:|--------|\n");

        let eng1 = &report.engines[0];
        let eng2 = &report.engines[1];

        let workload_names: Vec<String> = report.summary.per_workload_avg.iter()
            .map(|a| a.workload.clone())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();

        let mut eng1_wins = 0;
        let mut eng2_wins = 0;

        for wl_name in &workload_names {
            let eng1_avg = report.summary.per_workload_avg.iter()
                .find(|a| a.workload == *wl_name && a.engine == *eng1)
                .map(|a| a.avg_ops_sec)
                .unwrap_or(0.0);
            let eng2_avg = report.summary.per_workload_avg.iter()
                .find(|a| a.workload == *wl_name && a.engine == *eng2)
                .map(|a| a.avg_ops_sec)
                .unwrap_or(0.0);

            let delta = if eng2_avg > 0.0 { ((eng1_avg - eng2_avg) / eng2_avg) * 100.0 } else { 0.0 };
            let winner = if delta > 5.0 { eng1.clone() } else if delta < -5.0 { eng2.clone() } else { "tie".to_string() };
            if delta > 5.0 { eng1_wins += 1; } else if delta < -5.0 { eng2_wins += 1; }

            md.push_str(&format!(
                "| {} | {:.1} | {:.1} | {:+.1}% | {} |\n",
                wl_name, eng1_avg, eng2_avg, delta, winner
            ));
        }

        md.push_str(&format!("\n**{} wins: {} | {} wins: {}**\n", eng1, eng1_wins, eng2, eng2_wins));
    }

    md
}

fn print_summary(report: &UniversalReport, engines: &[EngineKind]) {
    tracing::info!("");
    tracing::info!("╔══════════════════════════════════════════════════════════╗");
    tracing::info!("║  UNIVERSAL BENCHMARK SUMMARY                            ║");
    tracing::info!("╠══════════════════════════════════════════════════════════╣");
    tracing::info!("║  Engines: {:<48} ║", engines.iter().map(|e| e.to_string()).collect::<Vec<_>>().join(", "));
    tracing::info!("║  Workloads: {:<3} | Total runs: {:<3}                      ║",
        report.summary.workload_count, report.summary.total_runs);
    tracing::info!("╠══════════════════════════════════════════════════════════╣");

    if engines.len() == 2 {
        let eng1 = &engines[0].to_string();
        let eng2 = &engines[1].to_string();

        let workload_names: Vec<String> = report.summary.per_workload_avg.iter()
            .map(|a| a.workload.clone())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();

        let mut eng1_wins = 0;
        let mut eng2_wins = 0;
        let mut total_delta = 0.0;
        let mut comparable = 0;

        for wl_name in &workload_names {
            let e1 = report.summary.per_workload_avg.iter()
                .find(|a| a.workload == *wl_name && a.engine == *eng1)
                .map(|a| a.avg_ops_sec).unwrap_or(0.0);
            let e2 = report.summary.per_workload_avg.iter()
                .find(|a| a.workload == *wl_name && a.engine == *eng2)
                .map(|a| a.avg_ops_sec).unwrap_or(0.0);

            if e1 > 0.0 && e2 > 0.0 {
                comparable += 1;
                let delta = ((e1 - e2) / e2) * 100.0;
                total_delta += delta;
                if delta > 5.0 { eng1_wins += 1; } else if delta < -5.0 { eng2_wins += 1; }
            }
        }

        let avg_delta = if comparable > 0 { total_delta / comparable as f64 } else { 0.0 };

        tracing::info!("║  {} wins: {:>3}  |  {} wins: {:>3}                  ║", eng1, eng1_wins, eng2, eng2_wins);
        tracing::info!("║  Avg throughput delta: {:+.1}%                              ║", avg_delta);
    }

    tracing::info!("╚══════════════════════════════════════════════════════════╝");
}

fn print_banner() {
    tracing::info!("╔══════════════════════════════════════════════════════════╗");
    tracing::info!("║  velocity-bench-universal — Universal Bench Harness     ║");
    tracing::info!("║  All engines: Velocity / Restate / DBOS / Temporal      ║");
    tracing::info!("╚══════════════════════════════════════════════════════════╝");
}
