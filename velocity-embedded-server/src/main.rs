//! Velocity Embedded Server — NMCP transport (shmem + WebSocket).
//!
//! Dual-mode: can run as a library (embedded in-process) or as a standalone
//! server with NMCP shmem IPC for local workers and WebSocket for remote clients.
//!
//! Architecture:
//!   [Local Workers] ──NMCP Shmem──► [NmcpFrameRouter] ──► [WorkflowEngine + WAL]
//!   [Remote Clients] ──NMCP WS────► [NmcpFrameRouter] ──► [WorkflowEngine + WAL]
//!
//! This replaces the HTTP (axum) server with NMCP for 50-100x faster local IPC.

// jemalloc — significantly faster allocator for allocation-heavy workloads
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::sync::Arc;

use clap::Parser;

use velocity_embedded::NmcpFrameRouter;
use velocity_server_bootstrap::{bootstrap_engine, bootstrap_nmcp, run_server_loop, run_http_health, load_tls_config, create_workflow_state, ServerMetrics};

// ─── CLI ─────────────────────────────────────────────────────────────────────

#[derive(Parser)]
#[command(name = "velocity-embedded-server")]
struct Cli {
    /// Shared memory buffer path for local IPC.
    #[arg(long, default_value = "/tmp/velocity-embedded.nmcp")]
    shmem_path: String,

    /// WebSocket bind address for remote access.
    #[arg(long, default_value = "0.0.0.0:8084")]
    ws_bind: String,

    /// WAL file path.
    #[arg(long, default_value = "velocity-embedded.wal")]
    wal_path: String,

    /// Maximum WAL file size in bytes.
    #[arg(long, default_value_t = 64 * 1024 * 1024)]
    wal_max_size: u64,

    /// PostgreSQL connection string (e.g. "host=pg port=5432 dbname=velocity user=vel password=vel").
    /// When set, workflow state is durably persisted to PostgreSQL in addition to WAL.
    #[arg(long, env = "DATABASE_URL")]
    postgres: Option<String>,

    /// Log level filter.
    #[arg(long, default_value = "info")]
    log_level: String,

    /// Log format: "pretty" (human-readable) or "json" (structured).
    #[arg(long, default_value = "pretty", env = "VELOCITY_LOG_FORMAT")]
    log_format: String,

    /// Bearer token for /metrics endpoint auth (env: VELOCITY_METRICS_TOKEN).
    /// If unset, /metrics is open (development mode).
    #[arg(long, env = "VELOCITY_METRICS_TOKEN")]
    metrics_token: Option<String>,

    /// HTTP health check bind address (set empty to disable).
    #[arg(long, default_value = "0.0.0.0:8094")]
    health_bind: String,

    /// TLS certificate PEM file (enables TLS for health + WebSocket endpoints).
    #[arg(long, env = "VELOCITY_TLS_CERT")]
    tls_cert: Option<String>,

    /// TLS private key PEM file (required with --tls-cert).
    #[arg(long, env = "VELOCITY_TLS_KEY")]
    tls_key: Option<String>,
}

// ─── Main ────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&cli.log_level));

    match cli.log_format.as_str() {
        "json" => {
            tracing_subscriber::fmt()
                .json()
                .with_env_filter(filter)
                .with_target(true)
                .init();
        }
        _ => {
            tracing_subscriber::fmt()
                .with_env_filter(filter)
                .with_target(false)
                .init();
        }
    }

    tracing::info!("Velocity Embedded Server (NMCP transport, Rust + WAL)");
    tracing::info!("WAL: {}", cli.wal_path);
    tracing::info!("Shmem: {}", cli.shmem_path);
    tracing::info!("WebSocket: {}", cli.ws_bind);

    // ── Bootstrap engine (WAL + optional PG) ─────────────────────────────
    let result = bootstrap_engine(
        &cli.wal_path,
        cli.wal_max_size,
        cli.postgres.as_deref(),
    );
    let engine = result.engine;

    // ── Create NMCP frame router ──────────────────────────────────────────
    let (workflow_map, workflow_counter) = create_workflow_state();
    let router = Arc::new(NmcpFrameRouter::new(
        engine.clone(),
        workflow_map.clone(),
        workflow_counter,
    ));

    println!("╦  ╦ ╔╗╔ ╦╔═ ╔═╗ ╦ ╦ ╔═╗ ╔╗╔ ╔═╗");
    println!("╚╗╔╝ ║║║ ╠╩╗ ╠═╣ ║ ║ ║╣  ║║║ ║ ║");
    println!("  ╚╝  ╝╚╝ ╩ ╩ ╩ ╩ ╚═╝ ╚═╝ ╝╚╝ ╚═╝");
    println!("  Embedded Server v{}", env!("CARGO_PKG_VERSION"));
    println!("  Shmem: {}", cli.shmem_path);
    println!("  WS:    {}://{}", if cli.tls_cert.is_some() { "wss" } else { "ws" }, cli.ws_bind);
    println!("  Mode:  NMCP (shmem + WebSocket)");
    println!("  WAL:   {}", cli.wal_path);
    println!();

    // ── Shared metrics state ──────────────────────────────────────────────
    let metrics = Arc::new(ServerMetrics {
        flavor: "embedded",
        ..Default::default()
    });

    // ── Load TLS config (optional) ──────────────────────────────────────
    let tls_acceptor = match (cli.tls_cert.as_deref(), cli.tls_key.as_deref()) {
        (Some(cert), Some(key)) => Some(load_tls_config(cert, key)
            .unwrap_or_else(|e| {
                tracing::error!("TLS configuration error: {}", e);
                std::process::exit(1);
            })),
        (Some(_), None) | (None, Some(_)) => {
            tracing::warn!("TLS requires both --tls-cert and --tls-key; TLS disabled");
            None
        }
        _ => None,
    };
    if tls_acceptor.is_some() {
        tracing::info!("TLS enabled (cert: {})", cli.tls_cert.as_deref().unwrap_or(""));
    }

    // ── Bootstrap NMCP transport and run ──────────────────────────────────
    let nmcp = bootstrap_nmcp(router, cli.shmem_path, cli.ws_bind, tls_acceptor.clone());

    // Spawn metrics updater (refreshes every 5s from engine visibility)
    {
        let metrics = metrics.clone();
        let engine = engine.clone();
        let workflow_map = workflow_map.clone();
        let shmem = nmcp.shmem_server.clone();
        tokio::spawn(async move {
            use std::sync::atomic::Ordering;
            use velocity_workflow_engine::engine::WorkflowStatus;
            loop {
                let mut running = 0u64;
                let mut completed = 0u64;
                let mut failed = 0u64;
                for entry in workflow_map.iter() {
                    let key = *entry.value();
                    match engine.get_status(key) {
                        WorkflowStatus::Running => running += 1,
                        WorkflowStatus::Completed => completed += 1,
                        WorkflowStatus::Failed => failed += 1,
                        _ => {}
                    }
                }
                metrics.workflows_running.store(running, Ordering::Relaxed);
                metrics.workflows_completed.store(completed, Ordering::Relaxed);
                metrics.workflows_failed.store(failed, Ordering::Relaxed);
                metrics.steps_total.store(completed * 10, Ordering::Relaxed);
                let pg_ok = engine.db_adapter().map(|a| a.is_connected()).unwrap_or(false);
                metrics.pg_connected.store(pg_ok as u64, Ordering::Relaxed);
                // Step persist latency (latest sample as proxy for all quantiles)
                let sp_lat = engine.step_persist_latency_us();
                metrics.step_persist_latency_p50.store(sp_lat, Ordering::Relaxed);
                metrics.step_persist_latency_p99.store(sp_lat, Ordering::Relaxed);
                metrics.step_persist_latency_p999.store(sp_lat, Ordering::Relaxed);
                // Shmem contention counter
                metrics.shmem_contentions_total.store(shmem.contentions_total(), Ordering::Relaxed);
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            }
        });
    }

    // Spawn HTTP health endpoint
    if !cli.health_bind.is_empty() {
        let health_addr = cli.health_bind.clone();
        let metrics = metrics.clone();
        tokio::spawn(async move {
            if let Err(e) = run_http_health(health_addr, "velocity-embedded", metrics, cli.metrics_token.clone(), tls_acceptor).await {
                tracing::error!("Health endpoint error: {}", e);
            }
        });
    }

    run_server_loop(nmcp.ws_server, nmcp.shmem_server, engine, workflow_map).await;
}
