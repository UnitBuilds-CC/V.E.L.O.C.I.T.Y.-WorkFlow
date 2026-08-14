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

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use clap::Parser;

use velocity_workflow_engine::engine::WorkflowEngine;
use velocity_embedded::{NmcpFrameRouter, NmcpShmemServer, NmcpWebSocketServer};

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

    /// Log level filter.
    #[arg(long, default_value = "info")]
    log_level: String,
}

// ─── Main ────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&cli.log_level)),
        )
        .with_target(false)
        .init();

    tracing::info!("Velocity Embedded Server (NMCP transport, Rust + WAL)");
    tracing::info!("WAL: {}", cli.wal_path);
    tracing::info!("Shmem: {}", cli.shmem_path);
    tracing::info!("WebSocket: {}", cli.ws_bind);

    // ── Create engine with WAL persistence ────────────────────────────────
    let engine = if cli.wal_path.is_empty() {
        WorkflowEngine::new()
    } else {
        let e = WorkflowEngine::with_wal(&cli.wal_path, cli.wal_max_size)
            .expect("Failed to initialize WAL");
        match e.recover_from_wal() {
            Ok((records, workflows)) => {
                if records > 0 {
                    tracing::info!(
                        records_replayed = records,
                        workflows_recovered = workflows,
                        "Crash recovery: replayed WAL on startup"
                    );
                }
            }
            Err(err) => {
                tracing::warn!(error = %err, "WAL recovery failed (starting fresh)");
            }
        }
        e
    };

    let engine = Arc::new(engine);
    let workflow_map = Arc::new(Mutex::new(HashMap::new()));
    let workflow_counter = Arc::new(AtomicU64::new(1));

    // ── Create NMCP frame router ──────────────────────────────────────────
    let router = Arc::new(NmcpFrameRouter::new(
        engine.clone(),
        workflow_map,
        workflow_counter,
    ));

    println!("╦  ╦ ╔╗╔ ╦╔═ ╔═╗ ╦ ╦ ╔═╗ ╔╗╔ ╔═╗");
    println!("╚╗╔╝ ║║║ ╠╩╗ ╠═╣ ║ ║ ║╣  ║║║ ║ ║");
    println!("  ╚╝  ╝╚╝ ╩ ╩ ╩ ╩ ╚═╝ ╚═╝ ╝╚╝ ╚═╝");
    println!("  Embedded Server v{}", env!("CARGO_PKG_VERSION"));
    println!("  Shmem: {}", cli.shmem_path);
    println!("  WS:    ws://{}", cli.ws_bind);
    println!("  Mode:  NMCP (shmem + WebSocket)");
    println!("  WAL:   {}", cli.wal_path);
    println!();

    // ── Start shmem server (blocking, run in dedicated thread) ────────────
    let shmem_server = Arc::new(NmcpShmemServer::new(router.clone(), cli.shmem_path.clone()));

    let shmem_handle = shmem_server.clone();
    std::thread::spawn(move || {
        shmem_handle.run();
    });

    // ── Start WebSocket server ────────────────────────────────────────────
    let ws_server = NmcpWebSocketServer::new(router.clone(), cli.ws_bind.clone());

    tokio::select! {
        result = ws_server.run() => {
            if let Err(e) = result {
                tracing::error!("WebSocket server error: {}", e);
            }
        }
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("Shutdown signal received");
            shmem_server.shutdown();
        }
    }
}
