//! Velocity Workflow Server — VCTP (zero-copy UDP) transport.
//!
//! Replaces gRPC (tonic/HTTP2) with VCTP for 5-15x faster RPC latency,
//! connectionless scalability to 100K+ clients, and simpler SDKs.
//!
//! Architecture:
//!   [SDK clients] ──VCTP/UDP──► [VctpRpcServer] ──► [WorkflowEngine + WAL]

use std::sync::Arc;

use clap::Parser;

use velocity_workflow_engine::engine::WorkflowEngine;
use velocity_workflow_engine::db_adapter::DatabaseConfig;
use velocity_workflow_engine::LivePostgresAdapter;
use velocity_workflow_engine::vctp_transport::{VctpTransport, VctpTransportConfig};
use velocity_workflow_engine::VctpRpcServer;

// ─── CLI ─────────────────────────────────────────────────────────────────────

#[derive(Parser)]
#[command(
    name = "velocity-server",
    about = "Velocity Workflow Server — VCTP transport (zero-copy UDP)"
)]
struct Cli {
    /// UDP port for VCTP traffic.
    #[arg(long, default_value_t = 7234, env = "VELOCITY_VCTP_PORT")]
    vctp_port: u16,

    /// Bind IP address.
    #[arg(long, default_value = "0.0.0.0")]
    ip: String,

    /// Log level filter.
    #[arg(long, default_value = "info")]
    log_level: String,

    /// WAL file path (empty = in-memory only).
    #[arg(long, default_value = "velocity.wal")]
    wal_path: String,

    /// Maximum WAL file size in bytes.
    #[arg(long, default_value_t = 64 * 1024 * 1024)]
    wal_max_size: u64,

    /// Optional encryption passphrase for VCTP traffic.
    #[arg(long, default_value = "")]
    encryption_key: String,

    /// PostgreSQL connection string (e.g. "host=pg port=5432 dbname=velocity user=vel password=vel").
    /// When set, workflow state is durably persisted to PostgreSQL in addition to WAL.
    #[arg(long, env = "DATABASE_URL")]
    postgres: Option<String>,
}

// ─── Main ────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&cli.log_level)),
        )
        .init();

    let bind_addr = format!("{}:{}", cli.ip, cli.vctp_port);

    println!("╦  ╦ ╔╗╔ ╦╔═ ╔═╗ ╦ ╦ ╔═╗ ╔╗╔ ╔═╗");
    println!("╚╗╔╝ ║║║ ╠╩╗ ╠═╣ ║ ║ ║╣  ║║║ ║ ║");
    println!("  ╚╝  ╝╚╝ ╩ ╩ ╩ ╩ ╚═╝ ╚═╝ ╝╚╝ ╚═╝");
    println!("  Workflow Server v{}", env!("CARGO_PKG_VERSION"));
    println!("  VCTP:  udp://{}", bind_addr);
    println!("  Mode:  Production (WAL persistence)");
    println!("  WAL:   {}", cli.wal_path);
    if !cli.encryption_key.is_empty() {
        println!("  Crypto: XOR-AES enabled");
    }
    println!();

    // ── Create the production engine with WAL persistence ─────────────────
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
    // ── Optionally enable PostgreSQL persistence ──────────────────────
    let mut engine = engine;
    if let Some(pg_conn) = cli.postgres {
        let config = DatabaseConfig::from_connection_string(&pg_conn);
        match LivePostgresAdapter::new(config) {
            Ok(adapter) => {
                tracing::info!("PostgreSQL persistence enabled: {}", pg_conn);
                engine.enable_db_adapter(std::sync::Arc::new(adapter));
            }
            Err(e) => {
                tracing::warn!("Failed to connect to PostgreSQL (continuing without DB): {}", e);
            }
        }
    }

    let engine = Arc::new(engine);

    // ── Create VCTP transport (UDP socket) ────────────────────────────────
    let transport_config = VctpTransportConfig {
        bind_addr: bind_addr.clone(),
        encryption_passphrase: cli.encryption_key.clone(),
        nonce: 0,
        max_retries: 5,
        rto_multiplier: 2,
        recv_buffer_size: 131072, // 128KB for large payloads
    };
    let transport = Arc::new(VctpTransport::new(transport_config)?);

    let actual_addr = transport.local_addr()?;
    tracing::info!("VCTP RPC server listening on {}", actual_addr);

    // ── Create and run VCTP RPC server ────────────────────────────────────
    let server = Arc::new(VctpRpcServer::new(transport.clone(), engine));

    // Handle graceful shutdown on SIGTERM/SIGINT
    let server_shutdown = server.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        tracing::info!("Shutdown signal received");
        server_shutdown.shutdown();
    });

    // VctpRpcServer::run() is a blocking loop — run it on a dedicated thread
    let server_thread = server.clone();
    std::thread::spawn(move || {
        server_thread.run();
    });

    // Print stats periodically
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
        let stats = server.stats();
        let t_stats = transport.stats();
        if stats.requests_received > 0 {
            tracing::info!(
                requests = stats.requests_received,
                responses = stats.responses_sent,
                errors = stats.errors,
                unknown = stats.unknown_methods,
                frag_req = stats.fragmented_requests,
                frag_resp = stats.fragmented_responses,
                udp_sent = t_stats.packets_sent,
                udp_recv = t_stats.packets_received,
                udp_dropped = t_stats.packets_dropped,
                checksum_fail = t_stats.checksum_failures,
                "VCTP server stats"
            );
        }
        if !transport.is_running() {
            break;
        }
    }

    Ok(())
}
