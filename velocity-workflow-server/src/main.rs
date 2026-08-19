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
use velocity_server_bootstrap::{ServerMetrics, HttpEndpointConfig};
use velocity_server_bootstrap::auth::AuthConfig;
use velocity_server_bootstrap::rate_limit::RateLimiter;
use velocity_server_bootstrap::audit::AuditLogger;

mod http_bench;

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

    /// Log format: "pretty" or "json" (for production log aggregation).
    #[arg(long, default_value = "pretty", env = "VELOCITY_LOG_FORMAT")]
    log_format: String,

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

    /// HTTP benchmark port (0 = disabled).  Provides a fair cross-engine
    /// comparison endpoint at POST /bench/simple_workflow.
    #[arg(long, default_value_t = 8080, env = "HTTP_BENCH_PORT")]
    http_bench_port: u16,

    /// Health/readiness/metrics endpoint bind address.
    #[arg(long, default_value = "0.0.0.0:8095", env = "VELOCITY_HEALTH_BIND")]
    health_bind: String,

    /// Bearer token for /metrics endpoint (empty = no auth).
    #[arg(long, default_value = "", env = "VELOCITY_METRICS_TOKEN")]
    metrics_token: String,

    /// API key for authenticating requests (can be specified multiple times).
    #[arg(long, env = "VELOCITY_API_KEYS", value_delimiter = ',')]
    api_keys: Vec<String>,

    /// JWT secret for HS256 token validation (empty = JWT disabled).
    #[arg(long, default_value = "", env = "VELOCITY_JWT_SECRET")]
    jwt_secret: String,

    /// JWT issuer claim to validate.
    #[arg(long, default_value = "", env = "VELOCITY_JWT_ISSUER")]
    jwt_issuer: String,

    /// JWT audience claim to validate.
    #[arg(long, default_value = "", env = "VELOCITY_JWT_AUDIENCE")]
    jwt_audience: String,

    /// Rate limit: max burst (tokens per client).
    #[arg(long, default_value_t = 100, env = "VELOCITY_RATE_LIMIT_BURST")]
    rate_limit_burst: u64,

    /// Rate limit: refill rate (tokens per second per client). 0 = disabled.
    #[arg(long, default_value_t = 0.0, env = "VELOCITY_RATE_LIMIT_REFILL")]
    rate_limit_refill: f64,

    /// Enable structured audit logging.
    #[arg(long, env = "VELOCITY_AUDIT_ENABLED")]
    audit_enabled: bool,

    /// mTLS CA certificate PEM file (enables client certificate verification).
    #[arg(long, env = "VELOCITY_MTLS_CA_CERT")]
    mtls_ca_cert: Option<String>,
}

// ─── Main ────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    // ── Initialize logging (JSON or pretty) ──────────────────────────────
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

    let bind_addr = format!("{}:{}", cli.ip, cli.vctp_port);

    println!(" #     #   #######   #          #####     ######   #######   #######   #     #");
    println!(" #     #   #         #         #     #   #            #          #       #     #");
    println!("  #   #    #         #         #     #   #            #          #        #   #");
    println!("  #   #    #####     #         #     #   #            #          #         # #");
    println!("   # #     #         #         #     #   #            #          #          #");
    println!("   # #     #         #         #     #   #            #          #          #");
    println!("    #      #######   #######    #####     ######   #######      #         #");
    println!("  Workflow Server v{}", env!("CARGO_PKG_VERSION"));
    println!("  VCTP:    udp://{}", bind_addr);
    println!("  Health:  http://{}/health", cli.health_bind);
    println!("  Mode:    Production (WAL persistence)");
    println!("  WAL:     {}", cli.wal_path);
    if !cli.encryption_key.is_empty() {
        println!("  Crypto:  XOR-AES enabled");
    }
    if !cli.metrics_token.is_empty() {
        println!("  Metrics: Bearer token auth enabled");
    }
    if !cli.api_keys.is_empty() {
        println!("  Auth:    {} API key(s) configured", cli.api_keys.len());
    }
    if !cli.jwt_secret.is_empty() {
        println!("  JWT:     HS256 validation enabled");
    }
    if cli.rate_limit_refill > 0.0 {
        println!("  Rate:    burst={}, refill={}/s", cli.rate_limit_burst, cli.rate_limit_refill);
    }
    if cli.audit_enabled {
        println!("  Audit:   Structured audit logging enabled");
    }
    if cli.mtls_ca_cert.is_some() {
        println!("  mTLS:    Client certificate verification enabled");
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
                // After WAL recovery + PG adapter init, recover step journals from PG.
                // This fills gaps where PG has steps that WAL didn't capture
                // (e.g., persist_step_async() PG write completed but WAL fsync didn't).
                match engine.recover_steps_from_pg() {
                    Ok((workflows, steps)) => {
                        if steps > 0 {
                            tracing::info!(
                                workflows_recovered = workflows,
                                steps_recovered = steps,
                                "PostgreSQL step journal recovery completed"
                            );
                        }
                    }
                    Err(err) => {
                        tracing::warn!(error = %err, "PG step journal recovery failed (continuing with WAL-only state)");
                    }
                }
            }
            Err(e) => {
                tracing::warn!("Failed to connect to PostgreSQL (continuing without DB): {}", e);
            }
        }
    }

    let engine = Arc::new(engine);

    // ── Shared metrics state ──────────────────────────────────────────────
    let metrics = Arc::new(ServerMetrics {
        flavor: "vctp",
        ..Default::default()
    });

    // Spawn metrics updater (refreshes every 5s from engine visibility)
    {
        let metrics = metrics.clone();
        let engine = engine.clone();
        tokio::spawn(async move {
            loop {
                let all = engine.visibility().list_all();
                let mut running = 0u64;
                let mut completed = 0u64;
                let mut failed = 0u64;
                for wf in &all {
                    match wf.status {
                        velocity_workflow_engine::engine::WorkflowStatus::Running => running += 1,
                        velocity_workflow_engine::engine::WorkflowStatus::Completed => completed += 1,
                        velocity_workflow_engine::engine::WorkflowStatus::Failed => failed += 1,
                        _ => {}
                    }
                }
                use std::sync::atomic::Ordering;
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
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            }
        });
    }

    // ── Start health/readiness/metrics endpoint ───────────────────────────
    let metrics_token = if cli.metrics_token.is_empty() {
        None
    } else {
        Some(cli.metrics_token.clone())
    };

    // Build auth config from API keys and JWT settings
    let auth_config = if !cli.api_keys.is_empty() || !cli.jwt_secret.is_empty() {
        Some(AuthConfig {
            api_keys: cli.api_keys.clone(),
            jwt_secret: cli.jwt_secret.clone(),
            jwt_issuer: cli.jwt_issuer.clone(),
            jwt_audience: cli.jwt_audience.clone(),
            ..Default::default()
        })
    } else {
        None
    };

    // Build rate limiter (0 refill = disabled)
    let rate_limiter = if cli.rate_limit_refill > 0.0 {
        Some(Arc::new(RateLimiter::new(cli.rate_limit_burst, cli.rate_limit_refill)))
    } else {
        None
    };

    // Build audit logger
    let audit_logger = if cli.audit_enabled {
        Some(Arc::new(AuditLogger::new(true)))
    } else {
        None
    };

    let endpoint_config = HttpEndpointConfig {
        metrics_token,
        auth_config,
        rate_limiter,
        audit_logger,
        tls_acceptor: None, // VCTP server uses UDP transport
    };

    let health_addr = cli.health_bind.clone();
    let metrics_for_health = metrics.clone();
    tokio::spawn(async move {
        if let Err(e) = velocity_server_bootstrap::run_http_health_with_config(
            health_addr,
            "velocity-workflow-server",
            metrics_for_health,
            endpoint_config,
        ).await {
            tracing::error!("Health endpoint error: {}", e);
        }
    });

    // ── Optionally start HTTP benchmark server ──────────────────────────
    if cli.http_bench_port > 0 {
        http_bench::spawn_http_bench(engine.clone(), cli.http_bench_port);
    }

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
    let server = Arc::new(VctpRpcServer::new(transport.clone(), engine.clone()));

    // Handle graceful shutdown on SIGTERM/SIGINT
    let server_shutdown = server.clone();
    let engine_shutdown = engine.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        tracing::info!("Shutdown signal received — starting graceful shutdown");
        
        // Step 1: Stop accepting new VCTP packets
        tracing::info!("Step 1: Stopping VCTP transport...");
        server_shutdown.shutdown();
        
        // Step 2: Wait for in-flight requests to complete (max 30s)
        tracing::info!("Step 2: Draining in-flight requests (max 30s)...");
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
        
        // Step 3: Flush WAL to disk
        tracing::info!("Step 3: Flushing WAL to disk...");
        engine_shutdown.sync_wal();
        
        // Step 4: Shutdown engine (flush PG writes, stop task queue)
        tracing::info!("Step 4: Shutting down engine...");
        engine_shutdown.shutdown();
        
        tracing::info!("Graceful shutdown complete");
        std::process::exit(0);
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
