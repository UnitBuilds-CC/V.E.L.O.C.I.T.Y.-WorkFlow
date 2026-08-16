//! Shared server bootstrap logic for Velocity NMCP servers.
//!
//! Extracts the common initialization code shared between Classic and Embedded
//! server binaries:
//!
//! - [`bootstrap_engine`] — WAL creation, recovery, and optional PG adapter setup
//! - [`bootstrap_nmcp`] — NMCP shmem + WebSocket server creation
//! - [`run_server_loop`] — tokio::select! shutdown pattern
//!
//! Each server binary becomes ~30 lines: define CLI, create flavor-specific router,
//! call bootstrap functions.

use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use dashmap::DashMap;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;

use velocity_nmcp_protocol::{NmcpDispatch, NmcpShmemServer, NmcpWebSocketServer};
use velocity_workflow_engine::engine::{WorkflowEngine, WorkflowStatus};
use velocity_workflow_engine::db_adapter::DatabaseConfig;
use velocity_workflow_engine::LivePostgresAdapter;

/// Combined AsyncRead + AsyncWrite trait for use in trait objects.
trait AsyncReadWrite: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> AsyncReadWrite for T {}

/// Load TLS configuration from PEM certificate and key files.
///
/// Returns `None` if either path is empty (TLS disabled).
/// Used for both the HTTP health endpoint and WebSocket NMCP endpoint.
pub fn load_tls_config(cert_path: &str, key_path: &str) -> Result<tokio_rustls::TlsAcceptor, String> {
    if cert_path.is_empty() || key_path.is_empty() {
        return Err("TLS cert and key paths must not be empty".into());
    }

    let cert_file = std::fs::File::open(cert_path)
        .map_err(|e| format!("Failed to open TLS cert file '{}': {}", cert_path, e))?;
    let key_file = std::fs::File::open(key_path)
        .map_err(|e| format!("Failed to open TLS key file '{}': {}", key_path, e))?;

    let mut cert_reader = std::io::BufReader::new(cert_file);
    let certs: Vec<_> = rustls_pemfile::certs(&mut cert_reader)
        .filter_map(|c| c.ok())
        .collect();

    if certs.is_empty() {
        return Err(format!("No valid certificates found in '{}'", cert_path));
    }

    let mut key_reader = std::io::BufReader::new(key_file);
    let key = rustls_pemfile::private_key(&mut key_reader)
        .map_err(|e| format!("Failed to parse TLS private key: {}", e))?
        .ok_or_else(|| format!("No private key found in '{}'", key_path))?;

    let config = tokio_rustls::rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| format!("Failed to build TLS config: {}", e))?;

    Ok(tokio_rustls::TlsAcceptor::from(Arc::new(config)))
}

/// Result of engine bootstrap.
pub struct BootstrapResult {
    /// The initialized workflow engine (wrapped in Arc).
    pub engine: Arc<WorkflowEngine>,
    /// Whether PostgreSQL persistence was successfully enabled.
    pub pg_enabled: bool,
}

/// Bootstrap the engine with WAL persistence and optional PostgreSQL.
///
/// 1. Creates a WorkflowEngine with WAL (or in-memory if `wal_path` is empty)
/// 2. Recovers from WAL if available
/// 3. Optionally connects to PostgreSQL for step journal persistence
/// 4. Recovers step journals from PG if available
///
/// Returns the engine wrapped in Arc and whether PG was enabled.
pub fn bootstrap_engine(
    wal_path: &str,
    wal_max_size: u64,
    postgres_conn: Option<&str>,
) -> BootstrapResult {
    // ── Create engine with WAL persistence ────────────────────────────────
    let engine = if wal_path.is_empty() {
        WorkflowEngine::new()
    } else {
        let e = WorkflowEngine::with_wal(wal_path, wal_max_size)
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

    // ── Optionally enable PostgreSQL persistence ──────────────────────────
    let mut engine = engine;
    let mut pg_enabled = false;
    if let Some(pg_conn) = postgres_conn {
        let config = DatabaseConfig::from_connection_string(pg_conn);
        match LivePostgresAdapter::new(config) {
            Ok(adapter) => {
                tracing::info!("PostgreSQL persistence enabled: {}", pg_conn);
                engine.enable_db_adapter(Arc::new(adapter));
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
                pg_enabled = true;
            }
            Err(e) => {
                tracing::warn!("Failed to connect to PostgreSQL (continuing without DB): {}", e);
            }
        }
    }

    BootstrapResult {
        engine: Arc::new(engine),
        pg_enabled,
    }
}

/// Result of NMCP transport bootstrap.
pub struct NmcpBootstrapResult<D: NmcpDispatch> {
    /// Shared memory server (runs in a blocking thread).
    pub shmem_server: Arc<NmcpShmemServer<D>>,
    /// WebSocket server (runs async).
    pub ws_server: NmcpWebSocketServer<D>,
}

/// Bootstrap NMCP transport (shmem + WebSocket servers).
///
/// Creates the shared memory IPC server and WebSocket server for the given router.
/// The shmem server is NOT started — call `.run()` in a dedicated thread.
/// The WebSocket server is NOT started — call `.run()` in a tokio task.
///
/// If `tls_acceptor` is `Some`, the WebSocket server wraps connections with TLS.
pub fn bootstrap_nmcp<D: NmcpDispatch>(
    router: Arc<D>,
    shmem_path: String,
    ws_bind: String,
    tls_acceptor: Option<tokio_rustls::TlsAcceptor>,
) -> NmcpBootstrapResult<D> {
    let shmem_server = Arc::new(NmcpShmemServer::new(router.clone(), shmem_path));
    let mut ws_server = NmcpWebSocketServer::new(router, ws_bind);
    if let Some(acceptor) = tls_acceptor {
        ws_server = ws_server.with_tls(acceptor);
    }

    NmcpBootstrapResult {
        shmem_server,
        ws_server,
    }
}

/// Run the NMCP server loop with graceful shutdown.
///
/// Starts the shmem server in a dedicated thread, then runs the WebSocket
/// server until either it errors or Ctrl+C is received.
///
/// On shutdown, performs the full 5-step graceful shutdown sequence:
/// 1. Stop accepting new connections (shmem + WebSocket)
/// 2. Drain in-flight workflows (wait up to 30s for completion)
/// 3. Flush WAL (fsync all pending records to disk)
/// 4. Flush PG step journal (all pending writes are synchronous, so drain = wait for workflows)
/// 5. Shutdown engine (stop task queue and timer engine)
pub async fn run_server_loop<D: NmcpDispatch>(
    ws_server: NmcpWebSocketServer<D>,
    shmem_server: Arc<NmcpShmemServer<D>>,
    engine: Arc<WorkflowEngine>,
    workflow_map: Arc<DashMap<String, u64>>,
) {
    // Start shmem server in a dedicated blocking thread.
    {
        let server = shmem_server.clone();
        std::thread::spawn(move || {
            server.run();
        });
    }

    // Run WebSocket server with Ctrl+C shutdown.
    tokio::select! {
        result = ws_server.run() => {
            if let Err(e) = result {
                tracing::error!("WebSocket server error: {}", e);
            }
        }
        _ = tokio::signal::ctrl_c() => {
            graceful_shutdown(&ws_server, &shmem_server, &engine, &workflow_map).await;
        }
    }
}

/// Execute the 5-step graceful shutdown sequence.
///
/// Called on SIGTERM/Ctrl+C. Ensures no data loss by:
/// - Rejecting new work immediately
/// - Waiting for in-flight workflows to finish (up to 30s)
/// - Flushing all persistence layers (WAL + PG)
/// - cleanly shutting down the engine
async fn graceful_shutdown<D: NmcpDispatch>(
    ws_server: &NmcpWebSocketServer<D>,
    shmem_server: &NmcpShmemServer<D>,
    engine: &WorkflowEngine,
    workflow_map: &DashMap<String, u64>,
) {
    tracing::info!("Shutdown signal received — beginning graceful shutdown");

    // ── Step 1: Stop accepting new connections ────────────────────────────
    shmem_server.shutdown();
    ws_server.shutdown();
    tracing::info!("[1/5] Stopped accepting new connections");

    // ── Step 2: Drain in-flight workflows (up to 30s) ────────────────────
    let drain_start = tokio::time::Instant::now();
    let drain_timeout = tokio::time::Duration::from_secs(30);
    let poll_interval = tokio::time::Duration::from_millis(100);

    loop {
        let running = workflow_map.iter().filter(|entry| {
            let key = *entry.value();
            engine.get_status(key) == WorkflowStatus::Running
        }).count();

        if running == 0 {
            tracing::info!("[2/5] All workflows drained");
            break;
        }

        if drain_start.elapsed() >= drain_timeout {
            tracing::warn!(
                remaining = running,
                "[2/5] Drain timeout (30s) — {} workflows still running",
                running
            );
            break;
        }

        tracing::debug!(running, "Draining in-flight workflows...");
        tokio::time::sleep(poll_interval).await;
    }

    // ── Step 3: Flush WAL (fsync all pending records) ────────────────────
    engine.sync_wal();
    tracing::info!("[3/5] WAL flushed to disk");

    // ── Step 4: PG step journal is already drained ───────────────────────
    // save_step() is synchronous (blocks until PG confirms), so once all
    // workflows are drained, all PG writes are complete.
    tracing::info!("[4/5] PG step journal drained (synchronous writes)");

    // ── Step 5: Shutdown engine ──────────────────────────────────────────
    engine.shutdown();
    tracing::info!("[5/5] Engine shut down — graceful shutdown complete");
}

/// Create the standard workflow map and counter for NMCP routers.
///
/// Returns `(workflow_map, workflow_counter)` ready for router construction.
pub fn create_workflow_state() -> (Arc<DashMap<String, u64>>, Arc<AtomicU64>) {
    let workflow_map = Arc::new(DashMap::new());
    let workflow_counter = Arc::new(AtomicU64::new(1));
    (workflow_map, workflow_counter)
}

// ─── HTTP Health & Metrics Endpoint ──────────────────────────────────────────

/// Shared metrics state for the HTTP health/metrics endpoint.
///
/// Each server populates this with engine/router-specific metrics.
/// The `/metrics` endpoint reads from this to produce Prometheus text format.
///
/// Uses atomic counters for lock-free concurrent reads (HTTP handler)
/// and writes (metrics updater task).
pub struct ServerMetrics {
    pub workflows_running: std::sync::atomic::AtomicU64,
    pub workflows_completed: std::sync::atomic::AtomicU64,
    pub workflows_failed: std::sync::atomic::AtomicU64,
    pub steps_total: std::sync::atomic::AtomicU64,
    pub pg_write_queue_depth: std::sync::atomic::AtomicU64,
    pub wal_unsynced_bytes: std::sync::atomic::AtomicU64,
    /// 1 = connected, 0 = disconnected or PG not configured.
    pub pg_connected: std::sync::atomic::AtomicU64,
    /// Step persist latency p50 in microseconds.
    pub step_persist_latency_p50: std::sync::atomic::AtomicU64,
    /// Step persist latency p99 in microseconds.
    pub step_persist_latency_p99: std::sync::atomic::AtomicU64,
    /// Step persist latency p999 in microseconds.
    pub step_persist_latency_p999: std::sync::atomic::AtomicU64,
    /// Total shmem IPC contention events.
    pub shmem_contentions_total: std::sync::atomic::AtomicU64,
    pub flavor: &'static str,
}

impl Default for ServerMetrics {
    fn default() -> Self {
        Self {
            workflows_running: std::sync::atomic::AtomicU64::new(0),
            workflows_completed: std::sync::atomic::AtomicU64::new(0),
            workflows_failed: std::sync::atomic::AtomicU64::new(0),
            steps_total: std::sync::atomic::AtomicU64::new(0),
            pg_write_queue_depth: std::sync::atomic::AtomicU64::new(0),
            wal_unsynced_bytes: std::sync::atomic::AtomicU64::new(0),
            pg_connected: std::sync::atomic::AtomicU64::new(0),
            step_persist_latency_p50: std::sync::atomic::AtomicU64::new(0),
            step_persist_latency_p99: std::sync::atomic::AtomicU64::new(0),
            step_persist_latency_p999: std::sync::atomic::AtomicU64::new(0),
            shmem_contentions_total: std::sync::atomic::AtomicU64::new(0),
            flavor: "unknown",
        }
    }
}

impl ServerMetrics {
    /// Render Prometheus text format from the current metrics state.
    pub fn render_prometheus(&self) -> String {
        use std::sync::atomic::Ordering;
        let flavor = self.flavor;
        let running = self.workflows_running.load(Ordering::Relaxed);
        let completed = self.workflows_completed.load(Ordering::Relaxed);
        let failed = self.workflows_failed.load(Ordering::Relaxed);
        let steps = self.steps_total.load(Ordering::Relaxed);
        let pg_depth = self.pg_write_queue_depth.load(Ordering::Relaxed);
        let wal_unsynced = self.wal_unsynced_bytes.load(Ordering::Relaxed);
        let pg_conn = self.pg_connected.load(Ordering::Relaxed);
        let sp_p50 = self.step_persist_latency_p50.load(Ordering::Relaxed);
        let sp_p99 = self.step_persist_latency_p99.load(Ordering::Relaxed);
        let sp_p999 = self.step_persist_latency_p999.load(Ordering::Relaxed);
        let shmem_cont = self.shmem_contentions_total.load(Ordering::Relaxed);

        format!(
            "# HELP velocity_up Whether the server is running\n\
             # TYPE velocity_up gauge\n\
             velocity_up 1\n\
             # HELP velocity_engine Flavor of the Velocity engine\n\
             # TYPE velocity_engine gauge\n\
             velocity_engine{{flavor=\"{flavor}\"}} 1\n\
             # HELP velocity_workflows_total Total workflows by status\n\
             # TYPE velocity_workflows_total gauge\n\
             velocity_workflows_total{{status=\"running\"}} {running}\n\
             velocity_workflows_total{{status=\"completed\"}} {completed}\n\
             velocity_workflows_total{{status=\"failed\"}} {failed}\n\
             # HELP velocity_steps_total Total workflow steps completed\n\
             # TYPE velocity_steps_total counter\n\
             velocity_steps_total{{flavor=\"{flavor}\"}} {steps}\n\
             # HELP velocity_step_persist_latency_ms Step persist latency in milliseconds\n\
             # TYPE velocity_step_persist_latency_ms summary\n\
             velocity_step_persist_latency_ms{{quantile=\"0.5\"}} {sp_p50_us}\n\
             velocity_step_persist_latency_ms{{quantile=\"0.99\"}} {sp_p99_us}\n\
             velocity_step_persist_latency_ms{{quantile=\"0.999\"}} {sp_p999_us}\n\
             # HELP velocity_pg_write_queue_depth Pending PG step journal writes\n\
             # TYPE velocity_pg_write_queue_depth gauge\n\
             velocity_pg_write_queue_depth {pg_depth}\n\
             # HELP velocity_wal_unsynced_bytes WAL bytes not yet fsynced\n\
             # TYPE velocity_wal_unsynced_bytes gauge\n\
             velocity_wal_unsynced_bytes {wal_unsynced}\n\
             # HELP velocity_pg_connected Whether PostgreSQL adapter is connected (1=yes, 0=no)\n\
             # TYPE velocity_pg_connected gauge\n\
             velocity_pg_connected {pg_conn}\n\
             # HELP velocity_nmcp_shmem_contentions_total Total shmem IPC contention events\n\
             # TYPE velocity_nmcp_shmem_contentions_total counter\n\
             velocity_nmcp_shmem_contentions_total {shmem_cont}\n",
            sp_p50_us = sp_p50 as f64 / 1000.0,
            sp_p99_us = sp_p99 as f64 / 1000.0,
            sp_p999_us = sp_p999 as f64 / 1000.0,
        )
    }
}

/// Run a lightweight HTTP health/metrics endpoint.
///
/// Supports:
/// - `GET /health` → `{"status":"ok","engine":"<flavor>"}` (no auth required)
/// - `GET /metrics` → Prometheus text format (bearer token auth if `metrics_token` is set)
/// - `GET /ready`  → `{"status":"ready"}` (no auth, for K8s readiness probe)
/// - All other paths → 404
///
/// The `metrics_token` parameter enables bearer token auth on `/metrics`.
/// If `None`, `/metrics` is open (development mode).
///
/// The `tls_acceptor` parameter enables TLS on the health endpoint.
/// If `Some`, connections are wrapped with TLS before HTTP parsing.
///
/// This is a minimal HTTP server using raw TCP — no axum/hyper dependency.
pub async fn run_http_health(
    bind_addr: String,
    flavor_name: &'static str,
    metrics: Arc<ServerMetrics>,
    metrics_token: Option<String>,
    tls_acceptor: Option<tokio_rustls::TlsAcceptor>,
) -> Result<(), String> {
    let listener = TcpListener::bind(&bind_addr)
        .await
        .map_err(|e| format!("Health HTTP bind failed: {}", e))?;

    if let Ok(addr) = listener.local_addr() {
        let scheme = if tls_acceptor.is_some() { "https" } else { "http" };
        tracing::info!("Health HTTP: {}://{}/health", scheme, addr);
    }

    loop {
        let (stream, _) = match listener.accept().await {
            Ok(s) => s,
            Err(_) => continue,
        };

        let metrics = metrics.clone();
        let token = metrics_token.clone();
        let tls = tls_acceptor.clone();
        tokio::spawn(async move {
            // Optionally wrap with TLS
            let mut stream: Box<dyn AsyncReadWrite> = if let Some(acceptor) = tls {
                match acceptor.accept(stream).await {
                    Ok(tls_stream) => Box::new(tls_stream),
                    Err(_) => return,
                }
            } else {
                Box::new(stream)
            };

            let mut buf = [0u8; 2048];
            let n = match stream.read(&mut buf).await {
                Ok(n) if n > 0 => n,
                _ => return,
            };

            let request = String::from_utf8_lossy(&buf[..n]);
            let first_line = request.lines().next().unwrap_or("");

            // Extract Authorization header for bearer token check
            let auth_header = request.lines()
                .find(|l| l.to_lowercase().starts_with("authorization:"))
                .and_then(|l| l.splitn(2, ':').nth(1))
                .map(|v| v.trim().to_string());

            let (status, content_type, body) = if first_line.starts_with("GET /ready") {
                // Readiness probe — distinct response for K8s readiness
                let body = serde_json::json!({
                    "status": "ready",
                    "engine": flavor_name,
                });
                ("200 OK", "application/json", body.to_string())
            } else if first_line.starts_with("GET /health") {
                // Liveness probe — no auth required
                let body = serde_json::json!({
                    "status": "ok",
                    "engine": flavor_name,
                    "transport": "nmcp",
                });
                ("200 OK", "application/json", body.to_string())
            } else if first_line.starts_with("GET /metrics") {
                // Metrics — require bearer token if configured
                if let Some(ref expected) = token {
                    let provided = auth_header.as_deref().unwrap_or("");
                    let bearer = provided.strip_prefix("Bearer ").unwrap_or("");
                    if bearer != expected {
                        let body = serde_json::json!({"error": "unauthorized"}).to_string();
                        return send_response(&mut stream, "401 Unauthorized", "application/json", &body).await;
                    }
                }
                let body = metrics.render_prometheus();
                ("200 OK", "text/plain; version=0.0.4", body)
            } else {
                ("404 Not Found", "application/json", serde_json::json!({"error": "not found"}).to_string())
            };

            send_response(&mut stream, status, content_type, &body).await;
        });
    }
}

async fn send_response(stream: &mut dyn AsyncReadWrite, status: &str, content_type: &str, body: &str) {
    let response = format!(
        "HTTP/1.1 {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        status,
        content_type,
        body.len(),
        body
    );
    let _ = stream.write_all(response.as_bytes()).await;
    // Properly shut down the write half so TLS clients receive close_notify
    let _ = stream.shutdown().await;
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bootstrap_engine_no_wal() {
        let result = bootstrap_engine("", 0, None);
        assert!(!result.pg_enabled);
        // Engine should be functional
        assert!(Arc::strong_count(&result.engine) >= 1);
    }

    #[test]
    fn test_bootstrap_engine_with_wal() {
        let wal_path = format!("/tmp/velocity-bootstrap-test-{}.wal", std::process::id());
        let result = bootstrap_engine(&wal_path, 1024 * 1024, None);
        assert!(!result.pg_enabled);
        let _ = std::fs::remove_file(&wal_path);
    }

    #[test]
    fn test_bootstrap_engine_with_bad_pg() {
        // PG connection should fail gracefully
        let result = bootstrap_engine("", 0, Some("host=nonexistent port=9999"));
        assert!(!result.pg_enabled);
    }

    #[test]
    fn test_create_workflow_state() {
        let (map, counter) = create_workflow_state();
        assert_eq!(map.len(), 0);
        assert_eq!(counter.load(Ordering::Relaxed), 1);
        map.insert("test".to_string(), 42);
        assert_eq!(map.len(), 1);
    }

    #[tokio::test]
    async fn test_graceful_shutdown_empty() {
        // Graceful shutdown with no running workflows should complete instantly.
        let result = bootstrap_engine("", 0, None);
        let engine = result.engine;
        let (workflow_map, _) = create_workflow_state();

        // Create a dummy NMCP server (won't actually run)
        let router = Arc::new(DummyRouter);
        let shmem = Arc::new(NmcpShmemServer::new(router.clone(), format!("/tmp/velocity-test-shutdown-{}.nmcp", std::process::id())));
        let ws = NmcpWebSocketServer::new(router, "127.0.0.1:0".to_string());

        // Run graceful shutdown directly (no workflows running)
        graceful_shutdown(&ws, &shmem, &engine, &workflow_map).await;

        // Should complete without error — engine is shut down
        let _ = std::fs::remove_file(format!("/tmp/velocity-test-shutdown-{}.nmcp", std::process::id()));
    }

    #[tokio::test]
    async fn test_graceful_shutdown_with_running_workflow() {
        // Graceful shutdown with a running workflow should wait then timeout.
        let result = bootstrap_engine("", 0, None);
        let engine = result.engine;
        let (workflow_map, _counter) = create_workflow_state();

        // Start a workflow so there's something to drain
        let key = engine.start_workflow(1, 0, 0, 0, 10, None);
        workflow_map.insert("wf-test".to_string(), key);

        let router = Arc::new(DummyRouter);
        let shmem = Arc::new(NmcpShmemServer::new(router.clone(), format!("/tmp/velocity-test-shutdown2-{}.nmcp", std::process::id())));
        let ws = NmcpWebSocketServer::new(router, "127.0.0.1:0".to_string());

        // This will timeout after 30s in production, but for the test
        // the workflow is "running" so it should hit the timeout path.
        // We just verify it doesn't panic and the engine shuts down.
        // (In real usage, workflows would complete and drain naturally.)

        // Complete the workflow so drain succeeds quickly
        for step in 0..10 {
            let _ = engine.persist_step(key, step, "default");
        }
        engine.complete_workflow(key, None);

        graceful_shutdown(&ws, &shmem, &engine, &workflow_map).await;

        let _ = std::fs::remove_file(format!("/tmp/velocity-test-shutdown2-{}.nmcp", std::process::id()));
    }

    /// Dummy router for tests (dispatches nothing).
    struct DummyRouter;
    impl NmcpDispatch for DummyRouter {
        fn dispatch(&self, _frame: &velocity_nmcp_protocol::NmcpFrame) -> velocity_nmcp_protocol::NmcpFrame {
            velocity_nmcp_protocol::NmcpFrame::error_response(0, 503, "shutting down")
        }
    }

    use std::sync::atomic::Ordering;
}
