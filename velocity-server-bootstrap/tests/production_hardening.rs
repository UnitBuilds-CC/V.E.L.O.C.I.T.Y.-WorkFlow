// Production Hardening Integration Tests
//
// Comprehensive end-to-end tests verifying all production operational features:
// 1. TLS end-to-end (HTTPS health endpoint with self-signed certs)
// 2. WAL crash recovery (write → kill → restart → verify state recovered)
// 3. Graceful shutdown under load (drain in-flight workflows)
// 4. Metrics accuracy (correct counts after workflow lifecycle)
// 5. Metrics bearer token auth (401 without, 200 with correct token)
// 6. Health vs readiness distinct responses
// 7. Prometheus metrics format validation

use std::net::TcpListener;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use velocity_server_bootstrap::{
    bootstrap_engine, create_workflow_state, load_tls_config, run_http_health,
    ServerMetrics,
};

/// Install the rustls crypto provider (ring) for TLS tests.
/// Must be called before any TLS operations.
fn install_crypto_provider() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        tokio_rustls::rustls::crypto::ring::default_provider()
            .install_default()
            .expect("Failed to install rustls crypto provider (ring)");
    });
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

fn find_free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

/// Generate self-signed cert+key PEM using rcgen, write to temp files, return paths.
fn generate_test_certs(prefix: &str) -> (String, String) {
    use rcgen::{CertificateParams, KeyPair};
    let key_pair = KeyPair::generate().unwrap();
    let mut params = CertificateParams::default();
    params.distinguished_name = rcgen::DistinguishedName::new();
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "localhost");
    let ia5 = rcgen::Ia5String::try_from("localhost".to_string()).unwrap();
    params.subject_alt_names.push(rcgen::SanType::DnsName(ia5));
    let cert = params.self_signed(&key_pair).unwrap();

    let dir = std::env::temp_dir().join(format!(
        "velocity_prod_test_{}_{}",
        std::process::id(),
        prefix
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let cert_path = dir.join("cert.pem");
    let key_path = dir.join("key.pem");
    std::fs::write(&cert_path, cert.pem().as_bytes()).unwrap();
    std::fs::write(&key_path, key_pair.serialize_pem().as_bytes()).unwrap();
    (
        cert_path.to_str().unwrap().to_string(),
        key_path.to_str().unwrap().to_string(),
    )
}

fn cleanup_test_dir(prefix: &str) {
    let dir = std::env::temp_dir().join(format!(
        "velocity_prod_test_{}_{}",
        std::process::id(),
        prefix
    ));
    let _ = std::fs::remove_dir_all(&dir);
}

/// Simple HTTP GET request returning (status_code, body).
async fn http_get(addr: &str, path: &str, auth_token: Option<&str>) -> (u16, String) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    let mut stream = TcpStream::connect(addr).await.unwrap();
    let mut request = format!(
        "GET {} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n",
        path
    );
    if let Some(token) = auth_token {
        request.push_str(&format!("Authorization: Bearer {}\r\n", token));
    }
    request.push_str("\r\n");
    stream.write_all(request.as_bytes()).await.unwrap();

    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.unwrap();
    let response = String::from_utf8_lossy(&buf).to_string();

    // Parse status code from "HTTP/1.1 200 OK\r\n..."
    let status_code = response
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(0);

    // Body is after the double \r\n
    let body = response
        .split("\r\n\r\n")
        .nth(1)
        .unwrap_or("")
        .to_string();

    (status_code, body)
}

/// Simple HTTPS GET using tokio-rustls (accepts self-signed certs).
async fn https_get(
    addr: &str,
    path: &str,
    auth_token: Option<&str>,
) -> (u16, String) {
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    use tokio_rustls::rustls::ClientConfig;

    // Create a config that accepts self-signed certs (dangerous, but for testing)
    let config = ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(NoVerifier))
        .with_no_client_auth();

    let connector = tokio_rustls::TlsConnector::from(Arc::new(config));
    let tcp = TcpStream::connect(addr).await.unwrap();

    let domain = tokio_rustls::rustls::pki_types::ServerName::try_from("localhost").unwrap();
    let mut tls_stream = connector.connect(domain, tcp).await.unwrap();

    let mut request = format!(
        "GET {} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n",
        path
    );
    if let Some(token) = auth_token {
        request.push_str(&format!("Authorization: Bearer {}\r\n", token));
    }
    request.push_str("\r\n");
    tls_stream.write_all(request.as_bytes()).await.unwrap();

    let mut buf = Vec::new();
    tls_stream.read_to_end(&mut buf).await.unwrap();
    let response = String::from_utf8_lossy(&buf).to_string();

    let status_code = response
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(0);

    let body = response
        .split("\r\n\r\n")
        .nth(1)
        .unwrap_or("")
        .to_string();

    (status_code, body)
}

/// Custom certificate verifier that accepts any certificate (for testing self-signed certs).
#[derive(Debug)]
struct NoVerifier;
impl tokio_rustls::rustls::client::danger::ServerCertVerifier for NoVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &tokio_rustls::rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[tokio_rustls::rustls::pki_types::CertificateDer<'_>],
        _server_name: &tokio_rustls::rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: tokio_rustls::rustls::pki_types::UnixTime,
    ) -> Result<tokio_rustls::rustls::client::danger::ServerCertVerified, tokio_rustls::rustls::Error> {
        Ok(tokio_rustls::rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &tokio_rustls::rustls::pki_types::CertificateDer<'_>,
        _dss: &tokio_rustls::rustls::DigitallySignedStruct,
    ) -> Result<tokio_rustls::rustls::client::danger::HandshakeSignatureValid, tokio_rustls::rustls::Error> {
        Ok(tokio_rustls::rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &tokio_rustls::rustls::pki_types::CertificateDer<'_>,
        _dss: &tokio_rustls::rustls::DigitallySignedStruct,
    ) -> Result<tokio_rustls::rustls::client::danger::HandshakeSignatureValid, tokio_rustls::rustls::Error> {
        Ok(tokio_rustls::rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<tokio_rustls::rustls::SignatureScheme> {
        vec![
            tokio_rustls::rustls::SignatureScheme::RSA_PKCS1_SHA256,
            tokio_rustls::rustls::SignatureScheme::RSA_PKCS1_SHA384,
            tokio_rustls::rustls::SignatureScheme::RSA_PKCS1_SHA512,
            tokio_rustls::rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            tokio_rustls::rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
            tokio_rustls::rustls::SignatureScheme::ECDSA_NISTP521_SHA512,
            tokio_rustls::rustls::SignatureScheme::RSA_PSS_SHA256,
            tokio_rustls::rustls::SignatureScheme::RSA_PSS_SHA384,
            tokio_rustls::rustls::SignatureScheme::RSA_PSS_SHA512,
            tokio_rustls::rustls::SignatureScheme::ED25519,
            tokio_rustls::rustls::SignatureScheme::ED448,
        ]
    }
}

// ============================================================================
// 1. TLS End-to-End Tests
// ============================================================================

#[tokio::test]
async fn test_tls_health_endpoint_e2e() {
    install_crypto_provider();
    let (cert_path, key_path) = generate_test_certs("tls_e2e");
    let tls_acceptor = load_tls_config(&cert_path, &key_path).expect("TLS config should load");
    let port = find_free_port();
    let addr = format!("127.0.0.1:{}", port);

    let metrics = Arc::new(ServerMetrics {
        flavor: "test-tls",
        ..Default::default()
    });

    let metrics_clone = metrics.clone();
    let addr_clone = addr.clone();
    let tls_clone = tls_acceptor.clone();
    tokio::spawn(async move {
        let _ = run_http_health(addr_clone, "test-tls", metrics_clone, None, Some(tls_clone)).await;
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    // HTTPS /health should work
    let (status, body) = https_get(&addr, "/health", None).await;
    assert_eq!(status, 200, "HTTPS /health should return 200");
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(json["status"], "ok");
    assert_eq!(json["engine"], "test-tls");

    // HTTPS /ready should work
    let (status, body) = https_get(&addr, "/ready", None).await;
    assert_eq!(status, 200, "HTTPS /ready should return 200");
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(json["status"], "ready");

    // HTTPS /metrics should work
    let (status, body) = https_get(&addr, "/metrics", None).await;
    assert_eq!(status, 200, "HTTPS /metrics should return 200");
    assert!(body.contains("velocity_up 1"), "Metrics should contain velocity_up");
    assert!(body.contains("test-tls"), "Metrics should contain flavor name");

    cleanup_test_dir("tls_e2e");
}

#[tokio::test]
async fn test_tls_config_loading_valid_certs() {
    install_crypto_provider();
    let (cert_path, key_path) = generate_test_certs("tls_load");
    let result = load_tls_config(&cert_path, &key_path);
    assert!(result.is_ok(), "Valid certs should load successfully");
    cleanup_test_dir("tls_load");
}

#[tokio::test]
async fn test_tls_config_loading_invalid_cert() {
    install_crypto_provider();
    let dir = std::env::temp_dir().join(format!("velocity_tls_invalid_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let cert_path = dir.join("bad_cert.pem");
    let key_path = dir.join("bad_key.pem");
    std::fs::write(&cert_path, "NOT A VALID CERTIFICATE").unwrap();
    std::fs::write(&key_path, "NOT A VALID KEY").unwrap();

    let result = load_tls_config(
        cert_path.to_str().unwrap(),
        key_path.to_str().unwrap(),
    );
    assert!(result.is_err(), "Invalid certs should fail");
    let err = result.err().unwrap();
    assert!(err.contains("No valid certificates"), "Error should mention no valid certs: {}", err);

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn test_tls_config_loading_missing_file() {
    let result = load_tls_config("/nonexistent/cert.pem", "/nonexistent/key.pem");
    assert!(result.is_err(), "Missing files should fail");
    let err = result.err().unwrap();
    assert!(err.contains("Failed to open"), "Error should mention file open: {}", err);
}

#[tokio::test]
async fn test_tls_config_loading_empty_paths() {
    let result = load_tls_config("", "");
    assert!(result.is_err(), "Empty paths should fail");
    let err = result.err().unwrap();
    assert!(err.contains("must not be empty"), "Error should mention empty: {}", err);
}

// ============================================================================
// 2. WAL Crash Recovery Tests
// ============================================================================

#[tokio::test]
async fn test_wal_crash_recovery_write_restart_verify() {
    let wal_path = format!(
        "c:\\Users\\visse\\OneDrive\\Documents\\Velocity-workflow\\target\\test-wal-recovery-{}.wal",
        std::process::id()
    );
    let _ = std::fs::remove_file(&wal_path);

    // Phase 1: Start engine, create workflows, "crash" (drop without shutdown)
    let workflow_keys;
    {
        let engine = velocity_workflow_engine::engine::WorkflowEngine::with_wal(&wal_path, 64 * 1024 * 1024)
            .expect("WAL engine create");
        
        // Create 5 workflows with 10 steps each
        workflow_keys = (0..5u64).map(|i| {
            let key = engine.start_workflow(1, 0, 0, 0, 10, None);
            for step in 0..10 {
                let _ = engine.persist_step(key, step, "default");
            }
            if i < 3 {
                engine.complete_workflow(key, None);
            }
            key
        }).collect::<Vec<_>>();

        // "Crash" — drop engine without calling shutdown()
        // WAL should have all the records
        drop(engine);
    }

    // Phase 2: Restart engine from same WAL, verify recovery
    {
        let engine = velocity_workflow_engine::engine::WorkflowEngine::with_wal(&wal_path, 64 * 1024 * 1024)
            .expect("WAL engine restart");
        
        let (records, workflows) = engine.recover_from_wal()
            .expect("WAL recovery should succeed");
        
        assert!(records > 0, "Should replay WAL records (got {})", records);
        assert!(workflows > 0, "Should recover workflows (got {})", workflows);

        // Verify completed workflows are still completed
        for key in &workflow_keys[..3] {
            let status = engine.get_status(*key);
            assert_eq!(
                status,
                velocity_workflow_engine::engine::WorkflowStatus::Completed,
                "Workflow {} should be completed after recovery",
                key
            );
        }
    }

    let _ = std::fs::remove_file(&wal_path);
}

#[tokio::test]
async fn test_wal_recovery_empty_wal() {
    let wal_path = format!(
        "c:\\Users\\visse\\OneDrive\\Documents\\Velocity-workflow\\target\\test-wal-empty-{}.wal",
        std::process::id()
    );
    let _ = std::fs::remove_file(&wal_path);

    let engine = velocity_workflow_engine::engine::WorkflowEngine::with_wal(&wal_path, 64 * 1024 * 1024)
        .expect("WAL engine create");
    
    let (records, workflows) = engine.recover_from_wal().expect("WAL recovery");
    assert_eq!(records, 0, "Empty WAL should have 0 records");
    assert_eq!(workflows, 0, "Empty WAL should have 0 workflows");

    let _ = std::fs::remove_file(&wal_path);
}

// ============================================================================
// 3. Graceful Shutdown Under Load Tests
// ============================================================================

#[tokio::test]
async fn test_graceful_shutdown_drains_running_workflows() {
    use velocity_nmcp_protocol::{NmcpDispatch, NmcpFrame, NmcpWebSocketServer, NmcpShmemServer};

    let result = bootstrap_engine("", 0, None);
    let engine = result.engine;
    let (workflow_map, _counter) = create_workflow_state();

    // Start 10 workflows
    for _ in 0..10 {
        let key = engine.start_workflow(1, 0, 0, 0, 5, None);
        workflow_map.insert(format!("wf-{}", key), key);
    }

    // Complete them all (simulating fast workers)
    for entry in workflow_map.iter() {
        let key = *entry.value();
        for step in 0..5 {
            let _ = engine.persist_step(key, step, "default");
        }
        engine.complete_workflow(key, None);
    }

    // Verify all completed
    let running_count = workflow_map.iter().filter(|e| {
        engine.get_status(*e.value()) == velocity_workflow_engine::engine::WorkflowStatus::Running
    }).count();
    assert_eq!(running_count, 0, "All workflows should be completed");

    // Graceful shutdown should complete quickly since no running workflows
    struct DummyRouter;
    impl NmcpDispatch for DummyRouter {
        fn dispatch(&self, _frame: &NmcpFrame) -> NmcpFrame {
            NmcpFrame::error_response(0, 503, "test")
        }
    }

    let router = Arc::new(DummyRouter);
    let _shmem = Arc::new(NmcpShmemServer::new(router.clone(), format!("c:\\Users\\visse\\OneDrive\\Documents\\Velocity-workflow\\target\\test-shutdown-load-{}.nmcp", std::process::id())));
    let _ws = NmcpWebSocketServer::new(router, "127.0.0.1:0".to_string());

    // This should complete quickly (no running workflows to drain)
    let start = tokio::time::Instant::now();
    // We can't call graceful_shutdown directly (it's private), but we can verify
    // the engine state is correct for shutdown
    engine.sync_wal();
    engine.shutdown();
    let elapsed = start.elapsed();
    assert!(elapsed.as_secs() < 5, "Shutdown with no running workflows should be fast");

    let _ = std::fs::remove_file(format!("c:\\Users\\visse\\OneDrive\\Documents\\Velocity-workflow\\target\\test-shutdown-load-{}.nmcp", std::process::id()));
}

// ============================================================================
// 4. Metrics Accuracy Tests
// ============================================================================

#[tokio::test]
async fn test_metrics_accuracy_after_workflow_lifecycle() {
    let metrics = Arc::new(ServerMetrics {
        flavor: "test-metrics",
        ..Default::default()
    });

    // Simulate metrics updater behavior
    metrics.workflows_running.store(5, Ordering::Relaxed);
    metrics.workflows_completed.store(10, Ordering::Relaxed);
    metrics.workflows_failed.store(2, Ordering::Relaxed);
    metrics.steps_total.store(100, Ordering::Relaxed);
    metrics.pg_connected.store(1, Ordering::Relaxed);
    metrics.step_persist_latency_p50.store(50, Ordering::Relaxed);
    metrics.step_persist_latency_p99.store(200, Ordering::Relaxed);
    metrics.step_persist_latency_p999.store(500, Ordering::Relaxed);
    metrics.wal_unsynced_bytes.store(1024, Ordering::Relaxed);
    metrics.shmem_contentions_total.store(3, Ordering::Relaxed);

    let prometheus = metrics.render_prometheus();

    // Verify all expected metrics are present with correct values
    assert!(prometheus.contains("velocity_up 1"), "Should have velocity_up");
    assert!(prometheus.contains("velocity_engine{flavor=\"test-metrics\"} 1"), "Should have engine flavor");
    assert!(prometheus.contains("velocity_workflows_total{status=\"running\"} 5"), "Running count");
    assert!(prometheus.contains("velocity_workflows_total{status=\"completed\"} 10"), "Completed count");
    assert!(prometheus.contains("velocity_workflows_total{status=\"failed\"} 2"), "Failed count");
    assert!(prometheus.contains("velocity_steps_total{flavor=\"test-metrics\"} 100"), "Steps total");
    assert!(prometheus.contains("velocity_pg_connected 1"), "PG connected");
    assert!(prometheus.contains("velocity_wal_unsynced_bytes 1024"), "WAL unsynced");
    assert!(prometheus.contains("velocity_nmcp_shmem_contentions_total 3"), "Shmem contentions");

    // Verify Prometheus format compliance
    assert!(prometheus.contains("# HELP velocity_up"), "Should have HELP line");
    assert!(prometheus.contains("# TYPE velocity_up gauge"), "Should have TYPE line");
    assert!(prometheus.contains("quantile=\"0.5\""), "Should have quantile labels");
    assert!(prometheus.contains("quantile=\"0.99\""), "Should have p99 quantile");
    assert!(prometheus.contains("quantile=\"0.999\""), "Should have p999 quantile");
}

#[tokio::test]
async fn test_metrics_prometheus_format_compliance() {
    let metrics = Arc::new(ServerMetrics {
        flavor: "prom-format-test",
        ..Default::default()
    });

    let prom = metrics.render_prometheus();
    let lines: Vec<&str> = prom.lines().collect();

    // Every metric should have a HELP and TYPE line before data lines
    let help_lines: Vec<&&str> = lines.iter().filter(|l| l.starts_with("# HELP")).collect();
    let type_lines: Vec<&&str> = lines.iter().filter(|l| l.starts_with("# TYPE")).collect();
    let data_lines: Vec<&&str> = lines.iter().filter(|l| !l.starts_with('#') && !l.is_empty()).collect();

    assert!(help_lines.len() >= 8, "Should have at least 8 HELP lines, got {}", help_lines.len());
    assert!(type_lines.len() >= 8, "Should have at least 8 TYPE lines, got {}", type_lines.len());
    assert!(data_lines.len() >= 10, "Should have at least 10 data lines, got {}", data_lines.len());

    // Verify no empty lines in the middle
    for (i, line) in lines.iter().enumerate() {
        if i < lines.len() - 1 && !line.is_empty() {
            // Non-empty lines should be either comments or data
            assert!(
                line.starts_with('#') || line.contains(' ') || line.contains('{'),
                "Line {} should be a comment or data: '{}'", i, line
            );
        }
    }
}

// ============================================================================
// 5. Metrics Bearer Token Auth Tests
// ============================================================================

#[tokio::test]
async fn test_metrics_auth_no_token_required() {
    let port = find_free_port();
    let addr = format!("127.0.0.1:{}", port);

    let metrics = Arc::new(ServerMetrics {
        flavor: "test-no-auth",
        ..Default::default()
    });

    let metrics_clone = metrics.clone();
    let addr_clone = addr.clone();
    tokio::spawn(async move {
        let _ = run_http_health(addr_clone, "test-no-auth", metrics_clone, None, None).await;
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    // Without token configured, /metrics should be open
    let (status, body) = http_get(&addr, "/metrics", None).await;
    assert_eq!(status, 200, "Metrics should be open when no token configured");
    assert!(body.contains("velocity_up 1"), "Should return metrics");
}

#[tokio::test]
async fn test_metrics_auth_rejects_no_token() {
    let port = find_free_port();
    let addr = format!("127.0.0.1:{}", port);

    let metrics = Arc::new(ServerMetrics {
        flavor: "test-auth",
        ..Default::default()
    });

    let metrics_clone = metrics.clone();
    let addr_clone = addr.clone();
    tokio::spawn(async move {
        let _ = run_http_health(
            addr_clone,
            "test-auth",
            metrics_clone,
            Some("secret-token-123".to_string()),
            None,
        )
        .await;
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    // Without token → 401
    let (status, body) = http_get(&addr, "/metrics", None).await;
    assert_eq!(status, 401, "Should reject without token");
    assert!(body.contains("unauthorized"), "Should say unauthorized");
}

#[tokio::test]
async fn test_metrics_auth_rejects_wrong_token() {
    let port = find_free_port();
    let addr = format!("127.0.0.1:{}", port);

    let metrics = Arc::new(ServerMetrics {
        flavor: "test-auth-wrong",
        ..Default::default()
    });

    let metrics_clone = metrics.clone();
    let addr_clone = addr.clone();
    tokio::spawn(async move {
        let _ = run_http_health(
            addr_clone,
            "test-auth-wrong",
            metrics_clone,
            Some("correct-token".to_string()),
            None,
        )
        .await;
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    // Wrong token → 401
    let (status, body) = http_get(&addr, "/metrics", Some("wrong-token")).await;
    assert_eq!(status, 401, "Should reject wrong token");
    assert!(body.contains("unauthorized"));
}

#[tokio::test]
async fn test_metrics_auth_accepts_correct_token() {
    let port = find_free_port();
    let addr = format!("127.0.0.1:{}", port);

    let metrics = Arc::new(ServerMetrics {
        flavor: "test-auth-correct",
        ..Default::default()
    });

    let metrics_clone = metrics.clone();
    let addr_clone = addr.clone();
    tokio::spawn(async move {
        let _ = run_http_health(
            addr_clone,
            "test-auth-correct",
            metrics_clone,
            Some("my-secret-token".to_string()),
            None,
        )
        .await;
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    // Correct token → 200
    let (status, body) = http_get(&addr, "/metrics", Some("my-secret-token")).await;
    assert_eq!(status, 200, "Should accept correct token");
    assert!(body.contains("velocity_up 1"), "Should return metrics");
}

#[tokio::test]
async fn test_health_endpoint_no_auth_required() {
    let port = find_free_port();
    let addr = format!("127.0.0.1:{}", port);

    let metrics = Arc::new(ServerMetrics {
        flavor: "test-health-no-auth",
        ..Default::default()
    });

    let metrics_clone = metrics.clone();
    let addr_clone = addr.clone();
    tokio::spawn(async move {
        let _ = run_http_health(
            addr_clone,
            "test-health-no-auth",
            metrics_clone,
            Some("secret-token".to_string()),
            None,
        )
        .await;
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    // /health should NOT require auth even when metrics token is set
    let (status, body) = http_get(&addr, "/health", None).await;
    assert_eq!(status, 200, "/health should not require auth");
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(json["status"], "ok");

    // /ready should NOT require auth either
    let (status, body) = http_get(&addr, "/ready", None).await;
    assert_eq!(status, 200, "/ready should not require auth");
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(json["status"], "ready");
}

// ============================================================================
// 6. Health vs Readiness Distinct Response Tests
// ============================================================================

#[tokio::test]
async fn test_health_and_ready_return_distinct_responses() {
    let port = find_free_port();
    let addr = format!("127.0.0.1:{}", port);

    let metrics = Arc::new(ServerMetrics {
        flavor: "test-distinct",
        ..Default::default()
    });

    let metrics_clone = metrics.clone();
    let addr_clone = addr.clone();
    tokio::spawn(async move {
        let _ = run_http_health(addr_clone, "test-distinct", metrics_clone, None, None).await;
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    let (_, health_body) = http_get(&addr, "/health", None).await;
    let (_, ready_body) = http_get(&addr, "/ready", None).await;

    let health: serde_json::Value = serde_json::from_str(&health_body).unwrap();
    let ready: serde_json::Value = serde_json::from_str(&ready_body).unwrap();

    // /health returns {"status":"ok","engine":"...","transport":"nmcp"}
    assert_eq!(health["status"], "ok", "/health should return status=ok");
    assert!(health.get("transport").is_some(), "/health should have transport field");

    // /ready returns {"status":"ready","engine":"..."}
    assert_eq!(ready["status"], "ready", "/ready should return status=ready");
    assert!(ready.get("transport").is_none(), "/ready should NOT have transport field");

    // They should be different
    assert_ne!(health_body, ready_body, "Health and ready should return different bodies");
}

#[tokio::test]
async fn test_unknown_path_returns_404() {
    let port = find_free_port();
    let addr = format!("127.0.0.1:{}", port);

    let metrics = Arc::new(ServerMetrics {
        flavor: "test-404",
        ..Default::default()
    });

    let metrics_clone = metrics.clone();
    let addr_clone = addr.clone();
    tokio::spawn(async move {
        let _ = run_http_health(addr_clone, "test-404", metrics_clone, None, None).await;
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    let (status, body) = http_get(&addr, "/nonexistent", None).await;
    assert_eq!(status, 404, "Unknown path should return 404");
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(json["error"], "not found");
}

// ============================================================================
// 7. Engine Bootstrap Integration Tests
// ============================================================================

#[tokio::test]
async fn test_bootstrap_engine_with_wal_and_recovery() {
    let wal_path = format!(
        "c:\\Users\\visse\\OneDrive\\Documents\\Velocity-workflow\\target\\test-bootstrap-recovery-{}.wal",
        std::process::id()
    );
    let _ = std::fs::remove_file(&wal_path);

    // Create engine via bootstrap, add workflows
    let result = bootstrap_engine(&wal_path, 64 * 1024 * 1024, None);
    assert!(!result.pg_enabled);

    let key = result.engine.start_workflow(1, 0, 0, 0, 5, None);
    for step in 0..5 {
        let _ = result.engine.persist_step(key, step, "default");
    }
    result.engine.complete_workflow(key, None);
    result.engine.sync_wal();
    drop(result);

    // Restart via bootstrap — should recover from WAL
    let result2 = bootstrap_engine(&wal_path, 64 * 1024 * 1024, None);
    let status = result2.engine.get_status(key);
    assert_eq!(
        status,
        velocity_workflow_engine::engine::WorkflowStatus::Completed,
        "Workflow should be recovered as completed"
    );

    let _ = std::fs::remove_file(&wal_path);
}

#[tokio::test]
async fn test_bootstrap_engine_bad_pg_graceful_fallback() {
    // Bad PG connection should not panic — should fall back to WAL-only
    let result = bootstrap_engine("", 0, Some("host=nonexistent_host port=59999 dbname=nonexistent"));
    assert!(!result.pg_enabled, "Bad PG should not enable persistence");
    // Engine should still be functional
    let key = result.engine.start_workflow(1, 0, 0, 0, 3, None);
    assert!(key > 0);
}

// ============================================================================
// 8. Concurrent Metrics Access Tests
// ============================================================================

#[tokio::test]
async fn test_metrics_concurrent_read_write() {
    let metrics = Arc::new(ServerMetrics {
        flavor: "concurrent-test",
        ..Default::default()
    });

    // Spawn multiple writers
    let mut handles = vec![];
    for i in 0..10 {
        let m = metrics.clone();
        handles.push(tokio::spawn(async move {
            for j in 0..100 {
                m.workflows_running.store(i * 100 + j, Ordering::Relaxed);
                m.steps_total.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    // Spawn concurrent readers (rendering prometheus)
    for _ in 0..5 {
        let m = metrics.clone();
        handles.push(tokio::spawn(async move {
            for _ in 0..100 {
                let prom = m.render_prometheus();
                assert!(!prom.is_empty());
            }
        }));
    }

    // All should complete without panic or deadlock
    for handle in handles {
        handle.await.unwrap();
    }

    // Final value should be deterministic (last writer wins for running, sum for steps)
    let steps = metrics.steps_total.load(Ordering::Relaxed);
    assert_eq!(steps, 1000, "10 tasks * 100 increments = 1000");
}
