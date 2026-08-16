//! Lightweight HTTP benchmark server for fair cross-engine comparison.
//!
//! Provides a single `POST /bench/simple_workflow` endpoint that executes a
//! 10-step workflow with real SHA-256 compute work and per-step PostgreSQL
//! persistence — the same workload that DBOS, Restate, and Temporal expose
//! through their HTTP endpoints.
//!
//! Uses raw `tokio::net::TcpListener` with minimal HTTP/1.1 parsing — no
//! extra dependencies (no axum, no hyper).  This is intentionally simple:
//! the goal is a level playing field where every engine serves the same
//! workload over the same protocol.

use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use velocity_workflow_engine::engine::WorkflowEngine;

/// Start the HTTP benchmark server.  Spawns a tokio task that accepts
/// connections and handles requests concurrently.
pub fn spawn_http_bench(engine: Arc<WorkflowEngine>, port: u16) {
    tokio::spawn(async move {
        let addr = format!("0.0.0.0:{}", port);
        let listener = match TcpListener::bind(&addr).await {
            Ok(l) => l,
            Err(e) => {
                eprintln!("HTTP bench: failed to bind {}: {}", addr, e);
                return;
            }
        };
        println!("  HTTP bench: http://0.0.0.0:{}/bench/simple_workflow", port);

        loop {
            let (mut stream, _addr) = match listener.accept().await {
                Ok(pair) => pair,
                Err(e) => {
                    eprintln!("HTTP bench: accept error: {}", e);
                    continue;
                }
            };

            let engine = engine.clone();
            tokio::spawn(async move {
                // Read the full HTTP request (we only need headers to find Content-Length).
                let mut buf = vec![0u8; 4096];
                let n = match stream.read(&mut buf).await {
                    Ok(n) if n > 0 => n,
                    _ => return,
                };
                let request = String::from_utf8_lossy(&buf[..n]);

                // Route: only POST /bench/simple_workflow
                let (status_code, body) = if request.starts_with("POST /bench/simple_workflow") {
                    match engine.run_bench_workflow("default") {
                        Ok(_key) => (200, r#"{"status":"completed","steps":10}"#),
                        Err(e) => (500, &format!(r#"{{"error":"{}"}}"#, e) as &str),
                    }
                } else if request.starts_with("GET /health") {
                    (200, r#"{"status":"ok","engine":"Velocity"}"#)
                } else {
                    (404, r#"{"error":"not found"}"#)
                };

                let response = format!(
                    "HTTP/1.1 {} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    status_code,
                    body.len(),
                    body
                );
                let _ = stream.write_all(response.as_bytes()).await;
            });
        }
    });
}
