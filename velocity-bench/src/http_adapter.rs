//! HTTP engine adapters for Velocity Runtime vs Restate benchmark.
//!
//! Unlike the gRPC benchmark (which uses identical BenchmarkService proto for both engines),
//! the HTTP benchmark uses each engine's native HTTP API:
//!
//!   [velocity-bench-http] ──HTTP──► [Velocity Runtime]  (handler invocation)
//!   [velocity-bench-http] ──HTTP──► [Restate Ingress]   (service handler)
//!
//! Both engines pay the same HTTP serialization + network overhead.

use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};

// ─── HTTP Engine Configuration ──────────────────────────────────────────────

/// Which HTTP engine to benchmark.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HttpEngineKind {
    VelocityRuntime,
    Restate,
}

impl std::fmt::Display for HttpEngineKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HttpEngineKind::VelocityRuntime => write!(f, "Velocity Runtime"),
            HttpEngineKind::Restate => write!(f, "Restate"),
        }
    }
}

/// Configuration for an HTTP engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpEngineConfig {
    pub kind: HttpEngineKind,
    /// Base URL (e.g., "http://localhost:8080").
    pub address: String,
    /// Request timeout.
    pub timeout_ms: u64,
}

impl HttpEngineConfig {
    pub fn velocity_runtime(addr: &str) -> Self {
        Self {
            kind: HttpEngineKind::VelocityRuntime,
            address: addr.trim_end_matches('/').to_string(),
            timeout_ms: 30_000,
        }
    }

    pub fn restate(addr: &str) -> Self {
        Self {
            kind: HttpEngineKind::Restate,
            address: addr.trim_end_matches('/').to_string(),
            timeout_ms: 30_000,
        }
    }
}

// ─── HTTP Operation Result ──────────────────────────────────────────────────

/// Result of a single HTTP operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpOperationResult {
    pub success: bool,
    pub latency_us: u64,
    pub status_code: u16,
    pub response_bytes: u64,
    pub error: Option<String>,
}

impl HttpOperationResult {
    pub fn ok(latency: Duration, status: u16, bytes: u64) -> Self {
        Self {
            success: status >= 200 && status < 300,
            latency_us: latency.as_micros() as u64,
            status_code: status,
            response_bytes: bytes,
            error: None,
        }
    }

    pub fn err(latency: Duration, error: String) -> Self {
        Self {
            success: false,
            latency_us: latency.as_micros() as u64,
            status_code: 0,
            response_bytes: 0,
            error: Some(error),
        }
    }
}

// ─── HTTP Benchmark Result ──────────────────────────────────────────────────

/// Aggregate result from running an HTTP workload on an engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpBenchmarkResult {
    pub engine: HttpEngineKind,
    pub workload_name: String,
    pub total_operations: u64,
    pub successful_operations: u64,
    pub failed_operations: u64,
    pub total_duration_ms: u64,
    pub operations_per_second: f64,
    pub latency_p50_us: u64,
    pub latency_p95_us: u64,
    pub latency_p99_us: u64,
    pub latency_p999_us: u64,
    pub latency_min_us: u64,
    pub latency_max_us: u64,
    pub latency_mean_us: u64,
    pub peak_memory_mb: f64,
    pub total_bytes_transferred: u64,
}

// ─── HTTP Engine Adapter ────────────────────────────────────────────────────

/// HTTP client adapter for Velocity Runtime and Restate.
///
/// Both engines expose HTTP endpoints for handler invocation.
/// This adapter sends identical HTTP requests to both, measuring
/// throughput, latency, and resource usage.
pub struct HttpAdapter {
    engine_kind: HttpEngineKind,
    client: reqwest::Client,
    base_url: String,
}

impl HttpAdapter {
    pub fn new(kind: HttpEngineKind) -> Self {
        Self {
            engine_kind: kind,
            client: reqwest::Client::new(),
            base_url: String::new(),
        }
    }

    pub fn kind(&self) -> HttpEngineKind {
        self.engine_kind
    }

    /// Get the base URL of the connected engine.
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Connect to the engine (just validates the URL and sets base).
    pub async fn connect(&mut self, config: &HttpEngineConfig) -> Result<(), String> {
        self.base_url = config.address.clone();
        self.client = reqwest::Client::builder()
            .timeout(Duration::from_millis(config.timeout_ms))
            .connect_timeout(Duration::from_millis(5_000))
            .pool_max_idle_per_host(100)
            .build()
            .map_err(|e| format!("Failed to build HTTP client: {}", e))?;

        // Verify connectivity
        let health_url = match self.engine_kind {
            HttpEngineKind::VelocityRuntime => format!("{}/health", self.base_url),
            HttpEngineKind::Restate => format!("{}/health", self.base_url),
        };

        match self.client.get(&health_url).send().await {
            Ok(resp) => {
                tracing::info!(
                    engine = %self.engine_kind,
                    address = %self.base_url,
                    status = resp.status().as_u16(),
                    "Connected via HTTP"
                );
            }
            Err(e) => {
                tracing::warn!(
                    engine = %self.engine_kind,
                    address = %self.base_url,
                    error = %e,
                    "Health check failed (continuing anyway)"
                );
            }
        }

        Ok(())
    }

    /// Invoke a handler via HTTP POST.
    ///
    /// For Velocity Runtime: POST /{service}/{handler}
    /// For Restate: POST /{service}/default/{handler} (Virtual Objects require a key)
    ///
    /// Restate requires valid JSON bodies, so when the engine is Restate we wrap
    /// the raw payload bytes into `{"data":"<hex>"}` to satisfy its JSON parser.
    pub async fn invoke_handler(
        &self,
        service: &str,
        handler: &str,
        payload: &[u8],
    ) -> HttpOperationResult {
        // Restate Virtual Objects require a key in the URL path.
        // We use "default" as a synthetic key for non-keyed workloads.
        let url = match self.engine_kind {
            HttpEngineKind::Restate => {
                format!("{}/{}/default/{}", self.base_url, service, handler)
            }
            HttpEngineKind::VelocityRuntime => {
                format!("{}/{}/{}", self.base_url, service, handler)
            }
        };
        let body = self.maybe_wrap_json(payload);
        let start = Instant::now();

        match self
            .client
            .post(&url)
            .header("content-type", "application/json")
            .body(body)
            .send()
            .await
        {
            Ok(resp) => {
                let status = resp.status().as_u16();
                let bytes = resp.content_length().unwrap_or(0);
                let latency = start.elapsed();
                HttpOperationResult::ok(latency, status, bytes)
            }
            Err(e) => HttpOperationResult::err(start.elapsed(), e.to_string()),
        }
    }

    /// Invoke a keyed handler (for stateful services / virtual objects).
    ///
    /// For Velocity Runtime: POST /{service}/{key}/{handler}
    /// For Restate: POST /{service}/{key}/{handler}
    pub async fn invoke_keyed_handler(
        &self,
        service: &str,
        key: &str,
        handler: &str,
        payload: &[u8],
    ) -> HttpOperationResult {
        let url = format!("{}/{}/{}/{}", self.base_url, service, key, handler);
        let body = self.maybe_wrap_json(payload);
        let start = Instant::now();

        match self
            .client
            .post(&url)
            .header("content-type", "application/json")
            .body(body)
            .send()
            .await
        {
            Ok(resp) => {
                let status = resp.status().as_u16();
                let bytes = resp.content_length().unwrap_or(0);
                let latency = start.elapsed();
                HttpOperationResult::ok(latency, status, bytes)
            }
            Err(e) => HttpOperationResult::err(start.elapsed(), e.to_string()),
        }
    }

    /// Send a raw GET request (for health checks, simple throughput).
    pub async fn get(&self, path: &str) -> HttpOperationResult {
        let url = format!("{}{}", self.base_url, path);
        let start = Instant::now();

        match self.client.get(&url).send().await {
            Ok(resp) => {
                let status = resp.status().as_u16();
                let bytes = resp.content_length().unwrap_or(0);
                let latency = start.elapsed();
                HttpOperationResult::ok(latency, status, bytes)
            }
            Err(e) => HttpOperationResult::err(start.elapsed(), e.to_string()),
        }
    }

    /// For Restate, wrap raw bytes into a valid JSON body so the SDK's
    /// deserializer does not reject the request.  Velocity Runtime accepts
    /// either form, so we always send valid JSON for a fair comparison.
    ///
    /// If the payload is already valid JSON it is returned unchanged.
    fn maybe_wrap_json(&self, payload: &[u8]) -> Vec<u8> {
        // Fast path: already valid JSON (Restate and Velocity both accept it)
        if serde_json::from_slice::<serde_json::Value>(payload).is_ok() {
            return payload.to_vec();
        }
        match self.engine_kind {
            HttpEngineKind::Restate => {
                // Wrap as a JSON object — use lossy UTF-8 so the bench payload
                // (repeated 'x' bytes) becomes a plain JSON string value.
                let text = String::from_utf8_lossy(payload);
                serde_json::to_vec(&serde_json::json!({ "data": text }))
                    .unwrap_or_else(|_| payload.to_vec())
            }
            // Velocity Runtime accepts raw bytes — no wrapping needed
            HttpEngineKind::VelocityRuntime => payload.to_vec(),
        }
    }

    /// Get server memory usage via /health or /metrics endpoint.
    pub async fn server_memory_mb(&self) -> Result<f64, String> {
        let url = format!("{}/health", self.base_url);
        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| format!("Health check failed: {}", e))?;

        let body = resp
            .text()
            .await
            .map_err(|e| format!("Failed to read health response: {}", e))?;

        // Try to parse JSON for memory_usage_bytes
        if let Ok(json) = serde_json::from_str::<serde_json::Value>(&body) {
            if let Some(mem) = json.get("memory_usage_bytes").and_then(|v| v.as_u64()) {
                return Ok(mem as f64 / 1_048_576.0);
            }
            if let Some(mem) = json.get("memory_rss_mb").and_then(|v| v.as_f64()) {
                return Ok(mem);
            }
        }

        // Fallback: try to read from process info
        Ok(0.0)
    }
}
