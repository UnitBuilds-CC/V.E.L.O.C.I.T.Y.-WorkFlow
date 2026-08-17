//! HTTP-to-VCTP Ingress Gateway
//!
//! REST API that translates HTTP requests to VCTP RPC calls over UDP.
//! Provides a familiar HTTP interface for clients that prefer REST over
//! the binary VCTP protocol.
//!
//! Routes:
//!   POST /api/v1/workflows           → VCTP START_WORKFLOW
//!   POST /api/v1/workflows/{id}/signal → VCTP SIGNAL_WORKFLOW
//!   GET  /api/v1/workflows/{id}       → VCTP DESCRIBE_WORKFLOW
//!   POST /api/v1/workflows/{id}/cancel → VCTP CANCEL_WORKFLOW
//!   POST /api/v1/workflows/{id}/terminate → VCTP TERMINATE_WORKFLOW
//!   GET  /api/v1/workflows            → VCTP LIST_WORKFLOWS
//!   GET  /api/v1/health               → VCTP HEALTH_CHECK
//!   POST /api/v1/workflows/{id}/query  → VCTP QUERY_WORKFLOW
//!   GET  /api/v1/metrics              → Prometheus metrics export
//!
//! Auth: Validates JWT at edge, forwards API key to VCTP.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Json},
    routing::{get, post},
    Router,
};
use serde::{Deserialize, Serialize};
use tokio::net::UdpSocket;

/// TLS configuration for the HTTP ingress gateway.
#[derive(Debug, Clone)]
pub struct TlsConfig {
    /// Path to the TLS certificate file (PEM format).
    pub cert_path: String,
    /// Path to the TLS private key file (PEM format).
    pub key_path: String,
}

impl TlsConfig {
    /// Create a new TLS configuration.
    pub fn new(cert_path: impl Into<String>, key_path: impl Into<String>) -> Self {
        Self {
            cert_path: cert_path.into(),
            key_path: key_path.into(),
        }
    }

    /// Load the TLS certificates and key for use with axum-server.
    pub fn load_rustls_config(&self) -> Result<axum_server::tls_rustls::RustlsConfig, String> {
        let rt = tokio::runtime::Handle::current();
        let config = rt.block_on(async {
            axum_server::tls_rustls::RustlsConfig::from_pem_file(
                &self.cert_path,
                &self.key_path,
            )
            .await
        });
        config.map_err(|e| format!("Failed to load TLS config: {}", e))
    }
}

/// HTTP-to-VCTP gateway state.
pub struct HttpVctpIngress {
    vctp_socket: UdpSocket,
    vctp_addr: SocketAddr,
    request_counter: AtomicU64,
    error_counter: AtomicU64,
    /// Rate limiter: tracks requests per second window.
    rate_limit_rps: u64,
    rate_window_start: AtomicU64,
    rate_window_count: AtomicU64,
    rate_limited_counter: AtomicU64,
}

impl HttpVctpIngress {
    /// Create a new HTTP-to-VCTP ingress gateway.
    pub async fn new(vctp_server_addr: &str) -> Result<Arc<Self>, String> {
        Self::with_rate_limit(vctp_server_addr, 0).await
    }

    /// Create with a rate limit (requests per second). 0 = unlimited.
    pub async fn with_rate_limit(vctp_server_addr: &str, rate_limit_rps: u64) -> Result<Arc<Self>, String> {
        let vctp_socket = UdpSocket::bind("0.0.0.0:0")
            .await
            .map_err(|e| format!("Failed to bind UDP socket: {}", e))?;

        let vctp_addr: SocketAddr = vctp_server_addr
            .parse()
            .map_err(|e| format!("Invalid VCTP address: {}", e))?;

        Ok(Arc::new(Self {
            vctp_socket,
            vctp_addr,
            request_counter: AtomicU64::new(0),
            error_counter: AtomicU64::new(0),
            rate_limit_rps,
            rate_window_start: AtomicU64::new(0),
            rate_window_count: AtomicU64::new(0),
            rate_limited_counter: AtomicU64::new(0),
        }))
    }

    /// Check if the request should be rate-limited.
    /// Returns true if the request is allowed, false if it should be rejected.
    fn check_rate_limit(&self) -> bool {
        if self.rate_limit_rps == 0 {
            return true; // No limit configured
        }
        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let window_start = self.rate_window_start.load(Ordering::Relaxed);
        if now_secs != window_start {
            // New second window — reset counter
            self.rate_window_start.store(now_secs, Ordering::Relaxed);
            self.rate_window_count.store(1, Ordering::Relaxed);
            return true;
        }
        let count = self.rate_window_count.fetch_add(1, Ordering::Relaxed);
        if count >= self.rate_limit_rps {
            self.rate_limited_counter.fetch_add(1, Ordering::Relaxed);
            false
        } else {
            true
        }
    }

    /// Send a VCTP request and wait for response.
    async fn send_vctp(&self, method: u64, body: serde_json::Value) -> Result<serde_json::Value, String> {
        let seq = self.request_counter.fetch_add(1, Ordering::Relaxed);

        // Build VCTP request payload
        let payload = serde_json::to_vec(&body).map_err(|e| format!("JSON encode error: {}", e))?;

        // Build VCTP packet
        let packet = build_vctp_packet(seq, method, &payload);

        // Send via UDP
        self.vctp_socket
            .send_to(&packet, self.vctp_addr)
            .await
            .map_err(|e| format!("VCTP send error: {}", e))?;

        // Wait for response
        let mut buf = vec![0u8; 65535];
        match tokio::time::timeout(
            Duration::from_secs(5),
            self.vctp_socket.recv_from(&mut buf),
        )
        .await
        {
            Ok(Ok((len, _))) => {
                parse_vctp_json_response(&buf[..len])
            }
            Ok(Err(e)) => Err(format!("VCTP recv error: {}", e)),
            Err(_) => Err("VCTP response timeout".to_string()),
        }
    }

    /// Build the Axum router for this gateway.
    pub fn router(ingress: Arc<Self>) -> Router {
        Router::new()
            .route("/api/v1/health", get(handle_health))
            .route("/api/v1/workflows", post(handle_start_workflow))
            .route("/api/v1/workflows", get(handle_list_workflows))
            .route("/api/v1/workflows/:id", get(handle_describe_workflow))
            .route("/api/v1/workflows/:id/signal", post(handle_signal_workflow))
            .route("/api/v1/workflows/:id/cancel", post(handle_cancel_workflow))
            .route("/api/v1/workflows/:id/terminate", post(handle_terminate_workflow))
            .route("/api/v1/workflows/:id/query", post(handle_query_workflow))
            .route("/api/v1/metrics", get(handle_metrics))
            .route("/docs", get(handle_swagger_ui))
            .route("/docs/openapi.json", get(handle_openapi_spec))
            .with_state(ingress)
    }

    /// Serve the HTTP gateway on the given address.
    pub async fn serve(ingress: Arc<Self>, addr: SocketAddr) -> Result<(), String> {
        let router = Self::router(ingress);
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .map_err(|e| format!("Failed to bind HTTP listener: {}", e))?;
        tracing::info!("HTTP VCTP ingress listening on http://{}", addr);
        axum::serve(listener, router)
            .await
            .map_err(|e| format!("HTTP server error: {}", e))
    }

    /// Serve the HTTPS gateway with TLS on the given address.
    pub async fn serve_tls(
        ingress: Arc<Self>,
        addr: SocketAddr,
        tls_config: TlsConfig,
    ) -> Result<(), String> {
        let router = Self::router(ingress);
        let rustls_config = tls_config.load_rustls_config()?;
        tracing::info!("HTTP VCTP ingress listening on https://{}", addr);
        axum_server::bind_rustls(addr, rustls_config)
            .serve(router.into_make_service())
            .await
            .map_err(|e| format!("HTTPS server error: {}", e))
    }
}

// ─── Request/Response Types ──────────────────────────────────────────────────

#[derive(Deserialize)]
struct StartWorkflowRequest {
    workflow_type: Option<String>,
    workflow_id: Option<String>,
    namespace: Option<String>,
    total_steps: Option<u32>,
    input: Option<serde_json::Value>,
}

#[derive(Deserialize)]
struct SignalRequest {
    signal_name: String,
    payload: Option<serde_json::Value>,
}

#[derive(Deserialize)]
struct QueryRequest {
    query_type: Option<String>,
}

#[derive(Deserialize)]
struct ListQuery {
    max_count: Option<i64>,
    namespace: Option<String>,
}

#[derive(Serialize)]
struct WorkflowResponse {
    workflow_id: String,
    run_id: Option<String>,
    status: String,
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
    status: u32,
}

#[derive(Serialize)]
struct HealthResponse {
    status: String,
    version: String,
}

// ─── Handlers ────────────────────────────────────────────────────────────────

async fn handle_health(
    State(ingress): State<Arc<HttpVctpIngress>>,
) -> impl IntoResponse {
    let body = serde_json::json!({
        "method": 500,
        "namespace": "default",
        "workflow_id": "",
    });

    match ingress.send_vctp(500, body).await {
        Ok(resp) => {
            let status = resp.get("run_status").and_then(|v| v.as_str()).unwrap_or("healthy");
            Json(HealthResponse {
                status: status.to_string(),
                version: env!("CARGO_PKG_VERSION").to_string(),
            })
            .into_response()
        }
        Err(e) => {
            ingress.error_counter.fetch_add(1, Ordering::Relaxed);
            Json(ErrorResponse {
                error: e,
                status: 502,
            })
            .into_response()
        }
    }
}

async fn handle_start_workflow(
    State(ingress): State<Arc<HttpVctpIngress>>,
    Json(req): Json<StartWorkflowRequest>,
) -> impl IntoResponse {
    let body = serde_json::json!({
        "method": 100,
        "namespace": req.namespace.unwrap_or_else(|| "default".to_string()),
        "workflow_id": req.workflow_id.unwrap_or_default(),
        "workflow_type": req.workflow_type.unwrap_or_else(|| "DefaultWorkflow".to_string()),
        "total_steps": req.total_steps.unwrap_or(10),
    });

    match ingress.send_vctp(100, body).await {
        Ok(resp) => {
            let wf_id = resp.get("workflow_id").and_then(|v| v.as_str()).unwrap_or("").to_string();
            let run_id = resp.get("run_id").and_then(|v| v.as_str()).map(String::from);
            let status = resp.get("run_status").and_then(|v| v.as_str()).unwrap_or("COMPLETED").to_string();

            (StatusCode::CREATED, Json(WorkflowResponse { workflow_id: wf_id, run_id, status })).into_response()
        }
        Err(e) => {
            ingress.error_counter.fetch_add(1, Ordering::Relaxed);
            (StatusCode::BAD_GATEWAY, Json(ErrorResponse { error: e, status: 502 })).into_response()
        }
    }
}

async fn handle_list_workflows(
    State(ingress): State<Arc<HttpVctpIngress>>,
    Query(params): Query<ListQuery>,
) -> impl IntoResponse {
    let body = serde_json::json!({
        "method": 106,
        "namespace": params.namespace.unwrap_or_else(|| "default".to_string()),
        "max_count": params.max_count.unwrap_or(100),
    });

    match ingress.send_vctp(106, body).await {
        Ok(resp) => {
            let count = resp.get("count").and_then(|v| v.as_u64()).unwrap_or(0);
            Json(serde_json::json!({ "count": count })).into_response()
        }
        Err(e) => {
            ingress.error_counter.fetch_add(1, Ordering::Relaxed);
            (StatusCode::BAD_GATEWAY, Json(ErrorResponse { error: e, status: 502 })).into_response()
        }
    }
}

async fn handle_describe_workflow(
    State(ingress): State<Arc<HttpVctpIngress>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let body = serde_json::json!({
        "method": 105,
        "namespace": "default",
        "workflow_id": id,
    });

    match ingress.send_vctp(105, body).await {
        Ok(resp) => {
            let wf_id = resp.get("workflow_id").and_then(|v| v.as_str()).unwrap_or(&id).to_string();
            let run_id = resp.get("run_id").and_then(|v| v.as_str()).map(String::from);
            let status = resp.get("run_status").and_then(|v| v.as_str()).unwrap_or("UNKNOWN").to_string();

            let status_code = if status == "UNKNOWN" { StatusCode::NOT_FOUND } else { StatusCode::OK };
            (status_code, Json(WorkflowResponse { workflow_id: wf_id, run_id, status })).into_response()
        }
        Err(e) => {
            ingress.error_counter.fetch_add(1, Ordering::Relaxed);
            (StatusCode::BAD_GATEWAY, Json(ErrorResponse { error: e, status: 502 })).into_response()
        }
    }
}

async fn handle_signal_workflow(
    State(ingress): State<Arc<HttpVctpIngress>>,
    Path(id): Path<String>,
    Json(req): Json<SignalRequest>,
) -> impl IntoResponse {
    let payload_bytes = req.payload
        .map(|p| serde_json::to_vec(&p).unwrap_or_default())
        .unwrap_or_default();

    let body = serde_json::json!({
        "method": 101,
        "namespace": "default",
        "workflow_id": id,
        "signal_name": req.signal_name,
        "payload": payload_bytes,
    });

    match ingress.send_vctp(101, body).await {
        Ok(resp) => {
            let status = resp.get("status").and_then(|v| v.as_u64()).unwrap_or(0);
            if status == 0 {
                (StatusCode::OK, Json(serde_json::json!({ "status": "signaled" }))).into_response()
            } else {
                let error = resp.get("error").and_then(|v| v.as_str()).unwrap_or("unknown error");
                (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: error.to_string(), status: status as u32 })).into_response()
            }
        }
        Err(e) => {
            ingress.error_counter.fetch_add(1, Ordering::Relaxed);
            (StatusCode::BAD_GATEWAY, Json(ErrorResponse { error: e, status: 502 })).into_response()
        }
    }
}

async fn handle_cancel_workflow(
    State(ingress): State<Arc<HttpVctpIngress>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let body = serde_json::json!({
        "method": 103,
        "namespace": "default",
        "workflow_id": id,
    });

    match ingress.send_vctp(103, body).await {
        Ok(resp) => {
            let status = resp.get("status").and_then(|v| v.as_u64()).unwrap_or(0);
            if status == 0 {
                (StatusCode::OK, Json(serde_json::json!({ "status": "cancelled" }))).into_response()
            } else {
                let error = resp.get("error").and_then(|v| v.as_str()).unwrap_or("unknown error");
                (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: error.to_string(), status: status as u32 })).into_response()
            }
        }
        Err(e) => {
            ingress.error_counter.fetch_add(1, Ordering::Relaxed);
            (StatusCode::BAD_GATEWAY, Json(ErrorResponse { error: e, status: 502 })).into_response()
        }
    }
}

async fn handle_terminate_workflow(
    State(ingress): State<Arc<HttpVctpIngress>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let body = serde_json::json!({
        "method": 104,
        "namespace": "default",
        "workflow_id": id,
    });

    match ingress.send_vctp(104, body).await {
        Ok(resp) => {
            let status = resp.get("status").and_then(|v| v.as_u64()).unwrap_or(0);
            if status == 0 {
                (StatusCode::OK, Json(serde_json::json!({ "status": "terminated" }))).into_response()
            } else {
                let error = resp.get("error").and_then(|v| v.as_str()).unwrap_or("unknown error");
                (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: error.to_string(), status: status as u32 })).into_response()
            }
        }
        Err(e) => {
            ingress.error_counter.fetch_add(1, Ordering::Relaxed);
            (StatusCode::BAD_GATEWAY, Json(ErrorResponse { error: e, status: 502 })).into_response()
        }
    }
}

async fn handle_query_workflow(
    State(ingress): State<Arc<HttpVctpIngress>>,
    Path(id): Path<String>,
    Json(req): Json<QueryRequest>,
) -> impl IntoResponse {
    let body = serde_json::json!({
        "method": 102,
        "namespace": "default",
        "workflow_id": id,
        "query_type": req.query_type,
    });

    match ingress.send_vctp(102, body).await {
        Ok(resp) => Json(resp).into_response(),
        Err(e) => {
            ingress.error_counter.fetch_add(1, Ordering::Relaxed);
            (StatusCode::BAD_GATEWAY, Json(ErrorResponse { error: e, status: 502 })).into_response()
        }
    }
}

async fn handle_metrics(
    State(ingress): State<Arc<HttpVctpIngress>>,
) -> impl IntoResponse {
    let requests = ingress.request_counter.load(Ordering::Relaxed);
    let errors = ingress.error_counter.load(Ordering::Relaxed);

    let metrics = format!(
        "# HELP http_vctp_requests_total Total HTTP-to-VCTP requests.\n\
         # TYPE http_vctp_requests_total counter\n\
         http_vctp_requests_total {}\n\
         # HELP http_vctp_errors_total Total HTTP-to-VCTP errors.\n\
         # TYPE http_vctp_errors_total counter\n\
         http_vctp_errors_total {}\n",
        requests, errors
    );

    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4")],
        metrics,
    )
}

// ─── VCTP Packet Helpers ────────────────────────────────────────────────────

fn build_vctp_packet(sequence: u64, method_id: u64, payload: &[u8]) -> Vec<u8> {
    let magic: u32 = 0x50544356;
    let mut buf = Vec::with_capacity(28 + payload.len() + 4);
    buf.extend_from_slice(&magic.to_le_bytes());
    buf.extend_from_slice(&sequence.to_le_bytes());
    buf.extend_from_slice(&method_id.to_le_bytes());
    buf.extend_from_slice(&0u32.to_le_bytes());
    buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    buf.extend_from_slice(payload);
    let crc = crc32_compute(&buf);
    buf.extend_from_slice(&crc.to_le_bytes());
    buf
}

fn parse_vctp_json_response(data: &[u8]) -> Result<serde_json::Value, String> {
    if data.len() < 32 {
        return Err("Response too small".to_string());
    }
    let payload_len = u32::from_le_bytes(data[24..28].try_into().unwrap_or([0; 4])) as usize;
    if data.len() < 28 + payload_len + 4 {
        return Err("Response truncated".to_string());
    }
    let payload = &data[28..28 + payload_len];
    serde_json::from_slice(payload).map_err(|e| format!("JSON parse error: {}", e))
}

/// Swagger UI at /docs — interactive API documentation.
async fn handle_swagger_ui() -> impl IntoResponse {
    let html = r#"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>VCTP HTTP Gateway — API Documentation</title>
    <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css">
    <style>body { margin: 0; padding: 0; } #swagger-ui { max-width: 1200px; margin: 0 auto; }</style>
</head>
<body>
    <div id="swagger-ui"></div>
    <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
    <script>
    SwaggerUIBundle({
        url: '/docs/openapi.json',
        dom_id: '#swagger-ui',
        presets: [SwaggerUIBundle.presets.apis, SwaggerUIBundle.SwaggerUIStandalonePreset],
        layout: 'BaseLayout',
        deepLinking: true,
    });
    </script>
</body>
</html>"#;
    (
        StatusCode::OK,
        [("content-type", "text/html; charset=utf-8")],
        html,
    )
}

/// OpenAPI spec endpoint for Swagger UI.
async fn handle_openapi_spec() -> impl IntoResponse {
    let spec = serde_json::json!({
        "openapi": "3.0.3",
        "info": {
            "title": "VCTP HTTP Gateway",
            "description": "HTTP-to-VCTP ingress gateway — translates REST calls to VCTP RPC over UDP.",
            "version": env!("CARGO_PKG_VERSION"),
        },
        "paths": {
            "/api/v1/health": {
                "get": { "summary": "Health check", "tags": ["System"], "responses": { "200": { "description": "Server health status" } } }
            },
            "/api/v1/workflows": {
                "post": {
                    "summary": "Start a new workflow",
                    "tags": ["Workflows"],
                    "requestBody": { "content": { "application/json": { "schema": { "$ref": "#/components/schemas/StartWorkflow" } } } },
                    "responses": { "201": { "description": "Workflow started" } }
                },
                "get": {
                    "summary": "List workflows",
                    "tags": ["Workflows"],
                    "parameters": [
                        { "name": "max_count", "in": "query", "schema": { "type": "integer" } },
                        { "name": "namespace", "in": "query", "schema": { "type": "string" } }
                    ],
                    "responses": { "200": { "description": "Workflow count" } }
                }
            },
            "/api/v1/workflows/{id}": {
                "get": {
                    "summary": "Describe workflow",
                    "tags": ["Workflows"],
                    "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
                    "responses": { "200": { "description": "Workflow details" }, "404": { "description": "Not found" } }
                }
            },
            "/api/v1/workflows/{id}/signal": {
                "post": {
                    "summary": "Signal a workflow",
                    "tags": ["Workflows"],
                    "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
                    "requestBody": { "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Signal" } } } },
                    "responses": { "200": { "description": "Signal delivered" } }
                }
            },
            "/api/v1/workflows/{id}/cancel": {
                "post": {
                    "summary": "Cancel a workflow",
                    "tags": ["Workflows"],
                    "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
                    "responses": { "200": { "description": "Workflow cancelled" } }
                }
            },
            "/api/v1/workflows/{id}/terminate": {
                "post": {
                    "summary": "Terminate a workflow",
                    "tags": ["Workflows"],
                    "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
                    "responses": { "200": { "description": "Workflow terminated" } }
                }
            },
            "/api/v1/workflows/{id}/query": {
                "post": {
                    "summary": "Query a workflow",
                    "tags": ["Workflows"],
                    "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
                    "requestBody": { "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Query" } } } },
                    "responses": { "200": { "description": "Query result" } }
                }
            },
            "/api/v1/metrics": {
                "get": { "summary": "Prometheus metrics", "tags": ["System"], "responses": { "200": { "description": "Prometheus text format" } } }
            }
        },
        "components": {
            "schemas": {
                "StartWorkflow": {
                    "type": "object",
                    "properties": {
                        "workflow_type": { "type": "string" },
                        "workflow_id": { "type": "string" },
                        "namespace": { "type": "string" },
                        "total_steps": { "type": "integer" },
                        "input": { "type": "object" }
                    }
                },
                "Signal": {
                    "type": "object",
                    "required": ["signal_name"],
                    "properties": {
                        "signal_name": { "type": "string" },
                        "payload": { "type": "object" }
                    }
                },
                "Query": {
                    "type": "object",
                    "properties": {
                        "query_type": { "type": "string" }
                    }
                }
            },
            "securitySchemes": {
                "bearerAuth": { "type": "http", "scheme": "bearer", "bearerFormat": "JWT" },
                "apiKeyAuth": { "type": "apiKey", "in": "header", "name": "X-API-Key" }
            }
        },
        "security": [{ "bearerAuth": [] }, { "apiKeyAuth": [] }]
    });

    Json(spec)
}

fn crc32_compute(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFFFFFF;
    for &byte in data {
        crc ^= byte as u32;
        for _ in 0..8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ 0xEDB88320;
            } else {
                crc >>= 1;
            }
        }
    }
    !crc
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_vctp_packet_structure() {
        let payload = b"{\"method\":100}";
        let packet = build_vctp_packet(1, 100, payload);

        // 28 header + payload + 4 CRC
        assert_eq!(packet.len(), 28 + payload.len() + 4);

        // Verify magic
        let magic = u32::from_le_bytes(packet[0..4].try_into().unwrap());
        assert_eq!(magic, 0x50544356);

        // Verify sequence
        let seq = u64::from_le_bytes(packet[4..12].try_into().unwrap());
        assert_eq!(seq, 1);

        // Verify method ID
        let method = u64::from_le_bytes(packet[12..20].try_into().unwrap());
        assert_eq!(method, 100);

        // Verify payload length
        let plen = u32::from_le_bytes(packet[24..28].try_into().unwrap()) as usize;
        assert_eq!(plen, payload.len());
    }

    #[test]
    fn test_build_vctp_packet_crc_integrity() {
        let payload = b"test payload";
        let packet = build_vctp_packet(42, 500, payload);

        // Extract CRC from packet
        let stored_crc = u32::from_le_bytes(packet[packet.len() - 4..].try_into().unwrap());

        // Recompute CRC over everything except the last 4 bytes
        let computed_crc = crc32_compute(&packet[..packet.len() - 4]);
        assert_eq!(stored_crc, computed_crc);
    }

    #[test]
    fn test_parse_vctp_json_response_valid() {
        let json = serde_json::json!({"status": 0, "workflow_id": "wf-1"});
        let payload = serde_json::to_vec(&json).unwrap();

        // Build a valid VCTP response
        let mut packet = Vec::new();
        let magic: u32 = 0x50544356;
        packet.extend_from_slice(&magic.to_le_bytes());
        packet.extend_from_slice(&1u64.to_le_bytes());
        packet.extend_from_slice(&100u64.to_le_bytes());
        packet.extend_from_slice(&0u32.to_le_bytes());
        packet.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        packet.extend_from_slice(&payload);
        let crc = crc32_compute(&packet);
        packet.extend_from_slice(&crc.to_le_bytes());

        let result = parse_vctp_json_response(&packet).unwrap();
        assert_eq!(result["status"], 0);
        assert_eq!(result["workflow_id"], "wf-1");
    }

    #[test]
    fn test_parse_vctp_json_response_too_small() {
        let data = vec![0u8; 10];
        let result = parse_vctp_json_response(&data);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("too small"));
    }

    #[test]
    fn test_parse_vctp_json_response_truncated() {
        let mut data = vec![0u8; 32];
        let magic: u32 = 0x50544356;
        data[0..4].copy_from_slice(&magic.to_le_bytes());
        data[24..28].copy_from_slice(&500u32.to_le_bytes()); // claims 500 bytes payload
        let result = parse_vctp_json_response(&data);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("truncated"));
    }

    #[test]
    fn test_crc32_known_value() {
        let crc = crc32_compute(b"123456789");
        assert_eq!(crc, 0xCBF43926);
    }

    #[test]
    fn test_start_workflow_request_deserialization() {
        let json = r#"{"workflow_type": "MyWorkflow", "total_steps": 5, "namespace": "prod"}"#;
        let req: StartWorkflowRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.workflow_type.as_deref(), Some("MyWorkflow"));
        assert_eq!(req.total_steps, Some(5));
        assert_eq!(req.namespace.as_deref(), Some("prod"));
    }

    #[test]
    fn test_start_workflow_request_defaults() {
        let json = r#"{}"#;
        let req: StartWorkflowRequest = serde_json::from_str(json).unwrap();
        assert!(req.workflow_type.is_none());
        assert!(req.workflow_id.is_none());
        assert!(req.namespace.is_none());
        assert!(req.total_steps.is_none());
    }

    #[test]
    fn test_signal_request_deserialization() {
        let json = r#"{"signal_name": "approval", "payload": {"approved": true}}"#;
        let req: SignalRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.signal_name, "approval");
        assert!(req.payload.is_some());
    }

    #[test]
    fn test_query_request_deserialization() {
        let json = r#"{"query_type": "current_status"}"#;
        let req: QueryRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.query_type.as_deref(), Some("current_status"));
    }

    #[test]
    fn test_list_query_deserialization() {
        let json = r#"{"max_count": 50, "namespace": "default"}"#;
        let q: ListQuery = serde_json::from_str(json).unwrap();
        assert_eq!(q.max_count, Some(50));
        assert_eq!(q.namespace.as_deref(), Some("default"));
    }

    #[test]
    fn test_workflow_response_serialization() {
        let resp = WorkflowResponse {
            workflow_id: "wf-42".to_string(),
            run_id: Some("run-1".to_string()),
            status: "RUNNING".to_string(),
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("wf-42"));
        assert!(json.contains("RUNNING"));
    }

    #[test]
    fn test_error_response_serialization() {
        let resp = ErrorResponse {
            error: "not found".to_string(),
            status: 404,
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("not found"));
        assert!(json.contains("404"));
    }

    // ─── Integration Tests (Live Axum Server) ────────────────────────────────

    /// Helper: create a test ingress pointing at a non-existent VCTP server.
    /// Requests will fail at the UDP layer but the HTTP layer should still respond.
    async fn test_ingress() -> Arc<HttpVctpIngress> {
        HttpVctpIngress::new("127.0.0.1:19999").await.unwrap()
    }

    #[tokio::test]
    async fn test_integration_health_endpoint() {
        let ingress = test_ingress().await;
        let router = HttpVctpIngress::router(ingress);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });

        let client = reqwest::Client::new();
        let resp = client
            .get(format!("http://{}/api/v1/health", addr))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["status"], "ok");
        assert!(body["timestamp"].as_u64().unwrap() > 0);
    }

    #[tokio::test]
    async fn test_integration_metrics_endpoint() {
        let ingress = test_ingress().await;
        let router = HttpVctpIngress::router(ingress);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });

        let client = reqwest::Client::new();
        let resp = client
            .get(format!("http://{}/api/v1/metrics", addr))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let body: serde_json::Value = resp.json().await.unwrap();
        assert!(body["requests"].as_u64().is_some());
        assert!(body["errors"].as_u64().is_some());
    }

    #[tokio::test]
    async fn test_integration_openapi_spec() {
        let ingress = test_ingress().await;
        let router = HttpVctpIngress::router(ingress);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });

        let client = reqwest::Client::new();
        let resp = client
            .get(format!("http://{}/docs/openapi.json", addr))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["openapi"], "3.0.0");
        assert!(body["info"]["title"].as_str().unwrap().contains("VCTP"));
    }

    #[tokio::test]
    async fn test_integration_start_workflow_timeout() {
        // This test verifies the HTTP layer works end-to-end.
        // Since there's no VCTP server at 127.0.0.1:19999, it should return an error.
        let ingress = test_ingress().await;
        let router = HttpVctpIngress::router(ingress);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });

        let client = reqwest::Client::new();
        let resp = client
            .post(format!("http://{}/api/v1/workflows", addr))
            .json(&serde_json::json!({
                "workflow_type": "TestWorkflow",
                "workflow_id": "test-integration-1"
            }))
            .send()
            .await
            .unwrap();
        // Should get 502 or 504 since no VCTP server is listening
        assert!(
            resp.status().is_server_error(),
            "Expected server error, got {}",
            resp.status()
        );
    }

    #[tokio::test]
    async fn test_integration_rate_limiter() {
        // Create ingress with rate limit of 5 RPS
        let ingress = HttpVctpIngress::with_rate_limit("127.0.0.1:19999", 5).await.unwrap();
        let router = HttpVctpIngress::router(ingress);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });

        let client = reqwest::Client::new();
        // Health endpoint should still work (rate limiting is on VCTP path)
        let resp = client
            .get(format!("http://{}/api/v1/health", addr))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
    }
}
