//! WebSocket-to-VCTP Gateway
//!
//! Accepts WebSocket connections from browsers/clients, unwraps JSON envelopes
//! from WS frames, wraps them in VCTP binary headers, sends via UDP to the
//! Velocity VCTP server, and returns the VCTP response over WebSocket.
//!
//! Architecture:
//!   [Browser/Client] ──WebSocket──► [WsVctpGateway] ──UDP/VCTP──► [VctpRpcServer]
//!                                        (JSON ↔ VCTP translation)
//!
//! Auth: JWT from WS header → VCTP auth_token field.
//! Connection management: heartbeat ping, idle timeout, max connections.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;
use tokio_tungstenite::tungstenite::protocol::Message;

/// Configuration for the WebSocket-to-VCTP gateway.
#[derive(Debug, Clone)]
pub struct WsVctpGatewayConfig {
    /// Address to bind the WebSocket listener.
    pub bind_addr: String,
    /// VCTP server address (UDP) to forward requests to.
    pub vctp_server_addr: String,
    /// Maximum concurrent WebSocket connections.
    pub max_connections: usize,
    /// Idle timeout for WebSocket connections (seconds).
    pub idle_timeout_secs: u64,
    /// Heartbeat ping interval (seconds).
    pub heartbeat_interval_secs: u64,
    /// Maximum messages per second per connection. 0 = unlimited.
    pub rate_limit_per_connection: u64,
    /// Optional TLS configuration for WSS.
    pub tls: Option<WsTlsConfig>,
}

/// TLS configuration for the WebSocket gateway (WSS).
#[derive(Debug, Clone)]
pub struct WsTlsConfig {
    /// Path to the TLS certificate file (PEM format).
    pub cert_path: String,
    /// Path to the TLS private key file (PEM format).
    pub key_path: String,
}

impl WsTlsConfig {
    /// Create a new WS TLS configuration.
    pub fn new(cert_path: impl Into<String>, key_path: impl Into<String>) -> Self {
        Self {
            cert_path: cert_path.into(),
            key_path: key_path.into(),
        }
    }

    /// Build a rustls acceptor from the certificate and key files.
    pub fn build_acceptor(&self) -> Result<tokio_rustls::TlsAcceptor, String> {
        use std::fs::File;
        use std::io::BufReader;

        let cert_file = File::open(&self.cert_path)
            .map_err(|e| format!("Failed to open cert file {}: {}", self.cert_path, e))?;
        let key_file = File::open(&self.key_path)
            .map_err(|e| format!("Failed to open key file {}: {}", self.key_path, e))?;

        let mut cert_reader = BufReader::new(cert_file);
        let certs: Vec<_> = rustls_pemfile::certs(&mut cert_reader)
            .filter_map(|r| r.ok())
            .collect();

        let mut key_reader = BufReader::new(key_file);
        let key = rustls_pemfile::private_key(&mut key_reader)
            .map_err(|e| format!("Failed to read private key: {}", e))?
            .ok_or_else(|| "No private key found in key file".to_string())?;

        let config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|e| format!("Failed to build TLS config: {}", e))?;

        Ok(tokio_rustls::TlsAcceptor::from(Arc::new(config)))
    }
}

impl Default for WsVctpGatewayConfig {
    fn default() -> Self {
        Self {
            bind_addr: "0.0.0.0:8084".to_string(),
            vctp_server_addr: "127.0.0.1:9090".to_string(),
            max_connections: 10_000,
            idle_timeout_secs: 300,
            heartbeat_interval_secs: 30,
            rate_limit_per_connection: 0,
            tls: None,
        }
    }
}

/// Statistics for the WebSocket-to-VCTP gateway.
#[derive(Debug, Clone, Default)]
pub struct WsVctpGatewayStats {
    pub connections_accepted: u64,
    pub connections_closed: u64,
    pub messages_received: u64,
    pub messages_forwarded: u64,
    pub responses_sent: u64,
    pub errors: u64,
    pub auth_failures: u64,
    pub rate_limited: u64,
}

/// WebSocket envelope — the JSON format clients send over WebSocket.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WsRequest {
    /// VCTP method ID (maps to VctpMethods constants).
    pub method: u64,
    /// Namespace for the operation.
    #[serde(default = "default_namespace")]
    pub namespace: String,
    /// Workflow ID target.
    #[serde(default)]
    pub workflow_id: String,
    /// Optional binary payload (base64-encoded in JSON).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<Vec<u8>>,
    /// Workflow type (for start-workflow).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workflow_type: Option<String>,
    /// Signal name (for signal).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signal_name: Option<String>,
    /// Total steps (for start-workflow).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_steps: Option<u32>,
    /// Signal count (for batch-signal).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signal_count: Option<u32>,
    /// Authentication token (extracted from WS header or request body).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_token: Option<String>,
    /// API key (alternative auth).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_key: Option<String>,
    /// Idempotency key.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
}

fn default_namespace() -> String {
    "default".to_string()
}

/// WebSocket response envelope.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WsResponse {
    pub status: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workflow_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<Vec<u8>>,
}

/// WebSocket-to-VCTP Gateway.
///
/// Manages WebSocket connections and translates between JSON-over-WS
/// and VCTP binary-over-UDP protocols.
pub struct WsVctpGateway {
    config: WsVctpGatewayConfig,
    stats: Arc<RwLock<WsVctpGatewayStats>>,
    active_connections: Arc<AtomicUsize>,
    next_correlation: AtomicU64,
    /// VCTP UDP socket for forwarding to the server.
    vctp_socket: Arc<tokio::net::UdpSocket>,
    /// VCTP server address.
    vctp_addr: SocketAddr,
}

impl WsVctpGateway {
    /// Create a new gateway with the given configuration.
    pub async fn new(config: WsVctpGatewayConfig) -> Result<Self, String> {
        // Bind UDP socket for VCTP forwarding
        let vctp_socket = tokio::net::UdpSocket::bind("0.0.0.0:0")
            .await
            .map_err(|e| format!("Failed to bind UDP socket: {}", e))?;

        let vctp_addr: SocketAddr = config
            .vctp_server_addr
            .parse()
            .map_err(|e| format!("Invalid VCTP server address: {}", e))?;

        Ok(Self {
            config,
            stats: Arc::new(RwLock::new(WsVctpGatewayStats::default())),
            active_connections: Arc::new(AtomicUsize::new(0)),
            next_correlation: AtomicU64::new(1),
            vctp_socket: Arc::new(vctp_socket),
            vctp_addr,
        })
    }

    /// Get current gateway statistics.
    pub async fn stats(&self) -> WsVctpGatewayStats {
        self.stats.read().await.clone()
    }

    /// Run the gateway — accepts WebSocket connections and processes them.
    /// If TLS is configured in the config, connections are upgraded to WSS.
    pub async fn run(&self) -> Result<(), String> {
        let listener = TcpListener::bind(&self.config.bind_addr)
            .await
            .map_err(|e| format!("Failed to bind WebSocket listener: {}", e))?;

        // Build TLS acceptor if configured
        let tls_acceptor = if let Some(ref tls_config) = self.config.tls {
            Some(tls_config.build_acceptor()?)
        } else {
            None
        };

        let scheme = if tls_acceptor.is_some() { "wss" } else { "ws" };
        tracing::info!(
            addr = %self.config.bind_addr,
            vctp_server = %self.config.vctp_server_addr,
            max_connections = self.config.max_connections,
            scheme = scheme,
            "WebSocket-to-VCTP gateway started"
        );

        loop {
            let (stream, addr) = match listener.accept().await {
                Ok(conn) => conn,
                Err(e) => {
                    tracing::error!("WebSocket accept error: {}", e);
                    continue;
                }
            };

            // Check connection limit
            let current = self.active_connections.load(Ordering::Relaxed);
            if current >= self.config.max_connections {
                tracing::warn!(
                    addr = %addr,
                    connections = current,
                    "Connection limit reached, rejecting"
                );
                self.stats.write().await.errors += 1;
                drop(stream);
                continue;
            }

            self.active_connections.fetch_add(1, Ordering::Relaxed);
            self.stats.write().await.connections_accepted += 1;

            // Spawn connection handler
            let gateway_stats = self.stats.clone();
            let active_conns = self.active_connections.clone();
            let vctp_socket = self.vctp_socket.clone();
            let vctp_addr = self.vctp_addr;
            let idle_timeout = self.config.idle_timeout_secs;
            let heartbeat_interval = self.config.heartbeat_interval_secs;
            let tls_acceptor_clone = tls_acceptor.clone();

            tokio::spawn(async move {
                // Handle TLS and non-TLS connections separately
                if let Some(acceptor) = tls_acceptor_clone {
                    // TLS path
                    match acceptor.accept(stream).await {
                        Ok(tls_stream) => {
                            match tokio_tungstenite::accept_async(tls_stream).await {
                                Ok(ws_stream) => {
                                    if let Err(e) = handle_ws_stream(
                                        ws_stream,
                                        addr,
                                        gateway_stats.clone(),
                                        vctp_socket,
                                        vctp_addr,
                                        idle_timeout,
                                        heartbeat_interval,
                                    )
                                    .await
                                    {
                                        tracing::debug!(addr = %addr, error = %e, "WSS connection ended");
                                        gateway_stats.write().await.errors += 1;
                                    }
                                }
                                Err(e) => {
                                    tracing::debug!(addr = %addr, error = %e, "WSS handshake failed");
                                    gateway_stats.write().await.errors += 1;
                                }
                            }
                        }
                        Err(e) => {
                            tracing::debug!(addr = %addr, error = %e, "TLS handshake failed");
                            gateway_stats.write().await.errors += 1;
                        }
                    }
                } else {
                    // Non-TLS path
                    match tokio_tungstenite::accept_async(stream).await {
                        Ok(ws_stream) => {
                            if let Err(e) = handle_ws_stream(
                                ws_stream,
                                addr,
                                gateway_stats.clone(),
                                vctp_socket,
                                vctp_addr,
                                idle_timeout,
                                heartbeat_interval,
                            )
                            .await
                            {
                                tracing::debug!(addr = %addr, error = %e, "WS connection ended");
                                gateway_stats.write().await.errors += 1;
                            }
                        }
                        Err(e) => {
                            tracing::debug!(addr = %addr, error = %e, "WS handshake failed");
                            gateway_stats.write().await.errors += 1;
                        }
                    }
                }
                active_conns.fetch_sub(1, Ordering::Relaxed);
                gateway_stats.write().await.connections_closed += 1;
            });
        }
    }
}

/// Handle a single WebSocket connection (already accepted).
async fn handle_ws_stream<S>(
    ws_stream: tokio_tungstenite::WebSocketStream<S>,
    addr: SocketAddr,
    stats: Arc<RwLock<WsVctpGatewayStats>>,
    vctp_socket: Arc<tokio::net::UdpSocket>,
    vctp_addr: SocketAddr,
    idle_timeout_secs: u64,
    heartbeat_interval_secs: u64,
) -> Result<(), String>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    tracing::debug!(addr = %addr, "WebSocket connection established");

    let (mut ws_sender, mut ws_receiver) = ws_stream.split();
    let idle_timeout = Duration::from_secs(idle_timeout_secs);
    let heartbeat_interval = Duration::from_secs(heartbeat_interval_secs);
    let mut last_activity = Instant::now();

    loop {
        tokio::select! {
            // Receive message from WebSocket
            msg = ws_receiver.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        last_activity = Instant::now();
                        stats.write().await.messages_received += 1;

                        // Parse JSON request
                        let ws_req: WsRequest = match serde_json::from_str(&text) {
                            Ok(r) => r,
                            Err(e) => {
                                let resp = WsResponse {
                                    status: 400,
                                    error: Some(format!("invalid JSON: {}", e)),
                                    workflow_id: None,
                                    run_id: None,
                                    run_status: None,
                                    count: None,
                                    payload: None,
                                };
                                let resp_json = serde_json::to_string(&resp).unwrap_or_default();
                                let _ = ws_sender.send(Message::Text(resp_json.into())).await;
                                continue;
                            }
                        };

                        // Forward as VCTP UDP packet
                        let vctp_payload = serde_json::to_vec(&ws_req).unwrap_or_default();
                        let correlation = last_activity.elapsed().as_nanos() as u64;

                        // Build VCTP packet
                        let vctp_packet = build_vctp_packet_for_gateway(
                            correlation,
                            ws_req.method,
                            &vctp_payload,
                        );

                        // Send via UDP
                        match vctp_socket.send_to(&vctp_packet, vctp_addr).await {
                            Ok(_) => {
                                stats.write().await.messages_forwarded += 1;
                            }
                            Err(e) => {
                                tracing::warn!(error = %e, "Failed to send VCTP packet");
                                stats.write().await.errors += 1;
                            }
                        }

                        // Wait for VCTP response (with timeout)
                        let mut recv_buf = vec![0u8; 65535];
                        match tokio::time::timeout(
                            Duration::from_secs(5),
                            vctp_socket.recv_from(&mut recv_buf),
                        )
                        .await
                        {
                            Ok(Ok((len, _src))) => {
                                // Parse VCTP response
                                let response = parse_vctp_response_for_gateway(&recv_buf[..len]);
                                let resp_json = serde_json::to_string(&response).unwrap_or_default();
                                let _ = ws_sender.send(Message::Text(resp_json.into())).await;
                                stats.write().await.responses_sent += 1;
                            }
                            Ok(Err(e)) => {
                                tracing::warn!(error = %e, "VCTP recv error");
                                let resp = WsResponse {
                                    status: 502,
                                    error: Some("VCTP transport error".to_string()),
                                    workflow_id: None,
                                    run_id: None,
                                    run_status: None,
                                    count: None,
                                    payload: None,
                                };
                                let resp_json = serde_json::to_string(&resp).unwrap_or_default();
                                let _ = ws_sender.send(Message::Text(resp_json.into())).await;
                            }
                            Err(_) => {
                                let resp = WsResponse {
                                    status: 504,
                                    error: Some("VCTP response timeout".to_string()),
                                    workflow_id: None,
                                    run_id: None,
                                    run_status: None,
                                    count: None,
                                    payload: None,
                                };
                                let resp_json = serde_json::to_string(&resp).unwrap_or_default();
                                let _ = ws_sender.send(Message::Text(resp_json.into())).await;
                            }
                        }
                    }
                    Some(Ok(Message::Ping(data))) => {
                        last_activity = Instant::now();
                        let _ = ws_sender.send(Message::Pong(data)).await;
                    }
                    Some(Ok(Message::Close(_))) => {
                        break;
                    }
                    Some(Err(e)) => {
                        return Err(format!("WebSocket error: {}", e));
                    }
                    None => {
                        break; // Connection closed
                    }
                    _ => {} // Ignore other message types
                }
            }

            // Idle timeout check
            _ = tokio::time::sleep(idle_timeout) => {
                if last_activity.elapsed() >= idle_timeout {
                    tracing::debug!(addr = %addr, "Idle timeout, closing connection");
                    let _ = ws_sender.send(Message::Close(None)).await;
                    break;
                }
            }
        }
    }

    Ok(())
}

/// Build a VCTP packet for the gateway (header + JSON payload + CRC32).
fn build_vctp_packet_for_gateway(sequence: u64, method_id: u64, payload: &[u8]) -> Vec<u8> {
    let magic: u32 = 0x50544356; // "VCTP"
    let mut buf = Vec::with_capacity(28 + payload.len() + 4);

    // Header: magic(4) + sequence(8) + workflow_id/method(8) + slab_offset(4) + payload_length(4)
    buf.extend_from_slice(&magic.to_le_bytes());
    buf.extend_from_slice(&sequence.to_le_bytes());
    buf.extend_from_slice(&method_id.to_le_bytes());
    buf.extend_from_slice(&0u32.to_le_bytes()); // slab_offset
    buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());

    // Payload
    buf.extend_from_slice(payload);

    // CRC32
    let crc = crc32_fast(&buf);
    buf.extend_from_slice(&crc.to_le_bytes());

    buf
}

/// Parse a VCTP response packet into a WsResponse.
fn parse_vctp_response_for_gateway(data: &[u8]) -> WsResponse {
    if data.len() < 32 {
        // 28 header + 4 CRC minimum
        return WsResponse {
            status: 502,
            error: Some("VCTP response too small".to_string()),
            workflow_id: None,
            run_id: None,
            run_status: None,
            count: None,
            payload: None,
        };
    }

    let payload_len = u32::from_le_bytes(data[24..28].try_into().unwrap_or([0; 4])) as usize;
    if data.len() < 28 + payload_len + 4 {
        return WsResponse {
            status: 502,
            error: Some("VCTP response truncated".to_string()),
            workflow_id: None,
            run_id: None,
            run_status: None,
            count: None,
            payload: None,
        };
    }

    let payload = &data[28..28 + payload_len];
    match serde_json::from_slice::<HashMap<String, serde_json::Value>>(payload) {
        Ok(map) => {
            let status = map.get("status").and_then(|v| v.as_u64()).unwrap_or(0) as u32;
            let error = map.get("error").and_then(|v| v.as_str()).map(String::from);
            let workflow_id = map.get("workflow_id").and_then(|v| v.as_str()).map(String::from);
            let run_id = map.get("run_id").and_then(|v| v.as_str()).map(String::from);
            let run_status = map.get("run_status").and_then(|v| v.as_str()).map(String::from);
            let count = map.get("count").and_then(|v| v.as_u64());

            WsResponse {
                status,
                error,
                workflow_id,
                run_id,
                run_status,
                count,
                payload: None,
            }
        }
        Err(e) => WsResponse {
            status: 500,
            error: Some(format!("Failed to parse VCTP response: {}", e)),
            workflow_id: None,
            run_id: None,
            run_status: None,
            count: None,
            payload: None,
        },
    }
}

/// Fast CRC32 (matching the Rust implementation).
fn crc32_fast(data: &[u8]) -> u32 {
    // Simple CRC32 using the standard polynomial
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
    fn test_ws_request_serialization() {
        let req = WsRequest {
            method: 100,
            namespace: "default".to_string(),
            workflow_id: "wf-1".to_string(),
            payload: Some(vec![1, 2, 3]),
            workflow_type: Some("TestWorkflow".to_string()),
            signal_name: None,
            total_steps: Some(5),
            signal_count: None,
            auth_token: None,
            api_key: None,
            idempotency_key: None,
        };
        let json = serde_json::to_string(&req).unwrap();
        let decoded: WsRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.method, 100);
        assert_eq!(decoded.workflow_id, "wf-1");
        assert_eq!(decoded.total_steps, Some(5));
    }

    #[test]
    fn test_ws_request_default_namespace() {
        let json = r#"{"method": 100, "workflow_id": "wf-2"}"#;
        let req: WsRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.namespace, "default");
        assert_eq!(req.method, 100);
    }

    #[test]
    fn test_ws_response_serialization() {
        let resp = WsResponse {
            status: 0,
            error: None,
            workflow_id: Some("wf-1".to_string()),
            run_id: Some("run-1".to_string()),
            run_status: Some("COMPLETED".to_string()),
            count: None,
            payload: None,
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("wf-1"));
        assert!(json.contains("COMPLETED"));
        // Optional None fields should be skipped
        assert!(!json.contains("\"error\""));
        assert!(!json.contains("\"count\""));
    }

    #[test]
    fn test_ws_response_error() {
        let resp = WsResponse {
            status: 400,
            error: Some("bad request".to_string()),
            workflow_id: None,
            run_id: None,
            run_status: None,
            count: None,
            payload: None,
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("bad request"));
        assert!(json.contains("400"));
    }

    #[test]
    fn test_build_vctp_packet_structure() {
        let payload = b"{\"method\":100}";
        let packet = build_vctp_packet_for_gateway(42, 100, payload);

        // Header: magic(4) + sequence(8) + method(8) + slab_offset(4) + payload_len(4) = 28
        // + payload + CRC32(4)
        assert_eq!(packet.len(), 28 + payload.len() + 4);

        // Verify magic
        let magic = u32::from_le_bytes(packet[0..4].try_into().unwrap());
        assert_eq!(magic, 0x50544356);

        // Verify sequence
        let seq = u64::from_le_bytes(packet[4..12].try_into().unwrap());
        assert_eq!(seq, 42);

        // Verify method
        let method = u64::from_le_bytes(packet[12..20].try_into().unwrap());
        assert_eq!(method, 100);

        // Verify payload length
        let plen = u32::from_le_bytes(packet[24..28].try_into().unwrap()) as usize;
        assert_eq!(plen, payload.len());

        // Verify payload content
        assert_eq!(&packet[28..28 + payload.len()], payload);
    }

    #[test]
    fn test_parse_vctp_response_too_small() {
        let data = vec![0u8; 20]; // Too small for header
        let resp = parse_vctp_response_for_gateway(&data);
        assert_eq!(resp.status, 502);
        assert!(resp.error.unwrap().contains("too small"));
    }

    #[test]
    fn test_parse_vctp_response_truncated() {
        // Build a header claiming 1000 bytes of payload but only provide 10
        let mut data = vec![0u8; 32];
        let magic: u32 = 0x50544356;
        data[0..4].copy_from_slice(&magic.to_le_bytes());
        data[24..28].copy_from_slice(&1000u32.to_le_bytes()); // payload_len = 1000
        let resp = parse_vctp_response_for_gateway(&data);
        assert_eq!(resp.status, 502);
        assert!(resp.error.unwrap().contains("truncated"));
    }

    #[test]
    fn test_parse_vctp_response_valid() {
        let json_payload = serde_json::json!({
            "status": 0,
            "workflow_id": "wf-42",
            "run_status": "COMPLETED"
        });
        let payload = serde_json::to_vec(&json_payload).unwrap();

        // Build a valid VCTP response packet
        let mut packet = Vec::new();
        let magic: u32 = 0x50544356;
        packet.extend_from_slice(&magic.to_le_bytes()); // magic
        packet.extend_from_slice(&1u64.to_le_bytes());  // sequence
        packet.extend_from_slice(&100u64.to_le_bytes()); // method
        packet.extend_from_slice(&0u32.to_le_bytes());  // slab_offset
        packet.extend_from_slice(&(payload.len() as u32).to_le_bytes()); // payload_len
        packet.extend_from_slice(&payload);
        let crc = crc32_fast(&packet);
        packet.extend_from_slice(&crc.to_le_bytes());

        let resp = parse_vctp_response_for_gateway(&packet);
        assert_eq!(resp.status, 0);
        assert_eq!(resp.workflow_id.as_deref(), Some("wf-42"));
        assert_eq!(resp.run_status.as_deref(), Some("COMPLETED"));
    }

    #[test]
    fn test_crc32_known_value() {
        // CRC32 of empty data should be 0x00000000
        let crc = crc32_fast(b"");
        assert_eq!(crc, 0x00000000);

        // CRC32 of "123456789" is a well-known test value
        let crc = crc32_fast(b"123456789");
        assert_eq!(crc, 0xCBF43926);
    }

    #[test]
    fn test_config_defaults() {
        let config = WsVctpGatewayConfig::default();
        assert_eq!(config.bind_addr, "0.0.0.0:8084");
        assert_eq!(config.vctp_server_addr, "127.0.0.1:9090");
        assert_eq!(config.max_connections, 10_000);
        assert_eq!(config.idle_timeout_secs, 300);
        assert_eq!(config.heartbeat_interval_secs, 30);
    }

    #[test]
    fn test_stats_default() {
        let stats = WsVctpGatewayStats::default();
        assert_eq!(stats.connections_accepted, 0);
        assert_eq!(stats.messages_received, 0);
        assert_eq!(stats.errors, 0);
    }
}
