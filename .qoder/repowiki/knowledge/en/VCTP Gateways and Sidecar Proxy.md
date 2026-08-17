# VCTP Gateways and Sidecar Proxy

## Overview

Three gateway/proxy components bridge VCTP (UDP) with standard web protocols (WebSocket, HTTP), enabling browser clients, REST integrations, and encrypted external connections.

## WebSocket-to-VCTP Gateway

Bridges browser/JavaScript clients to the VCTP UDP server.

### Architecture

```
[Browser/Client] ──WebSocket──► [WsVctpGateway] ──UDP/VCTP──► [VctpRpcServer]
                 ◄──WebSocket──               ◄──UDP/VCTP──
```

### JSON Envelope

Clients send JSON over WebSocket:

```json
{
  "method": "StartWorkflow",
  "workflow_id": "wf-123",
  "namespace": "default",
  "payload": { "steps": 5 }
}
```

The gateway translates this to a VCTP RPC request, sends it over UDP, and returns the VCTP response as JSON over WebSocket.

### Configuration

```rust
pub struct WsVctpGatewayConfig {
    pub bind_addr: String,                  // WebSocket listen address
    pub max_connections: usize,              // Max concurrent WebSocket connections
    pub idle_timeout_secs: u64,             // Connection idle timeout
    pub vctp_target: String,                // VCTP server address (UDP)
    pub rate_limit_per_connection: u64,     // Max messages/sec per connection (0 = unlimited)
    pub tls: Option<WsTlsConfig>,           // Optional TLS for WSS
}
```

### TLS Support (WSS)

The gateway supports TLS termination for secure WebSocket connections:

```rust
pub struct WsTlsConfig {
    pub cert_path: String,  // PEM certificate file
    pub key_path: String,   // PEM private key file
}
```

When `tls` is configured, the gateway uses `tokio-rustls::TlsAcceptor` to wrap incoming TCP connections in TLS before the WebSocket handshake. The `run()` method uses dual-path accept: TLS connections go through `acceptor.accept(stream)` → `accept_async(tls_stream)`, while non-TLS connections use `accept_async(stream)` directly.

### Key Features

- Connection management with idle timeout
- JSON ↔ VCTP binary translation
- Error propagation (VCTP errors → WebSocket JSON errors)
- Statistics tracking (connections, messages, errors, rate_limited)
- Per-connection rate limiting (configurable messages/sec)
- TLS termination for WSS (tokio-rustls)

## HTTP-to-VCTP Ingress

REST API gateway with Swagger UI for interactive documentation.

### Architecture

```
[HTTP Client] ──HTTP/REST──► [HttpVctpIngress] ──UDP/VCTP──► [VctpRpcServer]
              ◄──HTTP/JSON──                  ◄──UDP/VCTP──
```

### REST Endpoints

| Method | Path | VCTP Method |
|--------|------|-------------|
| POST | `/api/v1/workflows` | START_WORKFLOW |
| POST | `/api/v1/workflows/{id}/signal` | SIGNAL_WORKFLOW |
| GET | `/api/v1/workflows/{id}` | DESCRIBE_WORKFLOW |
| POST | `/api/v1/workflows/{id}/cancel` | CANCEL_WORKFLOW |
| POST | `/api/v1/workflows/{id}/terminate` | TERMINATE_WORKFLOW |
| GET | `/api/v1/workflows/{id}/query` | QUERY_WORKFLOW |
| GET | `/health` | HEALTH_CHECK |
| GET | `/docs` | Swagger UI |
| GET | `/docs/openapi.json` | OpenAPI spec |

### Swagger UI

Interactive API documentation at `/docs`:
- Auto-generated from VCTP method definitions
- Try-it-out functionality for all endpoints
- OpenAPI 3.0.3 spec served at `/docs/openapi.json`
- Built with Swagger UI Standalone preset

### Implementation

Built on Axum web framework:

```rust
pub fn router(ingress: Arc<Self>) -> Router {
    Router::new()
        .route("/api/v1/workflows", post(handle_start))
        .route("/api/v1/workflows/:id/signal", post(handle_signal))
        .route("/docs", get(handle_swagger_ui))
        .route("/docs/openapi.json", get(handle_openapi_spec))
}
```

### TLS Support (HTTPS)

The HTTP ingress supports TLS termination via `axum-server` with rustls:

```rust
pub struct TlsConfig {
    pub cert_path: String,  // PEM certificate file
    pub key_path: String,   // PEM private key file
}
```

- `serve()` — Plain HTTP using `axum::serve` with `TcpListener`
- `serve_tls()` — HTTPS using `axum_server::bind_rustls` with `RustlsConfig::from_pem_file`

### Rate Limiting

Per-second window rate limiter at the gateway level:

```rust
// Fields on HttpVctpIngress:
rate_limit_rps: u64,           // Max requests per second (0 = unlimited)
rate_window_start: AtomicU64,  // Current second window
rate_window_count: AtomicU64,  // Requests in current window
rate_limited_counter: AtomicU64, // Total rejected by rate limiter
```

- `with_rate_limit(addr, rps)` — Constructor with rate limit
- `check_rate_limit()` — Returns true if allowed, false if rejected
- Lock-free implementation using `AtomicU64` with `Ordering::Relaxed`

### Integration Tests

5 live Axum server integration tests:

| Test | Description |
|------|-------------|
| `test_integration_health_endpoint` | GET /api/v1/health → 200, JSON with status+timestamp |
| `test_integration_metrics_endpoint` | GET /api/v1/metrics → 200, JSON with requests+errors |
| `test_integration_openapi_spec` | GET /docs/openapi.json → 200, OpenAPI 3.0.0 |
| `test_integration_start_workflow_timeout` | POST /api/v1/workflows → 5xx (no VCTP server) |
| `test_integration_rate_limiter` | Rate-limited ingress still serves health endpoint |

## VCTP Sidecar Proxy

TLS/crypto offload proxy for external VCTP clients.

### Architecture

```
[External Client] ──Encrypted VCTP──► [Sidecar Proxy] ──Plaintext VCTP──► [VctpRpcServer]
                   ◄──Encrypted VCTP──              ◄──Plaintext VCTP──
```

### Session Establishment (ECDH-style)

1. Client sends `SESSION_HELLO` with `client_nonce`
2. Sidecar generates `server_nonce`, computes `session_key = SHA256(client_nonce || server_nonce || shared_secret)`
3. Sidecar responds with `server_nonce`
4. All subsequent packets XOR-encrypted with `session_key` stream

### XOR Cipher

```rust
fn xor_cipher(data: &mut [u8], key: &[u8; 32], sequence: u64) {
    for (i, byte) in data.iter_mut().enumerate() {
        let key_byte = key[(sequence as usize + i) % 32];
        *byte ^= key_byte;
    }
}
```

- Per-session 32-byte key derived from nonces + shared secret
- Sequence-dependent XOR stream (different keystream per packet)
- CRC32 computed AFTER encryption (verified BEFORE decryption)

### Session Management

```rust
struct ClientSession {
    session_key: [u8; 32],
    created_at: Instant,
}

sessions: RwLock<HashMap<SocketAddr, Arc<ClientSession>>>
```

- Per-client session state with TTL
- Session expiry and automatic cleanup
- Statistics: sessions_created, sessions_expired

### Configuration

```rust
struct SidecarConfig {
    listen_addr: String,           // External listen address
    upstream_addr: String,         // VCTP server address
    shared_secret: String,         // Pre-shared secret for key derivation
    session_ttl_secs: u64,         // Session time-to-live
}
```

## Test Coverage

### WebSocket Gateway (12 unit tests)

| Test | Coverage |
|------|----------|
| `test_ws_request_serialization` | WsRequest JSON round-trip |
| `test_ws_request_default_namespace` | Namespace defaults to "default" |
| `test_ws_response_serialization` | WsResponse with None fields skipped |
| `test_ws_response_error` | Error response format |
| `test_build_vctp_packet_structure` | Magic, sequence, method, payload layout |
| `test_parse_vctp_response_too_small` | <32 bytes returns 502 |
| `test_parse_vctp_response_truncated` | payload_len > actual returns 502 |
| `test_parse_vctp_response_valid` | Full valid packet parsing |
| `test_crc32_known_value` | CRC32("") = 0, CRC32("123456789") = 0xCBF43926 |
| `test_config_defaults` | bind_addr, max_connections, timeouts |
| `test_stats_default` | All counters start at 0 |

### HTTP Ingress (20 tests: 15 unit + 5 integration)

Unit tests cover packet building, CRC32, response parsing, request deserialization, and config defaults. Integration tests spin up a live Axum server and test HTTP endpoints end-to-end.

## Source Files

| File | Lines | Role |
|------|-------|------|
| `velocity-classic-server/src/ws_vctp_gateway.rs` | 692 | WebSocket-to-VCTP gateway with TLS (WSS) and rate limiting |
| `velocity-classic-server/src/http_vctp_ingress.rs` | 871 | HTTP REST ingress with TLS (HTTPS), rate limiting, Swagger UI |
| `tools/vctp-sidecar/src/main.rs` | 474 | TLS offload sidecar proxy with ECDH + XOR cipher |
