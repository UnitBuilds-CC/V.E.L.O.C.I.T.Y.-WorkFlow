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
    pub bind_addr: String,          // WebSocket listen address
    pub max_connections: usize,      // Max concurrent WebSocket connections
    pub idle_timeout_secs: u64,     // Connection idle timeout
    pub vctp_target: String,        // VCTP server address (UDP)
}
```

### Key Features

- Connection management with idle timeout
- JSON ↔ VCTP binary translation
- Error propagation (VCTP errors → WebSocket JSON errors)
- Statistics tracking (connections, messages, errors)

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

## Source Files

| File | Lines | Role |
|------|-------|------|
| `velocity-classic-server/src/ws_vctp_gateway.rs` | 478 | WebSocket-to-VCTP gateway |
| `velocity-classic-server/src/http_vctp_ingress.rs` | 583 | HTTP REST ingress + Swagger UI |
| `tools/vctp-sidecar/src/main.rs` | 559 | TLS offload sidecar proxy |
