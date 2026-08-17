# Velocity Classic Server — NMCP Protocol Upgrade Spec

**Date:** 2026-08-15  
**Branch:** `optimized`  
**Status:** Complete  
**Competitive Target:** Restate, Inngest, lightweight workflow engines (HTTP-based)

---

## Mission

Replace HTTP (axum/JSON REST) with NMCP — shared memory IPC for local clients, WebSocket for remote clients. Goal: 50-100x faster local IPC, zero network failure modes for co-located workers, and a smaller binary footprint than any HTTP-based competitor.

---

## Current State

| Component | Current | Location |
|-----------|---------|----------|
| Transport | HTTP/1.1+ (axum 0.7) | `velocity-classic-server/Cargo.toml` |
| API Surface | 9 REST endpoints (JSON) | `velocity-classic-server/src/main.rs` |
| Engine | WorkflowEngine + WAL | `velocity-workflow-engine/` |
| NMCP Foundation | 16-byte frame header + frame type registry | `Velocity-Drone/rust/crates/drone-core/src/protocol.rs` |
| NMCP Shmem | 64KB atomic state machine IPC | `Velocity-Drone/Drone.Native/src/shmem.rs` |
| NMCP WebSocket | JSON-RPC over WebSocket MCP server | `Velocity-Drone/rust/crates/drone-mcp/src/server.rs` |

### What HTTP Costs Us

- TCP accept + keepalive: ~2μs (keepalive), ~50μs (new connection)
- HTTP parse (axum): ~500ns per request
- JSON deserialize (serde_json): ~1-5μs per request
- Router dispatch: ~100ns
- JSON serialize: ~1-5μs per response
- **Total per request: ~3-12μs** (keepalive, small payload)
- Failure modes: TCP timeout, connection reset, port exhaustion, TLS handshake failure

### What NMCP Gives Us

**Shmem (local IPC):**
- Atomic state read: ~1ns
- Payload copy (4KB): ~50ns (memcpy)
- Atomic state write: ~1ns
- **Total per request: ~50-100ns** → **30-200x faster than HTTP**

**WebSocket (remote):**
- No HTTP parse overhead
- Binary NMCP framing: 16-byte header vs HTTP's ~200+ bytes of headers
- Persistent connection, no per-request setup
- **Total per request: ~1-3μs** → **2-5x faster than HTTP**

---

## Target Architecture

```
[Local Workers]                    [Remote Clients]
     │                                   │
     │ NMCP Shmem IPC                    │ NMCP WebSocket
     │ (64KB atomic buffer)              │ (JSON-RPC in NMCP frames)
     │ ~50-100ns per call                │ ~1-3μs per call
     │                                   │
     ▼                                   ▼
┌──────────────────────────────────────────────────────────┐
│                  velocity-classic-server                   │
│                                                          │
│  ┌────────────────────────────────────────────────────┐  │
│  │              NmcpFrameRouter                        │  │
│  │  • Shmem channel: poll → parse → dispatch → respond │  │
│  │  • WS channel: accept → recv → parse → dispatch     │  │
│  │  • Unified method dispatch for both transports      │  │
│  └──────────────────────┬─────────────────────────────┘  │
│                         │                                 │
│  ┌──────────────────────▼─────────────────────────────┐  │
│  │           ClassicMethodHandler                     │  │
│  │  • start_workflow    • signal_workflow              │  │
│  │  • get_workflow      • query_workflow               │  │
│  │  • cancel_workflow   • terminate_workflow           │  │
│  │  • update_workflow   • reset_workflow               │  │
│  │  • health_check                                    │  │
│  └──────────────────────┬─────────────────────────────┘  │
│                         │                                 │
│  ┌──────────────────────▼─────────────────────────────┐  │
│  │         WorkflowEngine + WAL (unchanged)            │  │
│  └────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────┘
```

---

## NMCP Frame Types for Classic Server

Extend NMCP's frame type registry with workflow-specific types:

```rust
/// Classic Server NMCP frame types (50-59 range).
pub struct ClassicFrameTypes;

impl ClassicFrameTypes {
    // Workflow Lifecycle (50-59)
    pub const START_WORKFLOW: u32 = 50;
    pub const GET_WORKFLOW: u32 = 51;
    pub const SIGNAL_WORKFLOW: u32 = 52;
    pub const QUERY_WORKFLOW: u32 = 53;
    pub const CANCEL_WORKFLOW: u32 = 54;
    pub const TERMINATE_WORKFLOW: u32 = 55;
    pub const UPDATE_WORKFLOW: u32 = 56;
    pub const RESET_WORKFLOW: u32 = 57;
    
    // System (60-69)
    pub const HEALTH_CHECK: u32 = 60;
    pub const SERVER_STATS: u32 = 61;
    
    // Response frames (use existing NMCP types)
    // JsonRpcResponse (2) for successful responses
    // Error responses encoded in payload
}
```

---

## Implementation Plan

### Phase 1: NMCP Frame Router (~300 lines)

**File:** `velocity-classic/src/nmcp_router.rs` (new module in classic library crate)

Unified dispatcher that handles NMCP frames from both shmem and WebSocket channels.

```rust
pub struct NmcpFrameRouter {
    engine: Arc<WorkflowEngine>,
    workflow_map: Arc<Mutex<HashMap<String, u64>>>,
    workflow_counter: Arc<AtomicU64>,
}

impl NmcpFrameRouter {
    /// Dispatch an NMCP frame and return a response frame.
    pub async fn dispatch(&self, frame: &NmcpFrame) -> NmcpFrame {
        match frame.frame_type {
            ClassicFrameTypes::START_WORKFLOW => self.handle_start(frame).await,
            ClassicFrameTypes::GET_WORKFLOW => self.handle_get(frame).await,
            ClassicFrameTypes::SIGNAL_WORKFLOW => self.handle_signal(frame).await,
            ClassicFrameTypes::QUERY_WORKFLOW => self.handle_query(frame).await,
            ClassicFrameTypes::CANCEL_WORKFLOW => self.handle_cancel(frame).await,
            ClassicFrameTypes::TERMINATE_WORKFLOW => self.handle_terminate(frame).await,
            ClassicFrameTypes::HEALTH_CHECK => self.handle_health(frame).await,
            _ => NmcpFrame::new(
                NmcpFrameTypes::JSON_RPC_RESPONSE,
                frame.sequence_id,
                serde_json::to_vec(&serde_json::json!({"error": "unknown frame type"})).unwrap(),
            ),
        }
    }
}
```

### Phase 2: Shmem Transport Integration (~200 lines)

**File:** `velocity-classic/src/nmcp_shmem.rs`

Port the Drone shmem protocol into the classic server. Use the same 5-state atomic machine:

```
IDLE → REQ_READY → PROCESSING → RES_READY → IDLE
```

```rust
pub struct NmcpShmemServer {
    router: Arc<NmcpFrameRouter>,
    buffer_path: String,
    buffer_size: usize,
    running: AtomicBool,
}

impl NmcpShmemServer {
    /// Run the shmem IPC server loop.
    pub async fn run(&self) {
        // Open memory-mapped file
        // Poll for REQ_READY state
        // Read request frame
        // Transition to PROCESSING
        // Dispatch through router
        // Write response frame
        // Transition to RES_READY
        // Wait for client to reset to IDLE
        // Repeat
    }
}
```

### Phase 3: WebSocket Transport Integration (~200 lines)

**File:** `velocity-classic/src/nmcp_websocket.rs`

Port the Drone MCP WebSocket server pattern. Accept WebSocket connections, wrap messages in NMCP frames.

```rust
pub struct NmcpWebSocketServer {
    router: Arc<NmcpFrameRouter>,
    listen_addr: String,
    auth_token: Option<String>,
    max_connections: usize,
}

impl NmcpWebSocketServer {
    /// Run the WebSocket server.
    pub async fn run(&self) {
        // TcpListener::bind
        // Accept loop
        // WebSocket handshake
        // For each message: parse NMCP frame → dispatch → send response
    }
}
```

### Phase 4: Server Binary Rewrite

**File:** `velocity-classic-server/src/main.rs` (rewrite)

- Remove `axum` dependency
- Start both `NmcpShmemServer` and `NmcpWebSocketServer` as concurrent tasks
- CLI: replace `--bind` with `--shmem-path` and `--ws-bind`
- Keep `WorkflowEngine` + WAL unchanged

### Phase 5: Client Library

**File:** `velocity-classic/src/nmcp_client.rs`

Provide both shmem and WebSocket client implementations:

```rust
/// Shmem client for co-located workers.
pub struct NmcpShmemClient {
    buffer_path: String,
    next_seq: AtomicU32,
}

impl NmcpShmemClient {
    /// Send a request and wait for response via shmem.
    pub fn call(&self, frame_type: u32, payload: Vec<u8>) -> NmcpFrame {
        // Write request to shmem buffer
        // Set state to REQ_READY
        // Poll for RES_READY
        // Read response
        // Reset to IDLE
    }
}

/// WebSocket client for remote access.
pub struct NmcpWebSocketClient {
    ws_url: String,
    next_seq: AtomicU32,
}
```

---

## Dependencies to Remove

| Crate | Current Usage | Replacement |
|-------|---------------|-------------|
| `axum` | HTTP server/router | NMCP shmem + WebSocket |
| `serde_json` | JSON request/response | Keep (NMCP payloads are JSON) |

## Dependencies to Add

| Crate | Purpose |
|-------|---------|
| `tokio-tungstenite` | WebSocket server (already in Drone) |
| `futures-util` | Stream splitting for WebSocket |

---

## Shmem Buffer Layout

Same as Drone's proven layout (64KB total):

```
Offset    Size    Description
─────────────────────────────────────
0         1       Request state byte (IDLE/REQ_READY/PROCESSING/RES_READY/ERROR)
1         4       Request payload length (i32 LE)
5         4096    Request payload (NMCP frame bytes)
4100      1       Response state byte
4101      4       Response payload length (i32 LE)
4105      61431   Response payload (NMCP frame bytes)
─────────────────────────────────────
Total: 65536 bytes (64KB)
```

---

## Competitive Advantages

| vs Restate | vs Inngest | vs HTTP-based engines |
|------------|------------|----------------------|
| 50-100x faster local IPC | No HTTP parsing overhead | Zero network failure modes |
| No port exhaustion | Smaller binary (no HTTP framework) | Shmem = no TCP stack |
| Atomic IPC (no race conditions) | Lower tail latency | File-backed buffer = offline resilience |
| Single binary, no sidecar | Simpler deployment | Lock-free (no mutex contention) |

---

## Success Criteria

| Metric | Target | Measurement |
|--------|--------|-------------|
| Local IPC latency | < 200ns | Benchmark: shmem loopback |
| Remote latency (WS) | < 5μs | Benchmark: WebSocket loopback |
| Binary size | < 5MB | `cargo build --release` |
| Memory (1000 local workers) | < 50MB | RSS (shmem = shared pages) |
| Failure rate (shmem) | 0% network failures | Stress test |
| Throughput (shmem) | > 1M calls/sec | Single-core benchmark |

---

## Risk Mitigation

| Risk | Mitigation |
|------|------------|
| Shmem single-writer limitation | One shmem buffer per worker process (scalable) |
| WebSocket connection limits | Max 16 concurrent connections (configurable), reject with backpressure |
| Payload size (shmem) | 4KB request / 61KB response (sufficient for workflow API) |
| Cross-platform shmem | File-backed (works on Windows/Linux/macOS); true mmap as optimization |
| Client library adoption | Provide both Rust and C FFI (via Drone.Native pattern) |

---

## Files to Create/Modify

| File | Action |
|------|--------|
| `velocity-classic/src/nmcp_router.rs` | **CREATE** — Frame dispatcher (~300 lines) |
| `velocity-classic/src/nmcp_shmem.rs` | **CREATE** — Shmem server (~200 lines) |
| `velocity-classic/src/nmcp_websocket.rs` | **CREATE** — WebSocket server (~200 lines) |
| `velocity-classic/src/nmcp_client.rs` | **CREATE** — Client library (~250 lines) |
| `velocity-classic/src/lib.rs` | **MODIFY** — Add NMCP modules |
| `velocity-classic/Cargo.toml` | **MODIFY** — Remove axum, add tokio-tungstenite |
| `velocity-classic-server/src/main.rs` | **REWRITE** — NMCP server binary |
| `velocity-classic-server/Cargo.toml` | **MODIFY** — Remove axum, update deps |
