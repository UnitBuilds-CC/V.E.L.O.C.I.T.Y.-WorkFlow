---
kind: transport_protocol
name: NMCP Protocol Transport
category: networking
scope:
    - 'velocity-nmcp-protocol/**'
    - 'velocity-classic-server/**'
    - 'velocity-embedded-server/**'
source_files:
    - velocity-nmcp-protocol/src/lib.rs
    - velocity-nmcp-protocol/src/frame.rs
    - velocity-nmcp-protocol/src/shmem.rs
    - velocity-nmcp-protocol/src/ws.rs
    - velocity-classic-server/src/main.rs
    - velocity-embedded-server/src/main.rs
---

The NMCP (Nano Message Communication Protocol) is Velocity's custom binary transport protocol, replacing HTTP/gRPC for inter-process communication between workers and servers. It provides **50-100x faster local IPC** than HTTP via shared memory.

**Architecture:**
- **Binary frame format** — 16-byte header + JSON payload (up to configurable max)
- **Dual transport** — Shared memory (shmem) for local workers, WebSocket for remote clients
- **Zero-copy shmem** — File-backed shared memory buffers for local IPC
- **NmcpDispatch trait** — Each flavor implements frame dispatch via its NmcpFrameRouter

**Frame Format:**
```rust
pub struct NmcpFrame {
    pub frame_type: u8,        // Request, Response, Heartbeat, etc.
    pub flags: u8,             // Compression, encryption flags
    pub sequence: u32,         // Monotonic sequence for ordering
    pub payload_len: u32,      // Length of JSON payload
    pub request_id: u64,       // Unique request identifier
    // Followed by payload_len bytes of JSON
}
```

**Shared Memory IPC:**
```rust
pub struct ShmemBuffer {
    // File-backed memory-mapped buffer
    // Ring buffer with head/tail pointers
    // Lock-free single-producer single-consumer
}

pub struct NmcpShmemServer {
    // Listens on a shmem path (e.g., /tmp/velocity-classic.nmcp)
    // Accepts connections from local workers
    // Dispatches frames via NmcpDispatch trait
}
```

**WebSocket Transport:**
```rust
pub struct NmcpWebSocketServer {
    // Binds to a TCP address (e.g., 0.0.0.0:8083)
    // Upgrades HTTP connections to WebSocket
    // Dispatches frames via NmcpDispatch trait
    // Supports TLS via rustls
}
```

**Transport Comparison:**
| Transport | Latency | Use Case |
|-----------|---------|----------|
| NMCP Shmem | ~0.01ms | Local workers on same machine |
| NMCP WebSocket | ~0.5ms | Remote clients, cross-machine |
| HTTP (axum) | ~1-5ms | Legacy, being replaced |
| gRPC (tonic) | ~2-10ms | Server flavor bench-suite only |

**Server Architecture (all flavors):**
```
[Local Workers] ──NMCP Shmem──► [NmcpFrameRouter] ──► [WorkflowEngine + WAL]
[Remote Clients] ──NMCP WS────► [NmcpFrameRouter] ──► [WorkflowEngine + WAL]
```

**Key files:**
- `velocity-nmcp-protocol/src/frame.rs` — Frame parsing and serialization
- `velocity-nmcp-protocol/src/shmem.rs` — Shared memory IPC implementation
- `velocity-nmcp-protocol/src/ws.rs` — WebSocket transport implementation
- `velocity-classic-server/src/main.rs` — Classic server using NMCP
- `velocity-embedded-server/src/main.rs` — Embedded server using NMCP

**Rules for developers:**
1. All new server flavors should use NMCP transport, not HTTP
2. Implement `NmcpDispatch` trait for your flavor's router
3. Use shmem for local workers (50-100x faster than WebSocket)
4. WebSocket is for remote/cross-machine access
5. Always support TLS on WebSocket endpoint for production
6. Frame payload is JSON for debuggability; binary optimization is future work
7. Test shmem IPC on the target OS (shmem paths differ: Linux vs macOS vs Windows)
