# Velocity Workflow Server — VCTP Protocol Upgrade Spec

**Date:** 2026-08-15  
**Branch:** `optimized`  
**Status:** In Progress  
**Competitive Target:** Temporal (gRPC)

---

## Mission

Replace gRPC (tonic/HTTP2) with VCTP (zero-copy UDP) as the primary client-facing transport for `velocity-workflow-server`. Goal: 5-15x faster RPC latency, connectionless scalability to 100K+ clients, and SDKs that are simpler than gRPC stubs.

---

## Current State

| Component | Current | Location |
|-----------|---------|----------|
| Transport | gRPC over HTTP/2 (tonic 0.12) | `velocity-workflow-server/Cargo.toml` |
| Service Definition | `BenchmarkService` (50+ RPCs) | `velocity-workflow-server/src/main.rs` |
| Proto Files | 10 files, 832+ lines workflow_service.proto | `proto/velocity/v1/` |
| Engine | WorkflowEngine + WAL | `velocity-workflow-engine/` |
| VCTP Foundation | Packet header + UDP transport + encryption + AIMD + ACK/retransmit | `velocity-workflow-core/src/vctp.rs`, `velocity-workflow-engine/src/vctp_transport.rs` |

### What gRPC Costs Us

- Protobuf encode/decode: ~200-500ns per message
- HTTP/2 framing + HPACK: ~100-300ns per message
- Connection setup (TLS + HTTP/2 SETTINGS): 100ms+ first request
- Per-connection memory: ~50KB (stream state, flow control windows)
- SDK complexity: proto codegen → 6 languages, massive dependency trees
- tonic runtime: interceptor chains, metadata propagation, async dispatch overhead

### What VCTP Gives Us

- 28-byte header, zero-copy parse: ~5-10ns
- No connection handshake (UDP): 0ms setup
- Built-in XOR-AES encryption: ~1ns/byte (cheaper than TLS)
- Built-in AIMD congestion control: graceful degradation under load
- Connectionless: 100K clients = 1 socket, no per-connection state
- No code generation: packets are self-describing

---

## Target Architecture

```
                        ┌────────────────────────────────────────┐
                        │       velocity-workflow-server          │
                        │                                        │
    [SDK clients]       │  ┌──────────────────────────────────┐  │
     Go, Python,  ──VCTP──►│  VctpRpcService                  │  │
     Java, TS,      UDP  │  │  • Method dispatch (frame_type)  │  │
     C#, PHP             │  │  • Sequence correlation          │  │
                        │  │  • Fragmentation/reassembly       │  │
                        │  │  • Auth token validation          │  │
                        │  └──────────────┬───────────────────┘  │
                        │                 │                       │
                        │  ┌──────────────▼───────────────────┐  │
                        │  │   RealEngineAdapter               │  │
                        │  │   (existing WorkflowEngine + WAL) │  │
                        │  └──────────────┬───────────────────┘  │
                        │                 │                       │
                        │  ┌──────────────▼───────────────────┐  │
                        │  │   VctpTransport (existing)        │  │
                        │  │   UDP socket + cipher + ACK       │  │
                        │  └──────────────────────────────────┘  │
                        └────────────────────────────────────────┘
```

---

## Implementation Plan

### Phase 1: VCTP RPC Layer (~650 lines)

Build a thin RPC layer on top of the existing `VctpTransport`.

**File:** `velocity-workflow-engine/src/vctp_rpc.rs`

#### 1.1 Method Registry

Map method names to frame types. VCTP's `workflow_id` field doubles as method discriminator.

```rust
/// VCTP RPC method frame types (allocated in the workflow_id field).
pub struct VctpMethods;

impl VctpMethods {
    // Workflow Lifecycle (100-199)
    pub const START_WORKFLOW: u64 = 100;
    pub const SIGNAL_WORKFLOW: u64 = 101;
    pub const QUERY_WORKFLOW: u64 = 102;
    pub const CANCEL_WORKFLOW: u64 = 103;
    pub const TERMINATE_WORKFLOW: u64 = 104;
    pub const DESCRIBE_WORKFLOW: u64 = 105;
    pub const LIST_WORKFLOWS: u64 = 106;
    pub const RESET_WORKFLOW: u64 = 107;
    pub const UPDATE_WORKFLOW: u64 = 108;
    
    // Task Dispatch (200-299)
    pub const POLL_WORKFLOW_TASK: u64 = 200;
    pub const POLL_ACTIVITY_TASK: u64 = 201;
    pub const COMPLETE_WORKFLOW_TASK: u64 = 202;
    pub const COMPLETE_ACTIVITY_TASK: u64 = 203;
    
    // Namespace Management (300-399)
    pub const REGISTER_NAMESPACE: u64 = 300;
    pub const DESCRIBE_NAMESPACE: u64 = 301;
    pub const UPDATE_NAMESPACE: u64 = 302;
    pub const DELETE_NAMESPACE: u64 = 303;
    
    // History & Visibility (400-499)
    pub const GET_HISTORY: u64 = 400;
    pub const GET_WORKFLOW_EXECUTION: u64 = 401;
    
    // System (500-599)
    pub const HEALTH_CHECK: u64 = 500;
    pub const RECORD_HEARTBEAT: u64 = 501;
}
```

#### 1.2 RPC Request/Response Envelope

JSON payload inside VCTP packet (payload field). Correlated by `sequence_number`.

```rust
/// RPC request envelope (serialized as JSON in VCTP payload).
#[derive(Serialize, Deserialize)]
pub struct VctpRpcRequest {
    pub method: u64,           // Maps to VctpMethods
    pub namespace: String,
    pub workflow_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<Vec<u8>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<HashMap<String, String>>,
}

/// RPC response envelope.
#[derive(Serialize, Deserialize)]
pub struct VctpRpcResponse {
    pub status: u32,           // 0 = OK, non-zero = error code
    pub sequence: u64,         // Correlates to request's sequence_number
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<Vec<u8>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}
```

#### 1.3 Fragmentation for Large Payloads

VCTP max payload is ~65KB. Workflow inputs can be larger. Fragment across multiple packets sharing the same `workflow_id` and a fragment index encoded in `slab_offset`.

```rust
/// Fragment header (packed into slab_offset field):
///   [0..2]  fragment_index (u16)
///   [2..4]  total_fragments (u16)
pub fn encode_fragment_meta(index: u16, total: u16) -> u32 {
    (index as u32) << 16 | total as u32
}
```

#### 1.4 Server-Side RPC Dispatcher

```rust
pub struct VctpRpcServer {
    transport: Arc<VctpTransport>,
    engine: Arc<RealEngineAdapter>,
    running: AtomicBool,
}

impl VctpRpcServer {
    /// Main receive loop: recv packets → dispatch → respond.
    pub async fn run(&self) {
        loop {
            let packets = self.transport.recv_packets();
            for (packet, src_addr) in packets {
                let request: VctpRpcRequest = match serde_json::from_slice(&packet.payload) {
                    Ok(r) => r,
                    Err(_) => continue,
                };
                
                let response = self.dispatch(request).await;
                let response_bytes = serde_json::to_vec(&response).unwrap_or_default();
                
                let _ = self.transport.send_packet(
                    src_addr,
                    0, // workflow_id = 0 for responses
                    0,
                    response_bytes,
                );
            }
            
            // Process retransmissions periodically
            self.transport.process_retransmissions();
            
            if !self.running.load(Ordering::Relaxed) {
                break;
            }
            
            tokio::task::yield_now().await;
        }
    }
    
    async fn dispatch(&self, req: VctpRpcRequest) -> VctpRpcResponse {
        match req.method {
            VctpMethods::START_WORKFLOW => self.handle_start_workflow(req).await,
            VctpMethods::SIGNAL_WORKFLOW => self.handle_signal_workflow(req).await,
            VctpMethods::QUERY_WORKFLOW => self.handle_query_workflow(req).await,
            VctpMethods::HEALTH_CHECK => VctpRpcResponse { status: 0, sequence: 0, payload: None, error: None },
            // ... all other methods
            _ => VctpRpcResponse { status: 404, sequence: 0, payload: None, error: Some("unknown method".into()) },
        }
    }
}
```

### Phase 2: Server Binary Rewrite

**File:** `velocity-workflow-server/src/main.rs` (rewrite)

- Remove all `tonic`, `prost`, `prost-types` dependencies
- Remove `build.rs` (proto compilation)
- Remove `BenchmarkServiceImpl` and `#[tonic::async_trait]` impl
- Add `VctpRpcServer` with `VctpTransport`
- Keep `RealEngineAdapter` and `WorkflowEngine` unchanged
- CLI: replace `--grpc-port` with `--vctp-port` (UDP)

### Phase 3: SDK Client Reference Implementation

**File:** `sdk/go/velocity_sdk_vctp/client.go` (new)

Demonstrate that a VCTP client is dramatically simpler than gRPC:

```go
// ~150 lines total. No proto codegen. No gRPC dependency.
type Client struct {
    conn    *net.UDPConn
    server  *net.UDPAddr
    seq     atomic.Uint64
}

func (c *Client) StartWorkflow(ctx context.Context, opts *StartWorkflowOptions) (*WorkflowHandle, error) {
    req := VctpRpcRequest{Method: 100, Namespace: opts.Namespace, WorkflowID: opts.WorkflowID}
    payload, _ := json.Marshal(req)
    
    seq := c.seq.Add(1)
    header := VctpPacketHeader{Magic: 0x50544356, SequenceNumber: seq, WorkflowId: 100, PayloadLength: uint32(len(payload))}
    
    // Write header + payload, send via UDP
    buf := header.ToBytes()
    buf = append(buf, payload...)
    _, err := c.conn.WriteToUDP(buf, c.server)
    
    // Receive response (correlated by seq)
    // ... ~30 more lines
}
```

### Phase 4: Remove Proto Files and gRPC Feature Gate

- Delete `proto/velocity/v1/*.proto` (or archive to `proto/legacy/`)
- Remove `grpc` feature from `velocity-workflow-engine/Cargo.toml`
- Remove `tonic-build` and `protox` from build dependencies
- Remove `velocity-workflow-server/build.rs`

---

## Dependencies to Remove

| Crate | Current Usage | Replacement |
|-------|---------------|-------------|
| `tonic` | gRPC server | VCTP UDP transport (already in engine) |
| `prost` | Protobuf serialization | `serde_json` for RPC envelopes |
| `prost-types` | Protobuf well-known types | Native Rust types |
| `tonic-build` | Proto code generation | Not needed |
| `protox` | Proto parsing | Not needed |
| `tokio-stream` | gRPC streaming | VCTP packet streams |

## Dependencies to Add

| Crate | Purpose |
|-------|---------|
| `serde_json` | RPC request/response envelope serialization (already a dependency) |

---

## Migration Path

The migrator tool (`tools/temporal2velocity/`) handles client migration:

1. **Temporal → Velocity migrator** translates Temporal client calls to VCTP RPC calls
2. **SDK migration guide** shows gRPC → VCTP mapping for each language
3. **Dual-listen period**: Server can listen on both gRPC and VCTP during transition (feature flag)

---

## Success Criteria

| Metric | Target | Measurement |
|--------|--------|-------------|
| RPC latency (LAN) | < 15μs | Benchmark: `velocity-bench` |
| RPC latency (loopback) | < 5μs | Benchmark: same machine |
| Concurrent clients | 100,000+ | Load test: connection count |
| SDK binary size (Go) | < 5MB | `go build` output |
| Server memory (100K clients) | < 200MB | `RSS` under load |
| Zero codegen | No proto files needed | Build verification |

---

## Risk Mitigation

| Risk | Mitigation |
|------|------------|
| UDP packet loss | VCTP already has ACK + retransmit tracker with EWMA RTT |
| Large payloads | Fragmentation layer (Phase 1.3) |
| Service discovery | Static config initially; multicast DNS later |
| Authentication | Auth token in RPC envelope metadata; VCTP encryption for transport security |
| Firewall/NAT | VCTP can fall back to TCP tunnel (future); initial target is LAN/cluster-internal |

---

## Files to Create/Modify

| File | Action |
|------|--------|
| `velocity-workflow-engine/src/vctp_rpc.rs` | **CREATE** — RPC layer (~400 lines) |
| `velocity-workflow-server/src/main.rs` | **REWRITE** — VCTP server (replace gRPC) |
| `velocity-workflow-server/Cargo.toml` | **MODIFY** — Remove tonic/prost, keep engine |
| `velocity-workflow-server/build.rs` | **DELETE** — No proto compilation |
| `velocity-workflow-engine/Cargo.toml` | **MODIFY** — Remove grpc feature gate |
| `velocity-workflow-engine/src/lib.rs` | **MODIFY** — Add `vctp_rpc` module |
| `sdk/go/velocity_sdk_vctp/client.go` | **CREATE** — Reference VCTP client |
| `proto/legacy/` | **CREATE** — Archive old proto files |
