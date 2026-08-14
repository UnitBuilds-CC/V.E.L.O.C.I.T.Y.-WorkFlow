# Velocity Embedded Server — NMCP Protocol Upgrade Spec

**Date:** 2026-08-15  
**Branch:** `optimized`  
**Status:** In Progress  
**Competitive Target:** DBOS (embedded durable execution over HTTP)

---

## Mission

Replace HTTP (axum/JSON REST) with NMCP shared memory IPC as the primary transport for `velocity-embedded-server`. Goal: true in-process durable execution (impossible for HTTP-first DBOS), 2-3x lower per-call overhead, and the ability to run the engine as a library with zero network stack.

---

## Current State

| Component | Current | Location |
|-----------|---------|----------|
| Transport | HTTP/1.1+ (axum 0.7) | `velocity-embedded-server/Cargo.toml` |
| API Surface | 7 REST endpoints (JSON) | `velocity-embedded-server/src/main.rs` |
| Engine | EmbeddedEngine + PostgreSQL | `velocity-embedded/` |
| Storage | PostgresAdapter (durable state) | `velocity-embedded/src/` |
| NMCP Foundation | 16-byte frame + shmem IPC | `Velocity-Drone/` (see classic spec for details) |

### DBOS's Architectural Weakness

DBOS is **HTTP-first by design**. Every durable function call traverses:

```
Application → HTTP request → Express/Fastify → deserialize → execute → serialize → HTTP response
```

Even when the caller and engine are in the **same process**, the call goes over HTTP. This is DBOS's biggest performance ceiling and it's **architecturally impossible to remove** without breaking their API contract.

### What HTTP Costs Embedded Server

```
HTTP call budget (axum, keepalive, small payload):
  HTTP parse: ~500ns
  JSON deserialize: ~1-5μs
  Router dispatch: ~100ns
  JSON serialize: ~1-5μs
  ─────────────────────
  HTTP overhead: ~3-11μs per call
  
PostgreSQL round-trip: ~1-5ms (the real bottleneck)
  
Total per durable call: ~1-5ms (PG dominates, but HTTP is 2nd largest cost)
```

### What NMCP Shmem Gives Us

```
NMCP shmem call budget:
  Atomic state read: ~1ns
  Payload copy (4KB): ~50ns
  Atomic state write: ~1ns
  ─────────────────────
  NMCP overhead: ~50-100ns per call

PostgreSQL round-trip: ~1-5ms (same bottleneck)

Total per durable call: ~1-5ms (same PG cost, but HTTP overhead ELIMINATED)
```

**The win isn't raw latency (PG dominates). The win is architectural:**
- True library-mode: engine runs in-process, transport = function call speed
- Zero network stack: no TCP, no HTTP, no ports, no TLS
- DBOS literally cannot do this — it's baked into their architecture

---

## Target Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                  velocity-embedded-server                      │
│                                                              │
│  ┌────────────────────────────────────────────────────────┐  │
│  │              Application Process                        │  │
│  │                                                        │  │
│  │  ┌──────────────────┐    ┌──────────────────────────┐  │  │
│  │  │  User Code        │    │  EmbeddedEngine           │  │  │
│  │  │  (durable funcs)  │◄──►│  (library mode)           │  │  │
│  │  │                   │    │  ┌─────────────────────┐  │  │  │
│  │  │  Direct function  │    │  │  PostgresAdapter     │  │  │  │
│  │  │  calls (in-proc)  │    │  │  (durable storage)   │  │  │  │
│  │  └──────────────────┘    │  └─────────────────────┘  │  │  │
│  │                           └──────────────────────────┘  │  │
│  └────────────────────────────────────────────────────────┘  │
│                                                              │
│  ┌────────────────────────────────────────────────────────┐  │
│  │              NMCP Shmem IPC Layer                        │  │
│  │  (for external/remote clients and benchmarking)         │  │
│  │                                                        │  │
│  │  ┌──────────────────┐    ┌──────────────────────────┐  │  │
│  │  │  Shmem Client     │───►│  NmcpEmbeddedRouter      │  │  │
│  │  │  (external proc)  │    │  • Frame dispatch         │  │  │
│  │  │  ~50-100ns        │    │  • Engine method calls    │  │  │
│  │  └──────────────────┘    └──────────────────────────┘  │  │
│  └────────────────────────────────────────────────────────┘  │
│                                                              │
│  ┌────────────────────────────────────────────────────────┐  │
│  │              NMCP WebSocket (optional)                   │  │
│  │  (for remote monitoring, multi-machine benchmarks)      │  │
│  └────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────┘
```

---

## Two Operating Modes

### Mode 1: Library Mode (Primary — DBOS Killer)

The engine runs **in-process** with the application. No transport layer at all — direct function calls.

```rust
// Application code: direct function calls, zero network overhead
use velocity_embedded::EmbeddedEngine;

#[tokio::main]
async fn main() {
    let engine = EmbeddedEngine::with_storage(config, Box::new(pg_adapter));
    engine.init().unwrap();
    
    // Durable execution: direct function call, no HTTP, no serialization
    let result = engine.execute("my-workflow", "process_order", input, |ctx, input| async {
        let step1 = ctx.run("validate", || async { validate(input) }).await?;
        let step2 = ctx.run("charge", || async { charge(step1) }).await?;
        let step3 = ctx.run("fulfill", || async { fulfill(step2) }).await?;
        Ok(step3)
    }).await;
}
```

**This is what DBOS cannot do.** Their HTTP-first architecture means every durable call pays HTTP overhead, even in-process.

### Mode 2: Server Mode (Benchmarking & Remote Access)

For benchmarking, monitoring, and multi-process scenarios, the NMCP shmem server wraps the same engine.

```rust
// Server mode: external clients connect via NMCP shmem
#[tokio::main]
async fn main() {
    let engine = Arc::new(EmbeddedEngine::with_storage(config, Box::new(pg_adapter)));
    
    // Start NMCP shmem server (for local benchmarking)
    let shmem_server = NmcpEmbeddedServer::new(engine.clone(), "/tmp/velocity-embedded.sock");
    
    // Optionally start WebSocket server (for remote monitoring)
    let ws_server = NmcpEmbeddedWsServer::new(engine.clone(), "0.0.0.0:8082");
    
    tokio::join!(shmem_server.run(), ws_server.run());
}
```

---

## NMCP Frame Types for Embedded Server

```rust
/// Embedded Server NMCP frame types (70-79 range).
pub struct EmbeddedFrameTypes;

impl EmbeddedFrameTypes {
    // Durable Execution (70-79)
    pub const EXECUTE_WORKFLOW: u32 = 70;
    pub const GET_WORKFLOW: u32 = 71;
    pub const SIGNAL_WORKFLOW: u32 = 72;
    pub const QUERY_WORKFLOW: u32 = 73;
    pub const COMPLETE_WORKFLOW: u32 = 74;
    
    // System (80-89)
    pub const HEALTH_CHECK: u32 = 80;
    pub const ENGINE_STATS: u32 = 81;
}
```

---

## Implementation Plan

### Phase 1: Library Mode Enhancement (~100 lines)

**File:** `velocity-embedded/src/lib.rs` (enhance existing)

Ensure the `EmbeddedEngine` API is clean for direct in-process usage:

```rust
impl EmbeddedEngine {
    /// Execute a durable workflow directly (library mode, zero transport overhead).
    pub async fn execute<F, Fut, I, O>(&self, workflow_id: &str, workflow_type: &str, input: I, func: F) -> Result<WorkflowHandle<O>, EngineError>
    where
        F: FnOnce(WorkflowContext, I) -> Fut + Send + 'static,
        Fut: Future<Output = Result<O, EngineError>> + Send,
        I: Serialize + DeserializeOwned + Send + 'static,
        O: Serialize + DeserializeOwned + Send + 'static,
    {
        // Existing implementation — already works for library mode
    }
    
    /// Get engine statistics (for monitoring).
    pub fn stats(&self) -> EngineStats {
        // Existing implementation
    }
}
```

### Phase 2: NMCP Embedded Router (~250 lines)

**File:** `velocity-embedded/src/nmcp_router.rs` (new)

```rust
pub struct NmcpEmbeddedRouter {
    engine: Arc<EmbeddedEngine>,
}

impl NmcpEmbeddedRouter {
    pub async fn dispatch(&self, frame: &NmcpFrame) -> NmcpFrame {
        match frame.frame_type {
            EmbeddedFrameTypes::EXECUTE_WORKFLOW => self.handle_execute(frame).await,
            EmbeddedFrameTypes::GET_WORKFLOW => self.handle_get(frame).await,
            EmbeddedFrameTypes::SIGNAL_WORKFLOW => self.handle_signal(frame).await,
            EmbeddedFrameTypes::QUERY_WORKFLOW => self.handle_query(frame).await,
            EmbeddedFrameTypes::HEALTH_CHECK => self.handle_health(frame).await,
            EmbeddedFrameTypes::ENGINE_STATS => self.handle_stats(frame).await,
            _ => self.error_response(frame.sequence_id, "unknown frame type"),
        }
    }
}
```

### Phase 3: NMCP Shmem Server (~200 lines)

**File:** `velocity-embedded/src/nmcp_shmem.rs` (new)

Same 5-state atomic protocol as classic-server spec. Wraps the router.

### Phase 4: Server Binary Rewrite

**File:** `velocity-embedded-server/src/main.rs` (rewrite)

- Remove `axum` dependency
- Add NMCP shmem server (primary) and WebSocket server (optional)
- CLI: `--shmem-path`, `--ws-bind` (optional), `--database-url`
- Keep `EmbeddedEngine` + `PostgresAdapter` unchanged

### Phase 5: Library + Server Dual Mode

The server binary supports both modes:

```rust
#[derive(Parser)]
struct Cli {
    /// Run in library mode (in-process, no transport)
    #[arg(long)]
    library_mode: bool,
    
    /// Shmem buffer path (server mode)
    #[arg(long, default_value = "/tmp/velocity-embedded.nmcp")]
    shmem_path: String,
    
    /// WebSocket bind address (optional, server mode)
    #[arg(long)]
    ws_bind: Option<String>,
    
    /// PostgreSQL connection URL
    #[arg(long, env = "DATABASE_URL")]
    database_url: String,
}
```

---

## Dependencies to Remove

| Crate | Current Usage | Replacement |
|-------|---------------|-------------|
| `axum` | HTTP server | NMCP shmem + WebSocket |
| `chrono` | Timestamps in HTTP responses | Keep (used in engine) |

## Dependencies to Add

| Crate | Purpose |
|-------|---------|
| `tokio-tungstenite` | WebSocket server (optional, for remote access) |
| `futures-util` | Stream splitting for WebSocket |

---

## Competitive Advantage vs DBOS

| Capability | DBOS | Velocity Embedded | Winner |
|------------|------|-------------------|--------|
| In-process durable execution | **Impossible** (HTTP-first) | **Native** (library mode) | **Velocity** |
| Per-call transport overhead | ~3-11μs (HTTP) | ~0 (direct call) or ~50-100ns (shmem) | **Velocity** |
| Network failure modes | TCP timeout, conn reset, port exhaustion | Shmem: **zero**. WS: reconnect only | **Velocity** |
| Binary size | Large (Node.js + HTTP framework) | Small (Rust, no HTTP framework) | **Velocity** |
| Memory footprint | High (V8 heap + HTTP buffers) | Low (Rust + shared memory pages) | **Velocity** |
| PostgreSQL round-trip | Same bottleneck | Same bottleneck | Tie |
| Durable execution API | TypeScript decorators | Rust async/await + closures | Comparable |
| Crash recovery | PostgreSQL-based | PostgreSQL-based | Tie |

**The killer differentiator:** Velocity embedded can run as a **true library** — the engine lives inside your process, durable function calls are direct async function calls with zero serialization. DBOS requires an HTTP round-trip even for co-located code. This is an architectural advantage DBOS **cannot remove** without rewriting their entire system.

---

## Success Criteria

| Metric | Target | Measurement |
|--------|--------|-------------|
| Library mode call overhead | < 100ns (beyond PG I/O) | Benchmark: in-process loopback |
| Shmem IPC latency | < 200ns | Benchmark: shmem loopback |
| Binary size (server mode) | < 8MB | `cargo build --release` |
| Memory (library mode) | < 30MB base | RSS (no HTTP buffers) |
| DBOS migration time | < 1 day per project | Migrator tool benchmark |
| Durable calls/sec (PG-bound) | Match PG throughput | No transport bottleneck |

---

## Risk Mitigation

| Risk | Mitigation |
|------|------------|
| PG is still the bottleneck | Acknowledged — we're removing the 2nd largest cost, not the largest |
| Library mode requires Rust | True — but SDK wrappers (TS, Python) can use FFI to call the Rust library |
| Shmem single-writer | One buffer per client process (same as classic-server) |
| WebSocket needed for monitoring | Optional WS server for remote stats/health |
| DBOS has TypeScript ecosystem | Migrator tool translates DBOS TypeScript → Velocity Rust |

---

## Files to Create/Modify

| File | Action |
|------|--------|
| `velocity-embedded/src/nmcp_router.rs` | **CREATE** — Frame dispatcher (~250 lines) |
| `velocity-embedded/src/nmcp_shmem.rs` | **CREATE** — Shmem server (~200 lines) |
| `velocity-embedded/src/lib.rs` | **MODIFY** — Add NMCP modules, enhance library API |
| `velocity-embedded/Cargo.toml` | **MODIFY** — Remove axum, add tokio-tungstenite |
| `velocity-embedded-server/src/main.rs` | **REWRITE** — Dual-mode NMCP server |
| `velocity-embedded-server/Cargo.toml` | **MODIFY** — Remove axum, update deps |
