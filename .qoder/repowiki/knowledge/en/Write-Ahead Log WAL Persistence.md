---
kind: persistence_system
name: Write-Ahead Log (WAL) Persistence
category: persistence
scope:
    - 'velocity-workflow-server/**'
source_files:
    - velocity-workflow-server/src/main.rs
    - velocity-workflow-engine/src/lib.rs
    - velocity-workflow-engine/src/engine.rs
---

The Velocity Server uses a Write-Ahead Log (WAL) for durable execution, ensuring workflow state survives crashes.

**Architecture:**
- **Append-only log** — All state changes are written sequentially to WAL files
- **Crash recovery** — On restart, replay WAL to restore workflow state
- **No external dependencies** — Pure file-based persistence, no database required
- **Fast writes** — Sequential I/O is much faster than random disk writes

**WAL Entry Format:**
```rust
pub struct WalEntry {
    pub sequence: u64,           // Monotonic sequence number
    pub workflow_id: String,     // Workflow identifier
    pub run_id: String,          // Execution run identifier
    pub event_type: WalEventType, // Type of event
    pub payload: Vec<u8>,        // Serialized event data
    pub timestamp: u64,          // Unix timestamp in microseconds
}

pub enum WalEventType {
    WorkflowStarted,
    WorkflowCompleted,
    WorkflowFailed,
    ActivityScheduled,
    ActivityCompleted,
    ActivityFailed,
    SignalReceived,
    QueryExecuted,
}
```

**Write Path:**
1. Client sends request (e.g., StartWorkflow)
2. Server creates WalEntry with event data
3. Entry appended to WAL file (fsync for durability)
4. Workflow engine processes the event
5. Completion entry written to WAL
6. Response sent to client

**Recovery Path:**
1. Server starts, detects WAL files
2. Reads all entries in sequence order
3. Rebuilds in-memory workflow state
4. Resumes any in-progress workflows
5. Ready to accept new requests

**Performance Characteristics:**
- Write latency: ~1-5ms (with fsync)
- Throughput: ~43.6 ops/s (simple workflow)
- Memory: ~98 MiB (includes WAL buffers)
- Recovery time: ~1-2s for 10k workflows

**Configuration:**
```rust
pub struct WalConfig {
    pub path: PathBuf,              // WAL directory (default: /data/velocity.wal)
    pub max_file_size: u64,         // Max size per WAL file (default: 64MB)
    pub sync_interval: Duration,    // fsync interval (default: 100ms)
    pub compression: bool,          // Enable zlib compression (default: false)
}

pub struct DurabilityConfig {
    pub sync_steps: u32,            // 0 = every step (strict), N = batch every N steps
    pub flush_interval_ms: u64,     // Time-based fsync floor (prevents unbounded data loss)
    pub direct_execution: bool,     // Skip task queue enqueue (caller drives loop)
}
```

**Configurable Durability (DurabilityConfig):**
- `DurabilityConfig::strict()` — fsync after every step (sync_steps=0). Maximum safety, lose nothing on crash.
- `DurabilityConfig::batched(N, ms)` — fsync every N steps or every ms, whichever first. Balanced.
- `DurabilityConfig::async_only(ms)` — background fsync thread only (sync_steps=u32::MAX). Maximum throughput.
- `.with_direct_execution()` — builder method to enable direct execution mode (skips task queue enqueue).
- Default is `strict()` with `direct_execution = false`. Bench server uses `--sync-steps`, `--flush-interval-ms`, and `--direct-execution` CLI flags.
- `complete_step_durable()` method replaces `complete_step_sync()` when configurable durability is active.

**Direct Execution Mode:**
When `direct_execution = true`, step completion skips the task queue enqueue. This eliminates 2 Mutex locks + condvar signal per step for callers that drive the step loop themselves (tight `for` loop calling `complete_step_durable` sequentially). Use for embedded/engine-local workloads where the caller owns the loop. Do NOT use for distributed worker-pool patterns where external workers poll the task queue.

**Key files:**
- `velocity-workflow-server/src/main.rs` — WAL integration
- `velocity-workflow-engine/src/lib.rs` — WAL writer and reader
- `velocity-workflow-engine/src/engine.rs` — DurabilityConfig implementation (strict/batched/async modes)

**Rules for developers:**
1. Always write to WAL before processing the event
2. Use fsync for critical state changes (workflow start/complete)
3. Batch non-critical writes for better throughput
4. Implement WAL rotation to prevent unbounded file growth
5. Test crash recovery by killing the process mid-write
