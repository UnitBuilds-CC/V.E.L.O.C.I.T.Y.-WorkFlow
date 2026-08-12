# VELOCITY-WorkFlow Architecture Deep Dive

> Internal architecture of the hardware-native workflow execution engine.

---

## Table of Contents

1. [System Architecture](#system-architecture)
2. [Slab Engine Internals](#slab-engine-internals)
3. [Workflow Lifecycle](#workflow-lifecycle)
4. [Task Queue Design](#task-queue-design)
5. [Persistence Model](#persistence-model)
6. [Replication Architecture](#replication-architecture)
7. [Security Model](#security-model)
8. [Performance Characteristics](#performance-characteristics)

---

## System Architecture

```
                            ┌─────────────────────────────────────────────────┐
                            │              VELOCITY-WorkFlow                   │
                            │                                                 │
  ┌──────────┐   gRPC       │  ┌───────────────────────────────────────────┐  │
  │ Go SDK   │──────────────┼─►│         gRPC Server (tonic)               │  │
  └──────────┘              │  │   WorkflowService — 21 RPCs               │  │
                            │  └─────────────────┬─────────────────────────┘  │
  ┌──────────┐   gRPC       │                    │                            │
  │ TS SDK   │──────────────┼─┐                  │                            │
  └──────────┘              │ │  ┌───────────────▼─────────────────────────┐  │
                            │ │  │        WorkflowEngine                    │  │
  ┌──────────┐   gRPC       │ │  │  ┌──────────────────────────────────┐   │  │
  │ Py SDK   │──────────────┼─┤  │  │  Slab Allocator (zero-alloc)     │   │  │
  └──────────┘              │ │  │  │  • SlabHeader [128 bytes]        │   │  │
                            │ │  │  │  • Bitmask256 (step tracking)    │   │  │
  ┌──────────┐   FFI        │ │  │  │  • Merkle root (SHA-256)         │   │  │
  │ C# SDK   │──────────────┼─┤  │  └──────────────────────────────────┘   │  │
  └──────────┘  (P/Invoke)  │ │  │  ┌──────────────────────────────────┐   │  │
                            │ │  │  │  TaskQueue (priority FIFO)       │   │  │
                            │ │  │  │  • VecDeque + Mutex + Condvar    │   │  │
                            │ │  │  │  • Per-queue blocking poll       │   │  │
                            │ │  │  └──────────────────────────────────┘   │  │
                            │ │  │  ┌──────────────────────────────────┐   │  │
                            │ │  │  │  TimerEngine (BinaryHeap)        │   │  │
                            │ │  │  │  • Min-heap by fire_at           │   │  │
                            │ │  │  │  • Background checker thread     │   │  │
                            │ │  │  └──────────────────────────────────┘   │  │
                            │ │  │  ┌──────────────────────────────────┐   │  │
                            │ │  │  │  WalManager (WAL)                │   │  │
                            │ │  │  │  • Append-only log               │   │  │
                            │ │  │  │  • CRC32 per record              │   │  │
                            │ │  │  │  • Recovery by replay            │   │  │
                            │ │  │  └──────────────────────────────────┘   │  │
                            │ │  └────────────────────────────────────────┘  │
                            │ │                                              │
                            │ │  ┌────────────────────────────────────────┐  │
                            │ └─►│  velocity-workflow-core                 │  │
                            │    │  • Slab allocator                       │  │
                            │    │  • Bitmask256                           │  │
                            │    │  • Merkle SHA-256 verification          │  │
                            │    │  • Arena allocator                      │  │
                            │    │  • CRDT support                         │  │
                            │    └────────────────────────────────────────┘  │
                            │                                                │
                            │  ┌────────────────────────────────────────┐    │
                            │  │  Subsystems                             │    │
                            │  │  ┌──────────┐ ┌──────────┐ ┌────────┐ │    │
                            │  │  │ Namespace│ │ Visibility│ │ Search │ │    │
                            │  │  │ Registry │ │ Index    │ │ Index  │ │    │
                            │  │  └──────────┘ └──────────┘ └────────┘ │    │
                            │  │  ┌──────────┐ ┌──────────┐ ┌────────┐ │    │
                            │  │  │ Auth/RBAC│ │ Rate     │ │ Metrics│ │    │
                            │  │  │ Manager  │ │ Limiter  │ │ Export │ │    │
                            │  │  └──────────┘ └──────────┘ └────────┘ │    │
                            │  │  ┌──────────┐ ┌──────────┐ ┌────────┐ │    │
                            │  │  │ Saga     │ │ Batch    │ │ Cron   │ │    │
                            │  │  │ Orchestr.│ │ Executor │ │ Sched. │ │    │
                            │  │  └──────────┘ └──────────┘ └────────┘ │    │
                            │  │  ┌──────────┐ ┌──────────┐ ┌────────┐ │    │
                            │  │  │ Shard    │ │ Partition│ │ Replay │ │    │
                            │  │  │ Manager  │ │ Manager  │ │ Engine │ │    │
                            │  │  └──────────┘ └──────────┘ └────────┘ │    │
                            │  └────────────────────────────────────────┘    │
                            └─────────────────────────────────────────────────┘
                                             │
                                             ▼
                            ┌─────────────────────────────────────────────────┐
                            │  PostgreSQL (via PostgresAdapter)               │
                            │  • Workflow records                             │
                            │  • Event history                                │
                            │  • Search attributes                            │
                            │  • Visibility index                             │
                            └─────────────────────────────────────────────────┘
```

### Component Summary

| Component | Module | Responsibility |
|-----------|--------|---------------|
| **WorkflowEngine** | `engine.rs` | Core state machine, workflow lifecycle |
| **SlabHeader** | `velocity-workflow-core/slab.rs` | Zero-alloc workflow state (128 bytes) |
| **Bitmask256** | `velocity-workflow-core/bitmask.rs` | O(1) step completion tracking |
| **TaskQueue** | `task_queue.rs` | Priority FIFO task dispatch |
| **TimerEngine** | `timer_engine.rs` | Durable timer management |
| **WalManager** | `wal.rs` | Write-ahead log for crash recovery |
| **NamespaceRegistry** | `namespace.rs` | Multi-tenant namespace management |
| **VisibilityIndex** | `visibility.rs` | Workflow search and listing |
| **ShardManager** | `sharding.rs` | Consistent hashing for distribution |
| **AuthManager** | `auth.rs` / `auth_v2.rs` | RBAC, API keys, OAuth2 |
| **RateLimiter** | `rate_limiter.rs` | Token bucket rate limiting |
| **MetricsRegistry** | `metrics.rs` | Prometheus metrics export |
| **SagaOrchestrator** | `saga.rs` | Saga pattern with compensation |
| **MultiRegionReplicator** | `multi_region.rs` | Cross-region replication |
| **ObservabilityContext** | `observability.rs` | Logging, metrics, tracing |

---

## Slab Engine Internals

The slab engine is the heart of VELOCITY-WorkFlow. Every workflow execution is represented by a fixed-size `SlabHeader` — a `#[repr(C)]` struct that lives in Rust-owned memory with zero managed heap allocations.

### SlabHeader Layout (128 bytes)

```
┌──────────────────────────────────────────────────────────────┐
│  Offset  │ Size  │ Field              │ Description          │
├──────────┼───────┼────────────────────┼──────────────────────┤
│  0       │ 4 B   │ magic              │ 0x564C4354 ("VLCT")  │
│  4       │ 4 B   │ schema_version     │ Schema version ID    │
│  8       │ 8 B   │ workflow_id        │ Unique workflow ID   │
│  16      │ 8 B   │ run_id             │ Unique run ID        │
│  24      │ 4 B   │ current_step       │ Current step index   │
│  28      │ 4 B   │ total_steps        │ Total planned steps  │
│  32      │ 32 B  │ merkle_root        │ SHA-256 state proof  │
│  64      │ 32 B  │ step_bitmask       │ Bitmask256 (steps)   │
│  96      │ 32 B  │ reserved_padding   │ Future migrations    │
└──────────────────────────────────────────────────────────────┘
```

### Zero-Allocation Design

- **No heap allocations** — the slab header is a value type stored in a contiguous array
- **No GC pressure** — Rust ownership model ensures deterministic deallocation
- **Cache-friendly** — 128-byte headers fit exactly in two CPU cache lines
- **Memory-mappable** — `#[repr(C)]` enables direct mmap to disk

### Bitmask256 — O(1) Step Tracking

The `Bitmask256` tracks up to 256 step completions using four `u64` words:

```rust
pub struct Bitmask256 {
    pub bits: [u64; 4],  // 256 bits total
}
```

**Operations (all O(1)):**

| Operation | Implementation |
|-----------|---------------|
| `set_step(i)` | `bits[i/64] \|= 1 << (i%64)` |
| `is_step_set(i)` | `(bits[i/64] & (1 << (i%64))) != 0` |
| `clear_step(i)` | `bits[i/64] &= !(1 << (i%64))` |
| `count_completed()` | Sum of `count_ones()` across all 4 words |

### Merkle Root — Cryptographic State Verification

Every state mutation recalculates a SHA-256 Merkle root over the slab header fields:

```rust
fn recalculate_merkle_root(&mut self) {
    let mut hasher = Sha256::new();
    hasher.update(&self.magic.to_le_bytes());
    hasher.update(&self.schema_version.to_le_bytes());
    hasher.update(&self.workflow_id.to_le_bytes());
    hasher.update(&self.run_id.to_le_bytes());
    hasher.update(&self.current_step.to_le_bytes());
    hasher.update(&self.total_steps.to_le_bytes());
    for word in &self.step_bitmask.bits {
        hasher.update(&word.to_le_bytes());
    }
    self.merkle_root.copy_from_slice(&hasher.finalize());
}
```

This provides:
- **Tamper detection** — any corruption of the slab header invalidates the Merkle root
- **Recovery verification** — after WAL replay, the Merkle root is verified
- **Replication integrity** — remote replicas can verify state consistency

---

## Workflow Lifecycle

### State Machine

```
                    ┌─────────┐
         start      │         │  complete_step (all steps)
  ─────────────────►│ Running │◄───────────────────────────────
                    │         │
                    └────┬────┘
                         │
           ┌─────────────┼─────────────┐
           │             │             │
           ▼             ▼             ▼
    ┌────────────┐ ┌──────────┐ ┌───────────┐
    │ Completed  │ │  Failed  │ │ Canceled  │
    └────────────┘ └──────────┘ └───────────┘
           │             │             │
           ▼             ▼             ▼
    ┌────────────┐ ┌──────────┐ ┌───────────┐
    │ Terminated │ │ TimedOut │ │ Continued │
    │            │ │          │ │  AsNew    │
    └────────────┘ └──────────┘ └───────────┘
```

### Lifecycle Phases

#### 1. Start

```rust
let key = engine.start_workflow(
    namespace_id,    // u64
    workflow_type_id,// u64
    task_queue_hash, // u64
    workflow_id,     // u64
    total_steps,     // u32
    input,           // Option<Vec<u8>>
);
```

- Allocates a slab entry in the engine's internal slab
- Creates a `SlabHeader` with magic `0x564C4354` and initial Merkle root
- Appends a `WorkflowStarted` record to the WAL
- Enqueues the first workflow task in the `TaskQueue`
- Indexes the workflow in `VisibilityIndex`

#### 2. Dispatch

The engine dispatches tasks to workers via the `TaskQueue`:

1. Worker calls `PollWorkflowTaskQueue` (gRPC) or `poll()` (FFI)
2. `TaskQueue` performs a blocking `Condvar::wait` until a task is available
3. Task is dequeued and returned with `workflow_key`, `step_index`, and `history`
4. Priority insertion ensures high-priority tasks are dequeued first

#### 3. Step Execution

Each step is completed by the worker:

```rust
engine.complete_step(workflow_key, step_index, result);
```

- `SlabHeader.mark_step_completed(step_index)` sets the bitmask
- Merkle root is recalculated
- `StepCompleted` WAL record is appended
- If more steps remain, the next task is enqueued
- If all steps are complete, the workflow transitions to `Completed`

#### 4. Signal

Signals are delivered to running workflows:

```rust
engine.signal_workflow(workflow_key, signal_name_id, signal_data);
```

- Signal is stored in the engine's signal map
- A `SignalReceived` WAL record is appended
- The workflow's next task dispatch includes the signal data

#### 5. Complete

```rust
engine.complete_workflow(workflow_key, Some(result));
```

- Status transitions to `Completed`
- `WorkflowCompleted` WAL record is appended
- Visibility index is updated with close time
- Parent workflow (if any) is notified

---

## Task Queue Design

### Architecture

The `TaskQueue` is a zero-allocation, multi-queue system:

```
┌─────────────────────────────────────────────────────┐
│  TaskQueue                                          │
│                                                     │
│  ┌──────────────────────────────────────────────┐   │
│  │  HashMap<u64, QueueState>                    │   │
│  │                                              │   │
│  │  tq_hash=1001 ──► QueueState {               │   │
│  │      deque: [Task, Task, Task, ...]           │   │
│  │      shutdown: false                          │   │
│  │  }                                           │   │
│  │                                              │   │
│  │  tq_hash=1002 ──► QueueState {               │   │
│  │      deque: [Task, Task, ...]                 │   │
│  │      shutdown: false                          │   │
│  │  }                                           │   │
│  └──────────────────────────────────────────────┘   │
│                                                     │
│  Condvar ──► notifies waiting pollers               │
│  next_task_id ──► AtomicU64 counter                 │
└─────────────────────────────────────────────────────┘
```

### Priority Scheduling

Tasks with higher priority (lower `priority` number) are inserted at the front of the deque:

```rust
if task.priority > 0 {
    let pos = state.deque.iter()
        .position(|t| t.priority > task.priority)
        .unwrap_or(state.deque.len());
    state.deque.insert(pos, task);
} else {
    state.deque.push_back(task);
}
```

### Sticky Affinity

The `StickyScheduler` (in `advanced_scheduler.rs`) provides worker affinity — tasks for a workflow are preferentially dispatched to the same worker that processed the previous task. This enables:

- **Cache warmth** — the worker's local cache contains relevant workflow state
- **Replay optimization** — the worker may already have the workflow's history in memory
- **Reduced serialization** — less data needs to be transferred over the wire

### Deadline Enforcement

Tasks carry a `deadline_ms` field. The `remove_expired()` method purges tasks past their deadline:

```rust
pub fn remove_expired(&self, now_ms: u64) -> usize {
    state.deque.retain(|t| t.deadline_ms == 0 || t.deadline_ms > now_ms);
}
```

---

## Persistence Model

### Write-Ahead Log (WAL)

The WAL is the primary durability mechanism. Every state-changing event is appended before being applied:

```
Record format:
┌────────────┬──────────────┬───────────┬──────────┬────────┐
│ event_type │ workflow_key │ data_len  │ data     │ crc32  │
│ 1 byte     │ 8 bytes      │ 4 bytes   │ N bytes  │ 4 bytes│
└────────────┴──────────────┴───────────┴──────────┴────────┘
```

**WAL Event Types:**

| Code | Event | Description |
|------|-------|-------------|
| 1 | `WorkflowStarted` | New workflow created |
| 2 | `StepCompleted` | A step was completed |
| 3 | `WorkflowCompleted` | Workflow finished successfully |
| 4 | `WorkflowFailed` | Workflow failed |
| 5 | `WorkflowCanceled` | Workflow was canceled |
| 6 | `WorkflowTerminated` | Workflow was terminated |
| 7 | `SignalReceived` | Signal delivered |
| 8 | `TimerScheduled` | Timer registered |
| 9 | `ActivityScheduled` | Activity dispatched |
| 10 | `ChildWorkflowStarted` | Child workflow created |

### Recovery Process

1. Open the WAL file
2. Read records sequentially
3. Verify CRC32 for each record
4. Apply events to reconstruct in-memory slab state
5. Verify Merkle roots after replay
6. Truncate the WAL after successful recovery

### PostgreSQL Adapter

For durable persistence beyond the WAL, the `PostgresAdapter` stores:

- **Workflow records** — `workflow_executions` table
- **Event history** — `workflow_events` table
- **Search attributes** — `search_attributes` table (JSONB)
- **Visibility index** — indexed views for fast listing

---

## Replication Architecture

### TCP Replication

The `TcpReplicationServer` provides reliable, ordered replication between engine clusters:

```
┌──────────┐     TCP      ┌──────────┐
│ Cluster A│◄────────────►│ Cluster B│
│ (Active) │  WireFrame   │ (Standby)│
└──────────┘              └──────────┘
```

**Wire Protocol:**

```
┌──────────┬───────────┬─────────────┬──────────────┐
│ MAGIC    │ FRAME_TYPE│ PAYLOAD_LEN │ PAYLOAD      │
│ 4B "VELO"│ 1 byte    │ 4 bytes     │ N bytes      │
└──────────┴───────────┴─────────────┴──────────────┘
```

**Frame Types:**

| Type | Code | Purpose |
|------|------|---------|
| `Handshake` | 1 | Cluster ID + failover version exchange |
| `TaskBatch` | 2 | Batch of replication tasks |
| `Ack` | 3 | Acknowledgement of received tasks |
| `Ping` | 4 | Heartbeat ping |
| `Pong` | 5 | Heartbeat response |
| `Shutdown` | 6 | Graceful connection close |

### UDP Replication

The `UdpReplicationTransport` provides low-latency, fire-and-forget replication for scenarios where occasional loss is acceptable:

- Uses `UdpSocket` for datagram-based transport
- No acknowledgement — relies on WAL for durability
- Suitable for cross-datacenter replication with high latency

### Multi-Region

The `MultiRegionReplicator` manages active/standby topology:

```
┌─────────────────┐         ┌─────────────────┐
│  us-east-1      │  repl   │  eu-west-1      │
│  (Active)       │────────►│  (Standby)      │
│  priority: 1    │         │  priority: 2    │
└─────────────────┘         └─────────────────┘
         │                           │
         └───────────────────────────┘
              FailoverController
```

**Region States:** `Active`, `Standby`, `Draining`, `Failed`

**Conflict Resolution:** The `ConflictResolutionStrategy` handles write conflicts during split-brain scenarios using last-writer-wins with vector clocks.

### Sharding

The `ShardManager` uses consistent hashing with 150 virtual nodes per host:

```
Hash Ring (BTreeMap<u64, String>):
  hash=0x00A1 ──► "node-1"
  hash=0x00B3 ──► "node-2"
  hash=0x01C7 ──► "node-1"
  hash=0x02D9 ──► "node-3"
  ...
```

Workflow keys are mapped to shards via `shard_for_key(workflow_key)`, which hashes the key and finds the nearest virtual node clockwise on the ring.

---

## Security Model

### Authentication

Two authentication mechanisms are supported:

1. **JWT/RBAC** (`AuthManager` + `auth_v2.rs`):
   - Roles: `admin`, `operator`, `reader`
   - Permissions: `StartWorkflow`, `SignalWorkflow`, `QueryWorkflow`, `TerminateWorkflow`, `CancelWorkflow`, `DescribeWorkflow`, `ListWorkflows`, `RegisterNamespace`, `PollActivityTask`, `RespondActivityTask`, `AdminAccess`
   - Claims contain: `subject`, `namespace_id`, `roles`

2. **API Keys** (`ApiKeyManager`):
   - Per-namespace scoped keys
   - Configurable permissions per key
   - Audit logging for all key usage

### Encryption

- **At rest** (`EncryptionAtRest`): AES-256 encryption for stored data
- **In transit**: TLS for gRPC and HTTP connections
- **Algorithms**: Configurable via `EncryptionAlgorithm` enum

### Audit Logging

The `AuditLogger` records all security-relevant events:

```rust
pub struct AuditLog {
    pub timestamp: u64,
    pub subject: String,
    pub action: String,
    pub resource: String,
    pub result: AuditResult,
    pub details: String,
}
```

### Rate Limiting

Two-tier token bucket:
- **Global** — protects the entire cluster
- **Per-namespace** — prevents noisy-neighbor issues

---

## Performance Characteristics

### O(1) Workflow Resumption

Unlike event-sourced engines that replay the full history to resume a workflow, VELOCITY-WorkFlow uses the slab header for O(1) resumption:

1. Look up the `SlabHeader` by `workflow_key` — O(1) slab access
2. Read `current_step` and `step_bitmask` — immediate state
3. Verify `merkle_root` — O(1) hash comparison
4. Dispatch the next task — no replay needed

### Zero-GC Runtime

The entire engine runs in Rust with:
- No garbage collector
- No managed heap
- No GC pauses
- Deterministic memory deallocation via Rust's ownership model

### Memory Efficiency

| Metric | Value |
|--------|-------|
| Per-workflow memory | ~256 bytes (slab header + metadata) |
| Slab header size | 128 bytes (fixed) |
| Bitmask storage | 32 bytes (256 steps) |
| Merkle root | 32 bytes (SHA-256) |

### Throughput Characteristics

| Operation | Complexity | Typical Latency |
|-----------|-----------|-----------------|
| Start workflow | O(1) | <1 ms |
| Complete step | O(1) | <0.1 ms |
| Signal delivery | O(1) | <0.5 ms |
| Task dispatch | O(1) amortized | <2 ms |
| Query | O(1) | <1 ms |
| WAL append | O(1) | <0.1 ms |
| Merkle verification | O(1) | <0.01 ms |

### Scalability

- **Vertical:** The engine benefits from larger CPU caches (slab headers are cache-line aligned)
- **Horizontal:** Consistent hashing distributes workflow keys across nodes with minimal remapping
- **Multi-region:** Active/standby with automatic failover via `FailoverController`

### Benchmarks

Run the built-in benchmarks:

```bash
cargo bench --manifest-path velocity-workflow-engine/Cargo.toml
```

Key benchmark scenarios:
- Workflow start/complete throughput
- Signal delivery latency
- Task queue enqueue/dequeue rate
- WAL append throughput
- Merkle root calculation speed
- Bitmask operation throughput
