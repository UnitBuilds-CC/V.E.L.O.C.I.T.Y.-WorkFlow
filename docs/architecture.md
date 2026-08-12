# VELOCITY-WorkFlow Architecture

> Complete system architecture reference for the hardware-native durable execution engine.

---

## Table of Contents

1. [System Architecture Overview](#system-architecture-overview)
2. [Component Diagram](#component-diagram)
3. [Data Flow](#data-flow)
4. [Slab Memory Model](#slab-memory-model)
5. [WAL and Durability](#wal-and-durability)
6. [Replication and Consistency](#replication-and-consistency)
7. [Task Queue Design](#task-queue-design)
8. [Timer Engine](#timer-engine)
9. [Security Model](#security-model)
10. [Performance Characteristics](#performance-characteristics)

---

## System Architecture Overview

VELOCITY-WorkFlow is a four-layer system. Each layer communicates through zero-copy FFI or memory-mapped shared memory, avoiding the serialization and GC overhead of traditional workflow engines.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  LAYER 1 — SDK & Developer Frontend                                        │
│  • 7 language SDKs (Python, TypeScript, Go, Java, Rust, PHP, Ruby)         │
│  • gRPC client or FFI (C-ABI P/Invoke for C#)                              │
│  • Developer writes standard async/await code with @Durable decorators      │
└────────────────────────────────┬────────────────────────────────────────────┘
                                 │ gRPC (HTTP/2) or FFI
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  LAYER 2 — Workflow Engine (velocity-workflow-engine)                       │
│  • WorkflowEngine: state machine driver, step advancement                   │
│  • TaskQueue: priority FIFO with per-queue blocking poll                    │
│  • TimerEngine: binary min-heap for delayed tasks and cron schedules        │
│  • VisibilityStore: SQL queryable workflow metadata index                   │
└────────────────────────────────┬────────────────────────────────────────────┘
                                 │ Direct memory pointers
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  LAYER 3 — Core Validation Engine (velocity-workflow-core)                  │
│  • #![no_std] Rust — zero heap allocation                                   │
│  • SlabHeader [128 bytes]: version, type_id, ns_id, step bitmask            │
│  • Bitmask256: O(1) step completion tracking (256 steps per slab)           │
│  • Merkle root: SHA-256 cryptographic state verification (963 ns)           │
│  • CRDT convergence: AWORSet, PNCounter for multi-region state              │
│  • Arena: lock-free bump allocator for dynamic overflow                     │
└────────────────────────────────┬────────────────────────────────────────────┘
                                 │ mmap / io_uring / shared memory
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  LAYER 4 — Transport & Persistence                                          │
│  • VCTP: zero-copy UDP ring buffer transport (kernel bypass)                │
│  • ChaCha20-Poly1305: native Rust encryption via FFI                        │
│  • WAL: vectorized micro-batch journal with io_uring fsync                  │
│  • Slab files: fixed-size .slab mmap persistence                            │
│  • PostgreSQL adapter: optional relational persistence backend              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Component Diagram

### Core Components

| Component | Location | Responsibility |
|-----------|----------|----------------|
| **WorkflowEngine** | `velocity-workflow-engine/` | Drives workflow state machines, advances steps |
| **SlabAllocator** | `velocity-workflow-core/src/slab.rs` | 128-byte slab header creation, Merkle hash |
| **Bitmask256** | `velocity-workflow-core/src/bitmask.rs` | O(1) step completion vector (256 bits) |
| **TaskQueue** | `velocity-workflow-engine/` | Priority FIFO dispatch with blocking poll |
| **TimerEngine** | `velocity-workflow-engine/` | Binary min-heap for delayed/cron tasks |
| **NdaEngine** | `velocity-workflow-core/src/nda.rs` | 48-byte binary document proof verification |
| **VctpTransport** | `velocity-workflow-core/src/vctp.rs` | 32-byte UDP packet header, AIMD pacing |
| **Arena** | `velocity-workflow-core/src/arena.rs` | Tier-2 lock-free bump allocation page |
| **CrdtStore** | `velocity-workflow-core/src/crdt.rs` | PNCounter, AWORSet for convergence |
| **VisibilityStore** | `velocity-workflow-engine/` | SQL-indexed workflow metadata |

### Component Interaction

```
Client SDK ──gRPC──► WorkflowEngine
                         │
              ┌──────────┼──────────┐
              ▼          ▼          ▼
         TaskQueue   TimerEngine  VisibilityStore
              │          │
              ▼          ▼
         SlabAllocator ──► Bitmask256
              │
              ▼
         Arena (overflow) ──► WAL (persistence)
              │
              ▼
         VctpTransport (replication)
```

---

## Data Flow

### Workflow Start

```
1. Client sends StartWorkflowExecution RPC
2. WorkflowEngine allocates a SlabHeader (128 bytes, zero-alloc)
3. Bitmask256 initialized to all-zeros (no steps completed)
4. Merkle root computed over initial slab state
5. Task dispatched to TaskQueue for the specified task queue
6. WAL entry written (vectorized micro-batch)
7. Visibility index updated
8. Response returned to client with workflow_key
```

### Task Dispatch & Execution

```
1. Worker calls PollTask(task_queue, timeout)
2. TaskQueue performs blocking dequeue (Mutex + Condvar)
3. Task payload delivered to worker (zero-copy where possible)
4. Worker executes workflow/activity logic
5. Worker calls CompleteStep or CompleteWorkflow
6. Bitmask256 updated: O(1) bit set for completed step
7. Merkle root recomputed (incremental SHA-256)
8. WAL entry appended
9. If all steps complete → workflow marked COMPLETED
10. Next task dispatched (or workflow finalized)
```

### Crash Recovery

```
1. Process crashes mid-execution
2. On restart, slab file is mmap'd back into memory
3. Bitmask256 reveals exactly which steps completed
4. WorkflowEngine resumes from the first incomplete step
5. No event replay — O(1) pointer cast to resume state
6. Recovery latency: < 0.001 ms regardless of history size
```

---

## Slab Memory Model

The slab is the fundamental unit of workflow state. It replaces the event-sourcing history log used by traditional engines.

### SlabHeader Layout (128 bytes, `repr(C)`)

```
Offset  Size  Field              Description
──────  ────  ───────────────    ───────────────────────────────────────
0x00    8     version            Schema version for forward compatibility
0x08    8     type_id            Workflow type identifier (hashed name)
0x10    8     ns_id              Namespace identifier
0x18    8     tq_hash            Task queue hash (FNV-1a)
0x20    8     workflow_key       Unique workflow instance key
0x28    4     total_steps        Total steps in this workflow
0x2C    4     current_step       Current execution step (0-based)
0x30    32    bitmask            Bitmask256 — step completion vector
0x50    32    merkle_root        SHA-256 Merkle root of slab state
0x70    8     status             Workflow status enum (Running/Completed/Failed)
0x78    8     flags              Reserved flags (cryptographic proof, etc.)
```

### Tier-1: Fixed-Size Slab

- Each slab is exactly 128 bytes of header + reserved slot padding
- Stored as `repr(C)` for zero-copy FFI across language boundaries
- Bitmask256 tracks up to 256 steps in 32 bytes (1 bit per step)
- Step completion is O(1): set bit at position `step_index`
- All steps complete check: `bitmask == (1 << total_steps) - 1`

### Tier-2: Bump Allocation Arena

- For dynamic data (strings, blobs) that exceed slab capacity
- Lock-free bump allocator — no mutex, no GC
- Pages allocated off-slab (unmanaged memory)
- Compaction via slab slot padding reuse

### Memory Properties

| Property | Value |
|----------|-------|
| Header size | 128 bytes (fixed) |
| Max steps per slab | 256 (Bitmask256) |
| Allocation latency | 0.0157 ns (pointer cast) |
| GC pressure | Zero (stack + bump arenas) |
| Access latency | 0.12 ns (direct pointer lookup) |

---

## WAL and Durability

### Write-Ahead Log Design

The WAL provides crash durability without the overhead of per-step event log inserts.

```
┌──────────────────────────────────────────────────┐
│  WAL Segment (4 MB default)                       │
│  ┌────────┐ ┌────────┐ ┌────────┐ ┌──────────┐  │
│  │ Entry  │ │ Entry  │ Entry  │ │ Entry    │  │
│  │ 64B    │ │ 128B   │ │ 96B    │ │ 48B      │  │
│  └────────┘ └────────┘ └────────┘ └──────────┘  │
│  ── Vectorized micro-batch ──► io_uring fsync     │
└──────────────────────────────────────────────────┘
```

### WAL Entry Format

```
Offset  Size  Field          Description
──────  ────  ────────────   ──────────────────────────
0x00    4     entry_size     Size of this entry in bytes
0x04    4     checksum       CRC-32 of entry payload
0x08    8     sequence       Monotonic sequence number
0x10    4     entry_type     Type enum (Step, Signal, Timer, etc.)
0x14    var   payload        Entry-specific data
```

### Durability Guarantees

- **fsync policy**: Configurable — every commit, every N entries, or timed interval
- **Crash recovery**: WAL replay from last known good checkpoint
- **Segment rotation**: Old segments archived or compacted automatically
- **Checksum verification**: CRC-32 on every entry to detect corruption

### Performance

| Metric | Value |
|--------|-------|
| Write latency (batched) | < 10 µs per entry |
| fsync latency (NVMe) | < 50 µs |
| Recovery time | Proportional to unflushed entries only |
| IOPS reduction vs Temporal | 99% (vectorized batching) |

---

## Replication and Consistency

### Single-Node Mode

In single-node mode, all state resides in local slab files and WAL segments. No replication overhead.

### Multi-Node Replication

```
┌────────────┐     VCTP      ┌────────────┐
│  Leader    │──────────────►│  Follower  │
│  (Primary) │   UDP Ring    │  (Replica) │
│            │◄──────────────│            │
│  Slab +    │   ACK/NACK    │  Slab +    │
│  WAL       │               │  WAL       │
└────────────┘               └────────────┘
```

### Consistency Model

- **Strong consistency** for single-region deployments (linearizable)
- **Eventual consistency** for multi-region with CRDT convergence
- **CRDT types**: AWORSet (add-wins), PNCounter (increment/decrement)
- **Conflict resolution**: Last-writer-wins with vector clocks for causal ordering

### Replication Protocol

1. Leader writes slab mutation to local WAL
2. Mutation replicated to followers via VCTP UDP ring
3. Followers apply mutation to their local slab copy
4. ACK/NACK back to leader (NACK deduplication via sequence numbers)
5. Leader advances commit point after quorum ACK
6. Merkle roots compared periodically for divergence detection

### Cross-Region Consistency

| Scenario | Consistency | Latency |
|----------|-------------|---------|
| Same datacenter | Strong (linearizable) | < 1 ms |
| Cross-datacenter | Eventual (CRDT) | 10-100 ms |
| Multi-cloud | Eventual (CRDT + vector clock) | 50-500 ms |

---

## Task Queue Design

The task queue replaces Temporal's Matching Service with a zero-allocation priority FIFO.

```
TaskQueue "orders"
┌─────┬─────┬─────┬─────┬─────┐
│ P=3 │ P=2 │ P=2 │ P=1 │ P=0 │   ← Priority levels (higher = first)
│ [T] │ [T] │ [T] │ [T] │ [T] │   ← VecDeque per priority
└─────┴─────┴─────┴─────┴─────┘
         ▲                       ← Producers (workflow completions)
         │
         ▼                       ← Consumers (worker polls)
   Blocking dequeue (Mutex + Condvar)
```

### Properties

- **Priority levels**: 0 (low) to 3 (critical)
- **Fair dispatch**: Round-robin across workers on the same queue
- **Blocking poll**: Workers sleep on a Condvar — no busy-waiting
- **Back-pressure**: Configurable queue depth limits with rejection

---

## Timer Engine

The timer engine handles delayed tasks, cron schedules, and sleep operations.

```
TimerEngine (Binary Min-Heap)
┌──────────────────────────┐
│  fire_at=10:00  task=A   │  ← Root (earliest fire time)
│  fire_at=10:05  task=B   │
│  fire_at=10:15  task=C   │
│  fire_at=10:30  task=D   │
└──────────────────────────┘
  ▲ Background checker thread (100ms tick)
  │ Fires tasks where fire_at <= now
```

---

## Security Model

| Layer | Mechanism |
|-------|-----------|
| Transport | ChaCha20-Poly1305 (Rust native, 1.51x faster than TLS) |
| Authentication | JWT tokens (optional, anonymous by default) |
| Authorization | Namespace-level ACLs |
| State integrity | SHA-256 Merkle root per slab (tamper detection) |
| Audit trail | Cryptographic proof chain across slab mutations |

---

## Performance Characteristics

### Latency

| Operation | Latency |
|-----------|---------|
| Slab creation + Merkle hash | 935 ns |
| Bitmask step mark | 998 ns |
| Merkle root verification | 963 ns |
| NDA document proof | 473 ns |
| VCTP packet header | 13.86 ns |
| Bump arena allocation | 11.64 ns |
| O(1) pointer resumption | 0.0157 ns |

### Throughput

| Metric | Value |
|--------|-------|
| In-memory read | 369 Gbps (7,800+ MB/s) |
| Task dispatch | Sub-millisecond |
| Crash recovery | < 0.001 ms |
| Storage per 10k steps | Megabytes (vs gigabytes for Temporal) |

### Scalability

- **Horizontal**: Add workers per task queue — linear throughput scaling
- **Vertical**: Single node handles 100k+ concurrent workflows
- **Memory**: Constant per-workflow overhead (128 bytes + overflow arena)
