---
kind: persistence_system
name: Per-Step Journal Persistence
category: persistence
scope:
    - 'velocity-embedded/**'
    - 'velocity-embedded-server/**'
    - 'velocity-workflow-engine/**'
source_files:
    - velocity-embedded/src/main.rs
    - velocity-workflow-engine/src/lib.rs
---

The per-step journal provides true durable execution by persisting each workflow step individually with batch INSERT operations. This ensures crash recovery can resume from the last persisted step, not just the last workflow-level checkpoint.

**Architecture:**
- **Append-only journal** — Each step execution is appended to an in-memory buffer
- **Batch INSERT** — Steps are flushed to PostgreSQL in batches for efficiency
- **Crash recovery** — On restart, replay journal to restore workflow state
- **Per-step durability** — Each step is individually durable, not just the workflow

**Write Path:**
```
1. Workflow step executes
2. Step result appended to journal buffer
3. Buffer flushed to PostgreSQL via batch INSERT (periodic or threshold-based)
4. Step marked as complete in workflow state
5. Continue to next step
```

**Recovery Path:**
```
1. Server starts, detects incomplete workflows
2. Read journal entries for each incomplete workflow
3. Replay steps in sequence order
4. Resume workflow from last persisted step
5. Continue execution
```

**Batch INSERT Optimization:**
- Instead of one INSERT per step (slow due to PG roundtrip)
- Buffer steps in memory and flush as a single batch INSERT
- Configurable batch size and flush interval
- Trade-off: small window of data loss between flushes (acceptable for most use cases)

**Comparison with WAL:**
| Feature | WAL | Per-Step Journal |
|---------|-----|------------------|
| Granularity | Workflow-level events | Individual step results |
| Storage | File-based | PostgreSQL table |
| Recovery | Replay all events | Resume from last step |
| Durability config | DurabilityConfig (strict/batched/async) | ACID (per-step transaction) |
| Batch support | Yes (group-commit) | Yes (batch INSERT) |
| Use case | Server flavor (no PG) | Embedded flavor (with PG) |

**Performance Characteristics:**
- Single step INSERT: ~1-2ms
- Batch INSERT (10 steps): ~3-5ms total (vs ~10-20ms individual)
- Journal replay: ~0.1ms per step
- Memory overhead: journal buffer (configurable size)

**Key files:**
- `velocity-embedded/src/main.rs` — Journal integration in embedded server
- `velocity-workflow-engine/src/lib.rs` — Journal writer and reader

**Rules for developers:**
1. Always persist step results before marking step complete
2. Use batch INSERT for multiple steps (never individual INSERTs in a loop)
3. Configure batch size based on latency tolerance (larger = faster but more data loss on crash)
4. Journal entries must be idempotent (replay-safe)
5. Test crash recovery by killing the process mid-batch
6. Monitor journal buffer size to prevent memory pressure
