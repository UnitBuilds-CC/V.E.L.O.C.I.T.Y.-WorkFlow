---
kind: coordination
name: PostgreSQL Advisory Locking
category: distributed_systems
scope:
    - 'velocity-workflow-engine/src/pg_advisory_lock.rs'
source_files:
    - velocity-workflow-engine/src/pg_advisory_lock.rs
---

When multiple Velocity server instances share a single PostgreSQL database, advisory locks prevent conflicting operations. Uses PostgreSQL's `pg_try_advisory_lock()` (non-blocking) and `pg_advisory_lock()` (blocking) primitives.

**Lock Key Space (64-bit partitioning):**
```
0xVE00_xxxx_xxxx_xxxx — Leader election (one per role)
0xVE10_xxxx_xxxx_xxxx — Workflow processing (one per workflow_key)
0xVE20_0000_0000_0000 — Schema migrations (one global)
```

**Three Lock Categories:**

1. **Leader Election:**
   - Exactly one instance runs periodic tasks (cleanup, archival)
   - Key derived from role string via `leader_election_key(role)`
   - Instances compete with non-blocking `pg_try_advisory_lock()`

2. **Workflow Locking:**
   - Only one instance processes a given workflow at a time
   - Key derived from workflow key via `workflow_lock_key(workflow_key)`
   - Prevents duplicate processing across instances

3. **Migration Locking:**
   - Only one instance runs schema migrations at startup
   - Single global key: `MIGRATION_LOCK_KEY = 0xVE20_0000_0000_0000`
   - Prevents concurrent migration conflicts

**Contention Handling:**
```
1. Try non-blocking pg_try_advisory_lock()
2. If failed, sleep with exponential backoff + jitter
3. Retry up to max_retries times
4. Return LockError::ContentionTimeout if all retries exhausted
```

**Lock Lifecycle:**
- Locks are session-level (automatically released when connection closes)
- No explicit unlock needed for crash safety
- Process crash → connection drops → lock auto-released

**Key Types:**
```rust
pub struct AdvisoryLockManager {
    // Manages lock acquisition, retry, and release
    // Tracks active locks for cleanup on shutdown
}

pub enum LockError {
    ContentionTimeout,  // Max retries exhausted
    ConnectionLost,     // PG connection dropped
    Internal(String),   // Other errors
}
```

**Key files:**
- `velocity-workflow-engine/src/pg_advisory_lock.rs` — Full implementation (918 lines)

**Rules for developers:**
1. Always use advisory locks for cross-instance coordination, not application-level locks
2. Use non-blocking `pg_try_advisory_lock()` with backoff, never blocking `pg_advisory_lock()` in async code
3. Lock keys must be deterministic (same input → same key)
4. Session-level locks are preferred — they auto-release on connection close
5. Test multi-instance scenarios with at least 2 competing instances
6. Monitor lock contention via metrics (retry count, wait time)
