---
kind: persistence_system
name: PostgreSQL Persistence with Connection Pooling
category: persistence
scope:
    - 'velocity-embedded/**'
    - 'migrations/**'
source_files:
    - velocity-embedded/src/main.rs
    - migrations/001_initial_schema.sql
---

Velocity Embedded uses PostgreSQL for ACID-compliant durability with connection pooling via `deadpool-postgres`.

**Architecture:**
- **Connection pool** — `deadpool-postgres` manages a pool of PostgreSQL connections
- **Transaction support** — All workflow operations use database transactions
- **Schema migrations** — Automatic migration on startup via `sqlx-cli`
- **Query optimization** — Indexes on workflow_id, run_id, and status columns

**Database Schema:**
```sql
-- Core workflow table
CREATE TABLE workflows (
    workflow_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    namespace TEXT NOT NULL,
    status TEXT NOT NULL,
    input JSONB,
    output JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);

-- Activity executions
CREATE TABLE activities (
    activity_id TEXT PRIMARY KEY,
    workflow_id TEXT REFERENCES workflows(workflow_id),
    activity_type TEXT NOT NULL,
    status TEXT NOT NULL,
    input JSONB,
    output JSONB,
    scheduled_at TIMESTAMPTZ DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ
);

-- Signals
CREATE TABLE signals (
    signal_id TEXT PRIMARY KEY,
    workflow_id TEXT REFERENCES workflows(workflow_id),
    signal_name TEXT NOT NULL,
    payload JSONB,
    delivered_at TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for performance
CREATE INDEX idx_workflows_namespace ON workflows(namespace);
CREATE INDEX idx_workflows_status ON workflows(status);
CREATE INDEX idx_activities_workflow ON activities(workflow_id);
CREATE INDEX idx_signals_workflow ON signals(workflow_id);
```

**Connection Pool Configuration:**
```rust
pub struct PoolConfig {
    pub max_size: u32,              // Max connections (default: 16)
    pub timeout: Duration,          // Connection timeout (default: 30s)
    pub idle_timeout: Duration,     // Idle connection timeout (default: 5m)
    pub statement_cache_size: usize, // Prepared statement cache (default: 100)
}
```

**Write Path:**
1. Client sends request (e.g., StartWorkflow)
2. Server acquires connection from pool
3. BEGIN transaction
4. INSERT workflow record
5. COMMIT transaction
6. Release connection to pool
7. Return result to client

**Transaction Isolation:**
- **Read Committed** — Default isolation level
- **Serializable** — Used for critical state transitions
- **Row-level locking** — `SELECT ... FOR UPDATE` for workflow state changes

**Performance Characteristics:**
- Connection acquire: ~0.1-0.5ms
- Query latency: ~1-10ms (depending on complexity)
- Throughput: ~61.25 ops/s (simple workflow)
- Memory: ~1.25 MiB (server) + ~68 MiB (PostgreSQL)

**Migration System:**
```bash
# Create new migration
sqlx migrate add <name>

# Run migrations
sqlx migrate run --database-url postgres://velocity:velocity@localhost/velocity

# Revert last migration
sqlx migrate revert --database-url postgres://velocity:velocity@localhost/velocity
```

**Key files:**
- `velocity-embedded/src/main.rs` — PostgreSQL integration
- `migrations/001_initial_schema.sql` — Initial schema
- `migrations/002_add_indexes.sql` — Performance indexes

**Rules for developers:**
1. Always use transactions for multi-statement operations
2. Use prepared statements for repeated queries
3. Index columns used in WHERE and JOIN clauses
4. Monitor connection pool saturation
5. Test migrations on a copy of production data before deploying
6. Use `EXPLAIN ANALYZE` to optimize slow queries
