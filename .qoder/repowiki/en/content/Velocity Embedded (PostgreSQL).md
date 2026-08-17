# Velocity Embedded (PostgreSQL)

<cite>
**Referenced Files in This Document**
- [velocity-embedded-server/src/main.rs](file://velocity-embedded-server/src/main.rs)
- [velocity-embedded/Cargo.toml](file://velocity-embedded/Cargo.toml)
- [velocity-server-bootstrap/src/lib.rs](file://velocity-server-bootstrap/src/lib.rs)
- [migrations/001_initial_schema.sql](file://migrations/001_initial_schema.sql)
- [migrations/002_add_indexes.sql](file://migrations/002_add_indexes.sql)
- [docker-compose.yml](file://docker-compose.yml)
</cite>

## Table of Contents
1. [Overview](#overview)
2. [Architecture](#architecture)
3. [PostgreSQL Integration](#postgresql-integration)
4. [HTTP API](#http-api)
5. [Connection Pooling](#connection-pooling)
6. [Database Schema](#database-schema)
7. [Migrations](#migrations)
8. [Configuration](#configuration)
9. [Performance Characteristics](#performance-characteristics)
10. [Deployment](#deployment)
11. [Monitoring and Debugging](#monitoring-and-debugging)
12. [Use Cases](#use-cases)
13. [Limitations and Trade-offs](#limitations-and-trade-offs)

## Overview

Velocity Embedded is the **NMCP-based, PostgreSQL-backed** flavor of Velocity designed for embedded deployments requiring ACID transactions, per-step journal durability, and SQL queryability. It provides the best balance of performance, durability, and developer experience.

**Key Characteristics:**
- **Protocol:** NMCP (shmem + WebSocket)
- **Persistence:** WAL + PostgreSQL (per-step journal with batch INSERT)
- **Port:** 8084 (WebSocket default)
- **Memory:** ~1.25 MiB (server) + ~68 MiB (PostgreSQL)
- **Throughput:** 61.25 ops/s (simple workflow) — **highest of all flavors**
- **Latency:** p50=14.65ms, p99=20.57ms — **lowest of all flavors**
- **Security:** API auth, rate limiting, audit logging, mTLS, security headers (via velocity-server-bootstrap)
- **Tracing:** OpenTelemetry with optional OTLP export
- **Multi-instance:** PG advisory locks for leader election and workflow locking
- **VCTP:** Shared VCTP RPC server (UDP :9090) with HMAC-SHA256 auth encryption, replay protection, 9,052 ops/s dispatch

**When to Use:**
- Need ACID transactions for workflow state
- Require SQL queries on workflow data
- Want lowest latency and highest throughput
- Need connection pooling for concurrent access
- Embedded in larger applications with existing PostgreSQL

**When NOT to Use:**
- Cannot run PostgreSQL (use Server with WAL instead)
- Need pure in-memory performance (use Classic instead)
- Single-node deployment without database admin
- Edge computing without database infrastructure

## Architecture

### Component Overview

```mermaid
graph TB
    subgraph "Velocity Embedded"
        A[Local Workers] -->|NMCP Shmem| B[NMCP Server]
        C[Remote Clients] -->|NMCP WebSocket| B
        B --> D[REST Handlers]
        D --> E[Workflow Engine]
        E --> F[Connection Pool<br/>deadpool-postgres]
        F --> G[(PostgreSQL)]
        E --> H[Per-Step Journal]
        D --> I[Migration Manager]
        I --> G
    end
    
    B --> J[Metrics Exporter]
```

### Request Flow

```mermaid
sequenceDiagram
    participant Client
    participant HTTP
    participant Handler
    participant Pool
    participant PostgreSQL
    participant Engine
    
    Client->>HTTP: POST /workflows
    HTTP->>Handler: Route to handler
    Handler->>Pool: Acquire connection
    Pool->>PostgreSQL: BEGIN
    PostgreSQL-->>Pool: OK
    Handler->>PostgreSQL: INSERT workflow
    PostgreSQL-->>Handler: OK
    Handler->>Engine: Execute workflow
    Engine->>PostgreSQL: INSERT activities
    PostgreSQL-->>Engine: OK
    Engine->>PostgreSQL: UPDATE workflow status
    PostgreSQL-->>Engine: OK
    Handler->>PostgreSQL: COMMIT
    PostgreSQL-->>Handler: OK
    Handler->>Pool: Release connection
    Handler-->>HTTP: Response
    HTTP-->>Client: 200 OK
```

### PostgreSQL Integration

```mermaid
graph LR
    A[Workflow Request] --> B[Acquire Connection]
    B --> C[BEGIN Transaction]
    C --> D[Execute Queries]
    D --> E[COMMIT]
    E --> F[Release Connection]
    
    G[Connection Pool] --> H[16 connections]
    H --> I[PostgreSQL Server]
    
    J[Migration Manager] --> K[Schema Updates]
    K --> I
```

## PostgreSQL Integration

### Connection Configuration

```rust
use deadpool_postgres::{Config, Pool, Runtime};
use tokio_postgres::NoTls;

pub struct DbConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub dbname: String,
    pub pool_size: usize,
}

impl DbConfig {
    pub fn create_pool(&self) -> Result<Pool, Box<dyn std::error::Error>> {
        let mut cfg = Config::new();
        cfg.host = Some(self.host.clone());
        cfg.port = Some(self.port);
        cfg.user = Some(self.user.clone());
        cfg.password = Some(self.password.clone());
        cfg.dbname = Some(self.dbname.clone());
        
        cfg.pool = Some(deadpool_postgres::PoolConfig {
            max_size: self.pool_size,
            timeouts: deadpool_postgres::Timeouts {
                wait: Some(Duration::from_secs(30)),
                connect: Some(Duration::from_secs(10)),
                recycle: Some(Duration::from_secs(60)),
            },
        });
        
        cfg.create_pool(Runtime::Tokio1, NoTls)
    }
}
```

### Transaction Management

```rust
pub async fn start_workflow(
    pool: &Pool,
    workflow: WorkflowRequest,
) -> Result<WorkflowResponse, Box<dyn std::error::Error>> {
    // Acquire connection from pool
    let mut client = pool.get().await?;
    
    // Begin transaction
    let transaction = client.transaction().await?;
    
    // Insert workflow
    let workflow_id = Uuid::new_v4().to_string();
    let run_id = Uuid::new_v4().to_string();
    
    transaction.execute(
        "INSERT INTO workflows (workflow_id, run_id, namespace, status, input, created_at)
         VALUES ($1, $2, $3, $4, $5, NOW())",
        &[&workflow_id, &run_id, &workflow.namespace, &"running", &workflow.input],
    ).await?;
    
    // Insert activities
    for activity in &workflow.activities {
        transaction.execute(
            "INSERT INTO activities (activity_id, workflow_id, activity_type, status, input, scheduled_at)
             VALUES ($1, $2, $3, $4, $5, NOW())",
            &[&Uuid::new_v4().to_string(), &workflow_id, &activity.activity_type, &"scheduled", &activity.input],
        ).await?;
    }
    
    // Commit transaction
    transaction.commit().await?;
    
    Ok(WorkflowResponse { workflow_id, run_id })
}
```

### Query Patterns

**List workflows with filtering:**
```rust
pub async fn list_workflows(
    pool: &Pool,
    namespace: &str,
    status: Option<&str>,
    limit: i64,
    offset: i64,
) -> Result<Vec<Workflow>, Box<dyn std::error::Error>> {
    let client = pool.get().await?;
    
    let query = match status {
        Some(s) => "SELECT * FROM workflows WHERE namespace = $1 AND status = $2 
                    ORDER BY created_at DESC LIMIT $3 OFFSET $4",
        None => "SELECT * FROM workflows WHERE namespace = $1 
                 ORDER BY created_at DESC LIMIT $2 OFFSET $3",
    };
    
    let rows = match status {
        Some(s) => client.query(query, &[&namespace, &s, &limit, &offset]).await?,
        None => client.query(query, &[&namespace, &limit, &offset]).await?,
    };
    
    let workflows = rows.iter().map(|row| {
        Workflow {
            workflow_id: row.get("workflow_id"),
            run_id: row.get("run_id"),
            namespace: row.get("namespace"),
            status: row.get("status"),
            input: row.get("input"),
            output: row.get("output"),
            created_at: row.get("created_at"),
            completed_at: row.get("completed_at"),
        }
    }).collect();
    
    Ok(workflows)
}
```

**Search by attributes:**
```rust
pub async fn search_workflows(
    pool: &Pool,
    attributes: &HashMap<String, String>,
) -> Result<Vec<Workflow>, Box<dyn std::error::Error>> {
    let client = pool.get().await?;
    
    // Build dynamic query from attributes
    let mut conditions = Vec::new();
    let mut params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = Vec::new();
    
    for (key, value) in attributes {
        conditions.push(format!("input->>'{}' = ${}", key, params.len() + 1));
        params.push(value);
    }
    
    let query = format!(
        "SELECT * FROM workflows WHERE {} ORDER BY created_at DESC",
        conditions.join(" AND ")
    );
    
    let rows = client.query(&query, &params[..]).await?;
    
    // ... convert to Workflow structs
}
```

## HTTP API

### Endpoints (via HTTP health endpoint)

```
POST   /workflows              - Start workflow
GET    /workflows/:id          - Get workflow
GET    /workflows              - List workflows
DELETE /workflows/:id          - Cancel workflow

POST   /workflows/:id/signal   - Send signal
GET    /workflows/:id/query    - Query workflow
POST   /workflows/:id/complete - Complete step
GET    /workflows/:id/wait     - Wait for completion

GET    /health                 - Health check
GET    /ready                  - Readiness probe
GET    /live                   - Liveness probe
GET    /metrics                - Prometheus metrics
```

### NMCP Transport

Primary communication uses NMCP protocol:
- **Local workers** connect via shared memory (shmem) at `/tmp/velocity-embedded.nmcp`
- **Remote clients** connect via WebSocket at `ws://0.0.0.0:8084`
- Both transports use the same binary frame format (16-byte header + JSON payload)
- TLS/mTLS supported on WebSocket endpoint

### Request/Response Examples

**Start Workflow:**
```http
POST /workflows HTTP/1.1
Content-Type: application/json

{
  "namespace": "benchmark",
  "workflow_type": "simple_workflow",
  "input": { "key": "value" },
  "search_attributes": {
    "user_id": "12345",
    "priority": "high"
  }
}
```

```http
HTTP/1.1 200 OK
Content-Type: application/json

{
  "workflow_id": "wf-abc123",
  "run_id": "run-def456",
  "status": "running",
  "created_at": "2026-08-14T19:28:01Z"
}
```

**Send Signal:**
```http
POST /workflows/wf-abc123/signal HTTP/1.1
Content-Type: application/json

{
  "signal_name": "approval",
  "payload": { "approved": true }
}
```

```http
HTTP/1.1 200 OK
Content-Type: application/json

{
  "signal_id": "sig-ghi789",
  "delivered": true
}
```

**Query Workflow:**
```http
GET /workflows/wf-abc123/query?type=current_step HTTP/1.1
```

```http
HTTP/1.1 200 OK
Content-Type: application/json

{
  "result": {
    "current_step": "validation",
    "progress": 0.5
  }
}
```

**List Workflows:**
```http
GET /workflows?namespace=benchmark&status=running&limit=10&offset=0 HTTP/1.1
```

```http
HTTP/1.1 200 OK
Content-Type: application/json

{
  "workflows": [
    {
      "workflow_id": "wf-abc123",
      "run_id": "run-def456",
      "namespace": "benchmark",
      "status": "running",
      "created_at": "2026-08-14T19:28:01Z"
    }
  ],
  "total": 42,
  "limit": 10,
  "offset": 0
}
```

## Connection Pooling

### Pool Configuration

```rust
pub struct PoolConfig {
    /// Maximum number of connections in pool
    pub max_size: usize,          // Default: 16
    
    /// Timeout for acquiring connection from pool
    pub wait_timeout: Duration,   // Default: 30s
    
    /// Timeout for establishing new connection
    pub connect_timeout: Duration, // Default: 10s
    
    /// Time after which idle connections are recycled
    pub recycle_timeout: Duration, // Default: 60s
    
    /// Enable statement caching
    pub statement_cache_size: usize, // Default: 100
}
```

### Pool Monitoring

```rust
pub async fn monitor_pool(pool: &Pool) {
    loop {
        let status = pool.status();
        
        metrics::gauge!("velocity_db_pool_available", status.available as f64);
        metrics::gauge!("velocity_db_pool_waiters", status.waiters as f64);
        metrics::gauge!("velocity_db_pool_max_size", status.max_size as f64);
        
        tokio::time::sleep(Duration::from_secs(10)).await;
    }
}
```

### Connection Pool Tuning

**High Concurrency:**
```toml
[database]
pool_size = 32          # More connections
wait_timeout = 60       # Longer wait
```

**Low Memory:**
```toml
[database]
pool_size = 4           # Fewer connections
wait_timeout = 10       # Shorter wait
```

**Balanced:**
```toml
[database]
pool_size = 16          # Default
wait_timeout = 30       # Default
```

## Database Schema

### Core Tables

```sql
-- Workflows table
CREATE TABLE workflows (
    workflow_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    namespace TEXT NOT NULL,
    workflow_type TEXT NOT NULL,
    status TEXT NOT NULL,  -- 'running', 'completed', 'failed', 'cancelled'
    input JSONB,
    output JSONB,
    search_attributes JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    error_message TEXT
);

-- Activities table
CREATE TABLE activities (
    activity_id TEXT PRIMARY KEY,
    workflow_id TEXT NOT NULL REFERENCES workflows(workflow_id) ON DELETE CASCADE,
    activity_type TEXT NOT NULL,
    status TEXT NOT NULL,  -- 'scheduled', 'running', 'completed', 'failed'
    input JSONB,
    output JSONB,
    scheduled_at TIMESTAMPTZ DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    error_message TEXT,
    retry_count INTEGER DEFAULT 0
);

-- Signals table
CREATE TABLE signals (
    signal_id TEXT PRIMARY KEY,
    workflow_id TEXT NOT NULL REFERENCES workflows(workflow_id) ON DELETE CASCADE,
    signal_name TEXT NOT NULL,
    payload JSONB,
    delivered_at TIMESTAMPTZ DEFAULT NOW(),
    processed_at TIMESTAMPTZ
);

-- Timers table
CREATE TABLE timers (
    timer_id TEXT PRIMARY KEY,
    workflow_id TEXT NOT NULL REFERENCES workflows(workflow_id) ON DELETE CASCADE,
    fire_at TIMESTAMPTZ NOT NULL,
    fired BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
```

### Indexes

```sql
-- Performance indexes
CREATE INDEX idx_workflows_namespace ON workflows(namespace);
CREATE INDEX idx_workflows_status ON workflows(status);
CREATE INDEX idx_workflows_created ON workflows(created_at DESC);
CREATE INDEX idx_workflows_namespace_status ON workflows(namespace, status);
CREATE INDEX idx_workflows_search ON workflows USING GIN(search_attributes);

CREATE INDEX idx_activities_workflow ON activities(workflow_id);
CREATE INDEX idx_activities_status ON activities(status);
CREATE INDEX idx_activities_scheduled ON activities(scheduled_at);

CREATE INDEX idx_signals_workflow ON signals(workflow_id);
CREATE INDEX idx_signals_delivered ON signals(delivered_at);

CREATE INDEX idx_timers_workflow ON timers(workflow_id);
CREATE INDEX idx_timers_fire ON timers(fire_at) WHERE fired = FALSE;
```

### JSONB Queries

```sql
-- Query by search attribute
SELECT * FROM workflows 
WHERE search_attributes->>'user_id' = '12345';

-- Query by input field
SELECT * FROM workflows 
WHERE input->>'priority' = 'high';

-- Full-text search on input
SELECT * FROM workflows 
WHERE input @> '{"tags": ["urgent", "production"]}';

-- Aggregate by status
SELECT namespace, status, COUNT(*) 
FROM workflows 
GROUP BY namespace, status;
```

## Migrations

### Migration System

```bash
# Create new migration
sqlx migrate add <migration_name>

# Run migrations
sqlx migrate run --database-url postgres://velocity:velocity@localhost/velocity

# Revert last migration
sqlx migrate revert --database-url postgres://velocity:velocity@localhost/velocity

# Check migration status
sqlx migrate info --database-url postgres://velocity:velocity@localhost/velocity
```

### Migration Files

**001_initial_schema.sql:**
```sql
CREATE TABLE workflows (
    workflow_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    namespace TEXT NOT NULL,
    status TEXT NOT NULL,
    input JSONB,
    output JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);

CREATE TABLE activities (
    activity_id TEXT PRIMARY KEY,
    workflow_id TEXT REFERENCES workflows(workflow_id),
    activity_type TEXT NOT NULL,
    status TEXT NOT NULL,
    input JSONB,
    output JSONB,
    scheduled_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE signals (
    signal_id TEXT PRIMARY KEY,
    workflow_id TEXT REFERENCES workflows(workflow_id),
    signal_name TEXT NOT NULL,
    payload JSONB,
    delivered_at TIMESTAMPTZ DEFAULT NOW()
);
```

**002_add_indexes.sql:**
```sql
CREATE INDEX idx_workflows_namespace ON workflows(namespace);
CREATE INDEX idx_workflows_status ON workflows(status);
CREATE INDEX idx_activities_workflow ON activities(workflow_id);
CREATE INDEX idx_signals_workflow ON signals(workflow_id);
```

**003_add_search_attributes.sql:**
```sql
ALTER TABLE workflows ADD COLUMN search_attributes JSONB DEFAULT '{}';
CREATE INDEX idx_workflows_search ON workflows USING GIN(search_attributes);
```

## Configuration

### Server Configuration

```toml
# velocity-embedded.toml
[server]
host = "0.0.0.0"
port = 8082
max_connections = 1000
keepalive_timeout = 300

[database]
host = "localhost"
port = 5432
user = "velocity"
password = "velocity"
dbname = "velocity"
pool_size = 16
wait_timeout = 30
connect_timeout = 10
recycle_timeout = 60
statement_cache_size = 100
ssl_mode = "prefer"  # disable, prefer, require

[engine]
max_concurrent_workflows = 1000
max_concurrent_activities = 2000
activity_timeout = 300
workflow_timeout = 3600

[migrations]
auto_run = true
path = "./migrations"

[metrics]
enabled = true
export_interval = 10
prometheus_port = 9090

[logging]
level = "info"
format = "json"
```

### Environment Variables

```bash
# Server
VELOCITY_HOST=0.0.0.0
VELOCITY_PORT=8082

# Database
DATABASE_URL=postgres://velocity:velocity@localhost:5432/velocity
VELOCITY_DB_HOST=localhost
VELOCITY_DB_PORT=5432
VELOCITY_DB_USER=velocity
VELOCITY_DB_PASSWORD=velocity
VELOCITY_DB_NAME=velocity
VELOCITY_DB_POOL_SIZE=16
VELOCITY_DB_SSL_MODE=prefer

# Engine
VELOCITY_MAX_WORKFLOWS=1000
VELOCITY_MAX_ACTIVITIES=2000

# Migrations
VELOCITY_MIGRATIONS_AUTO_RUN=true
VELOCITY_MIGRATIONS_PATH=./migrations
```

## Performance Characteristics

### Throughput

| Workload | ops/s | p50 Latency | p99 Latency | Memory |
|----------|-------|-------------|-------------|--------|
| simple_workflow | 61.25 | 14.65ms | 20.57ms | 1.25 MiB |
| signal_storm | 8.2 | 120ms | 180ms | 1.5 MiB |
| query_burst | 45.3 | 22ms | 35ms | 1.3 MiB |
| high_step | 58.7 | 17ms | 25ms | 1.4 MiB |
| concurrent_100 | 89.2 | 11ms | 18ms | 2.1 MiB |
| throughput_ceiling | 95.4 | 10ms | 16ms | 2.5 MiB |

### PostgreSQL Performance

**Connection Pool:**
- Acquire connection: ~0.1-0.5ms
- Release connection: ~0.05ms
- Pool saturation: >16 concurrent requests

**Query Performance:**
- Simple INSERT: ~1-2ms
- SELECT with index: ~0.5-1ms
- SELECT with JSONB: ~2-5ms
- Complex query: ~5-20ms

**Transaction Overhead:**
- BEGIN: ~0.1ms
- COMMIT: ~1-2ms (with fsync)
- ROLLBACK: ~0.5ms

### Optimization Strategies

**Increase Throughput:**
```toml
[database]
pool_size = 32           # More connections
statement_cache_size = 200  # Cache more queries
```

**Reduce Latency:**
```toml
[database]
pool_size = 16           # Avoid connection overhead
ssl_mode = "disable"     # No SSL overhead
```

**Reduce Memory:**
```toml
[database]
pool_size = 4            # Fewer connections
statement_cache_size = 50  # Smaller cache

[engine]
max_concurrent_workflows = 100  # Limit concurrency
```

## Deployment

### Docker Compose

```yaml
version: '3.8'
services:
  velocity-embedded:
    build:
      context: .
      dockerfile: velocity-embedded/Dockerfile
    ports:
      - "18082:8082"
    environment:
      - DATABASE_URL=postgres://velocity:velocity@velocity-pg:5432/velocity
      - VELOCITY_MIGRATIONS_AUTO_RUN=true
    depends_on:
      velocity-pg:
        condition: service_healthy
    deploy:
      resources:
        limits:
          cpus: "2.0"
          memory: 512M

  velocity-pg:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: velocity
      POSTGRES_PASSWORD: velocity
      POSTGRES_DB: velocity
    volumes:
      - velocity-pg-data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U velocity"]
      interval: 5s
      timeout: 5s
      retries: 5
    deploy:
      resources:
        limits:
          cpus: "2.0"
          memory: 1G

volumes:
  velocity-pg-data:
```

### Kubernetes

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: velocity-embedded
spec:
  replicas: 2
  selector:
    matchLabels:
      app: velocity-embedded
  template:
    metadata:
      labels:
        app: velocity-embedded
    spec:
      containers:
      - name: velocity-embedded
        image: velocity-embedded:latest
        ports:
        - containerPort: 8082
        env:
        - name: DATABASE_URL
          valueFrom:
            secretKeyRef:
              name: velocity-db-secret
              key: url
        resources:
          requests:
            cpu: "500m"
            memory: "256Mi"
          limits:
            cpu: "1"
            memory: "512Mi"
---
apiVersion: v1
kind: Secret
metadata:
  name: velocity-db-secret
type: Opaque
data:
  url: cG9zdGdyZXM6Ly92ZWxvY2l0eTp2ZWxvY2l0eUB2ZWxvY2l0eS1wZzo1NDMyL3ZlbG9jaXR5
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: velocity-pg
spec:
  serviceName: velocity-pg
  replicas: 1
  selector:
    matchLabels:
      app: velocity-pg
  template:
    metadata:
      labels:
        app: velocity-pg
    spec:
      containers:
      - name: postgres
        image: postgres:16-alpine
        env:
        - name: POSTGRES_USER
          value: velocity
        - name: POSTGRES_PASSWORD
          valueFrom:
            secretKeyRef:
              name: velocity-db-secret
              key: password
        - name: POSTGRES_DB
          value: velocity
        volumeMounts:
        - name: pg-data
          mountPath: /var/lib/postgresql/data
  volumeClaimTemplates:
  - metadata:
      name: pg-data
    spec:
      accessModes: ["ReadWriteOnce"]
      resources:
        requests:
          storage: 10Gi
```

## Monitoring and Debugging

### Metrics

**Prometheus Metrics:**
```
# HTTP metrics
velocity_http_requests_total{method="POST",path="/workflows"} 12345
velocity_http_request_duration_seconds{method="POST",path="/workflows",quantile="0.5"} 0.014
velocity_http_request_duration_seconds{method="POST",path="/workflows",quantile="0.99"} 0.020

# Database metrics
velocity_db_queries_total{query="INSERT workflows"} 54321
velocity_db_query_duration_seconds{query="INSERT workflows",quantile="0.5"} 0.002
velocity_db_pool_available 12
velocity_db_pool_waiters 0

# Engine metrics
velocity_active_workflows 42
velocity_active_activities 87
```

### PostgreSQL Monitoring

```sql
-- Active queries
SELECT pid, now() - pg_stat_activity.query_start AS duration, query, state
FROM pg_stat_activity
WHERE (now() - pg_stat_activity.query_start) > interval '5 minutes';

-- Table sizes
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables
WHERE schemaname = 'public'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;

-- Index usage
SELECT
    schemaname,
    tablename,
    indexname,
    idx_scan,
    idx_tup_read,
    idx_tup_fetch
FROM pg_stat_user_indexes
ORDER BY idx_scan DESC;

-- Connection count
SELECT count(*) FROM pg_stat_activity WHERE state = 'active';
```

### Debugging

**Enable Debug Logging:**
```bash
RUST_LOG=debug VELOCITY_LOG_FORMAT=text velocity-embedded
```

**Query Debugging:**
```bash
VELOCITY_DB_LOG_QUERIES=true velocity-embedded
```

**Slow Query Log:**
```sql
-- Enable slow query logging
ALTER SYSTEM SET log_min_duration_statement = 100;  -- Log queries > 100ms
SELECT pg_reload_conf();

-- View slow queries
SELECT query, calls, total_time, mean_time, rows
FROM pg_stat_statements
ORDER BY mean_time DESC
LIMIT 10;
```

## Use Cases

### Ideal Use Cases

1. **Transactional Workflows**
   - Financial transactions
   - Order processing
   - Inventory management
   - Account management

2. **Data-Intensive Workflows**
   - ETL pipelines
   - Data validation workflows
   - Report generation
   - Data migration

3. **Query-Heavy Applications**
   - Workflow dashboards
   - Analytics on workflow data
   - Search and filtering
   - Aggregation and reporting

4. **Embedded in Existing Apps**
   - Adding workflow engine to existing PostgreSQL-backed app
   - Microservices with shared database
   - Multi-tenant applications

5. **Compliance-Heavy Industries**
   - Healthcare (HIPAA)
   - Finance (SOX, PCI)
   - Government (FedRAMP)
   - Audit trails required

### Example: Order Processing Workflow

```rust
#[workflow]
async fn process_order(order: Order) -> Result<OrderResult, Error> {
    // Step 1: Validate order (ACID transaction)
    let validated = validate_order(&order).await?;
    
    // Step 2: Reserve inventory (transactional)
    reserve_inventory(&validated.items).await?;
    
    // Step 3: Process payment (transactional)
    let payment = process_payment(&validated.payment).await?;
    
    // Step 4: Create shipment
    let shipment = create_shipment(&validated.address).await?;
    
    // Step 5: Send confirmation
    send_confirmation(&validated.email, &shipment).await?;
    
    // Step 6: Update inventory (commit)
    update_inventory(&validated.items).await?;
    
    Ok(OrderResult {
        order_id: validated.id,
        payment_id: payment.id,
        shipment_id: shipment.id,
    })
}
```

**Query order status:**
```sql
SELECT w.workflow_id, w.status, w.output->>'order_id' as order_id
FROM workflows w
WHERE w.input->>'customer_id' = '12345'
ORDER BY w.created_at DESC;
```

## Limitations and Trade-offs

### Limitations

1. **PostgreSQL Dependency**
   - Requires PostgreSQL infrastructure
   - Database admin knowledge needed
   - Connection limits (max_connections)

2. **Single Node**
   - No horizontal scaling (yet)
   - Single point of failure (unless using PostgreSQL HA)
   - Limited by single PostgreSQL instance

3. **Connection Pool Overhead**
   - Memory per connection (~1-2MB)
   - Connection acquisition latency
   - Pool saturation under high load

4. **Transaction Overhead**
   - BEGIN/COMMIT latency
   - Lock contention under high concurrency
   - Vacuum/maintenance required

### Trade-offs

| Aspect | Velocity Embedded | Velocity Server | Trade-off |
|--------|-------------------|-----------------|-----------|
| Throughput | **61.25 ops/s** | 43.6 ops/s | PostgreSQL optimization |
| Latency | **14.65ms p50** | 180ms p50 | Connection pool vs WAL fsync |
| Memory | **1.25 MiB** | 98 MiB | Pool vs WAL buffers |
| Durability | **ACID** | WAL | Transactional vs crash recovery |
| Queryability | **Full SQL** | None | PostgreSQL vs WAL |
| Complexity | Higher | Lower | Database vs file-based |

## Conclusion

Velocity Embedded is the **best all-around flavor** for production deployments requiring ACID transactions, SQL queryability, and high performance. It excels in data-intensive applications, compliance-heavy industries, and scenarios where workflow state needs to be queried or integrated with existing PostgreSQL infrastructure.

**Key Strengths:**
- Highest throughput (61.25 ops/s)
- Lowest latency (14.65ms p50)
- ACID transactions
- Full SQL queryability
- Lowest memory footprint (1.25 MiB)

**Key Weaknesses:**
- Requires PostgreSQL infrastructure
- Higher operational complexity
- Connection pool management
- Database maintenance (vacuum, backups)

**Section sources**
- [velocity-embedded-server/src/main.rs](file://velocity-embedded-server/src/main.rs)
- [velocity-embedded/src/main.rs](file://velocity-embedded/src/main.rs)
- [migrations/001_initial_schema.sql](file://migrations/001_initial_schema.sql)
- [velocity-embedded/Cargo.toml](file://velocity-embedded/Cargo.toml)
- [velocity-server-bootstrap/src/lib.rs](file://velocity-server-bootstrap/src/lib.rs)
