# VELOCITY-WorkFlow Deployment Guide

> Complete guide for deploying the VELOCITY-WorkFlow platform — from local development to multi-region production clusters.

---

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Three Deployment Flavors](#three-deployment-flavors)
3. [Local Development Setup](#local-development-setup)
4. [Docker Deployment](#docker-deployment)
5. [Kubernetes Deployment](#kubernetes-deployment)
6. [Production Checklist](#production-checklist)
7. [Scaling Guide](#scaling-guide)
8. [Backup and Disaster Recovery](#backup-and-disaster-recovery)
9. [Monitoring and Alerting](#monitoring-and-alerting)

---

## Prerequisites

### Required Software

| Tool | Version | Purpose |
|------|---------|---------|
| **Rust** | 1.82+ (stable) | Build the workflow engine (cdylib + rlib) |
| **.NET SDK** | 10.0-preview | Build the server and HTTP API |
| **Docker** | 24+ | Container builds |
| **Docker Compose** | 2.20+ | Local multi-service orchestration |
| **kubectl** | 1.28+ | Kubernetes cluster management |
| **Helm** | 3.14+ | Kubernetes package management |
| **PostgreSQL** | 16+ | Persistence backend (included in Docker) |
| **protoc** | 25+ | Protocol Buffers compiler (for gRPC feature) |

### Hardware Minimums

| Environment | CPU | RAM | Disk | Network |
|-------------|-----|-----|------|---------|
| Development | 4 cores | 8 GB | 50 GB SSD | localhost |
| Staging | 8 cores | 16 GB | 200 GB SSD | 1 Gbps |
| Production | 16+ cores | 64+ GB | 1 TB NVMe SSD | 10 Gbps |

---

## Three Deployment Flavors

VELOCITY-WorkFlow can be deployed in three flavors:

### Flavor 1: Velocity Classic (gRPC)

Full Temporal-compatible gRPC API. Use when migrating from Temporal or when you need the full 33-RPC BenchmarkService.

```bash
# Dev server with gRPC
cargo run --release -p velocity-dev-server -- --grpc-port 7234

# Production server
cargo run --release -p velocity-workflow-server -- --ip 0.0.0.0 --grpc-port 7234
```

### Flavor 2: Velocity Runtime (HTTP)

Lightweight HTTP REST API. Use for serverless, lightweight integrations, or when migrating from Restate.

```bash
cargo run --release -p velocity-dev-server -- --port 7233

# API endpoints
# POST /api/v1/namespaces/{ns}/workflows — Start workflow
# GET  /api/v1/namespaces/{ns}/workflows — List workflows
# POST /api/v1/namespaces/{ns}/workflows/{id}/signal — Signal workflow
```

### Flavor 3: Velocity Embedded (PostgreSQL)

PostgreSQL-backed durability. Use when migrating from DBOS or when you need direct database access.

```bash
# Start PostgreSQL first
docker run -d --name velocity-pg -p 5432:5432 -e POSTGRES_PASSWORD=velocity postgres:16

# Start in embedded mode
cargo run --release -p velocity-dev-server -- --embedded-mode --port 7233
```

---

## Local Development Setup

### 1. Clone and Build the Rust Engine

```bash
cd VELOCITY-WorkFlow

# Build the core library
cargo build --release --manifest-path velocity-workflow-core/Cargo.toml

# Build the engine (default features — no gRPC)
cargo build --release --manifest-path velocity-workflow-engine/Cargo.toml

# Build with gRPC support
cargo build --release --manifest-path velocity-workflow-engine/Cargo.toml --features grpc
```

The engine produces two shared libraries:
- `libvelocity_workflow_core.so` (Linux) / `velocity_workflow_core.dll` (Windows)
- `libvelocity_workflow_engine.so` (Linux) / `velocity_workflow_engine.dll` (Windows)

### 2. Build the .NET Server

```bash
# Copy native libraries to the expected location
# Linux:
cp target/release/libvelocity_workflow_core.so \
   src/Velocity.Workflow.Core/runtimes/linux-x64/native/

# Windows:
copy target\release\velocity_workflow_core.dll \
   src\Velocity.Workflow.Core\runtimes\win-x64\native\

# Build and run
dotnet build src/Velocity.Workflow.Server/Velocity.Workflow.Server.csproj
dotnet run --project src/Velocity.Workflow.Server/Velocity.Workflow.Server.csproj
```

The server starts on:
- **HTTP API:** `http://localhost:7233`
- **gRPC:** `localhost:7234` (when gRPC feature is enabled)

### 3. Run Tests

```bash
# Engine unit and integration tests
cargo test --manifest-path velocity-workflow-engine/Cargo.toml

# Core library tests
cargo test --manifest-path velocity-workflow-core/Cargo.toml

# .NET server tests
dotnet test src/Velocity.Workflow.Server/Velocity.Workflow.Server.csproj
```

### 4. Run Benchmarks

```bash
cargo bench --manifest-path velocity-workflow-engine/Cargo.toml

# Or use the reproducible benchmark script
pwsh benchmarks/run_reproducible_benchmarks.ps1
```

---

## Docker Deployment

### Quick Start with Docker Compose

The included `docker-compose.yml` starts four services:

| Service | Port | Description |
|---------|------|-------------|
| `velocity-server` | 7233 (HTTP), 7234 (gRPC) | Workflow engine + API |
| `postgres` | 5432 | PostgreSQL 16 database |
| `prometheus` | 9090 | Metrics collection |
| `grafana` | 3000 | Dashboards and visualization |

```bash
# Set the database password
export POSTGRES_PASSWORD=your_secure_password

# Start all services
docker compose up -d

# Check status
docker compose ps

# View logs
docker compose logs -f velocity-server
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `ASPNETCORE_ENVIRONMENT` | `Production` | Runtime environment |
| `ASPNETCORE_URLS` | `http://+:7233` | HTTP listen addresses |
| `ConnectionStrings__Postgres` | — | PostgreSQL connection string |
| `GRPC__PORT` | `7234` | gRPC listen port |
| `VELOCITY__METRICS__ENABLED` | `true` | Enable Prometheus metrics |
| `VELOCITY__LOGGING__LEVEL` | `Information` | Log level (Trace/Debug/Info/Warning/Error) |
| `POSTGRES_PASSWORD` | `velocity_secret` | Database password |
| `GRAFANA_ADMIN_USER` | `admin` | Grafana admin username |
| `GRAFANA_ADMIN_PASSWORD` | `admin` | Grafana admin password |

### Building the Docker Image

The multi-stage `Dockerfile` builds in three stages:

1. **Rust Builder** — compiles engine shared libraries
2. **.NET Builder** — publishes the server with native libs
3. **Runtime** — minimal `dotnet/aspnet:9.0` image with non-root user

```bash
docker build -t velocity-workflow:latest .
docker run -p 7233:7233 -p 7234:7234 \
  -e ConnectionStrings__Postgres="Host=db;Database=velocity;Username=velocity;Password=secret" \
  velocity-workflow:latest
```

---

## Kubernetes Deployment

### Helm Chart

```bash
# Add the repository (or use local chart)
helm install velocity ./deploy/helm/velocity \
  --namespace velocity-system --create-namespace \
  --set postgres.password=your_secure_password \
  --set server.replicas=3
```

### Manual Kubernetes Deployment

Apply the manifests in order:

```bash
# 1. Create namespace
kubectl apply -f deploy/k8s/namespace.yaml

# 2. Set up RBAC
kubectl apply -f deploy/k8s/rbac.yaml

# 3. Deploy PostgreSQL
kubectl apply -f deploy/k8s/postgres.yaml

# 4. Deploy Prometheus
kubectl apply -f deploy/k8s/prometheus.yaml

# 5. Deploy the Velocity server
kubectl apply -f deploy/k8s/velocity-server.yaml
```

### Resource Requests and Limits

```yaml
resources:
  requests:
    cpu: "4"
    memory: "8Gi"
  limits:
    cpu: "16"
    memory: "32Gi"
```

### Health Checks

The server exposes:
- **Liveness:** `GET /health` — returns 200 if the process is running
- **Readiness:** `GET /ready` — returns 200 if the engine and database are connected

---

## Production Checklist

### Resource Sizing

| Component | Minimum | Recommended | High-Throughput |
|-----------|---------|-------------|-----------------|
| Engine CPU | 4 cores | 16 cores | 64 cores |
| Engine RAM | 8 GB | 32 GB | 128 GB |
| PostgreSQL | 4 cores / 8 GB | 16 cores / 64 GB | 32 cores / 256 GB |
| Disk IOPS | 3,000 | 10,000 | 50,000+ |

### Database Tuning

```sql
-- PostgreSQL production settings
ALTER SYSTEM SET shared_buffers = '8GB';
ALTER SYSTEM SET effective_cache_size = '24GB';
ALTER SYSTEM SET work_mem = '256MB';
ALTER SYSTEM SET maintenance_work_mem = '2GB';
ALTER SYSTEM SET max_wal_size = '8GB';
ALTER SYSTEM SET checkpoint_completion_target = 0.9;
ALTER SYSTEM SET wal_buffers = '256MB';
ALTER SYSTEM SET max_connections = 200;
```

### TLS Configuration

Enable TLS for both HTTP and gRPC:

```yaml
# appsettings.Production.json
{
  "Kestrel": {
    "Endpoints": {
      "Https": {
        "Url": "https://+:5001",
        "Certificate": {
          "Path": "/certs/tls.crt",
          "KeyPath": "/certs/tls.key"
        }
      }
    }
  },
  "GRPC": {
    "Port": 50052,
    "Tls": {
      "CertPath": "/certs/tls.crt",
      "KeyPath": "/certs/tls.key"
    }
  }
}
```

### Security Hardening

- Run the container as non-root user (`velocity`)
- Enable SCRAM-SHA-256 for PostgreSQL authentication
- Restrict network policies to allow only required ports
- Rotate API keys and TLS certificates regularly
- Enable audit logging via `AuditLogger`

---

## Scaling Guide

### Horizontal Scaling (Engine Nodes)

The engine uses **consistent hashing** via `ShardManager` with 150 virtual nodes per host. Workflow keys are distributed across shards, and each shard maps to a specific engine node.

```bash
# Scale engine replicas
kubectl scale deployment velocity-server --replicas=5

# The hash ring automatically redistributes workflow keys
# Only 1/N of keys are remapped when a node is added
```

**Key considerations:**
- Each node owns a subset of workflow keys based on the hash ring
- Adding/removing nodes triggers minimal remapping
- Use sticky affinity to keep related workflows on the same node

### Vertical Scaling

Increase resources for a single engine node:

```bash
kubectl set resources deployment velocity-server \
  --limits cpu=32,memory=64Gi \
  --requests cpu=16,memory=32Gi
```

The engine is designed for vertical scale — the slab allocator and zero-GC runtime benefit from large memory and CPU caches.

### Multi-Region Deployment

The engine supports active/standby multi-region replication via `MultiRegionReplicator`:

```yaml
# Region configuration
regions:
  - region_id: "us-east-1"
    endpoint: "velocity-us-east.internal:7234"
    priority: 1
    is_active: true
    replication_lag_tolerance_ms: 5000
  - region_id: "eu-west-1"
    endpoint: "velocity-eu-west.internal:7234"
    priority: 2
    is_active: false
    replication_lag_tolerance_ms: 10000
```

**Replication protocols:**
- **TCP** — reliable, ordered delivery via `TcpReplicationServer`
- **UDP** — low-latency fire-and-forget via `UdpReplicationTransport`

**Wire protocol:** `[MAGIC: 4B "VELO"][FRAME_TYPE: 1B][PAYLOAD_LEN: 4B][PAYLOAD: NB]`

Frame types: `Handshake`, `TaskBatch`, `Ack`, `Ping`, `Pong`, `Shutdown`.

**Failover:** The `FailoverController` monitors region health and promotes standby regions automatically when the active region fails.

---

## Backup and Disaster Recovery

### PostgreSQL Backups

```bash
# Full backup
pg_dump -h localhost -U velocity velocity > backup_$(date +%Y%m%d).sql

# WAL archiving (point-in-time recovery)
# Add to postgresql.conf:
# wal_level = replica
# archive_mode = on
# archive_command = 'cp %p /backups/wal/%f'
```

### Engine State Recovery

The engine uses a **Write-Ahead Log (WAL)** for crash recovery. On startup, the WAL is replayed to reconstruct in-memory state:

1. The WAL file is located at the configured path (default: `./velocity.wal`)
2. Record format: `[event_type: u8][workflow_key: u64][data_len: u32][data: bytes][crc32: u32]`
3. Each record is CRC-verified during replay — corrupted records halt recovery
4. After successful replay, the WAL is truncated

### Recovery Procedure

1. **Stop the engine** — ensure no writes are in-flight
2. **Restore PostgreSQL** from the most recent backup
3. **Start the engine** — it replays the WAL automatically
4. **Verify** — check workflow counts and statuses via `ListWorkflowExecutions`

### RPO/RTO Targets

| Tier | RPO | RTO | Strategy |
|------|-----|-----|----------|
| Standard | 1 hour | 15 minutes | Hourly pg_dump + WAL replay |
| Enhanced | 5 minutes | 5 minutes | Continuous WAL archiving |
| Critical | 0 (sync) | 1 minute | Multi-region synchronous replication |

---

## Monitoring and Alerting

### Prometheus Metrics

The engine exports Prometheus metrics at `GET /metrics` when `VELOCITY__METRICS__ENABLED=true`.

**Key metrics:**

| Metric | Type | Description |
|--------|------|-------------|
| `velocity_workflows_started_total` | Counter | Total workflows started |
| `velocity_workflows_completed_total` | Counter | Total workflows completed |
| `velocity_workflows_failed_total` | Counter | Total workflows failed |
| `velocity_workflow_duration_seconds` | Histogram | Workflow duration distribution |
| `velocity_task_queue_depth` | Gauge | Current task queue depth |
| `velocity_active_workflows` | Gauge | Currently running workflows |
| `velocity_signal_count_total` | Counter | Total signals delivered |
| `velocity_query_count_total` | Counter | Total queries served |
| `velocity_wal_records_written` | Counter | WAL records written |
| `velocity_replication_lag_ms` | Gauge | Replication lag in milliseconds |

### Grafana Dashboards

Pre-built dashboards are provisioned from `deploy/grafana/provisioning/`. Default access:

- **URL:** `http://localhost:3000`
- **Credentials:** `admin` / `admin` (change in production)

### Alert Rules

Recommended Prometheus alert rules:

```yaml
groups:
  - name: velocity-alerts
    rules:
      - alert: HighWorkflowFailureRate
        expr: rate(velocity_workflows_failed_total[5m]) / rate(velocity_workflows_completed_total[5m]) > 0.05
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Workflow failure rate exceeds 5%"

      - alert: TaskQueueBacklog
        expr: velocity_task_queue_depth > 10000
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "Task queue depth exceeds 10,000"

      - alert: ReplicationLagHigh
        expr: velocity_replication_lag_ms > 10000
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "Replication lag exceeds 10 seconds"

      - alert: EngineDown
        expr: up{job="velocity-server"} == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Velocity engine instance is down"
```

### Structured Logging

The engine uses `StructuredLogger` for JSON-formatted logs with configurable levels:

```json
{
  "timestamp": "2026-08-07T12:00:00Z",
  "level": "INFO",
  "service": "velocity-workflow-engine",
  "span_id": "a1b2c3d4",
  "message": "Workflow completed",
  "workflow_key": 42,
  "duration_ms": 1234
}
```

### Distributed Tracing

The `SpanTracker` provides OpenTelemetry-compatible tracing. Each workflow execution creates a root span, with child spans for activities, signals, and queries. Configure via:

```yaml
observability:
  enable_tracing: true
  service_name: "velocity-workflow-engine"
  export_endpoint: "http://jaeger:4317"
```
