# VELOCITY-WorkFlow Deployment Reference

> Docker, Kubernetes, and configuration quick reference. For the complete deployment walkthrough including local development, scaling, and disaster recovery, see the [Deployment Guide](deployment_guide.md).

---

## Table of Contents

1. [Deployment Options](#deployment-options)
2. [Docker Deployment](#docker-deployment)
3. [Kubernetes & Helm Deployment](#kubernetes--helm-deployment)
4. [Configuration Guide](#configuration-guide)
5. [Production Checklist](#production-checklist)
6. [Monitoring Setup](#monitoring-setup)
7. [Backup and Recovery](#backup-and-recovery)

---

## Deployment Options

| Option | Best For | Complexity | External Dependencies |
|--------|----------|------------|-----------------------|
| **Binary** | Development, testing | Low | None |
| **Docker** | Staging, small production | Medium | Docker |
| **Docker Compose** | Full stack local dev | Medium | Docker Compose |
| **Kubernetes + Helm** | Production, multi-region | High | K8s cluster |
| **Embedded** | In-process (no server) | Low | None |

---

## Docker Deployment

### Quick Start

```bash
# Clone the repository
git clone https://github.com/UnitBuilds-CC/V.E.L.O.C.I.T.Y.-WorkFlow.git
cd VELOCITY-WorkFlow

# Build and start all services
docker compose up -d

# Check status
docker compose ps

# View logs
docker compose logs -f velocity-server
```

### Docker Compose Configuration

The included `docker-compose.yml` provides:

```yaml
services:
  velocity-server:
    build: .
    ports:
      - "7234:7234"   # gRPC
      - "7233:7233"   # HTTP API
    environment:
      - VELOCITY_LOG_LEVEL=info
      - VELOCITY_DATA_DIR=/data
      - VELOCITY_MAX_WORKFLOWS=100000
    volumes:
      - velocity-data:/data
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:7233/health"]
      interval: 10s
      timeout: 5s
      retries: 3

  postgres:
    image: postgres:16
    environment:
      POSTGRES_DB: velocity
      POSTGRES_USER: velocity
      POSTGRES_PASSWORD: ${DB_PASSWORD:-velocity}
    volumes:
      - postgres-data:/var/lib/postgresql/data

volumes:
  velocity-data:
  postgres-data:
```

### Custom Docker Image

```dockerfile
FROM rust:1.82 AS engine-builder
WORKDIR /build
COPY velocity-workflow-core/ .
RUN cargo build --release

FROM mcr.microsoft.com/dotnet/sdk:10.0-preview AS server-builder
WORKDIR /build
COPY src/ src/
COPY --from=engine-builder /build/target/release/*.so /usr/lib/
RUN dotnet publish src/Velocity.Workflow.Server -c Release -o /app

FROM mcr.microsoft.com/dotnet/aspnet:10.0-preview
WORKDIR /app
COPY --from=server-builder /app .
EXPOSE 7234 7233
ENTRYPOINT ["dotnet", "Velocity.Workflow.Server.dll"]
```

---

## Kubernetes & Helm Deployment

### Helm Chart Structure

```
deploy/helm/velocity-workflow/
├── Chart.yaml
├── values.yaml
├── templates/
│   ├── deployment.yaml
│   ├── service.yaml
│   ├── configmap.yaml
│   ├── hpa.yaml
│   └── pdb.yaml
```

### Install with Helm

```bash
# Add the repository
helm repo add velocity https://charts.velocity-workflow.io
helm repo update

# Install with default values
helm install velocity velocity/velocity-workflow \
  --namespace velocity --create-namespace

# Install with custom values
helm install velocity velocity/velocity-workflow \
  --namespace velocity --create-namespace \
  -f my-values.yaml
```

### Production values.yaml

```yaml
replicaCount: 3

image:
  repository: ghcr.io/velocity-workflow/server
  tag: "latest"
  pullPolicy: IfNotPresent

resources:
  requests:
    cpu: "4"
    memory: "8Gi"
  limits:
    cpu: "16"
    memory: "64Gi"

persistence:
  enabled: true
  storageClass: "premium-ssd"
  size: 500Gi

autoscaling:
  enabled: true
  minReplicas: 3
  maxReplicas: 10
  targetCPUUtilization: 70

service:
  type: ClusterIP
  grpcPort: 7234
  httpPort: 7233

config:
  logLevel: "info"
  maxWorkflows: 500000
  slabSize: 4096
  walSyncPolicy: "batch"
  replication:
    enabled: true
    factor: 3
```

### kubectl Verification

```bash
# Check pod status
kubectl get pods -n velocity

# Check logs
kubectl logs -n velocity deployment/velocity-workflow --tail=100

# Port-forward for local testing
kubectl port-forward -n velocity svc/velocity-workflow 7234:7234
```

---

## Configuration Guide

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `VELOCITY_LOG_LEVEL` | `info` | Log level: `trace`, `debug`, `info`, `warn`, `error` |
| `VELOCITY_DATA_DIR` | `./data` | Directory for slab files and WAL segments |
| `VELOCITY_GRPC_PORT` | `7234` | gRPC server listen port |
| `VELOCITY_HTTP_PORT` | `7233` | HTTP API listen port |
| `VELOCITY_MAX_WORKFLOWS` | `100000` | Maximum concurrent workflow instances |
| `VELOCITY_SLAB_SIZE` | `4096` | Default slab size in bytes |
| `VELOCITY_WAL_SYNC_POLICY` | `batch` | WAL sync: `every`, `batch`, `timed` |
| `VELOCITY_WAL_SEGMENT_SIZE` | `4194304` | WAL segment size (4 MB default) |
| `VELOCITY_AUTH_ENABLED` | `false` | Enable JWT authentication |
| `VELOCITY_AUTH_SECRET` | — | JWT signing secret |
| `VELOCITY_REPLICATION_FACTOR` | `1` | Number of replicas (1 = no replication) |
| `VELOCITY_TLS_ENABLED` | `false` | Enable TLS for gRPC transport |
| `VELOCITY_TLS_CERT_PATH` | — | Path to TLS certificate file |
| `VELOCITY_TLS_KEY_PATH` | — | Path to TLS private key file |

### Configuration File

For complex deployments, use a TOML configuration file:

```toml
[server]
grpc_port = 7234
http_port = 7233
log_level = "info"

[storage]
data_dir = "/data/velocity"
slab_size = 4096
wal_sync_policy = "batch"
wal_segment_size = 4194304

[replication]
enabled = true
factor = 3
peers = ["node2:7234", "node3:7234"]

[auth]
enabled = true
secret = "your-jwt-secret"

[tls]
enabled = true
cert_path = "/etc/velocity/tls/cert.pem"
key_path = "/etc/velocity/tls/key.pem"
```

---

## Production Checklist

### Pre-Deployment

- [ ] Rust toolchain 1.82+ installed and verified
- [ ] .NET 10.0 SDK installed and verified
- [ ] Server binary built in Release mode
- [ ] All unit tests passing (`dotnet test`, `cargo test`)
- [ ] Benchmark suite run to establish baseline latency numbers
- [ ] Crash fuzz harness passed (1000/1000 kills)

### Infrastructure

- [ ] Minimum 16 CPU cores, 64 GB RAM, 1 TB NVMe SSD
- [ ] 10 Gbps network between nodes (for replication)
- [ ] Dedicated disk for slab files (avoid shared storage)
- [ ] NTP synchronized across all nodes
- [ ] Firewall rules: allow gRPC port 7234, HTTP port 7233

### Security

- [ ] JWT authentication enabled (`VELOCITY_AUTH_ENABLED=true`)
- [ ] TLS certificates provisioned and configured
- [ ] Namespace ACLs configured for each team/service
- [ ] Secrets stored in vault (not environment variables)
- [ ] Network policies restrict pod-to-pod communication

### Observability

- [ ] Health check endpoint configured (`/health`)
- [ ] Prometheus metrics endpoint enabled
- [ ] Structured logging (JSON) enabled
- [ ] Alerting rules for error rate, latency, disk usage
- [ ] Distributed tracing integration (OpenTelemetry)

### Data

- [ ] WAL sync policy set to `batch` or `every` (not `timed`)
- [ ] Backup schedule configured (slab files + WAL)
- [ ] Retention policy defined for completed workflows
- [ ] PostgreSQL connection configured (if using relational backend)
- [ ] Disk space monitoring with alerting at 80% capacity

---

## Monitoring Setup

### Prometheus Metrics

The server exposes Prometheus metrics at `http://localhost:7233/metrics`:

```
# Workflow metrics
velocity_workflows_active          # Current active workflows
velocity_workflows_completed_total # Total completed workflows
velocity_workflows_failed_total    # Total failed workflows

# Task queue metrics
velocity_taskqueue_depth           # Current queue depth per task queue
velocity_taskqueue_poll_total      # Total poll requests
velocity_taskqueue_dispatch_total  # Total tasks dispatched

# Latency metrics
velocity_step_latency_seconds      # Step completion latency histogram
velocity_poll_latency_seconds      # Poll request latency histogram
velocity_recovery_latency_seconds  # Crash recovery latency

# Storage metrics
velocity_slab_allocated_bytes      # Total slab memory allocated
velocity_wal_entries_total         # Total WAL entries written
velocity_wal_segments              # Current WAL segment count
```

### Grafana Dashboard

Import the pre-built dashboard from `deploy/grafana/velocity-workflow.json`:

```bash
# Via Grafana UI
Dashboards → Import → Upload JSON file → deploy/grafana/velocity-workflow.json
```

### Alerting Rules

```yaml
groups:
  - name: velocity-workflow
    rules:
      - alert: HighErrorRate
        expr: rate(velocity_workflows_failed_total[5m]) > 0.05
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "Workflow error rate above 5%"

      - alert: TaskQueueBacklog
        expr: velocity_taskqueue_depth > 1000
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "Task queue depth exceeding 1000"

      - alert: DiskSpaceLow
        expr: (node_filesystem_avail_bytes / node_filesystem_size_bytes) < 0.2
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Disk space below 20% on velocity data volume"
```

---

## Backup and Recovery

### Backup Strategy

#### Slab Files

```bash
# Live backup (slab files are append-only)
rsync -avz /data/velocity/slabs/ /backup/slabs/

# Compressed archive
tar czf velocity-slabs-$(date +%Y%m%d).tar.gz /data/velocity/slabs/
```

#### WAL Segments

```bash
# Backup unflushed WAL segments
rsync -avz /data/velocity/wal/ /backup/wal/
```

#### PostgreSQL (if used)

```bash
pg_dump -U velocity velocity > velocity-db-$(date +%Y%m%d).sql
```

### Automated Backup Script

```bash
#!/bin/bash
# /etc/cron.d/velocity-backup
BACKUP_DIR="/backup/velocity/$(date +%Y%m%d)"
mkdir -p "$BACKUP_DIR"

# Slab files
rsync -avz /data/velocity/slabs/ "$BACKUP_DIR/slabs/"

# WAL segments
rsync -avz /data/velocity/wal/ "$BACKUP_DIR/wal/"

# PostgreSQL
pg_dump -U velocity velocity > "$BACKUP_DIR/database.sql"

# Cleanup old backups (retain 30 days)
find /backup/velocity/ -maxdepth 1 -mtime +30 -exec rm -rf {} \;

echo "Backup completed: $BACKUP_DIR"
```

### Recovery Procedure

1. **Stop the server**: `systemctl stop velocity-server`
2. **Restore slab files**: `rsync -avz /backup/slabs/ /data/velocity/slabs/`
3. **Restore WAL segments**: `rsync -avz /backup/wal/ /data/velocity/wal/`
4. **Replay WAL**: The server automatically replays unflushed WAL entries on startup
5. **Verify Merkle roots**: Server validates slab integrity via SHA-256 Merkle proofs
6. **Start the server**: `systemctl start velocity-server`
7. **Verify health**: `curl http://localhost:7233/health`

### Point-in-Time Recovery

For point-in-time recovery, combine slab backups with WAL replay:

1. Restore the most recent slab backup
2. Place WAL segments from after the backup in the WAL directory
3. Start the server — it replays WAL entries to reach the desired point
4. Merkle verification ensures state consistency at every step
