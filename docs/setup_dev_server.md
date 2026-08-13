# VELOCITY-WorkFlow Dev Server Setup Guide

> One-command local development experience with all three flavors.

---

## Table of Contents

1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Building the Dev Server](#building-the-dev-server)
4. [Starting the Server](#starting-the-server)
5. [Flavor 1: Velocity Classic (gRPC)](#flavor-1-velocity-classic-grpc)
6. [Flavor 2: Velocity Runtime (HTTP)](#flavor-2-velocity-runtime-http)
7. [Flavor 3: Velocity Embedded (Postgres)](#flavor-3-velocity-embedded-postgres)
8. [API Reference](#api-reference)
9. [Health and Metrics](#health-and-metrics)
10. [TLS Configuration](#tls-configuration)
11. [CLI Options](#cli-options)
12. [Troubleshooting](#troubleshooting)

---

## Overview

The VELOCITY Dev Server is a single binary that provides a complete in-memory workflow engine with HTTP API, gRPC, and optional web UI. No external dependencies (no Postgres, no Cassandra) required for basic operation.

**Key features:**
- In-memory workflow engine with WAL persistence
- HTTP REST API (Velocity Runtime flavor)
- gRPC BenchmarkService (Velocity Classic flavor)
- Deep health checks (`/health`) with version, uptime, workflow counts
- Prometheus metrics (`/metrics`)
- AES-256-GCM encryption at rest with key rotation
- 10 MB request body size limit (DoS protection)
- X-Request-Id header propagation
- Content-Type validation on POST/PUT/PATCH
- jemalloc global allocator
- Graceful shutdown on SIGTERM/SIGINT

---

## Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| Rust | 1.82+ (stable) | Build the server |
| protoc | 25+ | Protocol Buffers compiler (for gRPC) |

### Install Rust

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source $HOME/.cargo/env
```

### Install protoc

```bash
# Linux
sudo apt-get install -y protobuf-compiler

# macOS
brew install protobuf

# Windows
choco install protoc
```

---

## Building the Dev Server

```bash
cd VELOCITY-WorkFlow

# Build in release mode (recommended)
cargo build --release -p velocity-dev-server

# Or build in debug mode for development
cargo build -p velocity-dev-server
```

The binary is located at:
- Release: `target/release/velocity-dev-server`
- Debug: `target/debug/velocity-dev-server`

---

## Starting the Server

```bash
# Start with defaults (HTTP on port 7233)
cargo run --release -p velocity-dev-server

# Or use the binary directly
./target/release/velocity-dev-server
```

The server starts and logs:
```
[VELOCITY] Dev Server starting...
[VELOCITY] HTTP API: http://127.0.0.1:7233
[VELOCITY] gRPC:     http://127.0.0.1:7234
[VELOCITY] Web UI:   http://127.0.0.1:8233
[VELOCITY] Health:   http://127.0.0.1:7233/health
[VELOCITY] Metrics:  http://127.0.0.1:7233/metrics
[VELOCITY] Ready to accept connections
```

---

## Flavor 1: Velocity Classic (gRPC)

Start with gRPC enabled for Temporal-compatible API:

```bash
cargo run --release -p velocity-dev-server -- --grpc-port 7234
```

This starts:
- HTTP API on port 7233 (default)
- gRPC on port 7234

### Connect with Temporal SDKs

```typescript
// TypeScript (using Temporal SDK)
import { Client } from '@temporalio/client';

const client = await Client.connect('localhost:7234');
// Use as normal — VELOCITY is API-compatible
```

```python
# Python (using Temporal SDK)
from temporalio.client import Client

client = await Client.connect("localhost:7234")
# Use as normal
```

---

## Flavor 2: Velocity Runtime (HTTP)

The default mode — HTTP REST API:

```bash
cargo run --release -p velocity-dev-server -- --port 7233
```

### Start a Workflow

```bash
curl -X POST http://localhost:7233/api/v1/namespaces/default/workflows \
  -H "Content-Type: application/json" \
  -H "X-Request-Id: my-request-1" \
  -d '{
    "workflow_type": "greeting",
    "task_queue": "greetings",
    "input": {"name": "World"}
  }'
```

### List Workflows

```bash
curl http://localhost:7233/api/v1/namespaces/default/workflows
```

### Signal a Workflow

```bash
curl -X POST http://localhost:7233/api/v1/namespaces/default/workflows/WORKFLOW_ID/signal \
  -H "Content-Type: application/json" \
  -d '{"signal_name": "payment-confirmed", "input": {"amount": 99.99}}'
```

---

## Flavor 3: Velocity Embedded (Postgres)

Start with PostgreSQL backend:

```bash
# First, start PostgreSQL
docker run -d --name velocity-pg -p 5432:5432 \
  -e POSTGRES_PASSWORD=velocity \
  -e POSTGRES_DB=velocity \
  postgres:16

# Start the server in embedded mode
cargo run --release -p velocity-dev-server -- --embedded-mode --port 7233
```

### Connect with DBOS-compatible tools

```bash
# Workflow API is the same HTTP REST API
curl -X POST http://localhost:7233/api/v1/namespaces/default/workflows \
  -H "Content-Type: application/json" \
  -d '{"workflow_type": "greeting", "task_queue": "greetings"}'

# Direct Postgres access for hybrid durability
psql -h localhost -U velocity -d velocity -c "SELECT * FROM workflows;"
```

---

## API Reference

### HTTP Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Deep health check (JSON) |
| `GET` | `/metrics` | Prometheus metrics |
| `POST` | `/api/v1/namespaces/{ns}/workflows` | Start a workflow |
| `GET` | `/api/v1/namespaces/{ns}/workflows` | List workflows |
| `GET` | `/api/v1/namespaces/{ns}/workflows/{id}` | Describe workflow |
| `POST` | `/api/v1/namespaces/{ns}/workflows/{id}/signal` | Signal a workflow |
| `GET` | `/api/v1/namespaces/{ns}/workflows/{id}/query` | Query a workflow |
| `DELETE` | `/api/v1/namespaces/{ns}/workflows/{id}` | Cancel/terminate |
| `POST` | `/api/v1/namespaces` | Create namespace |
| `GET` | `/api/v1/namespaces` | List namespaces |

### Request Headers

| Header | Required | Description |
|--------|----------|-------------|
| `Content-Type` | POST/PUT/PATCH to /api/ | Must be `application/json` (415 if missing) |
| `X-Request-Id` | Optional | Request correlation ID (generated if not provided) |
| `Content-Length` | Auto | Must be ≤ 10 MB (413 if exceeded) |

### Response Headers

All responses include:
- `X-Request-Id`: The request correlation ID
- `Content-Type: application/json`

---

## Health and Metrics

### Deep Health Check

```bash
curl http://localhost:7233/health
```

```json
{
  "status": "healthy",
  "version": "0.1.0",
  "uptime_secs": 3600,
  "workflow_count": 150,
  "running": 42,
  "completed": 105,
  "failed": 3,
  "namespace_count": 5
}
```

Use for Kubernetes liveness/readiness probes:

```yaml
livenessProbe:
  httpGet:
    path: /health
    port: 7233
  initialDelaySeconds: 5
  periodSeconds: 10
```

### Prometheus Metrics

```bash
curl http://localhost:7233/metrics
```

Key metrics:
```
# HELP velocity_workflows_started_total Total workflows started
# TYPE velocity_workflows_started_total counter
velocity_workflows_started_total 150

# HELP velocity_workflows_completed_total Total workflows completed
# TYPE velocity_workflows_completed_total counter
velocity_workflows_completed_total 105

# HELP velocity_active_workflows Currently running workflows
# TYPE velocity_active_workflows gauge
velocity_active_workflows 42

# HELP velocity_namespaces Number of active namespaces
# TYPE velocity_namespaces gauge
velocity_namespaces 5

# HELP velocity_task_queues Number of active task queues
# TYPE velocity_task_queues gauge
velocity_task_queues 12
```

---

## TLS Configuration

Enable TLS for production deployments:

```bash
cargo run --release -p velocity-dev-server -- \
  --tls-cert /path/to/cert.pem \
  --tls-key /path/to/key.pem
```

---

## CLI Options

```
Usage: velocity-dev-server [OPTIONS]

Options:
  --port <PORT>              HTTP API port [default: 7233]
  --grpc-port <PORT>         gRPC port [default: 7234]
  --ui-port <PORT>           Web UI port (0 to disable) [default: 8233]
  --ip <IP>                  Bind IP address [default: 127.0.0.1]
  --namespace <NS>           Default namespace [default: default]
  --log-level <LEVEL>        Log level (trace, debug, info, warn, error) [default: info]
  --shards <N>               Number of history shards [default: 4]
  --retention-days <N>       Workflow retention period in days [default: 7]
  --dynamic-config           Enable dynamic config updates via API [default: true]
  --sqlite-path <PATH>       SQLite database path (empty for in-memory) [default: ""]
  --cluster-mode             Enable cluster mode (multi-node simulation) [default: false]
  --cluster-nodes <N>        Number of simulated cluster nodes [default: 3]
  --auto-compact             Enable auto-compaction [default: true]
  --compact-interval-secs <N> Compaction interval in seconds [default: 300]
  --chaos                    Enable chaos testing mode [default: false]
  --otel                     Enable OpenTelemetry export [default: false]
  --otel-endpoint <URL>      OpenTelemetry endpoint [default: http://localhost:4317]
  --headless                 Headless mode (no interactive console) [default: false]
  --data-dir <PATH>          Data directory for persistence [default: ""]
  --search-attributes        Enable workflow search attributes [default: true]
  --max-history-size <N>     Max workflow execution history size (events) [default: 50000]
  --rate-limiting            Enable namespace-level rate limiting [default: false]
  --rate-limit-rps <N>       Rate limit (requests per second per namespace) [default: 1000]
  --tls-cert <PATH>          TLS certificate PEM file
  --tls-key <PATH>           TLS private key PEM file
  --auth-token <TOKEN>       Auth bearer token for API requests
  --embedded-mode            Enable PostgreSQL embedded mode
  --max-body-size <BYTES>    Max request body size [default: 10485760]
  -h, --help                 Print help
  -V, --version              Print version
```

---

## Troubleshooting

### Port Already in Use

```bash
# Check what's using port 7233
lsof -i :7233  # Linux/macOS
netstat -ano | findstr :7233  # Windows

# Use a different port
cargo run --release -p velocity-dev-server -- --port 8080
```

### Build Fails: protoc Not Found

```bash
# Install protoc
sudo apt-get install -y protobuf-compiler  # Linux
brew install protobuf                       # macOS

# Verify
protoc --version
```

### High Memory Usage

The dev server uses jemalloc by default. If memory is still high:
- Check workflow count — each workflow uses ~128 bytes slab + overflow
- Reduce `--max-body-size` if handling large payloads
- Monitor with `/metrics` endpoint
