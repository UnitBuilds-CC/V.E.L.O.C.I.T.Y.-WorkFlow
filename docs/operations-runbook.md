# Velocity Server — Operations Runbook

## Overview

Velocity servers come in two flavors:
- **velocity-classic-server** (port 8093 health, 8083 WebSocket)
- **velocity-embedded-server** (port 8094 health, 8084 WebSocket)

Both share the same operational interface: health endpoints, metrics, TLS, and graceful shutdown.

---

## Health & Readiness Endpoints

| Endpoint | Port | Auth | Purpose |
|----------|------|------|---------|
| `GET /health` | 8093/8094 | None | Liveness probe — returns `{"status":"ok"}` |
| `GET /ready` | 8093/8094 | None | Readiness probe — returns `{"status":"ok"}` |
| `GET /metrics` | 8093/8094 | Bearer token (optional) | Prometheus metrics |

### Checking Health
```bash
curl http://localhost:8093/health
# {"status":"ok","engine":"velocity-classic","transport":"nmcp"}
```

### Prometheus Metrics
```bash
# Without auth (development):
curl http://localhost:8093/metrics

# With auth (production):
curl -H "Authorization: Bearer $VELOCITY_METRICS_TOKEN" http://localhost:8093/metrics
```

### Key Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `velocity_workflows_running` | gauge | Currently running workflows |
| `velocity_workflows_completed` | counter | Total completed workflows |
| `velocity_workflows_failed` | counter | Total failed workflows |
| `velocity_steps_total` | counter | Total steps executed |
| `velocity_step_persist_latency_ms{quantile="0.5"}` | gauge | p50 step persist latency |
| `velocity_step_persist_latency_ms{quantile="0.99"}` | gauge | p99 step persist latency |
| `velocity_step_persist_latency_ms{quantile="0.999"}` | gauge | p999 step persist latency |
| `velocity_pg_connected` | gauge | PostgreSQL connection status (1=connected) |
| `velocity_pg_write_queue_depth` | gauge | Pending PG writes |
| `velocity_wal_unsynced_bytes` | gauge | WAL bytes not yet fsynced |
| `velocity_nmcp_shmem_contentions_total` | counter | Shmem IPC contention events |

---

## Configuration

### CLI Options

| Flag | Env Var | Default | Description |
|------|---------|---------|-------------|
| `--log-level` | — | `info` | Log level (trace/debug/info/warn/error) |
| `--log-format` | `VELOCITY_LOG_FORMAT` | `pretty` | Log format: `pretty` or `json` |
| `--metrics-token` | `VELOCITY_METRICS_TOKEN` | _(none)_ | Bearer token for `/metrics` auth |
| `--tls-cert` | `VELOCITY_TLS_CERT` | _(none)_ | TLS certificate PEM file |
| `--tls-key` | `VELOCITY_TLS_KEY` | _(none)_ | TLS private key PEM file |
| `--postgres` | `DATABASE_URL` | _(none)_ | PostgreSQL connection string |
| `--wal-path` | — | `velocity-{flavor}.wal` | WAL file path |
| `--wal-max-size` | — | `67108864` (64MB) | Max WAL file size before rotation |
| `--health-bind` | — | `0.0.0.0:8093/8094` | Health endpoint bind address |
| `--ws-bind` | — | `0.0.0.0:8083/8084` | WebSocket bind address |
| `--shmem-path` | — | `/tmp/velocity-{flavor}.nmcp` | Shared memory IPC path |

### Structured JSON Logging

For production log aggregation (Loki, ELK, CloudWatch):
```bash
VELOCITY_LOG_FORMAT=json ./velocity-classic-server
```

JSON output includes `timestamp`, `level`, `target`, `message`, and structured fields.

### TLS Configuration

Enable TLS for both health endpoint and WebSocket:
```bash
./velocity-classic-server \
  --tls-cert /etc/velocity/tls/tls.crt \
  --tls-key /etc/velocity/tls/tls.key
```

Both `--tls-cert` and `--tls-key` must be provided together. If only one is set, TLS is disabled with a warning.

### Metrics Authentication

Protect the `/metrics` endpoint with a bearer token:
```bash
VELOCITY_METRICS_TOKEN=secret123 ./velocity-classic-server
```

Prometheus scrapes must include the token:
```yaml
# prometheus.yml
scrape_configs:
  - job_name: velocity
    bearer_token: secret123
    static_configs:
      - targets: ['velocity:8093']
```

---

## Kubernetes Deployment

### Probes

The Helm chart configures probes against the dedicated health port:

- **Liveness**: `GET /health` on port `health` (8093)
- **Readiness**: `GET /ready` on port `health` (8093)
- **Startup**: Disabled by default

### Helm Values

```yaml
service:
  healthPort: 8093

logging:
  format: json    # "pretty" or "json"
  level: info

metrics:
  token: ""                    # Inline token (dev only)
  existingSecret: "velocity-metrics-token"  # Secret reference (prod)
```

### Creating the Metrics Secret

```bash
kubectl create secret generic velocity-metrics-token \
  --from-literal=token=$(openssl rand -hex 32) \
  -n velocity
```

---

## Graceful Shutdown

The server performs a 5-step graceful shutdown on SIGINT/SIGTERM:

1. **Stop accepting** new connections (shmem + WebSocket)
2. **Drain in-flight** workflows (wait up to 30s for completion)
3. **Flush WAL** — fsync all pending records to disk
4. **Flush PG** — wait for all pending PostgreSQL writes
5. **Shutdown engine** — stop task queue and timer engine

Set `terminationGracePeriodSeconds: 60` in K8s to allow time for drain.

---

## PostgreSQL Failover

When PostgreSQL is configured (`--postgres` / `DATABASE_URL`):

- The server auto-reconnects on connection loss
- Steps are buffered in-memory during PG downtime
- On reconnect, buffered steps are flushed to PG
- WAL remains the source of truth — no data loss on PG outage

### Monitoring PG Health
```bash
curl -s http://localhost:8093/metrics | grep velocity_pg_connected
# 1 = connected, 0 = disconnected
```

---

## WAL Management

### WAL Rotation
WAL files rotate automatically when they exceed `--wal-max-size` (default 64MB). Rotated files are named `{wal_path}.1`, `{wal_path}.2`, etc.

### Recovery
On startup, the engine replays the WAL to restore state:
```
Velocity Classic Server (NMCP transport, Rust + WAL)
WAL: velocity-classic.wal
  Recovered 1,234 WAL records, 56 workflows
```

### WAL Retention
Configure retention via `--wal-max-size`. Smaller values = more frequent rotation but more files. Typical production: 64MB.

---

## Troubleshooting

### Server won't start
- Check WAL file permissions: `ls -la velocity-*.wal`
- Check shmem path permissions: `ls -la /tmp/velocity-*.nmcp`
- Check port availability: `ss -tlnp | grep 8093`

### High latency
- Check `velocity_step_persist_latency_ms{quantile="0.99"}` — should be < 10ms
- Check `velocity_pg_write_queue_depth` — growing queue indicates PG bottleneck
- Check `velocity_nmcp_shmem_contentions_total` — rising counter indicates IPC saturation

### PostgreSQL disconnected
- Check `velocity_pg_connected` metric
- Verify `DATABASE_URL` is correct
- Check PG logs for connection limits or authentication failures
- The server will auto-reconnect; steps are buffered during downtime

### WebSocket connections failing
- Verify `--ws-bind` address is reachable
- If TLS is enabled, clients must use `wss://` instead of `ws://`
- Check `--max-connections` (default 64) — connections beyond this are rejected

### Out of memory
- Check `velocity_workflows_running` — too many concurrent workflows
- Reduce `--max-connections` on WebSocket server
- Increase K8s memory limits in `values.yaml`

---

## Benchmarking

### Quick validation (10s)
```bash
cargo test -p velocity-workflow-engine --test sustained_benchmark -- --ignored --nocapture
```

### Production benchmark (1 hour)
```bash
SUSTAINED_DURATION_SECS=3600 cargo test -p velocity-workflow-engine --test sustained_benchmark -- --ignored --nocapture
```

### Cross-flavor parity
```bash
cargo test -p velocity-workflow-engine --test sustained_benchmark test_cross_flavor_parity -- --nocapture
```
