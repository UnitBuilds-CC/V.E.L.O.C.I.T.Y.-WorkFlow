# Velocity — OTLP Tracing Configuration Guide

How to configure distributed tracing with OpenTelemetry for Velocity, including Jaeger, Tempo, and Grafana backends.

---

## 1. Overview

Velocity uses OpenTelemetry for distributed tracing. Traces cover the full request lifecycle:

```
Client Request → Auth → Rate Limit → Circuit Breaker → Dispatch → Step Execution → Response
```

**Key spans:**
- `workflow.execute` — Full workflow execution
- `step.persist` — Step persistence (WAL/PG write)
- `signal.deliver` — Signal delivery latency
- `vctp.receive` — VCTP UDP packet receive + pipeline
- `vctp.dispatch` — VCTP method dispatch

---

## 2. Enable OTLP Export

### Compile-Time Feature Flag

OTLP export requires the `otel` feature flag:

```bash
cargo build --release -p velocity-server-bootstrap --features otel
```

### Runtime Configuration

Set the OTLP endpoint via environment variable:

```bash
# Export to Jaeger (OTLP/gRPC)
VELOCITY_OTLP_ENDPOINT=http://jaeger:4317 cargo run --bin velocity-classic-server

# Export to Tempo (OTLP/gRPC)
VELOCITY_OTLP_ENDPOINT=http://tempo:4317 cargo run --bin velocity-classic-server

# Export to Grafana Agent (OTLP/HTTP)
VELOCITY_OTLP_ENDPOINT=http://grafana-agent:4318 cargo run --bin velocity-classic-server
```

### Sampling Configuration

Control trace volume with sampling:

```bash
# Sample every trace (development)
VELOCITY_OTLP_SAMPLING=always_on

# Sample 10% of traces (production)
VELOCITY_OTLP_SAMPLING=trace_id_ratio:0.1

# Parent-based sampling (follow upstream decision)
VELOCITY_OTLP_SAMPLING=parent_based
```

---

## 3. Helm Deployment with Tracing

### Enable in values.yaml

```yaml
monitoring:
  tracing:
    enabled: true
    exporter: otlp
    endpoint: "http://tempo:4317"
    sampling:
      strategy: "parent_based"
      ratio: 0.1

  # Optional: Deploy Tempo alongside Velocity
  tempo:
    enabled: true
    image:
      repository: grafana/tempo
      tag: "2.4"
    resources:
      limits:
        cpu: "1"
        memory: "2Gi"
    persistence:
      enabled: true
      size: 10Gi
```

### Deploy with Helm

```bash
helm upgrade velocity ./deploy/helm/velocity \
  --set monitoring.tracing.enabled=true \
  --set monitoring.tracing.endpoint="http://tempo:4317" \
  --set monitoring.tempo.enabled=true \
  -n velocity-system
```

---

## 4. Backend Configuration

### Jaeger (All-in-One for Development)

```bash
# Deploy Jaeger all-in-one
kubectl apply -f deploy/helm/velocity/templates/jaeger-deployment.yaml

# Access Jaeger UI
kubectl -n velocity-system port-forward svc/jaeger 16686:16686
# Open: http://localhost:16686
```

### Grafana Tempo (Production)

Tempo is already configured via Helm. To query traces:

1. Add Tempo as a data source in Grafana:
   - URL: `http://tempo:3200`
   - Type: Jaeger (Tempo is Jaeger-compatible)

2. Query traces by:
   - Trace ID: Direct lookup
   - Service name: `velocity-server`
   - Duration: Filter slow traces
   - Tags: `workflow_type`, `step_id`

### Grafana Loki (Log Aggregation)

Loki aggregates structured JSON logs:

```bash
# Deploy Loki
kubectl apply -f deploy/helm/velocity/templates/loki-deployment.yaml

# Add Loki data source in Grafana
# URL: http://loki:3100

# Query logs in Grafana Explore:
# {app="velocity"} |= "error"
# {app="velocity"} | json | line_format "{{.message}}"
```

---

## 5. Log Format Configuration

### JSON Format (Recommended for Production)

```bash
# Structured JSON logs → Loki
cargo run --bin velocity-classic-server -- --log-format json
```

Example JSON log output:
```json
{
  "timestamp": "2026-08-17T12:00:00.123Z",
  "level": "INFO",
  "target": "velocity_workflow_engine::vctp_rpc",
  "message": "VCTP request processed",
  "method": "start_workflow",
  "workflow_id": "wf-12345",
  "duration_ms": 2.3,
  "trace_id": "abc123def456",
  "span_id": "789xyz"
}
```

### Compact Format (Development)

```bash
# Human-readable logs
cargo run --bin velocity-classic-server -- --log-format compact
```

---

## 6. Prometheus + Grafana Dashboard

### Prometheus Scraping

Velocity exposes Prometheus metrics at `/metrics`:

```yaml
# ServiceMonitor (already in Helm)
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: velocity
spec:
  selector:
    matchLabels:
      app.kubernetes.io/name: velocity
  endpoints:
    - port: metrics
      interval: 15s
      path: /metrics
      bearerTokenSecret:
        name: velocity-metrics-token
        key: token
```

### Grafana Dashboard

A pre-built dashboard is deployed via Helm (`grafana-dashboard.yaml`). It includes:

- **Overview:** Request rate, error rate, latency (RED)
- **VCTP:** UDP throughput, circuit breaker state, replay detections
- **Persistence:** WAL write latency, PG queue depth, step persist time
- **Security:** Auth failures, rate limit rejections, audit events

---

## 7. Production Checklist

| Configuration | Development | Production |
|--------------|-------------|------------|
| OTLP endpoint | `http://jaeger:4317` | `http://tempo:4317` |
| Sampling | `always_on` | `parent_based` (10%) |
| Log format | `compact` | `json` |
| Log destination | stdout | Loki (via Promtail) |
| Metrics interval | 30s | 15s |
| Alert rules | Disabled | Enabled (17 rules) |
| Trace retention | 24h | 30 days (Tempo) |
| Log retention | 7 days | 30 days (Loki) |
