# VELOCITY-WorkFlow Kubernetes Deployment Guide

> Deploy all three flavors on Kubernetes with monitoring, benchmarking, and production hardening.

---

## Table of Contents

1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Quick Start](#quick-start)
4. [Manual Deployment](#manual-deployment)
5. [Helm Chart](#helm-chart)
6. [Running Benchmarks on K8s](#running-benchmarks-on-k8s)
7. [Monitoring](#monitoring)
8. [Scaling](#scaling)
9. [Production Checklist](#production-checklist)

---

## Overview

VELOCITY-WorkFlow deploys natively on Kubernetes with all three flavors running side-by-side. The deployment includes:

- **3 Engine Deployments**: Velocity Classic (gRPC), Runtime (HTTP), Embedded (Postgres)
- **PostgreSQL StatefulSet**: Shared database for embedded mode and persistence
- **2 Benchmark Jobs**: gRPC+HTTP benchmarks and Embedded (pgbench) benchmarks
- **Prometheus + Grafana**: Monitoring stack

---

## Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| kubectl | 1.28+ | Kubernetes CLI |
| Helm | 3.14+ | Package management (optional) |
| GKE / any K8s | 1.28+ | Kubernetes cluster |
| Docker | 24+ | Build container images |

---

## Quick Start

### Using the Automated Script

```bash
# Set your GCP project
export PROJECT_ID=velocity-live-test-001

# Run the K8s benchmark suite
bash cloud-bench/cloud_k8s_bench.sh
```

The script:
1. Creates a GKE cluster (or uses existing)
2. Builds and pushes Docker images
3. Applies K8s manifests (all 3 flavors + PostgreSQL)
4. Runs benchmark Jobs inside the cluster
5. Downloads results

### Manual Quick Start

```bash
# Apply all manifests
kubectl apply -f cloud-bench/k8s/k8s_bench_manifests.yaml

# Wait for all pods
kubectl wait --for=condition=ready pod -l app=velocity-classic -n velocity-bench --timeout=120s
kubectl wait --for=condition=ready pod -l app=velocity-runtime -n velocity-bench --timeout=120s
kubectl wait --for=condition=ready pod -l app=velocity-embedded -n velocity-bench --timeout=120s

# Check health
kubectl port-forward svc/velocity-classic 7234:7234 -n velocity-bench &
curl http://localhost:7234/health
```

---

## Manual Deployment

### 1. Create Namespace

```bash
kubectl create namespace velocity-bench
```

### 2. Deploy PostgreSQL

```bash
kubectl apply -f cloud-bench/k8s/k8s_bench_manifests.yaml -l app=postgres
```

Wait for PostgreSQL to be ready:
```bash
kubectl wait --for=condition=ready pod -l app=postgres -n velocity-bench --timeout=120s
```

### 3. Deploy All Three Flavors

```bash
kubectl apply -f cloud-bench/k8s/k8s_bench_manifests.yaml
```

This creates:
- `velocity-classic` Deployment + Service (gRPC on 7234)
- `velocity-runtime` Deployment + Service (HTTP on 7233)
- `velocity-embedded` Deployment + Service (HTTP on 7233, embedded mode)
- `postgres` StatefulSet + Service (PostgreSQL on 5432)

### 4. Verify Deployment

```bash
# Check all pods
kubectl get pods -n velocity-bench

# Check health endpoints
kubectl port-forward svc/velocity-classic 7234:7234 -n velocity-bench &
kubectl port-forward svc/velocity-runtime 7233:7233 -n velocity-bench &

curl http://localhost:7234/health
curl http://localhost:7233/health
```

---

## Helm Chart

```bash
# Install with Helm
helm install velocity ./deploy/helm/velocity \
  --namespace velocity-system --create-namespace \
  --set postgres.password=your_secure_password \
  --set server.replicas=3 \
  --set classic.enabled=true \
  --set runtime.enabled=true \
  --set embedded.enabled=true
```

### Helm Values

```yaml
# values.yaml
server:
  replicas: 3
  resources:
    requests:
      cpu: "4"
      memory: "8Gi"
    limits:
      cpu: "16"
      memory: "32Gi"

classic:
  enabled: true
  grpcPort: 7234

runtime:
  enabled: true
  httpPort: 7233

embedded:
  enabled: true
  httpPort: 7233

postgres:
  password: your_secure_password
  storage: 20Gi
```

---

## Running Benchmarks on K8s

### Benchmark Suite B: Kubernetes

The K8s benchmark suite tests all 3 flavor pairs inside the cluster:

| Benchmark | Velocity | Legacy | Protocol |
|-----------|----------|--------|----------|
| gRPC | Velocity Classic | Temporal | gRPC |
| HTTP | Velocity Runtime | Restate | HTTP |
| Embedded | Velocity Embedded | DBOS | HTTP+Postgres |

### Run the Benchmarks

```bash
# Start the benchmark Jobs
kubectl apply -f cloud-bench/k8s/k8s_bench_manifests.yaml -l app=velocity-bench-runner
kubectl apply -f cloud-bench/k8s/k8s_bench_manifests.yaml -l app=embedded-bench-runner

# Monitor progress
kubectl get pods -n velocity-bench -l job-name=velocity-bench-runner
kubectl get pods -n velocity-bench -l job-name=embedded-bench-runner

# View logs
kubectl logs -f job/velocity-bench-runner -n velocity-bench
kubectl logs -f job/embedded-bench-runner -n velocity-bench
```

### Collect Results

```bash
# Download results from benchmark Jobs
mkdir -p results/k8s
kubectl cp "velocity-bench/$(kubectl get pods -n velocity-bench -l app=velocity-bench-runner -o jsonpath='{.items[0].metadata.name}'):results/" results/k8s/grpc_http/
kubectl cp "velocity-bench/$(kubectl get pods -n velocity-bench -l app=embedded-bench-runner -o jsonpath='{.items[0].metadata.name}'):results/" results/k8s/embedded/
```

---

## Monitoring

### Prometheus Metrics

All three flavors export Prometheus metrics at `/metrics`:

```bash
# Port-forward and scrape
kubectl port-forward svc/velocity-classic 7234:7234 -n velocity-bench &
curl http://localhost:7234/metrics
```

### Grafana Dashboards

Deploy Grafana with pre-built dashboards:

```bash
kubectl apply -f deploy/grafana/
kubectl port-forward svc/grafana 3000:3000 -n velocity-bench &
# Open http://localhost:3000 (admin/admin)
```

### Health Checks

```yaml
# Liveness probe
livenessProbe:
  httpGet:
    path: /health
    port: 7233
  initialDelaySeconds: 5
  periodSeconds: 10

# Readiness probe
readinessProbe:
  httpGet:
    path: /health
    port: 7233
  initialDelaySeconds: 3
  periodSeconds: 5
```

---

## Scaling

### Horizontal Scaling

```bash
# Scale engine replicas
kubectl scale deployment velocity-classic --replicas=5 -n velocity-bench
kubectl scale deployment velocity-runtime --replicas=5 -n velocity-bench
```

### Resource Tuning

```yaml
resources:
  requests:
    cpu: "4"
    memory: "8Gi"
  limits:
    cpu: "16"
    memory: "32Gi"
```

---

## Production Checklist

- [ ] Set PostgreSQL password via Secret (not default)
- [ ] Enable TLS on all engine endpoints
- [ ] Configure resource requests and limits
- [ ] Set up Prometheus alerting rules
- [ ] Configure persistent volume for PostgreSQL
- [ ] Enable network policies
- [ ] Set up log aggregation (JSON structured logs)
- [ ] Configure graceful shutdown (terminationGracePeriodSeconds: 30)
- [ ] Enable encryption at rest (AES-256-GCM key rotation)
- [ ] Set X-Request-Id for request correlation
- [ ] Configure body size limits (10 MB default)
- [ ] Set up backup schedule for PostgreSQL
