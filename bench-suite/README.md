# Velocity Benchmark Suite

Multi-platform benchmark suite for comparing 6 workflow engines across Docker, Kubernetes, and cloud environments.

## Engines

| Engine | Language | Transport | Storage | Port |
|--------|----------|-----------|---------|------|
| Velocity Classic | Rust | gRPC | WAL (fsync) | 7234 |
| Velocity Runtime | Rust | gRPC + HTTP | WAL (fsync) | 7234 |
| Velocity Embedded | Rust | gRPC | SQLite + WAL | 7234 |
| DBOS | Python | HTTP | PostgreSQL | 8081 |
| Restate | Node.js | HTTP | Durable journal | 8082/9082 |
| Temporal | Python | gRPC + HTTP | Event-sourcing | 7233 |

## Quickstart

### Docker (local)

```bash
# Start all 6 engines
docker compose up -d

# Wait for health checks
bash scripts/wait_for_healthy.sh

# Run smoke test on each engine
bash scripts/run_local.sh smoke

# Run short benchmark (~5 min per engine)
bash scripts/run_local.sh short
```

### Kubernetes (local Docker Desktop)

```bash
# Deploy all engines
bash scripts/run_k8s.sh deploy

# Run smoke test
bash scripts/run_k8s.sh smoke

# Run short benchmark
bash scripts/run_k8s.sh short

# Clean up
bash scripts/run_k8s.sh cleanup
```

### Cloud (GCP)

```bash
# Deploy to GCE VMs
bash cloud/deploy_gce.sh

# Deploy to GKE
bash cloud/deploy_gke.sh

# Collect results
bash cloud/collect_results.sh
```

## Workload Profiles

| Profile | Ops | Steps | Duration | Purpose |
|---------|-----|-------|----------|---------|
| `smoke` | 10 | 5 | ~30s | Sanity check |
| `short` | 100 | 10 | ~5 min | Local Docker verification |
| `standard` | 1000 | 10 | ~10-15 min | Real benchmark |
| `stress` | 10000 | 10 | ~30 min | Cloud only |

## Directory Structure

```
bench-suite/
  docker/              # Per-engine Dockerfiles
  docker-compose.yml   # All 6 engines + dependencies
  k8s/                 # Kubernetes manifests
  kustomize/           # Kustomize overlays (local, GKE standard, GKE stress)
  cloud/               # GCE/GKE deployment scripts
  scripts/             # Orchestrator and utility scripts
  scenarios/           # Workload definitions (workloads.json)
```

## Results

Benchmark results are output as JSON in `results/` with per-engine breakdowns including:
- Throughput (ops/sec)
- Latency percentiles (p50, p95, p99)
- Memory usage (RSS)
- Error rates

See `scenarios/workloads.json` for the full workload matrix and `scenarios/README.md` for scenario documentation.
