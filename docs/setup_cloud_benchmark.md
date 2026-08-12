# VELOCITY-WorkFlow Cloud Benchmark Guide

> Run comprehensive benchmarks across all 3 flavors and their legacy competitors in the cloud.

---

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Prerequisites](#prerequisites)
4. [Suite A: Bare VM Benchmarks](#suite-a-bare-vm-benchmarks)
5. [Suite B: Kubernetes Benchmarks](#suite-b-kubernetes-benchmarks)
6. [Benchmark Profiles](#benchmark-profiles)
7. [Results and Analysis](#results-and-analysis)
8. [Cost Estimation](#cost-estimation)
9. [Troubleshooting](#troubleshooting)

---

## Overview

The VELOCITY cloud benchmark suite provides apples-to-apples performance comparisons across all 3 Velocity flavors and their legacy competitors:

| Pair | Velocity | Legacy | Protocol |
|------|----------|--------|----------|
| **gRPC** | Velocity Classic | Temporal | gRPC (33 RPCs) |
| **HTTP** | Velocity Runtime | Restate | HTTP/1.1 JSON |
| **Embedded** | Velocity Embedded | DBOS | HTTP + PostgreSQL |

Two benchmark suites:
- **Suite A (Bare VM)**: 6 dedicated GCE VMs, one per engine
- **Suite B (Kubernetes)**: All 6 engines on a GKE cluster

---

## Architecture

### Suite A: 6 Dedicated VMs

```
┌─────────────────────────────────────────────────────────────────────────┐
│  GCP Project: velocity-live-test-001 / Zone: us-east1-b                │
│                                                                          │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐      │
│  │ velocity-classic │  │ temporal-bench   │  │ velocity-runtime │      │
│  │ e2-standard-4    │  │ e2-standard-4    │  │ e2-standard-4    │      │
│  │ gRPC :7234       │  │ gRPC :7233       │  │ HTTP :7233       │      │
│  └──────────────────┘  └──────────────────┘  └──────────────────┘      │
│                                                                          │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐      │
│  │ restate-bench    │  │ velocity-embedded│  │ dbos-bench       │      │
│  │ e2-standard-4    │  │ e2-standard-4    │  │ e2-standard-4    │      │
│  │ HTTP :8080       │  │ HTTP+PG :7233    │  │ HTTP+PG :3000    │      │
│  └──────────────────┘  └──────────────────┘  └──────────────────┘      │
└─────────────────────────────────────────────────────────────────────────┘
```

Each VM gets its own dedicated e2-standard-4 (4 vCPU, 16 GB RAM, 50 GB SSD) — no resource contention between engines.

### Suite B: GKE Cluster

All engines deployed to a single GKE cluster with shared PostgreSQL. Benchmark Jobs run inside the cluster for minimal network overhead.

---

## Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| gcloud CLI | Latest | GCP management |
| Python | 3.10+ | Orchestration scripts |
| SSH | Any | VM access |
| kubectl | 1.28+ | Kubernetes (Suite B only) |
| GCP project | Billing enabled | Compute resources |

### Setup GCP

```bash
# Authenticate
gcloud auth login
gcloud config set project velocity-live-test-001

# Enable required APIs
gcloud services enable compute.googleapis.com
gcloud services enable container.googleapis.com
```

### Windows SSH Configuration

On Windows, `gcloud` uses PuTTY's `plink` by default, which wraps remote commands with Windows CMD syntax that breaks on Linux. Force OpenSSH instead:

```powershell
# PowerShell (set for current session)
$env:CLOUDSDK_CORE_PASSHROUGH_SSH_COMMAND = "ssh"

# Or set permanently
[System.Environment]::SetEnvironmentVariable("CLOUDSDK_CORE_PASSHROUGH_SSH_COMMAND", "ssh", "User")
```

The benchmark scripts (`run_cloud_bench_v3.py`) set this automatically.

---

## Suite A: Bare VM Benchmarks

### Step 1: Provision VMs

```bash
# Option A: Using Python script (recommended)
python cloud-bench/provision_vms.py

# Option B: Using gcloud directly
gcloud compute instances create velocity-runtime velocity-embedded temporal-bench restate-bench dbos-bench \
  --zone=us-east1-b --machine-type=e2-standard-4 \
  --image-family=ubuntu-2204-lts --image-project=ubuntu-os-cloud \
  --boot-disk-size=50GB --boot-disk-type=pd-ssd \
  --tags=velocity-bench
```

### Step 2: Setup All VMs

```bash
# Setup installs Rust, Docker, Node.js, PostgreSQL on each VM
python cloud-bench/run_cloud_bench_v3.py setup
```

### Step 3: Build and Upload

```bash
# Uploads the repo tarball and builds Rust binaries
python cloud-bench/run_cloud_bench_v3.py build
```

### Step 4: Run Benchmarks

```bash
# Runs all benchmarks in parallel across all 6 VMs
python cloud-bench/run_cloud_bench_v3.py bench
```

### Step 5: Collect Results

```bash
# Downloads results from all VMs
python cloud-bench/run_cloud_bench_v3.py collect
```

### Using the Master Script

Run everything in one command:

```bash
# PowerShell (Windows)
.\cloud-bench\cloud_3flavor.ps1

# Bash (Linux/macOS)
bash cloud-bench/cloud_3flavor.sh
```

---

## Suite B: Kubernetes Benchmarks

### Step 1: Create GKE Cluster

```bash
gcloud container clusters create velocity-bench \
  --zone=us-east1-b \
  --num-nodes=4 \
  --machine-type=e2-standard-4 \
  --project=velocity-live-test-001
```

### Step 2: Run the K8s Benchmark

```bash
bash cloud-bench/cloud_k8s_bench.sh
```

This script:
1. Builds Docker images for all engines
2. Pushes to Google Container Registry
3. Applies K8s manifests (Deployments, Services, StatefulSet)
4. Waits for all pods to be ready
5. Runs `velocity-bench-runner` Job (gRPC + HTTP benchmarks)
6. Runs `embedded-bench-runner` Job (pgbench + Embedded benchmarks)
7. Downloads results

---

## Benchmark Profiles

| Profile | Duration | Use Case |
|---------|----------|----------|
| `quick` | ~5 min | Smoke test, CI validation |
| `standard` | ~15 min | Full comparison report |
| `stress` | ~45 min | Maximum load testing |

### Workloads

Each benchmark runs these workload categories:

**gRPC (Classic vs Temporal):**
- simple_workflow, signal_storm, query_burst, high_step (10K steps)
- concurrent_1k, child_workflows, saga_pattern, timer_workflow
- search_attributes, batch_operations, payload_1kb/1mb
- throughput_ceiling, memory_scaling, cold_start, crash_recovery

**HTTP (Runtime vs Restate):**
- Sequential workflow CRUD
- Concurrent workflow creation (100 parallel)
- Sustained load (30 seconds)

**Embedded (Embedded vs DBOS):**
- pgbench raw Postgres TPS baseline
- Sequential workflow CRUD via HTTP
- Concurrent workflow creation
- Sustained load

---

## Results and Analysis

### Output Format

Results are saved as JSON files per flavor:

```
cloud-bench/results/
├── velocity-classic/
│   └── classic_results.json
├── temporal/
│   └── temporal_results.json
├── velocity-runtime/
│   └── runtime_results.json
├── restate/
│   └── restate_results.json
├── velocity-embedded/
│   └── embedded_results.json
├── dbos/
│   └── dbos_results.json
└── k8s/
    ├── grpc_http/
    └── embedded/
```

### Aggregate Results

```bash
# Generate comparison report
python cloud-bench/aggregate_results.py
```

This produces:
- `benchmark_comparison.json` — Machine-readable comparison
- `benchmark_comparison.md` — Human-readable Markdown report

### Metrics Collected

| Metric | Description |
|--------|-------------|
| ops/sec | Operations per second (throughput) |
| p50_latency | Median latency |
| p95_latency | 95th percentile latency |
| p99_latency | 99th percentile latency |
| p999_latency | 99.9th percentile latency |
| peak_memory_mb | Peak memory usage |
| error_rate | Percentage of failed operations |

---

## Cost Estimation

### Suite A (6 VMs)

| Resource | Cost/Hour | Duration | Total |
|----------|-----------|----------|-------|
| 6x e2-standard-4 | $0.67 | ~2 hours | ~$8.04 |
| 6x 50GB pd-ssd | $0.014 | ~2 hours | ~$0.17 |
| **Total** | | | **~$8.21** |

### Suite B (GKE)

| Resource | Cost/Hour | Duration | Total |
|----------|-----------|----------|-------|
| 4x e2-standard-4 (nodes) | $0.53 | ~2 hours | ~$4.24 |
| GKE management fee | $0.10 | ~2 hours | ~$0.20 |
| **Total** | | | **~$4.44** |

### Cleanup

```bash
# Delete VMs
gcloud compute instances delete velocity-runtime velocity-embedded temporal-bench restate-bench dbos-bench \
  --zone=us-east1-b --project=velocity-live-test-001 --quiet

# Delete GKE cluster
gcloud container clusters delete velocity-bench --zone=us-east1-b --quiet
```

---

## Troubleshooting

### SSH Connection Refused

```bash
# Ensure firewall allows SSH
gcloud compute firewall-rules list --filter="name:default-allow-ssh"

# If missing, create it
gcloud compute firewall-rules create allow-ssh --allow tcp:22 --project=velocity-live-test-001
```

### Build Fails on VM

```bash
# SSH in and check
gcloud compute ssh velocity-classic --zone=us-east1-b

# Check Rust installation
source $HOME/.cargo/env
cargo --version

# Check disk space
df -h
```

### Benchmark Timeout

```bash
# Use the quick profile for faster results
python cloud-bench/run_cloud_bench_v3.py bench --profile quick
```
