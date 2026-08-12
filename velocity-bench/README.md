# velocity-bench

Side-by-side benchmark harness: **VELOCITY-WorkFlow vs Temporal**.

Runs identical workloads on both engines and produces granular comparison reports with latency percentiles, throughput, memory, CPU, and error rates.

## Quick Start

```bash
# 1. Start Temporal (requires Docker)
docker-compose -f docker-compose.temporal.yml up -d

# 2. Run smoke test (3 quick workloads, VELOCITY only)
cargo run --release -- --workloads smoke --engine velocity

# 3. Run full comparison
cargo run --release -- --workloads all --format all --output bench_report

# 4. Run specific workload
cargo run --release -- --workload signal_storm --profile stress
```

## Architecture

```
┌─────────────────────┐
│  Workload Definitions │  18 canonical workloads
│  (workloads.rs)       │  Identical on both engines
└──────────┬──────────┘
           │
    ┌──────▼──────┐
    │ BenchmarkEngine │  Common trait interface
    │ (engine.rs)     │  start/signal/query/complete/terminate
    └──┬─────────┬──┘
       │         │
┌──────▼──┐  ┌──▼────────┐
│ VELOCITY │  │ Temporal   │
│ Adapter  │  │ Adapter    │
│ (direct) │  │ (gRPC)     │
└────┬─────┘  └─────┬─────┘
     │              │
     └──────┬───────┘
            │
   ┌────────▼────────┐
   │ MetricsCollector  │  Latency histograms, memory, CPU
   │ (metrics.rs)      │  HdrHistogram, sysinfo
   └────────┬────────┘
            │
   ┌────────▼────────┐
   │ ReportGenerator   │  Side-by-side comparison
   │ (report.rs)       │  Markdown, CSV, JSON
   └──────────────────┘
```

## Workloads (18 Canonical)

| # | Workload | What It Measures |
|---|----------|-----------------|
| 1 | `simple_workflow` | Basic throughput: start → 10 steps → complete × 1000 |
| 2 | `signal_storm` | Signal throughput: 100 signals per workflow × 100 workflows |
| 3 | `query_burst` | Query throughput: 100 queries per workflow × 100 workflows |
| 4 | `high_step` | Step overhead: single workflow with 10K steps |
| 5 | `concurrent_1k` | Scheduling: 1000 concurrent workflows at 100 concurrency |
| 6 | `child_workflows` | Hierarchy: parent spawns 10 children |
| 7 | `saga_pattern` | Transactions: 5-step saga with compensation |
| 8 | `timer_workflow` | Timer accuracy: workflow with sleep |
| 9 | `search_attributes` | Visibility: start with attrs → query by attrs × 1000 |
| 10 | `signal_query_mix` | Mixed: interleaved signals + queries |
| 11 | `batch_operations` | Admin: batch start/terminate/query 5000 |
| 12 | `payload_1kb` | Serialization: 1KB payloads × 1000 |
| 13 | `payload_1mb` | Large payloads: 1MB payloads × 100 |
| 14 | `namespace_isolation` | Isolation: workflows across 5 namespaces |
| 15 | `throughput_ceiling` | Max throughput: 100K workflows, 1000 concurrency |
| 16 | `memory_scaling` | Memory: measure at 1K/10K/100K workflows |
| 17 | `cold_start` | First execution after engine startup |
| 18 | `crash_recovery` | Recovery: start → crash → restart → verify |

## Metrics Collected

| Metric | Granularity | Unit |
|--------|------------|------|
| Start latency | p50/p90/p95/p99/p999/min/max/mean | µs |
| Signal latency | p50/p90/p95/p99/p999/min/max/mean | µs |
| Query latency | p50/p90/p95/p99/p999/min/max/mean | µs |
| Completion latency | p50/p90/p95/p99/p999/min/max/mean | µs |
| Throughput | operations/second | ops/sec |
| Peak memory | RSS | MB |
| Peak CPU | utilization | % |
| Error rate | by category | % |
| Total operations | count | — |

## Report Format

### Markdown (default)
```
# VELOCITY-WorkFlow vs Temporal — Benchmark Report

## Summary
| Metric | Value |
|--------|-------|
| VELOCITY wins | 14 |
| Temporal wins | 2 |
| Comparable | 2 |
| Avg throughput delta | +245.3% |

## Detailed Comparison
| Workload | VELOCITY ops/s | Temporal ops/s | Δ | VELOCITY p99 | Temporal p99 | Verdict |
|----------|---------------|----------------|---|-------------|-------------|---------|
| simple   | 125,000       | 8,500          | +1370% | 45µs | 1,200µs | VELOCITY dominates |
```

### CSV
Machine-parseable for graphing and further analysis.

### JSON
Full structured data including all latency percentiles and memory samples.

## CLI Options

```
Usage: velocity-bench [OPTIONS]

Options:
      --workloads <WORKLOADS>    Which workloads: all, smoke [default: smoke]
      --workload <NAME>          Run a single specific workload
      --engine <ENGINE>          Which engine(s): both, velocity, temporal [default: both]
      --temporal-address <ADDR>  Temporal gRPC address [default: http://localhost:7233]
      --format <FORMAT>          Output: markdown, csv, json, all [default: markdown]
  -o, --output <PATH>            Output file (stdout if not specified)
      --profile <PROFILE>        Workload profile: quick, standard, stress [default: standard]
  -v, --verbose                  Enable verbose logging
  -h, --help                     Print help
  -V, --version                  Print version
```

## Profiles

| Profile | Workflows | Concurrency | Duration | Use Case |
|---------|-----------|-------------|----------|----------|
| `quick` | 10 | 4 | 5s | Smoke testing |
| `standard` | 100-1000 | 10 | 30s | Regular benchmarks |
| `stress` | 10K-100K | 100-1000 | 60-120s | Push to limits |

## Interpreting Results

### Verdicts

| Verdict | Criteria |
|---------|----------|
| **VELOCITY dominates** | >50% faster AND lower p99 latency |
| **VELOCITY faster** | >20% faster throughput |
| **Comparable** | Within ±20% throughput |
| **Temporal faster** | >20% faster throughput |

### Viability Assessment

The overall verdict answers the question: *"Is VELOCITY a viable Temporal replacement?"*

- **"VELOCITY is a viable Temporal replacement"** — wins in >2/3 of workloads
- **"VELOCITY is competitive"** — wins in some, loses in some
- **"More optimization needed"** — Temporal wins in most workloads

## Prerequisites

- **Rust** (stable, 1.75+)
- **Docker** (for Temporal server)
- **protoc** (optional, for full Temporal gRPC integration)

## Project Structure

```
velocity-bench/
├── Cargo.toml
├── build.rs
├── docker-compose.temporal.yml
├── README.md
└── src/
    ├── lib.rs          # Library root
    ├── main.rs         # CLI orchestrator
    ├── engine.rs       # BenchmarkEngine trait + adapters
    ├── workloads.rs    # 18 canonical workload definitions
    ├── metrics.rs      # Metrics collection (latency, memory, CPU)
    └── report.rs       # Side-by-side comparison report generator
```
