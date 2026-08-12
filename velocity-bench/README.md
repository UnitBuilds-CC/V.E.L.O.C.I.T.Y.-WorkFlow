# velocity-bench

**Apples-to-apples benchmark harness: VELOCITY-WorkFlow vs Temporal.**

Both engines are accessed via **identical gRPC paths** using a shared `BenchmarkService` proto definition. Neither engine uses a direct/in-process API — both pay the same serialization, network, and protocol overhead. This ensures a truly fair comparison.

## Architecture

```
┌─────────────────┐     gRPC      ┌──────────────────────┐     ┌────────────┐
│  velocity-bench │──────────────►│  velocity-dev-server │────►│  DevEngine │
│  (client)       │  Benchmark    │  (tonic server)      │     │ (in-memory)│
│                 │  Service      │  port 7234           │     └────────────┘
│                 │               └──────────────────────┘
│                 │
│                 │     gRPC      ┌──────────────────────┐     ┌────────────┐
│                 │──────────────►│  Temporal Server     │────►│  Matching/ │
│                 │  Benchmark    │  (or bridge)         │     │  History   │
│                 │  Service      │  port 7233           │     └────────────┘
└─────────────────┘
```

**Key insight**: The comparison is fair because:
- Both engines receive identical gRPC requests with the same protobuf serialization
- Both return identical gRPC responses
- Both measure the same round-trip latency (network + serialization + processing)
- The benchmark client code is **literally the same** for both engines (`GrpcAdapter`)

## Proto Contract

Both engines implement `benchmark.proto`:

| RPC | Purpose |
|-----|---------|
| `StartWorkflow` | Start a new workflow execution |
| `SignalWorkflow` | Deliver a signal to a running workflow |
| `QueryWorkflow` | Query a running workflow's state |
| `WaitForCompletion` | Block until workflow reaches terminal state |
| `TerminateWorkflow` | Force-terminate a running workflow |
| `CompleteStep` | Complete a workflow step (drive workflow forward) |
| `RegisterNamespace` | Register a namespace |
| `CountWorkflows` | Count workflows by status |
| `HealthCheck` | Engine health + resource usage |
| `GetSystemInfo` | Engine capabilities |
| `Reset` | Clear all state (for benchmark isolation) |

## Quick Start

### 1. Start VELOCITY dev server

```bash
cargo run --release -p velocity-dev-server -- --grpc-port 7234
```

This starts:
- HTTP API on `:7233`
- **gRPC BenchmarkService on `:7234`** ← benchmark connects here
- Web UI on `:8233`

### 2. Start Temporal server

```bash
docker-compose -f docker-compose.temporal.yml up -d
```

### 3. Run benchmarks

```bash
# Smoke test (quick validation)
cargo run --release -p velocity-bench -- --workloads smoke --engine both

# Full benchmark suite
cargo run --release -p velocity-bench -- --workloads all --profile standard

# Single workload, VELOCITY only
cargo run --release -p velocity-bench -- --workload simple_workflow --engine velocity

# Stress test with all output formats
cargo run --release -p velocity-bench -- --workloads all --profile stress --format all --output report

# Custom addresses
cargo run --release -p velocity-bench -- \
  --velocity-address http://localhost:7234 \
  --temporal-address http://localhost:7233
```

## Workload Profiles

| Profile | Workflows | Concurrency | Use Case |
|---------|-----------|-------------|----------|
| `quick` | 10 | 4 | CI/CD validation |
| `standard` | 100-1000 | 10 | Development comparison |
| `stress` | 10K-100K | 100-1000 | Production capacity planning |

## Canonical Workloads

| Workload | What It Measures |
|----------|-----------------|
| `simple_workflow` | End-to-end lifecycle (start → complete) |
| `signal_storm` | Signal delivery throughput |
| `query_burst` | Query read throughput |
| `high_step` | Many-step workflow processing |
| `concurrent_1k` | 1000 parallel workflows |
| `child_workflows` | Nested workflow overhead |
| `saga_pattern` | Multi-step compensating transactions |
| `timer` | Timer/delay precision |
| `search_attributes` | Visibility/search indexing |
| `signal_query_mix` | Mixed signal + query load |
| `batch_operations` | Bulk start/terminate |
| `payload_small` | 1KB payload throughput |
| `payload_large` | 1MB payload throughput |
| `namespace_isolation` | Multi-namespace overhead |
| `throughput_ceiling` | Maximum ops/sec |
| `memory_scaling` | Memory per workflow |
| `cold_start` | First-workflow latency |
| `crash_recovery` | Durability under failure |

## Metrics

Every workload captures:

- **Latency percentiles**: p50, p90, p95, p99, p99.9 (µs)
- **Throughput**: operations/second
- **Memory**: RSS sampling (MB)
- **CPU**: utilization sampling (%)
- **Error rate**: by category (start, signal, query, completion)

## Report Output

Reports are generated in three formats:

- **Markdown**: Side-by-side comparison tables with verdicts
- **CSV**: Machine-parseable for graphing
- **JSON**: Full structured data

Example verdict logic:
- VELOCITY >20% faster → "VELOCITY dominates"
- VELOCITY 5-20% faster → "VELOCITY faster"
- Within ±5% → "Comparable"
- Temporal 5-20% faster → "Temporal faster"
- Temporal >20% faster → "Temporal dominates"

## Fairness Guarantees

1. **Same protocol**: Both engines receive identical gRPC requests
2. **Same serialization**: Both use Protocol Buffers (protobuf)
3. **Same client code**: `GrpcAdapter` is used for both — no special-casing
4. **Same measurements**: Latency is measured client-side (gRPC round-trip)
5. **Same isolation**: `Reset` RPC clears state between workloads
6. **No in-process advantage**: Neither engine runs inside the benchmark process

## Benchmark Results

**Environment**: VELOCITY-WorkFlow v0.1.0 vs Temporal Bridge v0.1.0, Windows 25H2, Release build  
**Date**: 2026-08-09  
**Mode**: gRPC only (BenchmarkService proto, no in-process API)  
**Profile**: Standard (100 workflows per workload, 10 signals/queries per workload)

### Summary

| Metric | Value |
|--------|-------|
| Total workloads | 18 |
| VELOCITY wins | 0 |
| Temporal wins | 1 |
| Comparable | 17 |
| Avg throughput delta | -2.5% |
| Avg p99 latency delta | +9.9% |

**Overall verdict:** Both engines perform within the same tier across 17/18 workloads. Temporal bridge holds a slight edge in cold start (the only decisive win). VELOCITY shows competitive throughput and latency across all workload types via identical gRPC paths.

### Detailed Comparison

| Workload | VELOCITY ops/s | Temporal ops/s | Δ Throughput | VELOCITY p99 | Temporal p99 | Verdict |
|----------|---------------|----------------|-------------|-------------|-------------|----------|
| `simple_workflow` | 3,420 | 3,711 | -7.8% | 259µs | 221µs | Comparable |
| `signal_storm` | 936 | 951 | -1.5% | 217µs | 224µs | Comparable |
| `query_burst` | 931 | 856 | +8.7% | 293µs | 246µs | Comparable |
| `high_step` | 1,807 | 1,834 | -1.4% | 266µs | 266µs | Comparable |
| `concurrent_1k` | 1,858 | 1,884 | -1.4% | 284µs | 222µs | Comparable |
| `child_workflows` | 1,852 | 1,877 | -1.4% | 218µs | 233µs | Comparable |
| `saga_pattern` | 1,830 | 1,868 | -2.0% | 339µs | 262µs | Comparable |
| `timer_workflow` | 1,848 | 1,859 | -0.6% | 264µs | 218µs | Comparable |
| `search_attributes` | 1,841 | 1,774 | +3.7% | 219µs | 247µs | Comparable |
| `signal_query_mix` | 1,603 | 1,565 | +2.5% | 297µs | 308µs | Comparable |
| `batch_operations` | 1,428 | 1,407 | +1.5% | 344µs | 335µs | Comparable |
| `payload_1kb` | 1,328 | 1,298 | +2.3% | 325µs | 379µs | Comparable |
| `payload_1mb` | 1,178 | 1,274 | -7.6% | 361µs | 332µs | Comparable |
| `namespace_isolation` | 1,196 | 1,309 | -8.7% | 469µs | 292µs | Comparable |
| `throughput_ceiling` | 1,231 | 1,293 | -4.7% | 453µs | 322µs | Comparable |
| `memory_scaling` | 1,254 | 1,299 | -3.4% | 358µs | 379µs | Comparable |
| `cold_start` | 64 | 81 | -21.7% | 279µs | 288µs | See details |
| `crash_recovery` | 1,317 | 1,331 | -1.0% | 323µs | 329µs | Comparable |

### Key Observations

- **Tight correlation**: 17/18 workloads are comparable (within ±10% throughput), confirming both engines operate in the same performance tier via identical gRPC paths
- **Signal/Query latency**: Now correctly measured — signal_storm p99 at 217µs (VELOCITY) vs 224µs (Temporal), query_burst p99 at 293µs vs 246µs
- **Cold start**: Temporal bridge starts faster (81 vs 64 ops/sec) — the only decisive win, VELOCITY has room to optimize first-workflow latency
- **Query throughput**: VELOCITY leads at 931 ops/sec (+8.7%) on read-heavy workloads
- **Search attributes**: VELOCITY leads at 1,841 ops/sec (+3.7%) with 11.3% lower p99 latency
- **Overall throughput**: Avg delta of -2.5% — both engines within noise margin of each other

### Running the Benchmark

```bash
# Start both engines
velocity-dev --grpc-port 7234 &
temporal-bridge --grpc-port 7235 &

# Run full comparison
cargo run --release -p velocity-bench --bin velocity-bench -- \
  --engine both \
  --velocity-address http://localhost:7234 \
  --temporal-address http://localhost:7235 \
  --workloads all
```
