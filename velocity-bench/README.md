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
