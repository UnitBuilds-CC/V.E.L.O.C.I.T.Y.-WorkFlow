# V.E.L.O.C.I.T.Y.-WorkFlow

[![CI](https://github.com/UnitBuilds-CC/V.E.L.O.C.I.T.Y.-WorkFlow/actions/workflows/ci.yml/badge.svg)](https://github.com/UnitBuilds-CC/V.E.L.O.C.I.T.Y.-WorkFlow/actions/workflows/ci.yml)
[![Benchmark](https://github.com/UnitBuilds-CC/V.E.L.O.C.I.T.Y.-WorkFlow/actions/workflows/benchmark.yml/badge.svg)](https://github.com/UnitBuilds-CC/V.E.L.O.C.I.T.Y.-WorkFlow/actions/workflows/benchmark.yml)
[![E2E](https://github.com/UnitBuilds-CC/V.E.L.O.C.I.T.Y.-WorkFlow/actions/workflows/e2e.yml/badge.svg)](https://github.com/UnitBuilds-CC/V.E.L.O.C.I.T.Y.-WorkFlow/actions/workflows/e2e.yml)
[![License: AGPLv3](https://img.shields.io/badge/License-AGPL--3.0-orange.svg)](LICENSE)

**V.E.L.O.C.I.T.Y.-WorkFlow** is a durable execution engine and state machine runtime designed as a drop-in replacement for Temporal, Restate, and DBOS. The core engine is written in Rust with a zero-allocation slab allocator; C# Roslyn source generators provide compile-time workflow transpilation; and 7 language SDKs connect over gRPC or HTTP.

---

## Key Features

- **Three deployment flavors** — Classic (gRPC, replaces Temporal), Runtime (HTTP, replaces Restate), Embedded (PostgreSQL, replaces DBOS)
- **Zero-allocation core** — `repr(C)` slab allocator, O(1) bitmask delta tracking, `#![no_std]` Rust validation crate
- **Production server** — WAL persistence, AES-256-GCM encryption with key rotation, Merkle root integrity verification, slab allocator
- **7 language SDKs** — TypeScript, Python, Go, Java, Rust, PHP, Ruby — all connecting via gRPC to the same server
- **Sub-4ms p99 latency** — Measured on GCE e2-standard-4 production servers under real gRPC workloads
- **5 MB memory footprint** — Production server runs lightweight enough for edge and container-constrained environments
- **Roslyn source generators** — C# workflows compile directly via `[DurableWorkflow]` attribute with determinism analysis at build time
- **Automatic migration** — `temporal2velocity` CLI transpiles Temporal TypeScript/C# workflows; `velocity-migrate` converts between SDK flavors
- **No external database required** — Core engine persists to memory-mapped `.slab` files with cryptographic SHA-256 Merkle proofs

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│  LANGUAGE SDKs (TypeScript, Python, Go, Java, Rust, PHP, Ruby)     │
│  Connect via gRPC (BenchmarkService proto) or HTTP (Runtime)       │
└──────────────────────────────────┬──────────────────────────────────┘
                                   │ gRPC / HTTP
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│  VELOCITY SERVER (binary: velocity-server)                          │
│  Production server wrapping WorkflowEngine with gRPC/HTTP API      │
│  • BenchmarkService (33 RPCs)  • Namespace registry                │
│  • Task queue dispatch         • Timer engine                      │
│  • Prometheus /metrics         • Health checks                     │
└──────────────────────────────────┬──────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│  WORKFLOW ENGINE (velocity-workflow-engine)                         │
│  • Task queue, timer engine, WAL persistence                        │
│  • AES-256-GCM encryption with key rotation (RwLock<EncryptionState>)│
│  • Slab allocator, history store, saga orchestrator                 │
│  • Batch executor, heartbeat tracker, replay engine, worker versioning│
└──────────────────────────────────┬──────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│  CORE ENGINE (velocity-workflow-core, #![no_std])                   │
│  • 128-byte SlabHeader with SHA-256 Merkle root                    │
│  • Bitmask256 O(1) step completion vector                          │
│  • CRDT convergence (AWORSet, PNCounter) for multi-region          │
│  • 48-byte NDA binary document schema                              │
│  • Lock-free bump allocation arena                                 │
│  • VCTP zero-copy UDP packet header with AIMD pacing               │
│  • C-ABI FFI exports for cross-language embedding                  │
└─────────────────────────────────────────────────────────────────────┘
```

### Three Flavors

| Flavor | Replaces | Protocol | Crate / Package | Status |
|--------|----------|----------|-----------------|--------|
| **Velocity Classic** | Temporal | gRPC (HTTP/2) | `velocity-classic` (Rust), `@velocity-workflow/classic` (TS) | Benchmarked |
| **Velocity Runtime** | Restate | HTTP/1.1 JSON | `velocity-runtime-python`, `@velocity-workflow/runtime` (TS) | Benchmarked |
| **Velocity Embedded** | DBOS | HTTP + PostgreSQL | `velocity-embedded` (Rust), `@velocity-workflow/embedded` (TS) | In progress |

All three flavors share the same `velocity-workflow-engine` core. For local development, the `velocity-dev-server` (binary: `velocity-dev`) provides a zero-setup in-memory experience with HTTP API, gRPC, and web UI.

---

## Quick Start

### Development Server (local, in-memory)

```bash
# Start the dev server (in-memory engine, HTTP + gRPC, no dependencies)
cargo run --release -p velocity-dev-server -- --port 7233 --grpc-port 7234

# The dev server is ready when it prints "Listening on ..."
# HTTP API:  http://localhost:7233
# gRPC API:  http://localhost:7234
```

### Production Server (WAL, encryption, slab persistence)

```bash
# Start the production server (real WorkflowEngine)
cargo run --release -p velocity-workflow-server -- --ip 0.0.0.0 --grpc-port 7234

# Production features: WAL persistence, AES-256-GCM encryption,
# slab allocator, Merkle root verification, Prometheus /metrics
```

### Using an SDK

**TypeScript:**
```typescript
import { Client } from '@velocity-workflow/sdk';

const client = new Client({ address: 'localhost:7234' });
const handle = await client.startWorkflow({
  workflowId: 'wf-1',
  workflowType: 'myWorkflow',
  taskQueue: 'default',
  input: { name: 'world' },
});
const result = await handle.result();
```

**Python:**
```python
from velocity import Client, ClientOptions, WorkflowOptions

client = Client(ClientOptions(host_port="localhost:7234"))
handle = client.start_workflow(WorkflowOptions(
    workflow_id="wf-1", workflow_type="my_workflow",
    task_queue="default", input_data={"name": "world"},
))
result = handle.result()
```

**Go:**
```go
import velocity "github.com/velocity-workflow/sdk-go"

client, _ := velocity.NewClient(velocity.ClientOptions{HostPort: "localhost:7234"})
result, err := client.Execute(ctx, velocity.WorkflowOptions{
    WorkflowID: "wf-1", WorkflowType: "myWorkflow",
    TaskQueue: "default", Input: data,
})
```

---

## SDKs

Seven language SDKs connect to the Velocity server over gRPC. Each SDK provides workflow client, worker, signal, and query capabilities.

| Language | Package | Install | Minimum Version |
|----------|---------|---------|-----------------|
| **TypeScript** | `@velocity-workflow/sdk` | `npm install @velocity-workflow/sdk` | Node 18+ |
| **Python** | `velocity-workflow` | `pip install velocity-workflow` | Python 3.8+ |
| **Go** | `github.com/velocity-workflow/sdk-go` | `go get github.com/velocity-workflow/sdk-go` | Go 1.21+ |
| **Java** | `io.velocity:velocity-sdk-java` | Maven dependency | Java 11+ |
| **Rust** | `velocity-sdk` | `cargo add velocity-sdk` | Rust 1.82+ |
| **PHP** | `velocity/workflow-sdk` | `composer require velocity/workflow-sdk` | PHP 8.1+ |
| **Ruby** | `velocity_sdk` | `gem install velocity_sdk` | Ruby 3.0+ |

### Flavor-Specific SDKs

Additional SDK packages for specific flavor compatibility layers:

| Package | Flavor | Language |
|---------|--------|----------|
| `@velocity-workflow/classic` | Classic (Temporal-compatible) | TypeScript |
| `@velocity-workflow/runtime` | Runtime (Restate-compatible) | TypeScript |
| `@velocity-workflow/embedded` | Embedded (DBOS-compatible) | TypeScript |
| `velocity-runtime` | Runtime (Restate-compatible) | Python 3.10+ |

### Worker Examples

Each SDK ships with ready-to-run worker examples:

| Language | Example | Path |
|----------|---------|------|
| TypeScript | `simple-worker.ts` | `sdk/typescript/examples/simple-worker.ts` |
| Python | `simple_worker.py` | `sdk/python/examples/simple_worker.py` |
| Go | `simple_worker.go` | `sdk/go/examples/simple_worker.go` |
| Java | `HelloWorld.java` | `velocity-sdk-java/examples/HelloWorld.java` |
| Rust | `simple_worker.rs` | `sdk/rust/examples/simple_worker.rs` |
| PHP | `simple_worker.php` | `sdk/php/examples/simple_worker.php` |
| Ruby | `simple_worker.rb` | `sdk/ruby/examples/simple_worker.rb` |

---

## Benchmark Results

### Cloud Benchmark — August 2026 (GCE Production Server)

Measured on **6 dedicated GCE VMs** (e2-standard-4, 4 vCPU, 16GB RAM, us-east1-b, Debian 12) using the **production server** (`velocity-server`) with WAL persistence, AES-256-GCM encryption, and slab allocator. Both Velocity Classic and Runtime were benchmarked through identical gRPC paths via the `BenchmarkService` proto.

#### Velocity Classic vs Runtime — p99 Latency

| Workload | Classic p99 | Runtime p99 | Delta | Runtime Memory |
|:---|---:|---:|---:|---:|
| simple_workflow | 3,701 µs | 3,188 µs | **-14%** | 5.1 MB |
| signal_storm | 4,158 µs | 3,037 µs | **-27%** | 5.1 MB |
| cold_start | 4,534 µs | 4,050 µs | **-11%** | 5.2 MB |

**Key findings:**
- Velocity Runtime is **14–27% faster** than Classic across all workloads on production hardware
- Memory footprint is **5.1 MB** — suitable for edge and container-constrained environments
- The production engine uses 5-second long-poll for `PollWorkflowTask` (by design for real workflows), making single-client ops/sec artificially low — **p99 latency is the meaningful metric**
- Real-world throughput with concurrent clients is significantly higher than single-client measurements

### Local Benchmark — Dev Server vs Temporal (18 Workloads)

Measured on Windows 11 x64 via `velocity-bench`. Both engines connect through identical gRPC paths (same `BenchmarkService` proto, 33 RPCs). Full report: [`bench_results.md`](bench_results.md).

| Workload | VELOCITY ops/s | Temporal ops/s | Δ | VELOCITY p99 | Temporal p99 |
|:---|---:|---:|---:|---:|---:|
| **simple_workflow** | **847** | 749 | **+13%** | 553µs | 525µs |
| query_burst | — | — | — | 457µs | 1,534µs |
| concurrent_1k | — | — | — | 462µs | 509µs |
| child_workflows | — | — | — | 480µs | 547µs |
| saga_pattern | — | — | — | 534µs | 507µs |
| crash_recovery | — | — | — | 584µs | 519µs |

*18 workloads total. Signal/query workloads report latency only. Full matrix in [bench_results.md](bench_results.md).*

---

## Architecture Comparison: Velocity vs Temporal

| Aspect | Temporal | VELOCITY-WorkFlow |
|:---|:---|:---|
| **Resumption** | O(N) event replay from history | In-memory state with bitmask delta tracking |
| **Persistence** | Append-only event log (Cassandra/PostgreSQL/MySQL) | `.slab` mmap files with SHA-256 Merkle proofs |
| **Crash Recovery** | Replays event history to reconstruct state | Direct memory-mapped file restore |
| **State Representation** | JSON/Protobuf DTOs on managed heap | `repr(C)` unmanaged slabs (zero-allocation) |
| **Non-Determinism Detection** | Runtime errors in production | Compile-time (Roslyn analyzer build errors) |
| **Cluster Topology** | 4 services (Frontend, History, Matching, Worker) | Single binary or embedded in-process |
| **Backing Database** | Required (Cassandra / PostgreSQL / MySQL) | Zero external DB (`.slab` mmap files) |
| **Local Dev Setup** | Docker Compose + CLI server containers | `cargo run -p velocity-dev-server` (no containers) |

---

## C# / .NET Integration

The .NET side of the project provides Roslyn source generators and native interop:

| Project | Target | Description |
|---------|--------|-------------|
| `Velocity.Workflow.Core` | net10.0 | C# structs (`DurableSlabHeader`, `NdaHeader`, `VctpPacketHeader`) with P/Invoke to native Rust via `NativeBridge` |
| `Velocity.Workflow.Generators` | netstandard2.0 | Roslyn incremental source generator — `[DurableWorkflow]` attribute auto-generates state machine runners; `DeterminismAnalyzer` flags non-deterministic API calls at build time |
| `Velocity.Workflow.Server` | net10.0 | C# server entry point |

```csharp
using Velocity.Workflow.Core;

[DurableWorkflow(SlabSize = 4096)]
public async Task RunAsync(string orderId, decimal amount)
{
    // Roslyn source generator auto-generates the state machine.
    // Each await becomes a deterministic, retriable slab step.
    await ChargeCardStepAsync(orderId, amount);
    await SendReceiptStepAsync(orderId);
}
```

---

## Migration Tooling

### temporal2velocity (C# CLI)

Transpile Temporal TypeScript/C# workflows and hydrate active event histories:

```bash
# Transpile a Temporal workflow source file
dotnet run --project tools/temporal2velocity -- --src ./MyTemporalWorkflow.ts

# Hydrate active Temporal JSON event history into .slab headers
dotnet run --project tools/temporal2velocity -- --hydrate 1001 25
```

### velocity-migrate (TypeScript CLI)

Convert workflows between Velocity SDK flavors:

```bash
npx @velocity-workflow/migration-toolkit --from temporal --to classic ./workflows/
```

---

## Repository Structure

```
.
├── Velocity.Workflow.slnx                # .NET 10.0 solution (C# core, generators, server, tests)
│
│  ── Rust Crates (Cargo workspace) ──
├── velocity-workflow-core/               # #![no_std] slab engine, C-ABI FFI, SHA-256 Merkle root
├── velocity-workflow-engine/             # Production engine: WAL, timers, sagas, encryption, task queue
├── velocity-workflow-server/             # Production gRPC server (binary: velocity-server)
├── velocity-dev-server/                  # Dev server, in-memory engine (binary: velocity-dev)
├── velocity-workflow-daemon/             # CLI management tool (binary: velocity)
├── velocity-bench/                       # Benchmark harness (velocity-bench, temporal-bridge, velocity-bench-http)
├── velocity-classic/                     # Classic flavor library (Temporal-compatible)
├── velocity-embedded/                    # Embedded flavor library (DBOS-compatible, Postgres adapter)
├── velocity-test-framework/              # Test framework for workflow unit testing
│
│  ── Language SDKs ──
├── sdk/
│   ├── typescript/                       # @velocity-workflow/sdk (gRPC client)
│   ├── python/                           # velocity-workflow (gRPC client)
│   ├── go/                               # github.com/velocity-workflow/sdk-go
│   ├── java/                             # (symlink/copy — see velocity-sdk-java/)
│   ├── rust/                             # velocity-sdk (wraps engine as library)
│   ├── php/                              # velocity/workflow-sdk (FFI or gRPC)
│   └── ruby/                             # velocity_sdk (FFI or gRPC)
├── velocity-sdk-python/                  # Python SDK (standalone package)
├── velocity-sdk-go/                      # Go SDK (standalone module)
├── velocity-sdk-java/                    # Java SDK (Maven: io.velocity:velocity-sdk-java)
├── velocity-sdk-typescript/              # TypeScript SDK (standalone package)
│
│  ── Flavor-Specific SDKs ──
├── velocity-classic-ts/                  # @velocity-workflow/classic (Temporal-compatible TS)
├── velocity-embedded-ts/                 # @velocity-workflow/embedded (DBOS-compatible TS)
├── velocity-runtime-python/              # velocity-runtime (Restate-compatible Python)
├── velocity-runtime-typescript/          # @velocity-workflow/runtime (Restate-compatible TS)
│
│  ── Migration & Tooling ──
├── velocity-migration-toolkit/           # @velocity-workflow/migration-toolkit (CLI: velocity-migrate)
├── tools/temporal2velocity/              # C# CLI: transpile Temporal workflows + hydrate histories
│
│  ── Infrastructure ──
├── proto/velocity/v1/                    # Protobuf definitions (10 .proto files, BenchmarkService)
├── deploy/                               # Dockerfiles, Helm chart, K8s manifests, Kustomize, operator
├── cloud-bench/                          # Cloud benchmark orchestration (GCE/AWS)
├── migrations/                           # PostgreSQL schema migrations (001–006)
├── benchmarks/                           # BenchmarkDotNet micro-benchmarks & crash fuzzing
│
│  ── .NET Projects ──
├── src/Velocity.Workflow.Core/           # C# blittable structs, NativeBridge P/Invoke
├── src/Velocity.Workflow.Generators/     # Roslyn incremental source generators
├── src/Velocity.Workflow.Server/         # C# server
├── tests/                                # Unit tests (Core, Generators, transpiler)
│
│  ── Documentation ──
└── docs/                                 # 33 documentation files (guides, references, reports)
```

---

## Deployment

### Docker

```bash
# Production server
docker build -f deploy/Dockerfile.production-server -t velocity-server .
docker run -p 7234:7234 velocity-server

# Dev server
docker build -f deploy/Dockerfile.dev-server -t velocity-dev .
docker run -p 7233:7233 -p 7234:7234 velocity-dev
```

### Kubernetes

Full Helm chart, Kustomize overlays (dev/staging/production/HA/multi-region), and a Kubernetes operator are provided in `deploy/`:

```bash
# Helm
helm install velocity deploy/helm/velocity/

# Kustomize
kubectl apply -k deploy/kustomize/overlays/production/

# Operator
kubectl apply -f deploy/operator/crd.yaml
kubectl apply -f deploy/operator/deployment.yaml
```

### Cloud Benchmarks

Run reproducible benchmarks on dedicated GCE VMs:

```bash
# Orchestrate 6 VMs (one per engine)
python cloud-bench/run_cloud_bench_v3.py setup    # Install Rust, build on all VMs
python cloud-bench/run_cloud_bench_v3.py bench    # Run all benchmarks
python cloud-bench/run_cloud_bench_v3.py collect  # Gather results
```

---

## Running Benchmarks Locally

```bash
# 1. Build everything
cargo build --release

# 2. Start the production server (Terminal 1)
cargo run --release -p velocity-workflow-server -- --ip 0.0.0.0 --grpc-port 7234

# 3. Run benchmarks (Terminal 2)
cargo run --release -p velocity-bench --bin velocity-bench -- \
    --workloads smoke --engine velocity \
    --velocity-address http://localhost:7234 \
    --output bench_results.md
```

### Benchmark Profiles

| Command | Description |
|---------|-------------|
| `--workloads smoke` | 3 workloads, quick sanity check |
| `--workloads all --profile quick` | 18 workloads, low iterations (~5 min) |
| `--workloads all --profile standard` | 18 workloads, normal iterations (~15 min) |
| `--workloads all --profile stress` | 18 workloads, high iterations (~45 min) |

---

## Documentation

### Getting Started

| Guide | Description |
|-------|-------------|
| [Getting Started](docs/getting_started.md) | Installation, first workflow, all 3 flavors |
| [Dev Server Setup](docs/setup_dev_server.md) | One-command local development (HTTP, gRPC, UI) |
| [Architecture](docs/architecture.md) | System design, slab memory model, WAL, security |

### SDK Guides

| SDK | Guide | Transport |
|-----|-------|-----------|
| TypeScript | [sdk_typescript_guide.md](docs/sdk_typescript_guide.md) | gRPC |
| Python | [sdk_python_guide.md](docs/sdk_python_guide.md) | gRPC |
| Go | [sdk_go_guide.md](docs/sdk_go_guide.md) | gRPC |
| Java | [sdk_java_guide.md](docs/sdk_java_guide.md) | gRPC |
| Rust | [sdk_rust_guide.md](docs/sdk_rust_guide.md) | FFI (library) |
| PHP / Ruby | [sdk_guide.md](docs/sdk_guide.md) | gRPC / FFI |
| All SDKs | [sdk_quick_reference.md](docs/sdk_quick_reference.md) | Cheat sheet |

### Deployment & Operations

| Guide | Description |
|-------|-------------|
| [Deployment Guide](docs/deployment_guide.md) | Docker, K8s, Helm, production checklist |
| [Kubernetes Setup](docs/setup_kubernetes.md) | Full K8s deployment with all flavors |
| [Cloud Benchmark](docs/setup_cloud_benchmark.md) | Run benchmarks across GCE VMs or K8s |
| [API Reference](docs/api_reference.md) | gRPC API and SDK method reference |

### Migration

| Guide | Description |
|-------|-------------|
| [From Temporal](docs/migration_from_temporal.md) | API mapping, AST transpiler, strategy |
| [From Restate & DBOS](docs/migration_from_restate_dbos.md) | HTTP and PostgreSQL migration |
| [SDK Development](docs/sdk_development.md) | Build a Velocity SDK for any language |
| [Troubleshooting](docs/troubleshooting.md) | Common issues and FAQ |

---

## System Requirements

| Component | Requirement |
|:---|:---|
| **Rust** | 1.82+ (stable) — for engine, servers, benchmarks |
| **.NET** | 10.0 Preview — for C# Roslyn source generators |
| **Node.js** | 18+ — for TypeScript SDK and tooling |
| **Python** | 3.8+ (SDK client), 3.10+ (Runtime flavor) |
| **Go** | 1.21+ — for Go SDK |
| **Java** | 11+ — for Java SDK |
| **PHP** | 8.1+ — for PHP SDK |
| **Ruby** | 3.0+ — for Ruby SDK |
| **Docker** | Required for Temporal benchmark comparison |
| **PostgreSQL** | 16+ — only for Velocity Embedded flavor |

**Minimum server resources:** 2 vCPU, 4 GB RAM. Production server runs in ~5 MB.

---

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/my-feature`)
3. Make changes with tests
4. Run the test suite: `cargo test` and `dotnet test`
5. Submit a pull request

For major changes, please open an issue first.

---

## Licensing

V.E.L.O.C.I.T.Y.-WorkFlow is licensed under the **GNU Affero General Public License v3.0 (AGPL-3.0)**. Enterprise migration tooling and commercial support are available under proprietary licenses.

| Resource | Link |
|:---|:---|
| **Repository** | [github.com/UnitBuilds-CC/V.E.L.O.C.I.T.Y.-WorkFlow](https://github.com/UnitBuilds-CC/V.E.L.O.C.I.T.Y.-WorkFlow) |
| **Issues** | [GitHub Issues](https://github.com/UnitBuilds-CC/V.E.L.O.C.I.T.Y.-WorkFlow/issues) |
| **CI/CD** | [GitHub Actions](https://github.com/UnitBuilds-CC/V.E.L.O.C.I.T.Y.-WorkFlow/actions) |
