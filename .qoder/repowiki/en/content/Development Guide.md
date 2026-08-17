# Development Guide

<cite>
**Referenced Files in This Document**
- [Cargo.toml](file://Cargo.toml)
- [velocity-workflow-server/Cargo.toml](file://velocity-workflow-server/Cargo.toml)
- [velocity-server-bootstrap/Cargo.toml](file://velocity-server-bootstrap/Cargo.toml)
- [velocity-nmcp-protocol/Cargo.toml](file://velocity-nmcp-protocol/Cargo.toml)
- [proto/bench/v1/bench.proto](file://proto/bench/v1/bench.proto)
- [velocity-workflow-server/src/main.rs](file://velocity-workflow-server/src/main.rs)
- [velocity-workflow-engine/src/vctp_transport.rs](file://velocity-workflow-engine/src/vctp_transport.rs)
- [velocity-workflow-engine/src/vctp_rpc.rs](file://velocity-workflow-engine/src/vctp_rpc.rs)
- [tools/vctp-sidecar/src/main.rs](file://tools/vctp-sidecar/src/main.rs)
- [proto/vctp_service.json](file://proto/vctp_service.json)
</cite>

## Table of Contents
1. [Development Environment Setup](#development-environment-setup)
2. [Project Architecture](#project-architecture)
3. [Building and Testing](#building-and-testing)
4. [VCTP Development](#vctp-development)
5. [Code Style and Conventions](#code-style-and-conventions)
6. [Adding New Features](#adding-new-features)
7. [Protocol Buffers](#protocol-buffers)
8. [SDK Development](#sdk-development)
9. [Benchmark Development](#benchmark-development)
10. [Docker Development](#docker-development)
11. [Performance Profiling](#performance-profiling)
12. [Security and Production Hardening](#security-and-production-hardening)

## Development Environment Setup

### Prerequisites

**Rust Toolchain:**
```bash
rustup install stable
rustup default stable
cargo install cargo-watch cargo-edit
```

**Node.js (for TypeScript components):**
```bash
# Install Node.js 22+ (required for Promise.withResolvers)
# https://nodejs.org/
npm install -g typescript ts-node
```

**Docker:**
```bash
# Install Docker Desktop
# https://www.docker.com/products/docker-desktop
```

**Database (for Embedded flavor):**
```bash
docker run -d --name velocity-dev-pg \
  -e POSTGRES_USER=velocity \
  -e POSTGRES_PASSWORD=velocity \
  -e POSTGRES_DB=velocity \
  -p 5432:5432 \
  postgres:16-alpine
```

### IDE Setup

**VS Code Extensions:**
- rust-analyzer
- TypeScript and JavaScript
- Docker
- Protocol Buffers
- YAML

**Settings (`.vscode/settings.json`):**
```json
{
  "rust-analyzer.checkOnSave.command": "clippy",
  "editor.formatOnSave": true,
  "[rust]": {
    "editor.defaultFormatter": "rust-lang.rust-analyzer"
  },
  "[typescript]": {
    "editor.defaultFormatter": "esbenp.prettier-vscode"
  }
}
```

## Project Architecture

### Workspace Structure

```mermaid
graph TB
    subgraph "Core Library"
        A[src/lib.rs]
        B[velocity-workflow-core<br/>Slab + Bitmask256]
        C[velocity-workflow-engine<br/>WAL, PG, VCTP transport + RPC]
    end
    
    subgraph "Protocol"
        D[velocity-nmcp-protocol<br/>shmem + WebSocket]
    end
    
    subgraph "Bootstrap"
        E[velocity-server-bootstrap<br/>auth, rate-limit, audit, tracing]
    end
    
    subgraph "Servers"
        F[velocity-workflow-server<br/>gRPC + WAL]
        G[velocity-classic-server<br/>NMCP + VCTP gateways]
        H[velocity-embedded-server<br/>NMCP + PostgreSQL]
    end
    
    subgraph "Runtimes"
        I[velocity-runtime-typescript]
        J[velocity-runtime-python]
    end
    
    subgraph "SDKs (+ VCTP)"
        K[velocity-sdk-typescript]
        L[velocity-sdk-python]
        M[velocity-sdk-go]
        N[velocity-sdk-java]
    end
    
    subgraph "Benchmarks"
        O[bench-suite/prod-bench]
        P[velocity-bench]
        Q[cloud-bench]
    end
    
    subgraph "Tools"
        R[velocity-dev-server]
        S[velocity-test-framework]
        T[velocity-migration-toolkit]
    end

    subgraph "VCTP Tools"
        V1[vctp-sidecar<br/>ECDH + XOR]
        V2[vctp-cli<br/>Python CLI]
        V3[vctp-wireshark<br/>Dissector]
        V4[vctp-openapi<br/>Spec gen]
    end
    
    A --> B
    A --> C
    B --> F
    B --> G
    B --> H
    C --> F
    C --> G
    C --> H
    D --> G
    D --> H
    E --> F
    E --> G
    E --> H
    I --> G
    J --> G
    K --> G
    L --> G
    V1 -->|VCTP UDP| C
```

### Key Modules

**Core Engine (`src/`):**
- Workflow execution primitives
- Activity and signal handling
- State management
- Persistence abstractions

**Server Bootstrap (`velocity-server-bootstrap/`):**
- Shared server initialization (bootstrap_engine, bootstrap_nmcp, run_server_loop)
- API authentication (API key + JWT with key rotation)
- Rate limiting (token bucket per client IP)
- Audit logging (structured API call logs)
- Distributed tracing (OpenTelemetry/OTLP)
- mTLS support (rustls)
- Chaos and failure injection tests

**NMCP Protocol (`velocity-nmcp-protocol/`):**
- Binary frame format (16-byte header + JSON payload)
- Shared memory IPC for local workers
- WebSocket transport for remote clients
- NmcpDispatch trait for flavor-specific routing

**Classic Server (`velocity-classic-server/`):**
- NMCP shmem + WebSocket transport
- Replaced TypeScript engine with Rust
- WAL + optional PostgreSQL persistence
- Temporal-compatible API patterns

**Embedded Server (`velocity-embedded-server/`):**
- NMCP shmem + WebSocket transport
- PostgreSQL integration with per-step journal
- Connection pooling via deadpool-postgres
- Automatic schema migrations

**VCTP Transport (`velocity-workflow-core/src/vctp.rs`, 623 lines):**
- HMAC-SHA256 authenticated encryption with constant-time MAC verification
- VctpReplayWindow: 64-depth sliding window for O(1) replay detection
- XOR cipher with AES-256 key schedule (defense-in-depth)

**VCTP Transport (`velocity-workflow-engine/src/vctp_transport.rs`, 481 lines):**
- UDP wire format (28-byte header + payload + CRC32)
- Non-blocking socket with retransmission tracking
- AIMD congestion control
- Packet fragmentation and reassembly

**VCTP RPC Server (`velocity-workflow-engine/src/vctp_rpc.rs`, 2,767 lines):**
- Full request pipeline: drain → circuit breaker → rate limit → auth → idempotency → dispatch
- Tokio async worker pool (`run_async`)
- Heartbeat mechanism (30s interval, 90s eviction)
- Prometheus metrics export with 6 VCTP-specific alert rules
- OpenTelemetry tracing spans
- 27 RwLock safety improvements (unwrap → expect with descriptive messages)
- Chaos engineering tests (reorder overflow, 10K flood, malformed packets, drain-under-load)
- Concurrent stress benchmark (100 clients × 50 requests, ≥2,000 ops/s)
- Cross-network benchmark (4 zones with artificial latency, ≥1,000 ops/s)
- HMAC-SHA256 benchmark (≥100K ops/s) and replay window benchmark (≥10M ops/s)

**VCTP Gateways (`velocity-classic-server/`):**
- WebSocket-to-VCTP gateway (`ws_vctp_gateway.rs`, 692 lines) — TLS termination (WSS) via tokio-rustls, per-connection rate limiting
- HTTP-to-VCTP ingress with Swagger UI at `/docs` (`http_vctp_ingress.rs`, 871 lines) — TLS termination (HTTPS) via axum-server + rustls, per-second window rate limiting, 5 integration tests

**VCTP Tools (`tools/`):**
- Sidecar proxy with ECDH + XOR cipher (`vctp-sidecar/`, 474 lines, separate workspace)
- Python CLI tool (`vctp-cli/`, 267 lines)
- Wireshark Lua dissector (`vctp-wireshark/`, 221 lines)
- OpenAPI 3.0 spec generator (`vctp-openapi/`, 407 lines)

## Building and Testing

### Rust Components

**Build all:**
```bash
cargo build --release
```

**Build specific package:**
```bash
cargo build -p velocity-workflow-server --release
cargo build -p velocity-embedded --release
```

**Run tests:**
```bash
cargo test --workspace
cargo test -p velocity-workflow-server
```

**Run with watch:**
```bash
cargo watch -x run -x clear
```

### TypeScript Components

**Build:**
```bash
cd velocity-classic-ts
npm install
npm run build
```

**Test:**
```bash
npm test
```

**Development mode:**
```bash
npm run dev
```

### Integration Tests

**Run all integration tests:**
```bash
cd tests
cargo test --test integration_tests
```

**Run specific test:**
```bash
cargo test --test integration_tests simple_workflow
```

## VCTP Development

### Building VCTP Components

The VCTP transport and RPC server are part of the main workspace:
```bash
# Build everything (includes VCTP)
cargo build --release

# Build just the engine (contains VCTP transport + RPC)
cargo build -p velocity-workflow-engine --release
```

The VCTP sidecar has its own Cargo workspace:
```bash
# Build sidecar (separate workspace)
cd tools/vctp-sidecar
cargo build --release
```

### Testing VCTP

```bash
# Run all engine tests (includes 61 VCTP tests)
cargo test -p velocity-workflow-engine

# Run only VCTP-specific tests
cargo test -p velocity-workflow-engine -- vctp

# Run VCTP benchmarks (throughput + latency)
cargo test -p velocity-workflow-engine --release -- vctp_bench

# Run sidecar tests (separate workspace)
cd tools/vctp-sidecar
cargo test

# Total: 2,541 engine tests + 6 sidecar tests = 2,547 passing
```

### Running VCTP CLI Tool

```bash
# Health check
python tools/vctp-cli/vctp_cli.py health --server 127.0.0.1:9090

# Start a workflow
python tools/vctp-cli/vctp_cli.py start-workflow --server 127.0.0.1:9090 --type test-wf --steps 5

# Signal, query, cancel
python tools/vctp-cli/vctp_cli.py signal --server 127.0.0.1:9090 --workflow-id wf-123 --name my-signal
python tools/vctp-cli/vctp_cli.py query --server 127.0.0.1:9090 --workflow-id wf-123
python tools/vctp-cli/vctp_cli.py cancel --server 127.0.0.1:9090 --workflow-id wf-123
```

### VCTP Test Categories

| Category | Count | Description |
|----------|-------|-------------|
| Core VCTP | 18 | Wire format, CRC32, reorder buffer, heartbeat |
| Security integration | 12 | Auth, rate limiting, circuit breaker tests |
| HMAC authenticated encryption | 4 | MAC computation, verification, tamper detection, constant-time |
| Replay protection | 6 | Window first/sequential/duplicate/old/out-of-order/large-jump |
| Chaos engineering | 4 | Reorder overflow, 10K flood, malformed packets, drain-under-load |
| Concurrent stress | 2 | 100-thread stress (≥2,000 ops/s), delivery ratio >90% |
| Cross-network | 2 | 4-zone simulation (≥1,000 ops/s), delivery ratio >85% |
| Performance benchmarks | 6 | Throughput, latency, HMAC (≥100K), replay (≥10M), E2E latency |
| Cross-gateway E2E | 4 | WebSocket/HTTP/sidecar → VCTP roundtrip |
| Drain tests | 2 | Graceful drain + 503 rejection |
| Gateway integration (HTTP) | 5 | Live HTTP ingress tests (start, signal, query, cancel, health) |
| **Total VCTP** | **61** | Engine tests: 2,541 total + sidecar: 6 = **2,547 passing** |

### VCTP Protocol Schema

The protocol is defined in `proto/vctp_service.json` (machine-readable JSON Schema). To regenerate the OpenAPI spec:

```bash
python tools/vctp-openapi/gen_openapi.py --output openapi.yaml
```

### Wireshark Dissector

Install the VCTP dissector for packet inspection:
```bash
# Copy to Wireshark plugins
cp tools/vctp-wireshark/vctp.lua ~/.local/lib/wireshark/plugins/
# Windows: %APPDATA%\Wireshark\plugins\vctp.lua

# Filter: vctp.magic, vctp.sequence, vctp.method
```

### TLS Configuration for Gateways

The HTTP and WebSocket gateways support TLS termination for secure external access:

**HTTPS (HTTP-to-VCTP Ingress):**
```bash
# Generate self-signed cert for testing
openssl req -x509 -newkey rsa:2048 -keyout key.pem -out cert.pem -days 365 -nodes

# Run with TLS
cargo run --bin velocity-classic-server -- --https-cert cert.pem --https-key key.pem --https-port 8443
```

**WSS (WebSocket-to-VCTP Gateway):**
```bash
# Run with TLS
cargo run --bin velocity-classic-server -- --wss-cert cert.pem --wss-key key.pem --wss-port 8444
```

**Helm deployment with TLS:**
```yaml
vctp:
  tls:
    enabled: true
    secretName: velocity-tls    # K8s Secret with tls.crt and tls.key
    httpsPort: 8443
    wssPort: 8444
```

### VCTP Authenticated Encryption

VCTP packets can be authenticated with HMAC-SHA256 for integrity verification:

```rust
// Compute MAC over packet data
let mac = encryption.compute_mac(&payload, sequence_number);
// Verify MAC (constant-time comparison)
let valid = encryption.verify_mac(&payload, sequence_number, &received_mac);
```

Replay protection uses a 64-depth sliding window (`VctpReplayWindow`):
```rust
let mut window = VctpReplayWindow::new(64);
assert!(window.check_and_record(0));   // First packet: accepted
assert!(window.check_and_record(1));   // Sequential: accepted
assert!(!window.check_and_record(0));  // Duplicate: rejected
assert!(!window.check_and_record(0));  // Old (outside window): rejected
```

## Code Style and Conventions

### Rust

**Formatting:**
```bash
cargo fmt --all
```

**Linting:**
```bash
cargo clippy --workspace --all-targets -- -D warnings
```

**Conventions:**
- Use `snake_case` for functions, variables, modules
- Use `PascalCase` for types, traits
- Use `UPPER_SNAKE_CASE` for constants
- Document public APIs with `///` comments
- Use `#[must_use]` for functions returning Results
- Prefer `?` operator over `.unwrap()` in library code

### TypeScript

**Formatting:**
```bash
cd velocity-classic-ts
npm run format
```

**Linting:**
```bash
npm run lint
```

**Conventions:**
- Use `camelCase` for variables, functions
- Use `PascalCase` for classes, interfaces, types
- Use `UPPER_SNAKE_CASE` for constants
- Document public APIs with JSDoc comments
- Use async/await over Promises
- Prefer interfaces over type aliases for object shapes

## Adding New Features

### Adding a New Workflow Primitive

1. **Define in core:**
   ```rust
   // src/workflow_core.rs
   pub struct NewPrimitive {
       // fields
   }
   
   impl NewPrimitive {
       pub fn new() -> Self {
           // implementation
       }
   }
   ```

2. **Add tests:**
   ```rust
   #[cfg(test)]
   mod tests {
       use super::*;
       
       #[test]
       fn test_new_primitive() {
           // test implementation
       }
   }
   ```

3. **Update server:**
   ```rust
   // velocity-workflow-server/src/main.rs
   // Wire up the new primitive
   ```

4. **Update SDKs:**
   - Add TypeScript bindings
   - Add Python bindings
   - Add Go bindings

### Adding a New SDK Method

1. **Define proto (if gRPC):**
   ```protobuf
   // proto/bench/v1/bench.proto
   rpc NewMethod(NewMethodRequest) returns (NewMethodResponse);
   ```

2. **Implement in server:**
   ```rust
   async fn new_method(
       &self,
       request: Request<NewMethodRequest>,
   ) -> Result<Response<NewMethodResponse>, Status> {
       // implementation
   }
   ```

3. **Add to SDK:**
   ```typescript
   // velocity-sdk-typescript/src/client.ts
   async newMethod(input: NewMethodInput): Promise<NewMethodOutput> {
       // implementation
   }
   ```

## Protocol Buffers

### Proto Structure

```
proto/
├── bench/v1/
│   └── bench.proto          # Benchmark service
├── velocity/v1/
│   ├── workflow.proto       # Workflow service
│   ├── activity.proto       # Activity service
│   └── common.proto         # Common types
└── google/
    └── api/                 # Google API annotations
```

### Generating Code

**Rust:**
```bash
# Proto generation happens automatically via build.rs
cargo build -p velocity-workflow-server
```

**TypeScript:**
```bash
cd proto
npm install
npm run generate
```

### Adding New RPC Methods

1. **Define in proto:**
   ```protobuf
   service BenchmarkService {
     rpc NewMethod(NewMethodRequest) returns (NewMethodResponse);
   }
   
   message NewMethodRequest {
     string input = 1;
   }
   
   message NewMethodResponse {
     string output = 1;
   }
   ```

2. **Regenerate code:**
   ```bash
   cargo build  # Rust auto-generates
   cd proto && npm run generate  # TypeScript
   ```

3. **Implement in server:**
   ```rust
   async fn new_method(
       &self,
       request: Request<NewMethodRequest>,
   ) -> Result<Response<NewMethodResponse>, Status> {
       let input = request.into_inner().input;
       // implementation
       Ok(Response::new(NewMethodResponse { output }))
   }
   ```

## SDK Development

### TypeScript SDK

**Structure:**
```
velocity-sdk-typescript/
├── src/
│   ├── client.ts          # Main client
│   ├── workflow.ts        # Workflow base class
│   ├── activity.ts        # Activity base class
│   └── types.ts           # Type definitions
├── tests/
├── package.json
└── tsconfig.json
```

**Adding a new feature:**
1. Add types to `types.ts`
2. Implement in appropriate module
3. Export from `index.ts`
4. Add tests
5. Update documentation

### Python SDK

**Structure:**
```
velocity-sdk-python/
├── velocity_sdk/
│   ├── __init__.py
│   ├── client.py
│   ├── workflow.py
│   └── activity.py
├── tests/
├── setup.py
└── requirements.txt
```

### Go SDK

**Structure:**
```
velocity-sdk-go/
├── client.go
├── workflow.go
├── activity.go
├── go.mod
└── go.sum
```

## Benchmark Development

### Adding a New Workload

1. **Define workload:**
   ```rust
   // bench-suite/prod-bench/src/workloads.rs
   pub enum WorkloadKind {
       // existing workloads
       NewWorkload,
   }
   ```

2. **Implement in each client:**
   ```rust
   // bench-suite/prod-bench/src/velocity_client.rs
   pub async fn run_new_workload(&self, id: &str) -> Result<f64, String> {
       // implementation
   }
   ```

3. **Add to benchmark runner:**
   ```rust
   // bench-suite/prod-bench/src/main.rs
   WorkloadKind::NewWorkload => {
       client.run_new_workload(&id).await?
   }
   ```

### Running Custom Benchmarks

```bash
# Single workload
./target/release/prod-bench --engines velocity --workload simple_workflow

# Custom profile
./target/release/prod-bench --engines all --profile stress

# Output to file
./target/release/prod-bench --engines all --format json --output results.json
```

### Configurable Durability in Bench Server

The bench server (`bench-suite/velocity-bench-server`) supports `DurabilityConfig` CLI flags:

```bash
# Strict mode (default — fsync every step, maximum safety)
velocity-bench-server --sync-steps 0

# Batched mode (fsync every 10 steps or every 5ms)
velocity-bench-server --sync-steps 10 --flush-interval-ms 5

# Async mode (background fsync every 100ms, maximum throughput)
velocity-bench-server --sync-steps 4294967295 --flush-interval-ms 100

# Direct execution mode (skip task queue enqueue, caller drives steps)
velocity-bench-server --sync-steps 10 --flush-interval-ms 5 --direct-execution
```

**Direct Execution Mode:** When `--direct-execution` is enabled, step completion skips the task queue enqueue. This eliminates 2 Mutex locks + condvar signal per step for callers that drive the step loop themselves. Use for embedded/engine-local workloads where the caller owns the loop. Do NOT use for distributed worker-pool patterns where external workers poll the task queue.

Use `complete_step_durable()` in bench workloads to respect these settings.

### Bench Server HTTP Routes

The bench server (`bench-suite/velocity-bench-server`) exposes both standard and keyed benchmark routes:

**Standard routes:**
- `/bench/simple_workflow`, `/bench/multi_step`, `/bench/signal_storm`, etc.
- `/bench/invoke` — Lightweight throughput measurement (minimal WAL work)
- `/bench/durablePromise` — camelCase alias for Restate-compatible durable_promise workload

**Keyed routes (Restate Virtual Object compatible):**
- `/keyed_bench/:key/stateful` — Keyed stateful workflow (read + write steps)
- `/keyed_bench/:key/invoke` — Keyed lightweight invocation

### C# Lifecycle Benchmarks

The `benchmarks/Velocity.Workflow.Benchmarks/` directory contains a .NET benchmark suite that complements the Rust prod-bench. It includes two benchmark modes:

**Default (head-to-head):** `dotnet run -c Release` — runs `TemporalVsVelocityBenchmark` (synthetic memory-primitive comparison).

**Lifecycle (engine via FFI):** `dotnet run -c Release -- --lifecycle` — runs `WorkflowLifecycleBenchmark` which exercises the actual Velocity workflow engine via C# FFI (`WorkflowRuntime`). Each iteration performs a full lifecycle: Start → CompleteStep(0..N) → Signal → Query → Complete. Uses BenchmarkDotNet with `[Params(1, 10, 100)]` steps-per-workflow and `[MemoryDiagnoser]` for allocation tracking.

```bash
cd benchmarks/Velocity.Workflow.Benchmarks
dotnet run -c Release              # Synthetic head-to-head
dotnet run -c Release -- --lifecycle  # Real engine lifecycle (FFI)
```

## Docker Development

### Building Images

```bash
# Velocity Server
docker build -t velocity-server -f velocity-workflow-server/Dockerfile .

# Velocity Embedded
docker build -t velocity-embedded -f velocity-embedded/Dockerfile .

# Velocity Classic
docker build -t velocity-classic -f velocity-classic-ts/Dockerfile .
```

### Local Development with Docker

```bash
# Start dependencies only
docker compose -f bench-suite/prod-bench/docker-compose.yml up -d velocity-postgres

# Run server locally
cargo run --bin velocity-server

# Run benchmarks against local server
./target/release/prod-bench --engines velocity
```

### Debugging Containers

```bash
# View logs
docker logs -f pb-velocity

# Exec into container
docker exec -it pb-velocity sh

# Inspect container
docker inspect pb-velocity
```

## Performance Profiling

### Rust Profiling

**CPU Profiling:**
```bash
# Install flamegraph
cargo install flamegraph

# Profile
cargo flamegraph --bin velocity-server

# Or use perf
perf record --call-graph dwarf target/release/velocity-server
perf report
```

**Memory Profiling:**
```bash
# Use valgrind
valgrind --tool=massif target/release/velocity-server

# Or use heaptrack
heaptrack target/release/velocity-server
```

### TypeScript Profiling

**CPU Profiling:**
```bash
node --inspect velocity-classic-ts/dist/main.js
# Open chrome://inspect in Chrome
```

**Memory Profiling:**
```bash
node --inspect --expose-gc velocity-classic-ts/dist/main.js
```

### Database Profiling

```sql
-- Enable query logging
ALTER SYSTEM SET log_min_duration_statement = 100;
SELECT pg_reload_conf();

-- View slow queries
SELECT query, calls, total_time, mean_time
FROM pg_stat_statements
ORDER BY mean_time DESC
LIMIT 10;
```

## Continuous Integration

### GitHub Actions

The project uses GitHub Actions for CI:

- `.github/workflows/ci.yml` — Main CI pipeline (includes chaos/failure injection tests)
- `.github/workflows/benchmark.yml` — Benchmark runs with regression gates
- `.github/workflows/e2e.yml` — End-to-end tests
- `.github/workflows/release.yml` — Release builds

**CI Security:**
- Trivy container security scanning
- Chaos/failure injection tests in CI pipeline
- Production validation for all 3 flavors

### CI Benchmark Gates

The benchmark CI pipeline enforces performance regression thresholds:

| Gate | Threshold | Description |
|------|-----------|-------------|
| Benchmark regression | ≥500 ops/s | Full-stack VCTP dispatch throughput |
| Tail latency (p99) | <100ms | Sustained workload end-to-end latency |
| Error rate | <5% | Error tolerance under sustained load |

### Prometheus Alert Rules

**VCTP-specific alerts (6 rules):**

| Alert | Condition | Severity |
|-------|-----------|----------|
| VctpHighErrorRate | Error rate >5% for 2m | critical |
| VctpHighLatency | p99 >100ms for 5m | warning |
| VctpCircuitBreakerOpen | Circuit breaker open | critical |
| VctpDrainStuck | Drain >60s with inflight >0 | warning |
| VctpReplayDetected | Replay attempts >0 | warning |
| VctpLowThroughput | <500 ops/s for 5m | warning |

**HTTP alerts (11 rules):** Standard HTTP error rate, latency, and saturation alerts.

**Total: 17 Prometheus alert rules** (11 HTTP + 6 VCTP)

## Security and Production Hardening

### Authentication
All servers support optional API key and JWT authentication via `velocity-server-bootstrap`:
```bash
# Enable API key auth
VELOCITY_API_KEYS=key1,key2 cargo run --bin velocity-classic-server

# Enable JWT auth
VELOCITY_JWT_SECRET=my-secret cargo run --bin velocity-classic-server
```

### Rate Limiting
Token bucket rate limiter per client IP:
```bash
VELOCITY_RATE_LIMIT_BURST=100 VELOCITY_RATE_LIMIT_RATE=10.0 cargo run --bin velocity-classic-server
```

### Distributed Tracing
OpenTelemetry tracing with optional OTLP export. The `otel` feature flag must be enabled at compile time for OTLP export:
```bash
# Build with OpenTelemetry feature
cargo build --release -p velocity-server-bootstrap --features otel

# With OTLP export (Jaeger/Tempo/Grafana)
VELOCITY_OTLP_ENDPOINT=http://localhost:4317 cargo run --bin velocity-classic-server

# Local JSON logs only (works without otel feature)
cargo run --bin velocity-classic-server -- --log-format json
```

### mTLS
TLS certificate + key for secure WebSocket:
```bash
cargo run --bin velocity-classic-server -- --tls-cert cert.pem --tls-key key.pem
```

### TLS Gateway Termination (HTTPS/WSS)
TLS termination at the gateway level for secure external access:
```bash
# HTTPS ingress (HTTP-to-VCTP with TLS)
cargo run --bin velocity-classic-server -- --https-cert cert.pem --https-key key.pem --https-port 8443

# WSS gateway (WebSocket-to-VCTP with TLS)
cargo run --bin velocity-classic-server -- --wss-cert cert.pem --wss-key key.pem --wss-port 8444
```

Dependencies: axum-server 0.7 (tls-rustls), rustls 0.23, tokio-rustls 0.26

### VCTP Authenticated Encryption
Packet-level authenticated encryption with HMAC-SHA256 and replay protection:
- HMAC-SHA256 with constant-time verification (prevents timing attacks)
- 64-depth sliding window replay detection (≥10M checks/s)
- XOR cipher with AES-256 key schedule for defense-in-depth

### Operations Runbooks
See `docs/ops-runbooks.md` for common incident scenarios and resolution procedures (15 runbooks covering WAL recovery, VCTP circuit breaker, replay attacks, HMAC failures, TLS issues, and throughput degradation).

### Security Audit
See `docs/security-audit-checklist.md` for a 65-point security audit checklist covering all 39 production hardening items across 10 categories (transport, encryption, access control, network, persistence, observability, monitoring, container, CI/CD, and operational readiness).

### Distributed Tracing
See `docs/otlp-tracing-guide.md` for OpenTelemetry/OTLP configuration with Jaeger, Tempo, and Grafana backends.

### Production Load Testing
Run `deploy/scripts/vctp-prod-loadtest.sh` to validate production readiness from within the Kubernetes cluster. The script launches 100 concurrent load generator pods, validates against CI thresholds (500 ops/s, <5% error rate), and collects Prometheus metrics.

### WAL Backup
Run `deploy/scripts/wal-backup.sh` for encrypted WAL backups with SHA-256 integrity checksums, S3 upload, and configurable retention. The Helm backup CronJob also backs up WAL files alongside PostgreSQL dumps.

### Running CI Locally

```bash
# Using act (https://github.com/nektos/act)
act -j test
act -j benchmark
```

## Common Development Tasks

### Adding a New Migration

```bash
# Create migration
cd migrations
sqlx migrate add <migration_name>

# Edit migration file
# Run migration
sqlx migrate run --database-url postgres://velocity:velocity@localhost/velocity
```

### Updating Dependencies

```bash
# Rust
cargo update
cargo upgrade  # with cargo-edit

# TypeScript
cd velocity-classic-ts
npm update
```

### Debugging Workflow Execution

```bash
# Enable debug logging
RUST_LOG=debug cargo run --bin velocity-server

# Enable trace logging
RUST_LOG=trace cargo run --bin velocity-server
```

**Section sources**
- [Cargo.toml](file://Cargo.toml)
- [velocity-workflow-server/Cargo.toml](file://velocity-workflow-server/Cargo.toml)
- [velocity-server-bootstrap/Cargo.toml](file://velocity-server-bootstrap/Cargo.toml)
- [velocity-nmcp-protocol/Cargo.toml](file://velocity-nmcp-protocol/Cargo.toml)
- [proto/bench/v1/bench.proto](file://proto/bench/v1/bench.proto)
- [velocity-workflow-engine/src/vctp_transport.rs](file://velocity-workflow-engine/src/vctp_transport.rs)
- [velocity-workflow-engine/src/vctp_rpc.rs](file://velocity-workflow-engine/src/vctp_rpc.rs)
- [tools/vctp-sidecar/src/main.rs](file://tools/vctp-sidecar/src/main.rs)
- [proto/vctp_service.json](file://proto/vctp_service.json)
