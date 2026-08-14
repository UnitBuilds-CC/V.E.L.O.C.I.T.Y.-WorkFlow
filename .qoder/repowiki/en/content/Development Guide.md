# Development Guide

<cite>
**Referenced Files in This Document**
- [Cargo.toml](file://Cargo.toml)
- [velocity-workflow-server/Cargo.toml](file://velocity-workflow-server/Cargo.toml)
- [velocity-classic-ts/package.json](file://velocity-classic-ts/package.json)
- [bench-suite/prod-bench/Cargo.toml](file://bench-suite/prod-bench/Cargo.toml)
- [proto/bench/v1/bench.proto](file://proto/bench/v1/bench.proto)
- [velocity-workflow-server/src/main.rs](file://velocity-workflow-server/src/main.rs)
</cite>

## Table of Contents
1. [Development Environment Setup](#development-environment-setup)
2. [Project Architecture](#project-architecture)
3. [Building and Testing](#building-and-testing)
4. [Code Style and Conventions](#code-style-and-conventions)
5. [Adding New Features](#adding-new-features)
6. [Protocol Buffers](#protocol-buffers)
7. [SDK Development](#sdk-development)
8. [Benchmark Development](#benchmark-development)
9. [Docker Development](#docker-development)
10. [Performance Profiling](#performance-profiling)

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
        B[velocity-workflow-core]
        C[velocity-workflow-engine]
    end
    
    subgraph "Servers"
        D[velocity-workflow-server<br/>gRPC + WAL]
        E[velocity-embedded<br/>HTTP + PostgreSQL]
        F[velocity-classic-ts<br/>HTTP + Temporal API]
    end
    
    subgraph "Runtimes"
        G[velocity-runtime-typescript]
        H[velocity-runtime-python]
    end
    
    subgraph "SDKs"
        I[velocity-sdk-typescript]
        J[velocity-sdk-python]
        K[velocity-sdk-go]
        L[velocity-sdk-java]
    end
    
    subgraph "Benchmarks"
        M[bench-suite/prod-bench]
        N[velocity-bench]
        O[cloud-bench]
    end
    
    A --> B
    A --> C
    B --> D
    B --> E
    C --> D
    C --> E
    G --> F
    H --> F
    I --> F
    J --> F
```

### Key Modules

**Core Engine (`src/`):**
- Workflow execution primitives
- Activity and signal handling
- State management
- Persistence abstractions

**Server (`velocity-workflow-server/`):**
- gRPC server implementation
- WAL persistence
- Benchmark service
- Health checks

**Embedded (`velocity-embedded/`):**
- HTTP server
- PostgreSQL integration
- Connection pooling
- Migration management

**Classic TypeScript (`velocity-classic-ts/`):**
- Worker implementation
- Workflow/Activity classes
- Temporal-compatible API
- HTTP server

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

- `.github/workflows/ci.yml` — Main CI pipeline
- `.github/workflows/benchmark.yml` — Benchmark runs
- `.github/workflows/e2e.yml` — End-to-end tests
- `.github/workflows/release.yml` — Release builds

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
- [bench-suite/prod-bench/Cargo.toml](file://bench-suite/prod-bench/Cargo.toml)
- [proto/bench/v1/bench.proto](file://proto/bench/v1/bench.proto)
