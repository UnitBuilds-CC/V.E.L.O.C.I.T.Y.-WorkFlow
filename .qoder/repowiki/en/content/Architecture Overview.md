# Architecture Overview

<cite>
**Referenced Files in This Document**
- [Cargo.toml](file://Cargo.toml)
- [src/lib.rs](file://src/lib.rs)
- [velocity-workflow-server/src/main.rs](file://velocity-workflow-server/src/main.rs)
- [velocity-embedded/src/main.rs](file://velocity-embedded/src/main.rs)
- [velocity-classic-ts/src/index.ts](file://velocity-classic-ts/src/index.ts)
- [velocity-workflow-engine/src/lib.rs](file://velocity-workflow-engine/src/lib.rs)
- [proto/bench/v1/bench.proto](file://proto/bench/v1/bench.proto)
</cite>

## Table of Contents
1. [System Architecture](#system-architecture)
2. [Engine Flavors](#engine-flavors)
3. [Persistence Layers](#persistence-layers)
4. [Protocol Buffers and gRPC](#protocol-buffers-and-grpc)
5. [SDK Architecture](#sdk-architecture)
6. [Benchmark Architecture](#benchmark-architecture)
7. [Deployment Architecture](#deployment-architecture)
8. [Data Flow](#data-flow)

## System Architecture

V.E.L.O.C.I.T.Y. is a multi-flavor workflow engine ecosystem designed for high performance, durability, and flexibility. The system is organized into three layers:

```mermaid
graph TB
    subgraph "Client Layer"
        C1[TypeScript SDK]
        C2[Python SDK]
        C3[Go SDK]
        C4[Java SDK]
    end
    
    subgraph "Server Layer"
        S1[Velocity Server<br/>gRPC + WAL]
        S2[Velocity Embedded<br/>HTTP + PostgreSQL]
        S3[Velocity Classic<br/>HTTP + Temporal API]
    end
    
    subgraph "Core Engine"
        E1[velocity-workflow-core]
        E2[velocity-workflow-engine]
    end
    
    subgraph "Persistence Layer"
        P1[WAL Files]
        P2[(PostgreSQL)]
        P3[In-Memory]
    end
    
    C1 --> S1
    C1 --> S2
    C1 --> S3
    C2 --> S1
    C2 --> S2
    C3 --> S1
    C4 --> S1
    
    S1 --> E1
    S2 --> E1
    S3 --> E2
    
    E1 --> E2
    E2 --> P1
    E2 --> P2
    E2 --> P3
```

**Key Design Principles:**
- **Multi-flavor deployment** — Same core engine, different persistence and API layers
- **Zero-allocation hot paths** — Fixed-size buffers in critical paths
- **Deterministic execution** — Workflow state is fully reproducible
- **Protocol-first design** — gRPC/protobuf for all inter-service communication
- **SDK diversity** — Native SDKs for TypeScript, Python, Go, Java

## Engine Flavors

### Velocity Server (Single Binary)

The production gRPC server with Write-Ahead Log persistence. Optimized for maximum throughput.

```mermaid
graph LR
    A[gRPC Client] -->|HTTP/2| B[Velocity Server]
    B --> C[WAL Writer]
    C --> D[WAL Files]
    B --> E[Workflow Engine]
    E --> F[Activity Executor]
    B --> G[Benchmark Service]
```

**Characteristics:**
- **Protocol:** gRPC (HTTP/2)
- **Persistence:** Write-Ahead Log (WAL)
- **Port:** 7234 (default), 17234 (Docker)
- **Memory:** ~98 MiB
- **Throughput:** ~43.6 ops/s (simple workflow)
- **Use case:** Maximum throughput, simple deployment

**Key files:**
- `velocity-workflow-server/src/main.rs` — Server entry point
- Uses `WorkflowEngine` with WAL backend
- Implements `BenchmarkService` proto

### Velocity Embedded

PostgreSQL-backed server for embedded deployments. Best balance of performance and durability.

```mermaid
graph LR
    A[HTTP Client] -->|REST| B[Velocity Embedded]
    B --> C[Connection Pool]
    C --> D[(PostgreSQL)]
    B --> E[Workflow Engine]
    E --> F[Activity Executors]
    B --> G[Migration Manager]
```

**Characteristics:**
- **Protocol:** HTTP/REST
- **Persistence:** PostgreSQL
- **Port:** 8082 (default), 18082 (Docker)
- **Memory:** ~1.25 MiB (server) + ~68 MiB (PostgreSQL)
- **Throughput:** ~61.25 ops/s (simple workflow)
- **Use case:** Embedded deployments, database durability

**Key files:**
- `velocity-embedded/src/main.rs` — Server entry point
- Uses `async-pg` for PostgreSQL connection pooling
- Implements HTTP endpoints for workflow operations

### Velocity Classic

TypeScript SDK with Temporal-compatible API. Designed for easy migration from Temporal.

```mermaid
graph LR
    A[Temporal Client] -->|HTTP| B[Velocity Classic]
    B --> C[Worker Pool]
    C --> D[Workflow Instances]
    B --> E[Activity Registry]
    E --> F[Activity Executors]
    B --> G[Task Queue]
```

**Characteristics:**
- **Protocol:** HTTP/REST
- **Persistence:** In-Memory (configurable)
- **Port:** 8083 (default), 18083 (Docker)
- **Memory:** ~9.23 MiB
- **Throughput:** ~61.54 ops/s (simple workflow)
- **Use case:** Temporal migration, TypeScript-native workflows

**Key files:**
- `velocity-classic-ts/src/index.ts` — Worker, Workflow, Activity classes
- `velocity-classic-ts/src/main.ts` — Server entry point
- Implements Temporal-compatible API

## Persistence Layers

### Write-Ahead Log (WAL)

Used by Velocity Server for durable execution.

```mermaid
sequenceDiagram
    participant Client
    participant Server
    participant WAL
    participant Engine
    
    Client->>Server: Start Workflow
    Server->>WAL: Write "workflow_started"
    WAL-->>Server: ACK
    Server->>Engine: Execute workflow
    Engine->>WAL: Write "activity_completed"
    WAL-->>Engine: ACK
    Engine->>WAL: Write "workflow_completed"
    WAL-->>Server: ACK
    Server-->>Client: Result
```

**Characteristics:**
- Append-only log files
- Crash recovery from last committed state
- Fast writes (sequential I/O)
- No external dependencies

### PostgreSQL

Used by Velocity Embedded for relational durability.

```mermaid
sequenceDiagram
    participant Client
    participant Server
    participant Pool
    participant PostgreSQL
    
    Client->>Server: Start Workflow
    Server->>Pool: Acquire connection
    Pool->>PostgreSQL: BEGIN
    PostgreSQL-->>Pool: OK
    Server->>PostgreSQL: INSERT workflow
    PostgreSQL-->>Server: OK
    Server->>PostgreSQL: COMMIT
    PostgreSQL-->>Server: OK
    Server-->>Client: Result
```

**Characteristics:**
- ACID transactions
- Connection pooling via `deadpool-postgres`
- Automatic schema migrations
- Query optimization via indexes

### In-Memory

Used by Velocity Classic for maximum performance.

**Characteristics:**
- No persistence (data lost on restart)
- Fastest possible execution
- Suitable for development/testing
- Can be configured with external persistence

## Protocol Buffers and gRPC

### Proto Structure

```mermaid
graph TB
    subgraph "Proto Files"
        A[proto/bench/v1/bench.proto]
        B[proto/velocity/v1/workflow.proto]
        C[proto/velocity/v1/activity.proto]
        D[proto/velocity/v1/common.proto]
    end
    
    subgraph "Generated Code"
        E[Rust: tonic]
        F[TypeScript: protobuf-ts]
        G[Python: grpcio-tools]
    end
    
    A --> E
    A --> F
    A --> G
    B --> E
    B --> F
    C --> E
    C --> F
    D --> E
    D --> F
```

### Benchmark Service Proto

```protobuf
service BenchmarkService {
  // Workflow operations
  rpc StartWorkflow(StartWorkflowRequest) returns (StartWorkflowResponse);
  rpc SignalWorkflow(SignalWorkflowRequest) returns (SignalWorkflowResponse);
  rpc QueryWorkflow(QueryWorkflowRequest) returns (QueryWorkflowResponse);
  rpc CompleteStep(CompleteStepRequest) returns (CompleteStepResponse);
  rpc WaitForCompletion(WaitForCompletionRequest) returns (WaitForCompletionResponse);
  
  // Administrative
  rpc RegisterNamespace(RegisterNamespaceRequest) returns (RegisterNamespaceResponse);
  rpc HealthCheck(HealthCheckRequest) returns (HealthCheckResponse);
}
```

### Code Generation

**Rust (via build.rs):**
```rust
// velocity-workflow-server/build.rs
fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile(&["proto/bench/v1/bench.proto"], &["proto"])?;
    Ok(())
}
```

**TypeScript:**
```bash
cd proto
npx protoc --ts_out . --proto_path . bench/v1/bench.proto
```

## SDK Architecture

### TypeScript SDK

```mermaid
graph TB
    A[Client Application] --> B[Velocity Client]
    B --> C[Workflow Base Class]
    B --> D[Activity Base Class]
    C --> E[Workflow Instance]
    D --> F[Activity Instance]
    E --> G[Worker]
    F --> G
    G --> H[HTTP/gRPC Transport]
    H --> I[Velocity Server]
```

**Key classes:**
- `Worker` — Manages workflow and activity execution
- `Workflow` — Base class for workflow definitions
- `Activity` — Base class for activity implementations
- `VelocityServer` — HTTP server for receiving requests

### Python SDK

```mermaid
graph TB
    A[Client Application] --> B[VelocityClient]
    B --> C[workflow decorator]
    B --> D[activity decorator]
    C --> E[Workflow Instance]
    D --> F[Activity Instance]
    E --> G[Worker]
    F --> G
    G --> H[HTTP Transport]
    H --> I[Velocity Server]
```

**Key components:**
- `@workflow` — Decorator for workflow functions
- `@activity` — Decorator for activity functions
- `Worker` — Manages execution
- `VelocityClient` — HTTP client

### Go SDK

```mermaid
graph TB
    A[Client Application] --> B[Client]
    B --> C[WorkflowFunc]
    B --> D[ActivityFunc]
    C --> E[Workflow Execution]
    D --> F[Activity Execution]
    E --> G[Worker]
    F --> G
    G --> H[gRPC Transport]
    H --> I[Velocity Server]
```

## Benchmark Architecture

### Production Benchmark Suite

```mermaid
graph TB
    subgraph "Benchmark Runner"
        A[prod-bench binary]
        B[Workload Definitions]
        C[Result Aggregator]
    end
    
    subgraph "Engine Clients"
        D[VelocityClient<br/>gRPC]
        E[VelocityEmbeddedClient<br/>HTTP]
        F[VelocityClassicClient<br/>HTTP]
        G[DbosClient<br/>HTTP]
        H[RestateClient<br/>HTTP]
        I[TemporalClient<br/>gRPC]
    end
    
    subgraph "Docker Services"
        J[Velocity Server]
        K[Velocity Embedded]
        L[Velocity Classic]
        M[DBOS + PostgreSQL]
        N[Restate]
        O[Temporal + PostgreSQL]
    end
    
    A --> B
    A --> C
    B --> D
    B --> E
    B --> F
    B --> G
    B --> H
    B --> I
    
    D --> J
    E --> K
    F --> L
    G --> M
    H --> N
    I --> O
```

**Workload types:**
- `simple_workflow` — Basic workflow execution
- `signal_storm` — 100 signals per workflow
- `query_burst` — 100 queries per workflow
- `high_step` — 100-step workflow
- `concurrent_100` — 100 concurrent workflows
- `mixed_operations` — Mixed signal/query/complete
- `search_attributes` — Attribute-based queries
- `throughput_ceiling` — Maximum throughput test
- `tail_latency` — Long-running latency test
- `cold_start` — Cold start measurement
- `payload_1kb` — 1KB payload roundtrip

## Deployment Architecture

### Docker Compose (Development)

```mermaid
graph TB
    subgraph "Docker Network: bench-net"
        A[velocity<br/>:17234]
        B[velocity-embedded<br/>:18082]
        C[velocity-classic<br/>:18083]
        D[velocity-embedded-pg<br/>:5432]
        E[dbos<br/>:18081]
        F[dbos-pg<br/>:5432]
        G[restate<br/>:19070]
        H[restate-bench-svc<br/>:19071]
        I[temporal<br/>:17233]
        J[temporal-postgres<br/>:5432]
        K[prod-bench<br/>runner]
    end
    
    B --> D
    E --> F
    I --> J
    K --> A
    K --> B
    K --> C
    K --> E
    K --> G
    K --> I
```

### Kubernetes (Production)

```mermaid
graph TB
    subgraph "Kubernetes Cluster"
        subgraph "Namespace: velocity"
            A[velocity-server<br/>Deployment]
            B[velocity-service<br/>Service]
            C[velocity-hpa<br/>HPA]
        end
        
        subgraph "Namespace: velocity-data"
            D[postgresql<br/>StatefulSet]
            E[postgresql-service<br/>Service]
            F[postgresql-pvc<br/>PVC]
        end
        
        subgraph "Namespace: velocity-monitoring"
            G[prometheus<br/>Deployment]
            H[grafana<br/>Deployment]
        end
    end
    
    A --> B
    A --> D
    D --> E
    D --> F
    G --> A
    G --> D
    H --> G
```

## Data Flow

### Workflow Execution Flow

```mermaid
sequenceDiagram
    participant Client
    participant Server
    participant Engine
    participant Persistence
    participant Activity
    
    Client->>Server: StartWorkflow
    Server->>Engine: Create workflow instance
    Engine->>Persistence: Save workflow state
    Persistence-->>Engine: ACK
    Engine->>Activity: Execute activity
    Activity-->>Engine: Activity result
    Engine->>Persistence: Update workflow state
    Persistence-->>Engine: ACK
    Engine->>Server: Workflow complete
    Server-->>Client: Result
```

### Signal Flow

```mermaid
sequenceDiagram
    participant Client
    participant Server
    participant Engine
    participant Workflow
    participant Persistence
    
    Client->>Server: SignalWorkflow
    Server->>Engine: Deliver signal
    Engine->>Workflow: Send signal to workflow
    Workflow->>Engine: Process signal
    Engine->>Persistence: Update state
    Persistence-->>Engine: ACK
    Engine->>Server: Signal processed
    Server-->>Client: ACK
```

### Query Flow

```mermaid
sequenceDiagram
    participant Client
    participant Server
    participant Engine
    participant Workflow
    participant Persistence
    
    Client->>Server: QueryWorkflow
    Server->>Engine: Execute query
    Engine->>Workflow: Query current state
    Workflow-->>Engine: Query result
    Engine->>Persistence: Read state if needed
    Persistence-->>Engine: State
    Engine->>Server: Query response
    Server-->>Client: Result
```

## Performance Characteristics

### Throughput Comparison

```mermaid
graph LR
    A[Velocity Embedded<br/>61.25 ops/s] --> B[Velocity Classic<br/>61.54 ops/s]
    B --> C[DBOS<br/>59.59 ops/s]
    C --> D[Velocity Server<br/>43.6 ops/s]
    D --> E[Restate<br/>41.14 ops/s]
    E --> F[Temporal<br/>35.9 ops/s]
```

### Memory Usage

```mermaid
graph LR
    A[Velocity Embedded<br/>1.25 MiB] --> B[Velocity Classic<br/>9.23 MiB]
    B --> C[Velocity Server<br/>98.76 MiB]
    C --> D[DBOS<br/>63.23 MiB]
    D --> E[Restate<br/>200.36 MiB]
    E --> F[Temporal<br/>563 MiB]
```

### Latency (p50)

```mermaid
graph LR
    A[Velocity Classic<br/>14.51ms] --> B[Velocity Embedded<br/>14.65ms]
    B --> C[DBOS<br/>15.52ms]
    C --> D[Restate<br/>23.02ms]
    D --> E[Temporal<br/>176ms]
    E --> F[Velocity Server<br/>180ms]
```

**Section sources**
- [Cargo.toml](file://Cargo.toml)
- [src/lib.rs](file://src/lib.rs)
- [velocity-workflow-server/src/main.rs](file://velocity-workflow-server/src/main.rs)
- [velocity-embedded/src/main.rs](file://velocity-embedded/src/main.rs)
- [velocity-classic-ts/src/index.ts](file://velocity-classic-ts/src/index.ts)
- [proto/bench/v1/bench.proto](file://proto/bench/v1/bench.proto)
