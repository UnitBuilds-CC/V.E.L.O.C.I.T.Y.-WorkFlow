# Flavor Comparison Guide

<cite>
**Referenced Files in This Document**
- [velocity-workflow-server/src/main.rs](file://velocity-workflow-server/src/main.rs)
- [velocity-classic-server/src/main.rs](file://velocity-classic-server/src/main.rs)
- [velocity-embedded-server/src/main.rs](file://velocity-embedded-server/src/main.rs)
- [velocity-server-bootstrap/src/lib.rs](file://velocity-server-bootstrap/src/lib.rs)
- [velocity-nmcp-protocol/src/lib.rs](file://velocity-nmcp-protocol/src/lib.rs)
- [bench-suite/prod-bench/src/main.rs](file://bench-suite/prod-bench/src/main.rs)
</cite>

## Table of Contents
1. [Overview](#overview)
2. [Quick Comparison Table](#quick-comparison-table)
3. [Performance Comparison](#performance-comparison)
4. [Architecture Comparison](#architecture-comparison)
5. [Persistence Comparison](#persistence-comparison)
6. [API Comparison](#api-comparison)
7. [Deployment Comparison](#deployment-comparison)
8. [Use Case Matrix](#use-case-matrix)
9. [Migration Paths](#migration-paths)
10. [Decision Framework](#decision-framework)

## Overview

Velocity provides three distinct flavors, each optimized for different use cases:

- **Velocity Server** — Single binary with gRPC and WAL persistence for maximum simplicity
- **Velocity Embedded** — NMCP-based with PostgreSQL (per-step journal) for ACID transactions and SQL queries
- **Velocity Classic** — Rust server with NMCP transport for Temporal compatibility and lowest latency

This guide helps you choose the right flavor for your use case and understand the trade-offs.

## Quick Comparison Table

| Feature | Velocity Server | Velocity Embedded | Velocity Classic |
|---------|----------------|-------------------|------------------|
| **Protocol** | gRPC (HTTP/2) | NMCP (shmem + WebSocket) | NMCP (shmem + WebSocket) |
| **Persistence** | WAL (group-commit) | WAL + PostgreSQL (per-step journal) | WAL + optional PostgreSQL |
| **Language** | Rust | Rust | Rust (replaced TypeScript) |
| **Port** | 7234 (17234 Docker) | 8084 (WebSocket) | 8083 (WebSocket) |
| **Throughput** | 43.6 ops/s | 61.25 ops/s | 61.54 ops/s |
| **p50 Latency** | 180ms | 14.65ms | 14.51ms |
| **p99 Latency** | 332ms | 20.57ms | 18.1ms |
| **Memory** | 98.76 MiB | 1.25 MiB | 9.23 MiB |
| **Crash Recovery** | Yes (WAL) | Yes (WAL + ACID) | Yes (WAL) |
| **SQL Queries** | No | Yes | No |
| **ACID Transactions** | No | Yes (per-step journal) | No |
| **Multi-Instance** | No | Yes (PG advisory locks) | Yes (PG advisory locks) |
| **Temporal Compatible** | No | No | Yes (API patterns) |
| **Auth/Rate Limit** | Yes (bootstrap) | Yes (bootstrap) | Yes (bootstrap) |
| **Configurable Durability** | Yes (DurabilityConfig) | No (ACID fixed) | Yes (DurabilityConfig) |
| **Direct Execution Mode** | Yes (skip task queue) | No (ACID fixed) | Yes (skip task queue) |
| **Distributed Tracing** | Yes (OpenTelemetry) | Yes (OpenTelemetry) | Yes (OpenTelemetry) |
| **mTLS** | Yes | Yes | Yes |
| **VCTP Support** | Yes (shared engine) | Yes (shared engine) | Yes (gateways hosted here) |
| **VCTP UDP Port** | 9090 (shared) | 9090 (shared) | 9090 (shared) |
| **Gateway TLS** | Yes (HTTPS/WSS) | Yes (HTTPS/WSS) | Yes (HTTPS/WSS) |
| **External Dependencies** | None | PostgreSQL | None (default) |
| **Best For** | Simple deployments, bench-suite | Transactional workflows | Temporal migration, low-latency |

> **Note:** All three flavors share the VCTP (Velocity Transfer Protocol) subsystem — the VCTP RPC server, transport layer, and gateways are common across all flavors. The VCTP RPC server runs on UDP port 9090 with 9,052 ops/s full-stack dispatch throughput, HMAC-SHA256 authenticated encryption, replay protection, and circuit breaker protection. The Classic server hosts the WebSocket-to-VCTP and HTTP-to-VCTP gateways with TLS termination (HTTPS on port 8443, WSS on port 8444).

## Performance Comparison

### Throughput (ops/s)

```mermaid
graph LR
    A[Velocity Classic<br/>61.54] --> B[Velocity Embedded<br/>61.25]
    B --> C[Velocity Server<br/>43.6]
    
    style A fill:#90EE90
    style B fill:#90EE90
    style C fill:#FFB6C1
```

**Analysis:**
- **Classic** and **Embedded** are nearly identical in throughput (~61 ops/s)
- **Server** is ~30% slower due to WAL fsync overhead
- All three handle concurrent workflows well

### Latency (p50)

```mermaid
graph LR
    A[Velocity Classic<br/>14.51ms] --> B[Velocity Embedded<br/>14.65ms]
    B --> C[Velocity Server<br/>180ms]
    
    style A fill:#90EE90
    style B fill:#90EE90
    style C fill:#FFB6C1
```

**Analysis:**
- **Classic** has the lowest latency (no I/O overhead)
- **Embedded** is nearly as fast (connection pool optimization)
- **Server** has 12x higher latency (WAL fsync on every write)

### Memory Usage

```mermaid
graph LR
    A[Velocity Embedded<br/>1.25 MiB] --> B[Velocity Classic<br/>9.23 MiB]
    B --> C[Velocity Server<br/>98.76 MiB]
    
    style A fill:#90EE90
    style B fill:#FFB6C1
    style C fill:#FFB6C1
```

**Analysis:**
- **Embedded** is most memory-efficient (Rust + connection pool)
- **Classic** uses moderate memory (Node.js runtime)
- **Server** uses most memory (WAL buffers + Rust runtime)

### Detailed Workload Comparison

| Workload | Server | Embedded | Classic | Winner |
|----------|--------|----------|---------|--------|
| simple_workflow | 43.6 ops/s | 61.25 ops/s | 61.54 ops/s | Classic |
| signal_storm | 5.4 ops/s | 8.2 ops/s | 9.8 ops/s | Classic |
| query_burst | 1.1 ops/s | 45.3 ops/s | 52.3 ops/s | Classic |
| high_step | 29.0 ops/s | 58.7 ops/s | 60.2 ops/s | Classic |
| concurrent_100 | 66.4 ops/s | 89.2 ops/s | 92.1 ops/s | Classic |
| throughput_ceiling | 59.4 ops/s | 95.4 ops/s | 98.7 ops/s | Classic |

**Key Insights:**
- **Classic** wins on almost all workloads (in-memory advantage)
- **Embedded** excels at query-heavy workloads (SQL optimization)
- **Server** struggles with query_burst (no indexing)

## Architecture Comparison

### Component Architecture

```mermaid
graph TB
    subgraph "Velocity Server"
        S1[gRPC Server] --> S2[Workflow Engine]
        S2 --> S3[WAL Writer<br/>group-commit]
        S3 --> S4[WAL Files]
    end
    
    subgraph "Velocity Embedded"
        E1[NMCP Shmem/WS] --> E2[Workflow Engine]
        E2 --> E3[Connection Pool]
        E3 --> E4[(PostgreSQL)]
        E2 --> E5[Per-Step Journal]
    end
    
    subgraph "Velocity Classic"
        C1[NMCP Shmem/WS] --> C2[Workflow Engine]
        C2 --> C3[WAL Writer]
        C3 --> C4[WAL Files]
        C2 --> C5[Optional PostgreSQL]
    end
```

### Request Processing

**Velocity Server:**
1. gRPC request → Parse protobuf
2. Write to WAL (group-commit via background thread)
3. Execute workflow
4. Write completion to WAL
5. Return response

**Velocity Embedded:**
1. NMCP frame → Parse JSON from shmem/WebSocket
2. Dispatch via NmcpFrameRouter
3. Execute workflow with per-step journal
4. Batch INSERT steps to PostgreSQL
5. Return response via NMCP

**Velocity Classic:**
1. NMCP frame → Parse JSON from shmem/WebSocket
2. Dispatch via NmcpFrameRouter
3. Execute workflow with WAL
4. Write completion to WAL
5. Return response via NMCP

### Concurrency Model

**Velocity Server:**
- Single-threaded workflow execution
- Activity parallelism within workflow
- Limited by single CPU core
- WAL group-commit reduces fsync overhead

**Velocity Embedded:**
- Connection pool (16 connections default)
- Parallel workflow execution
- PostgreSQL handles concurrency
- PG advisory locks for multi-instance coordination
- Per-step journal for fine-grained durability

**Velocity Classic:**
- NMCP shmem for local workers (50-100x faster than HTTP)
- WebSocket for remote clients
- jemalloc global allocator
- Optional PostgreSQL for cross-instance durability

## Persistence Comparison

### Durability Guarantees

| Flavor | Crash Recovery | Data Loss Risk | Durability Level | Configurable |
|--------|----------------|----------------|------------------|-------------|
| Server | Yes (WAL group-commit) | Low (group-commit window) | High | Yes (DurabilityConfig) |
| Embedded | Yes (WAL + ACID journal) | Very Low (per-step) | Very High | ACID (fixed) |
| Classic | Yes (WAL) | Low (group-commit window) | High | Yes (DurabilityConfig) |

### Configurable Durability (DurabilityConfig)

Server and Classic flavors support user-configurable fsync batching:

| Mode | Behavior | Data Loss Risk | Throughput |
|------|----------|----------------|------------|
| `strict()` | fsync every step | None | Baseline |
| `batched(N, ms)` | fsync every N steps or every ms | ≤N steps | Higher |
| `async_only(ms)` | background fsync only | ≤ms of work | Maximum |

**Direct Execution Mode:** All WAL-based flavors support `direct_execution = true`, which skips task queue enqueue on step completion. This eliminates 2 Mutex locks + condvar signal per step for callers that drive the step loop themselves (embedded/engine-local workloads). Not applicable to Embedded (uses ACID transactions, not task queue).

### Recovery Mechanisms

**Velocity Server (WAL):**
```
Crash → Read WAL → Replay events → Restore state → Resume workflows
```
- Recovery time: 1-2s for 10k workflows
- Data loss: Only unfsync'd writes (<100ms)

**Velocity Embedded (PostgreSQL):**
```
Crash → PostgreSQL auto-recovery → Connections reconnect → Resume workflows
```
- Recovery time: <1s (PostgreSQL handles it)
- Data loss: None (ACID transactions)

**Velocity Classic (WAL):**
```
Crash → Read WAL → Replay events → Restore state → Resume workflows
```
- Recovery time: 1-2s for 10k workflows
- Data loss: Only unfsync'd writes (group-commit window)

### Query Capabilities

**Velocity Server:**
- No queries (WAL is append-only)
- Cannot search workflows
- Cannot filter by attributes
- Must track workflow IDs externally

**Velocity Embedded:**
- Full SQL queries
- Search by any field
- Filter by attributes (JSONB)
- Aggregations and joins
- Indexes for performance

**Velocity Classic:**
- No queries (in-memory only)
- Cannot search workflows
- Must track workflow IDs externally
- Can add external persistence (Redis, PostgreSQL)

## API Comparison

### Protocol

**Velocity Server:**
- gRPC (HTTP/2)
- Protocol Buffers
- Binary serialization
- Streaming support
- Code generation for all languages

**Velocity Embedded:**
- NMCP (shmem + WebSocket)
- JSON payload in binary frames
- Shmem for local IPC (50-100x faster)
- WebSocket for remote access
- TLS/mTLS support

**Velocity Classic:**
- NMCP (shmem + WebSocket)
- JSON payload in binary frames
- Shmem for local IPC (50-100x faster)
- WebSocket for remote access
- TLS/mTLS support

### Client SDKs

**Velocity Server:**
```rust
// Rust
let client = VelocityClient::connect("http://localhost:7234").await?;
let (wf_id, run_id) = client.start_workflow("my_workflow", input).await?;
```

**Velocity Embedded:**
```rust
// Rust (NMCP WebSocket client)
// Connect via WebSocket to remote server
let client = NmcpWebSocketClient::connect("ws://localhost:8084").await?;
let response = client.send_request(workflow_request).await?;
```

**Velocity Classic:**
```rust
// Rust (NMCP shmem for local IPC)
let client = NmcpShmemClient::connect("/tmp/velocity-classic.nmcp")?;
let response = client.send_request(workflow_request)?;
// 50-100x faster than HTTP for local workers
```

### Temporal Compatibility

Only **Velocity Classic** provides Temporal API compatibility:

| Temporal Feature | Server | Embedded | Classic |
|------------------|--------|----------|---------|
| Workflow classes | No | No | Yes |
| Activity classes | No | No | Yes |
| executeActivity | No | No | Yes |
| Signals | No | No | Yes |
| Queries | No | No | Yes |
| Timers | No | No | Yes |

## Deployment Comparison

### Dependencies

**Velocity Server:**
- None (single binary)
- Just run the binary
- Simplest deployment

**Velocity Embedded:**
- PostgreSQL required
- Connection string configuration
- Database migrations on startup

**Velocity Classic:**
- Node.js 22+ required (for TypeScript SDK clients)
- npm dependencies (for SDK)
- No external services (default)
- Server binary is Rust (no Node.js needed for server)

### Resource Requirements

**Velocity Server:**
- CPU: 1-2 cores
- Memory: 256MB - 1GB
- Disk: 10GB+ (WAL files)
- Network: gRPC port

**Velocity Embedded:**
- CPU: 1-2 cores (server) + 1-2 cores (PostgreSQL)
- Memory: 256MB (server) + 512MB (PostgreSQL)
- Disk: 10GB+ (PostgreSQL data)
- Network: HTTP port + PostgreSQL port

**Velocity Classic:**
- CPU: 1-2 cores
- Memory: 256MB - 512MB
- Disk: Minimal (in-memory)
- Network: HTTP port

### Scaling

All three flavors now support **multi-instance coordination** via PostgreSQL advisory locks (Embedded and Classic with PG):

**Current Capabilities:**
- PG advisory locks for leader election
- Workflow-level locking across instances
- Migration locking for safe schema updates
- Exponential backoff with jitter for contention

**Limitations:**
- No automatic sharding (planned)
- No automatic failover (manual)
- Single PostgreSQL instance (use PG HA for data redundancy)

## Use Case Matrix

### Best Fit by Use Case

| Use Case | Recommended Flavor | Reason |
|----------|-------------------|--------|
| **Edge computing** | Server | No dependencies, crash recovery |
| **Microservices orchestration** | Server | Simple, durable |
| **Financial transactions** | Embedded | ACID, audit trail |
| **Order processing** | Embedded | ACID, SQL queries |
| **Temporal migration** | Classic | API compatibility |
| **TypeScript development** | Classic | Native TypeScript |
| **Low-latency workflows** | Classic | 14.51ms p50 |
| **Data pipelines** | Embedded | SQL, aggregations |
| **Event processing** | Server | High throughput, durable |
| **Development/testing** | Classic | Fast, no dependencies |
| **Compliance-heavy** | Embedded | ACID, audit trail |
| **IoT coordination** | Server | Edge-friendly, durable |
| **Batch processing** | Embedded | SQL, parallel queries |
| **Real-time workflows** | Classic | Lowest latency |
| **Dashboard/analytics** | Embedded | SQL queries |

### Decision Tree

```mermaid
graph TD
    A[Start] --> B{Need crash recovery?}
    B -->|Yes| C{Need ACID transactions?}
    B -->|No| D{Need lowest latency?}
    C -->|Yes| E[Velocity Embedded]
    C -->|No| F{Can run PostgreSQL?}
    F -->|Yes| E
    F -->|No| G[Velocity Server]
    D -->|Yes| H[Velocity Classic]
    D -->|No| I{Migrating from Temporal?}
    I -->|Yes| H
    I -->|No| J{Need SQL queries?}
    J -->|Yes| E
    J -->|No| G
```

## Migration Paths

### Server → Embedded

**When to migrate:**
- Need SQL queries
- Need ACID transactions
- Can add PostgreSQL

**Migration steps:**
1. Deploy PostgreSQL
2. Update configuration to use Embedded
3. Run migrations
4. Replay WAL to PostgreSQL (custom script)
5. Switch traffic to Embedded

**Challenges:**
- WAL format ≠ PostgreSQL schema
- Need custom migration tool
- Downtime during migration

### Embedded → Server

**When to migrate:**
- Remove PostgreSQL dependency
- Simplify deployment
- Edge deployment needed

**Migration steps:**
1. Export workflows from PostgreSQL
2. Import to WAL format (custom script)
3. Switch to Server
4. Remove PostgreSQL

**Challenges:**
- Lose SQL queries
- Lose ACID transactions
- Custom migration needed

### Classic → Embedded

**When to migrate:**
- Need durability
- Need SQL queries
- Production deployment

**Migration steps:**
1. Deploy PostgreSQL
2. Switch to Embedded flavor
3. Update client code (HTTP → HTTP, same API)
4. Add persistence configuration

**Challenges:**
- Lose in-memory performance
- Need PostgreSQL infrastructure

### Classic → Server

**When to migrate:**
- Need crash recovery
- Remove Node.js dependency
- Better performance

**Migration steps:**
1. Rewrite TypeScript workflows in Rust
2. Deploy Server
3. Update client code (HTTP → gRPC)
4. Test thoroughly

**Challenges:**
- Language mismatch (TypeScript → Rust)
- API change (HTTP → gRPC)
- Significant rewrite

## Decision Framework

### Scoring System

Rate each flavor 1-5 for your requirements:

| Requirement | Weight | Server | Embedded | Classic |
|-------------|--------|--------|----------|---------|
| Throughput | 3 | 3 | 4 | 5 |
| Latency | 3 | 2 | 4 | 5 |
| Durability | 5 | 4 | 5 | 1 |
| Queryability | 4 | 1 | 5 | 1 |
| Simplicity | 4 | 5 | 3 | 4 |
| Temporal Compat | 2 | 1 | 1 | 5 |
| Memory Efficiency | 3 | 2 | 5 | 3 |

**Calculate weighted scores:**
- Server: (3×3) + (3×2) + (5×4) + (4×1) + (4×5) + (2×1) + (3×2) = 9 + 6 + 20 + 4 + 20 + 2 + 6 = **67**
- Embedded: (3×4) + (3×4) + (5×5) + (4×5) + (4×3) + (2×1) + (3×5) = 12 + 12 + 25 + 20 + 12 + 2 + 15 = **98**
- Classic: (3×5) + (3×5) + (5×1) + (4×1) + (4×4) + (2×5) + (3×3) = 15 + 15 + 5 + 4 + 16 + 10 + 9 = **74**

**Winner: Velocity Embedded** (for this example)

### Quick Decision Guide

**Choose Velocity Server if:**
- ✅ Need crash recovery
- ✅ Cannot run PostgreSQL
- ✅ Want simplest deployment
- ✅ Edge computing scenario
- ✅ gRPC is acceptable

**Choose Velocity Embedded if:**
- ✅ Need ACID transactions
- ✅ Need SQL queries
- ✅ Want highest throughput with durability
- ✅ Have PostgreSQL infrastructure
- ✅ Compliance requirements

**Choose Velocity Classic if:**
- ✅ Migrating from Temporal
- ✅ TypeScript-native development
- ✅ Need lowest latency
- ✅ Don't need durability (or can add it)
- ✅ Development/testing focus

## Conclusion

Each Velocity flavor excels in different scenarios:

- **Velocity Server** — Best for **simple, durable deployments** without external dependencies
- **Velocity Embedded** — Best for **production transactional workflows** with SQL queries
- **Velocity Classic** — Best for **Temporal migration** and **low-latency TypeScript workflows**

**Recommendation:** Start with **Velocity Embedded** for most production use cases. It offers the best balance of performance, durability, and queryability. Use **Classic** for development/testing or Temporal migration. Use **Server** for edge computing or when PostgreSQL is not available.

**Section sources**
- [bench-suite/prod-bench/src/main.rs](file://bench-suite/prod-bench/src/main.rs)
- [velocity-workflow-server/src/main.rs](file://velocity-workflow-server/src/main.rs)
- [velocity-classic-server/src/main.rs](file://velocity-classic-server/src/main.rs)
- [velocity-embedded-server/src/main.rs](file://velocity-embedded-server/src/main.rs)
- [velocity-server-bootstrap/src/lib.rs](file://velocity-server-bootstrap/src/lib.rs)
