# Velocity .qoder Directory Structure

This directory contains AI-optimized documentation and specifications for the V.E.L.O.C.I.T.Y. Workflow project, following the pattern established by the Dwarven Stronghold project.

## Structure

```
.qoder/
├── repowiki/                        # Comprehensive documentation wiki
│   ├── en/                          # English documentation
│   │   ├── content/                 # Main documentation pages
│   │   │   ├── Getting Started.md
│   │   │   ├── Development Guide.md
│   │   │   ├── Architecture Overview.md
│   │   │   ├── Flavor Comparison Guide.md
│   │   │   ├── Velocity Server (Single Binary).md
│   │   │   ├── Velocity Embedded (PostgreSQL).md
│   │   │   └── Velocity Classic (TypeScript).md
│   │   └── meta/                    # Metadata and indexes
│   │       └── repowiki-metadata.json
│   └── knowledge/                   # Knowledge cards (patterns, conventions)
│       └── en/
│           ├── _index.yaml
│           ├── Write-Ahead Log WAL Persistence.md
│           ├── PostgreSQL Persistence with Connection Pooling.md
│           ├── gRPC Benchmark Service API.md
│           ├── NMCP Protocol Transport.md
│           ├── Server Bootstrap and Production Hardening.md
│           ├── API Authentication and Security.md
│           ├── Distributed Tracing OpenTelemetry.md
│           ├── PostgreSQL Advisory Locking.md
│           ├── Per-Step Journal Persistence.md
│           ├── VCTP Protocol Transport.md
│           ├── VCTP RPC Server.md
│           ├── VCTP Gateways and Sidecar Proxy.md
│           ├── VCTP SDKs and Developer Tools.md
│           ├── Slab Engine Merkle Root State Proof.md
│           └── VCTP Kubernetes Deployment.md
└── specs/                           # Feature specifications
    ├── classic_server_nmcp_upgrade.md
    ├── embedded_server_nmcp_upgrade.md
    ├── workflow_server_vctp_upgrade.md
    ├── Velocity_Flavors_Audit_Report.md
    └── Distributed_Workflow_Sharding_and_Horizontal_Scaling.md
```

### Additional Directories (not shown in tree above)
- `benchmarks/Velocity.Workflow.Benchmarks/` — C# lifecycle benchmark suite (complements Rust prod-bench)
- `docs/ops-runbooks.md` — Operations runbooks for common incident scenarios (15 runbooks incl. VCTP circuit breaker, replay, HMAC, TLS)
- `docs/security-audit-checklist.md` — 65-point security audit checklist covering all 39 hardening items
- `docs/otlp-tracing-guide.md` — OTLP/Jaeger/Tempo distributed tracing configuration guide
- `deploy/scripts/vctp-prod-loadtest.sh` — Kubernetes-native VCTP production load test (100 clients, CI thresholds)
- `deploy/scripts/wal-backup.sh` — WAL backup script with encryption, checksums, S3 upload, and retention

## Documentation Pages

### Getting Started.md
Introduction to Velocity, project structure, installation, running engines, and benchmarking.

**Contents:**
- Project overview and three flavors (Server, Embedded, Classic)
- Directory structure and core components (15 workspace crates + VCTP tools)
- VCTP RPC server (HMAC-SHA256 auth encryption, replay protection), gateways (TLS/HTTPS/WSS), and developer tools
- Installation and setup instructions
- Running individual flavors (NMCP shmem + WebSocket transport, VCTP UDP :9090, HTTPS :8443, WSS :8444)
- Benchmark commands, VCTP benchmarks (9 benchmarks with CI thresholds), and results summary
- Troubleshooting guide

### Development Guide.md
Comprehensive guide for developers contributing to Velocity.

**Contents:**
- Development environment setup (Rust, Node.js, Docker)
- Project architecture and module organization (15 crates + VCTP tools)
- Building and testing procedures (2,547 total tests: 2,541 engine + 6 sidecar)
- VCTP development workflow (building, testing with 11 test categories, CLI, Wireshark, OpenAPI)
- TLS configuration for gateways (HTTPS/WSS setup)
- VCTP authenticated encryption (HMAC-SHA256, replay protection)
- Code style and conventions (Rust, TypeScript)
- Adding new features and SDK methods
- Protocol buffer development
- SDK development (TypeScript, Python, Go, Java — all with VCTP transport)
- Benchmark development
- Docker development workflow
- Performance profiling techniques
- CI/CD with GitHub Actions (chaos tests, Trivy scanning, benchmark regression gates)
- Production hardening and security features (TLS, HMAC, replay, chaos engineering, Prometheus alerts)

### Architecture Overview.md
Deep dive into Velocity's system architecture and design decisions.

**Contents:**
- System architecture diagram (15 workspace crates + VCTP tools + gateway layer)
- Engine flavors comparison (Server, Embedded, Classic — all Rust)
- NMCP protocol transport (shmem IPC + WebSocket)
- VCTP protocol transport (UDP, 28-byte header, CRC32, circuit breaker, heartbeat, drain, HMAC-SHA256, replay protection, XOR cipher)
- VCTP gateways and sidecar proxy (WebSocket with WSS + rate limiting, HTTP with HTTPS + rate limiting, ECDH+XOR)
- VCTP performance table (9 benchmarks with CI thresholds)
- Slab engine with Merkle root state proof (SHA-256, Bitmask256)
- Persistence layers (WAL, PostgreSQL, Per-Step Journal)
- Security layer (auth, rate limiting, audit logging, TLS termination, VCTP authenticated encryption, replay protection, 17 Prometheus alerts)
- Distributed tracing (OpenTelemetry/OTLP)
- PG advisory locking for multi-instance coordination
- Protocol buffers and gRPC (legacy, still used by bench-suite)
- SDK architecture (TypeScript, Python, Go, Java — all with VCTP transport)
- Benchmark architecture
- Deployment architecture (Docker, Kubernetes with VCTP UDP port, TLS config)
- Data flow diagrams

### Flavor Comparison Guide.md
Side-by-side comparison of all three flavors to help choose the right one.

**Contents:**
- Quick comparison table
- Performance comparison (throughput, latency, memory)
- Architecture comparison (NMCP transport, persistence)
- API comparison (shmem IPC vs WebSocket vs gRPC)
- Deployment comparison
- Use case matrix and decision framework

### Velocity Server (Single Binary).md
Detailed documentation for the gRPC + WAL server flavor.

**Contents:**
- Architecture (gRPC BenchmarkService + WAL persistence)
- WAL persistence with group-commit optimization
- gRPC API reference
- Configuration and performance characteristics
- Deployment (Docker, K8s, bare metal)

### Velocity Embedded (PostgreSQL).md
Detailed documentation for the PostgreSQL-backed server flavor.

**Contents:**
- Architecture (NMCP transport + PostgreSQL)
- PostgreSQL integration with connection pooling
- Per-step journal persistence
- HTTP API reference
- Database schema and migrations
- Configuration and performance characteristics

### Velocity Classic (TypeScript).md
Detailed documentation for the Rust Classic server (replaced TypeScript).

**Contents:**
- Architecture (NMCP transport + WAL + optional PostgreSQL)
- Worker system and Temporal compatibility
- NMCP shmem IPC for local workers
- WebSocket for remote clients
- Configuration and performance characteristics

## Knowledge Cards

### Write-Ahead Log WAL Persistence.md
Documents the WAL persistence system used by Velocity Server.

**Key topics:**
- WAL entry format and event types
- Write path and recovery path
- Group-commit optimization (background thread)
- Performance characteristics
- Configuration options

### PostgreSQL Persistence with Connection Pooling.md
Documents the PostgreSQL persistence used by Velocity Embedded.

**Key topics:**
- Database schema design
- Connection pooling with deadpool-postgres
- Transaction isolation levels
- Migration system
- Query optimization

### gRPC Benchmark Service API.md
Documents the gRPC API used for workflow benchmarking.

**Key topics:**
- Protocol buffer service definition
- Message types and RPCs
- Server and client implementations
- Code generation for multiple languages

### NMCP Protocol Transport.md
Documents the NMCP binary protocol used by all 3 flavors.

**Key topics:**
- Binary frame format (16-byte header + JSON payload)
- Shared memory IPC (shmem) for local workers
- WebSocket transport for remote clients
- NmcpDispatch trait and router pattern
- 50-100x faster local IPC than HTTP

### Server Bootstrap and Production Hardening.md
Documents the shared server bootstrap crate and production hardening.

**Key topics:**
- bootstrap_engine / bootstrap_nmcp / run_server_loop
- Chaos and failure injection tests
- Production validation for all 3 flavors
- TLS/mTLS support
- jemalloc global allocator

### API Authentication and Security.md
Documents the authentication and security layer.

**Key topics:**
- API key authentication (plain + SHA-256 hashed)
- JWT validation (HS256/RS256) with key rotation
- Token bucket rate limiting
- Structured audit logging
- Security headers and Trivy container scanning

### Distributed Tracing OpenTelemetry.md
Documents the distributed tracing foundation.

**Key topics:**
- OpenTelemetry integration with optional OTLP export
- Span hierarchy (workflow.execute, step.persist, signal.deliver)
- Configurable sampling rate
- Log formats (compact, JSON)
- Integration with Jaeger/Tempo/Grafana

### PostgreSQL Advisory Locking.md
Documents multi-instance coordination via PG advisory locks.

**Key topics:**
- Leader election (one instance runs periodic tasks)
- Workflow locking (one instance per workflow)
- Migration locking (one instance runs schema migrations)
- Lock key space partitioning
- Exponential backoff with jitter

### Per-Step Journal Persistence.md
Documents per-step durability with batch INSERT.

**Key topics:**
- Append-only step journal
- Batch INSERT for per-step durability
- Crash recovery from journal
- Integration with PostgreSQL adapter

### VCTP Protocol Transport.md
Documents the VCTP (Velocity Transfer Protocol) zero-copy UDP-based RPC protocol.

**Key topics:**
- Binary wire format (28-byte header + payload + CRC32)
- UDP socket transport with non-blocking I/O
- Retransmission tracking and AIMD congestion control
- Reorder buffer (BTreeMap in-order delivery)
- Packet fragmentation and reassembly
- HMAC-SHA256 authenticated encryption with constant-time verification
- VctpReplayWindow: 64-depth sliding window for O(1) replay detection
- XOR cipher with AES-256 key schedule
- Cross-network benchmark (4 zones with artificial latency)
- Performance benchmarks (9,052 ops/s full-stack, 9 benchmark metrics with CI thresholds)

### VCTP RPC Server.md
Documents the VCTP RPC server request processing pipeline.

**Key topics:**
- Full pipeline: drain → circuit breaker → rate limit → auth → idempotency → dispatch
- Circuit breaker (Closed/Open/HalfOpen states)
- Heartbeat mechanism (30s interval, 90s eviction)
- Graceful drain with K8s preStop hook
- Tokio async worker pool
- Prometheus metrics export with 6 VCTP-specific alert rules
- RwLock safety improvements (27 unwrap → expect with descriptive messages)
- Chaos engineering tests (4 tests: reorder overflow, 10K flood, malformed packets, drain-under-load)
- Concurrent stress benchmark (100 clients × 50 requests, ≥2,000 ops/s)
- Cross-network benchmark (4 zones, ≥1,000 ops/s)

### VCTP Gateways and Sidecar Proxy.md
Documents the protocol bridge gateways and crypto offload proxy.

**Key topics:**
- WebSocket-to-VCTP gateway for browser clients (WSS via tokio-rustls, per-connection rate limiting, 692 lines)
- HTTP-to-VCTP ingress with Swagger UI at /docs (HTTPS via axum-server + rustls, per-second window rate limiting, 5 integration tests, 871 lines)
- Sidecar proxy with ECDH session + XOR cipher (474 lines)
- TLS configuration (cert/key paths, dual-path accept for WS)
- Test coverage (12 WS tests + 20 HTTP tests = 32 gateway tests)

### VCTP SDKs and Developer Tools.md
Documents the VCTP developer ecosystem.

**Key topics:**
- TypeScript SDK (358 lines)
- Python SDK (316 lines)
- Go SDK (451 lines)
- vctp-cli Python CLI tool
- Wireshark Lua dissector
- OpenAPI 3.0 spec generator
- Protocol definition schema (JSON Schema)

### Slab Engine Merkle Root State Proof.md
Documents the cryptographic state verification system.

**Key topics:**
- repr(C) SlabHeader (128 bytes) with SHA-256 Merkle root
- Bitmask256 O(1) step completion tracking
- Merkle root recomputed on every step completion
- Verification detects any state tampering
- Database persistence via WorkflowRecord::from_context()

### VCTP Kubernetes Deployment.md
Documents K8s deployment with Helm charts.

**Key topics:**
- VCTP UDP port (9090) exposure
- Health probes (HTTP + VCTP exec)
- Graceful drain with preStop hook
- Helm values for circuit breaker, heartbeat, security
- TLS gateway termination config (secretName, HTTPS 8443, WSS 8444, cert-manager compatible)
- Prometheus alert rules (6 VCTP-specific alerts + note about 17 total)

## Specifications

### Distributed_Workflow_Sharding_and_Horizontal_Scaling.md
Specification for implementing horizontal scaling via workflow sharding.

### VCTP/NMCP Protocol Upgrade Specs
Specifications for upgrading all 3 flavors to VCTP/NMCP transport:
- `workflow_server_vctp_upgrade.md` — Server gRPC → VCTP
- `classic_server_nmcp_upgrade.md` — Classic HTTP → NMCP
- `embedded_server_nmcp_upgrade.md` — Embedded HTTP → NMCP

### Velocity_Flavors_Audit_Report.md
Comprehensive audit report for all three Velocity flavors.

## Metadata

### repowiki-metadata.json
YAML-formatted metadata about the project:
- Project information (name, description, repository)
- Workspace crates (15 members)
- Engine flavors with performance metrics
- SDK status and paths
- Security features and hardening (TLS, HMAC-SHA256, replay protection, XOR cipher, gateway rate limiting, RwLock safety)
- VCTP protocol (18 features, gateways, 9 benchmarks, 2,547 total tests)
- Competitor benchmarks
- Build configuration (Rust, TypeScript)
- Deployment information (Docker, Kubernetes with TLS config)
- CI workflows with benchmark regression gates

## Usage

This documentation is designed to be:
1. **AI-readable** — Structured for AI assistants to understand the codebase
2. **Developer-friendly** — Clear guides for human developers
3. **Comprehensive** — Covers architecture, patterns, and conventions
4. **Maintainable** — Easy to update as the project evolves

### For AI Assistants
When working on Velocity, reference these docs to understand:
- Where to find specific functionality
- What patterns and conventions to follow
- How different components interact
- What performance characteristics to expect
- Security and authentication requirements

### For Developers
Use these docs to:
- Get started with Velocity development
- Understand the architecture before making changes
- Follow established patterns and conventions
- Learn about persistence, NMCP transport, and security
- Plan and implement new features

## Contributing

When adding new features or patterns:
1. Update relevant documentation pages in `repowiki/en/content/`
2. Add knowledge cards for new patterns in `repowiki/knowledge/en/`
3. Create specs for major features in `specs/`
4. Update metadata in `repowiki/en/meta/repowiki-metadata.json`
5. Update `_index.yaml` when adding new knowledge cards

## License

This documentation is part of the V.E.L.O.C.I.T.Y. Workflow project and follows the same license.
