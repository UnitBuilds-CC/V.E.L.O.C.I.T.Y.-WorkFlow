# Changelog

All notable changes to the Velocity Workflow Engine are documented in this file.

---

## [1.0.0] — 2026-08-17

Initial production release. A hardware-native, zero-allocation durable workflow engine with VCTP (Velocity Compact Transport Protocol) providing authenticated encryption, replay protection, and sub-millisecond latency.

### Server Flavors

- **Velocity Server (VCTP)** — Zero-copy UDP transport with HMAC-SHA256 authenticated encryption, 64-depth replay protection window, and circuit breaker overload protection
- **Velocity Classic Server** — HTTP/HTTPS + WebSocket/WSS gateways with TLS 1.3 (rustls), per-connection rate limiting, and JWT/API key authentication
- **Velocity Embedded Server** — PostgreSQL-backed single binary for simplified deployment with full WAL crash recovery

### Transport Security

- TLS 1.3 for HTTPS gateway (axum-server + rustls 0.23)
- TLS 1.3 for WSS gateway (tokio-rustls 0.26)
- mTLS for server-to-server communication
- cert-manager integration for automatic certificate issuance and renewal
- HMAC-SHA256 packet-level authenticated encryption with constant-time verification
- 64-depth sliding window replay detection (≥10M checks/s)
- XOR cipher with AES-256 key schedule for defense-in-depth

### Access Control

- API key authentication (`VELOCITY_API_KEYS`)
- JWT authentication (HS256/RS256) with zero-downtime key rotation
- Server-side rate limiting (DashMap-based token bucket per client IP)
- Gateway rate limiting (HTTP per-second, WebSocket per-connection)

### Persistence

- Write-Ahead Log (WAL) with CRC-verified crash recovery
- Per-step journal entries with SHA-256 Merkle root tamper evidence
- PostgreSQL step persistence with async write queue
- Encrypted WAL backup with SHA-256 integrity checksums and S3 upload

### Observability

- 17 Prometheus alert rules (11 HTTP + 6 VCTP)
- Structured JSON audit logging for all API calls
- OpenTelemetry/OTLP distributed tracing (Jaeger, Tempo, Grafana backends)
- Pre-built Grafana dashboard (RED metrics, VCTP throughput, persistence latency)
- Loki log aggregation with Promtail

### Monitoring & Alerting

- `VctpCircuitBreakerOpen` — Circuit breaker state change
- `VctpHighErrorRate` — >5% error rate for 2m
- `VctpHighLatency` — p99 >100ms for 5m
- `VctpReplayDetected` — Replay attempts >0
- `VctpHighThroughput` / `VctpLowThroughput` — Throughput bounds

### Kubernetes Deployment

- Full Helm chart with production defaults
- Network policies (deny-all + allow Velocity traffic)
- Pod security contexts (non-root, read-only filesystem)
- RBAC with minimal service account permissions
- Horizontal pod autoscaler
- Pod disruption budget
- Graceful drain with preStop hook
- Backup CronJob (WAL snapshot + PostgreSQL dump + retention cleanup)
- Velero integration for disaster recovery

### CI/CD Gates

- Benchmark regression gate: ≥500 ops/s
- Tail latency gate: p99 <100ms
- Error rate gate: <5%
- Chaos/failure injection tests
- Trivy container scanning

### SDKs

- **TypeScript** (`@velocity-workflow/sdk`) — npm package with client, worker, workflow, activity, and VCTP transport
- **Python** (`velocity-workflow`) — PyPI package with gRPC client and async support
- **Go** (`github.com/velocity-workflow/sdk-go`) — Go module with client, worker, and VCTP sub-package
- **Java** (`io.velocity:velocity-sdk-java`) — Maven Central with gRPC stubs
- **Rust** (`velocity-sdk`) — Crates.io with native engine integration

### Distribution

- Docker images: `ghcr.io/velocity-workflow/velocity-workflow-server` (linux/amd64, linux/arm64)
- Helm chart: `velocity/velocity` v1.0.0
- Native binaries: linux/amd64, linux/arm64, darwin/amd64, darwin/arm64, windows/amd64
- 3 server variants per platform: `velocity-server`, `velocity-classic-server`, `velocity-embedded-server`
- `cargo install velocity-classic-server` / `velocity-embedded-server` / `velocity-workflow-server`

### Operational Documentation

- [Operations Runbooks](docs/ops-runbooks.md) — 15 incident response procedures
- [Security Audit Checklist](docs/security-audit-checklist.md) — 65-point pre-go-live review
- [OTLP Tracing Guide](docs/otlp-tracing-guide.md) — Distributed tracing configuration
- [WAL Backup Script](deploy/scripts/wal-backup.sh) — Encrypted backup with S3 support
- [Production Load Test](deploy/scripts/vctp-prod-loadtest.sh) — Kubernetes-native VCTP load validation

### Performance

- Zero-allocation hot path (Rust, no GC)
- ≥500 ops/s sustained throughput (CI gate)
- <100ms p99 tail latency (CI gate)
- <5% error rate under load (CI gate)
- ≥10M replay checks/s (64-depth sliding window)
- jemalloc allocator on non-MSVC targets

---

*For the full architectural analysis, see [docs/](docs/).*
