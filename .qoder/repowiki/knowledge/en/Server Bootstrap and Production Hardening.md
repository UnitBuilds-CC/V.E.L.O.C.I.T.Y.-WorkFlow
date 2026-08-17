---
kind: infrastructure
name: Server Bootstrap and Production Hardening
category: operations
scope:
    - 'velocity-server-bootstrap/**'
source_files:
    - velocity-server-bootstrap/src/lib.rs
    - velocity-server-bootstrap/src/auth.rs
    - velocity-server-bootstrap/src/rate_limit.rs
    - velocity-server-bootstrap/src/audit.rs
    - velocity-server-bootstrap/src/tracing_setup.rs
    - velocity-server-bootstrap/tests/chaos_tests.rs
    - velocity-server-bootstrap/tests/production_hardening.rs
---

The `velocity-server-bootstrap` crate extracts common initialization code shared between all Velocity server binaries (Classic, Embedded, Server). Each server binary becomes ~30 lines: define CLI, create flavor-specific router, call bootstrap functions.

**Core Bootstrap Functions:**
- `bootstrap_engine()` — WAL creation, recovery, and optional PG adapter setup
- `bootstrap_nmcp()` — NMCP shmem + WebSocket server creation
- `run_server_loop()` — tokio::select! shutdown pattern with graceful drain
- `run_http_health_with_config()` — HTTP health/readiness/metrics endpoint
- `load_tls_config()` — TLS certificate and key loading for mTLS
- `create_workflow_state()` — Initialize workflow engine with persistence

**Production Hardening Features:**
- **Chaos/failure injection tests** — Validate behavior under network partitions, disk failures, OOM conditions
- **Production validation** — Automated scripts test all 3 flavors in Docker
- **Graceful shutdown** — tokio::select! with SIGTERM/SIGINT handling
- **Health probes** — K8s-compatible /health, /ready, /live endpoints
- **Metrics endpoint** — Prometheus-compatible /metrics with optional auth

**Module Structure:**
```
velocity-server-bootstrap/
├── src/
│   ├── lib.rs              # Core bootstrap functions
│   ├── auth.rs             # API key + JWT authentication
│   ├── rate_limit.rs       # Token bucket rate limiter
│   ├── audit.rs            # Structured audit logging
│   └── tracing_setup.rs    # OpenTelemetry distributed tracing
└── tests/
    ├── chaos_tests.rs          # Chaos/failure injection
    └── production_hardening.rs # Production validation tests
```

**Server Binary Pattern (~30 lines each):**
```rust
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    
    // Bootstrap engine (WAL + optional PG)
    let engine = bootstrap_engine(&cli.wal_path, cli.wal_max_size, cli.postgres.as_deref()).await?;
    
    // Bootstrap NMCP transport
    let (shmem, ws) = bootstrap_nmcp(&cli.shmem_path, &cli.ws_bind, router).await?;
    
    // Run server loop with graceful shutdown
    run_server_loop(shmem, ws, engine).await
}
```

**Allocator:**
- Uses `jemalloc` (tikv-jemallocator) as global allocator on non-MSVC targets
- Significantly faster for allocation-heavy workloads

**Key files:**
- `velocity-server-bootstrap/src/lib.rs` — Core bootstrap logic (773 lines)
- `velocity-server-bootstrap/src/auth.rs` — Authentication (597 lines)
- `velocity-server-bootstrap/src/tracing_setup.rs` — Tracing (459 lines)

**Rules for developers:**
1. All new server features should be added to bootstrap crate, not individual servers
2. Each server binary should remain minimal (~30 lines)
3. Always test bootstrap changes against all 3 flavors
4. Chaos tests must pass before merging production hardening changes
5. Health endpoints must never require authentication (K8s compatibility)
