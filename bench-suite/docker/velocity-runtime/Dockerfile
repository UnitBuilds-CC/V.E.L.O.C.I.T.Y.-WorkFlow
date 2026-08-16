# Velocity Production Benchmark Server — Rust HTTP server with WAL
#
# Builds velocity-bench-server: HTTP API on port 7234, WAL persistence.
# Uses the REAL WorkflowEngine — not a mock or in-memory DevEngine.
#
# Architecture:
#   [benchmark client] ──HTTP──► [velocity-bench-server] ──► [WorkflowEngine + WAL]
#
# Build context: repository root (velocity-workflow/)

FROM rust:1.88-slim-bookworm AS builder

RUN apt-get update && \
    apt-get install -y protobuf-compiler && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Minimal workspace manifest for the bench server
RUN echo '[workspace]\nmembers = ["velocity-workflow-core", "velocity-workflow-engine", "bench-suite/velocity-bench-server"]\nresolver = "2"\n\n[profile.release]\nopt-level = 2\nlto = false\ncodegen-units = 16\n' > Cargo.toml

COPY velocity-workflow-core/ ./velocity-workflow-core/
COPY velocity-workflow-engine/ ./velocity-workflow-engine/
COPY bench-suite/velocity-bench-server/ ./bench-suite/velocity-bench-server/
COPY migrations/ ./migrations/

RUN cargo build --release -p velocity-bench-server

# ── Runtime image ────────────────────────────────────────────────────────────
FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y curl ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/velocity-bench-server /usr/local/bin/

EXPOSE 7234

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:7234/health || exit 1

CMD ["velocity-bench-server", "--bind", "0.0.0.0:7234"]
