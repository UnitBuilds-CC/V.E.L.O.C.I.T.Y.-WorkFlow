# Velocity Classic — Rust HTTP server with synchronous WAL durability
#
# Builds velocity-bench-server with /api/* routes (Temporal-compatible).
# Uses synchronous per-step fsync — matches DBOS/Restate/Temporal durability.
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
COPY migrations/               ./migrations/

RUN cargo build --release -p velocity-bench-server

# ── Runtime image ────────────────────────────────────────────────────────────
FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y curl ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/velocity-bench-server /usr/local/bin/

EXPOSE 8083

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8083/api/health || exit 1

# Run velocity-bench-server with /api/* routes on port 8083
# Synchronous durability (fsync per step) — matches competitor durability model
CMD ["velocity-bench-server", "--bind", "0.0.0.0:8083", "--wal-path", "/tmp/velocity-classic.wal"]
