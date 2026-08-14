# Velocity Runtime — Rust gRPC + HTTP server with WAL
#
# Builds velocity-workflow-server: gRPC on 7234, HTTP on 8080, WAL persistence.
# This is the Restate-compatible deployment flavor.
#
# Build context: repository root (velocity-workflow/)

FROM rust:1.88-slim-bookworm AS builder

RUN apt-get update && \
    apt-get install -y protobuf-compiler libmnl-dev libnetfilter-log-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# ── Proto files (needed by build.rs) ─────────────────────────────────────────
COPY proto/velocity /proto/velocity
COPY velocity-bench/proto/ /bench-proto/

# ── Engine + core source (Rust dependencies) ─────────────────────────────────
COPY velocity-workflow-engine/ /engine/
COPY velocity-workflow-core/   /velocity-workflow-core/

# ── Migrations (included via include_str!) ───────────────────────────────────
COPY migrations/ /migrations/

# ── Server source ────────────────────────────────────────────────────────────
COPY velocity-workflow-server/Cargo.toml velocity-workflow-server/Cargo.toml
COPY velocity-workflow-server/build.rs   velocity-workflow-server/build.rs
COPY velocity-workflow-server/src/       velocity-workflow-server/src/

# Fix proto / dependency paths for the container layout
RUN sed -i 's|../velocity-bench/proto/|/bench-proto/|g' velocity-workflow-server/build.rs && \
    sed -i 's|path = "../velocity-workflow-engine"|path = "/engine"|g' velocity-workflow-server/Cargo.toml && \
    sed -i 's|path = "../velocity-workflow-core"|path = "/velocity-workflow-core"|g' /engine/Cargo.toml

# ── Build ────────────────────────────────────────────────────────────────────
WORKDIR /app/velocity-workflow-server
RUN cargo build --release

# ── Runtime image ────────────────────────────────────────────────────────────
FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y netcat-openbsd ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/velocity-workflow-server/target/release/velocity-server /usr/local/bin/velocity-server

EXPOSE 7234

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD nc -z localhost 7234 || exit 1

CMD ["velocity-server", \
     "--ip", "0.0.0.0", \
     "--grpc-port", "7234"]
