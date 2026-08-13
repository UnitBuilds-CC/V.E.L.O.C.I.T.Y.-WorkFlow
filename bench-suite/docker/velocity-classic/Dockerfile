# Velocity Classic — Rust gRPC server with WAL (benchmark engine)
#
# Builds velocity-workflow-server from source and runs it in "classic" mode:
# gRPC on port 7234, real WAL with fsync, no HTTP transport.
#
# Build context: repository root (velocity-workflow/)

FROM rust:1.88-slim-bookworm

RUN apt-get update && \
    apt-get install -y protobuf-compiler libmnl-dev libnetfilter-log-dev \
                       iproute2 netcat-openbsd && \
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

# ── Runtime ──────────────────────────────────────────────────────────────────
EXPOSE 7234

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD nc -z localhost 7234 || exit 1

CMD ["./target/release/velocity-server", \
     "--ip", "0.0.0.0", \
     "--grpc-port", "7234", \
     "--real-engine"]
