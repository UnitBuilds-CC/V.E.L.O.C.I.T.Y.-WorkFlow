# Velocity Embedded — Rust gRPC server with SQLite + WAL (benchmark engine)
#
# Same binary as velocity-classic, but uses SQLite as the storage backend
# instead of the default in-memory/WAL store.  SQLite data is persisted to
# /data so it survives container restarts.
#
# Build context: repository root (velocity-workflow/)

FROM rust:1.88-slim-bookworm

RUN apt-get update && \
    apt-get install -y protobuf-compiler libmnl-dev libnetfilter-log-dev \
                       iproute2 netcat-openbsd && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY proto/velocity /proto/velocity
COPY velocity-bench/proto/ /bench-proto/
COPY velocity-workflow-engine/ /engine/
COPY velocity-workflow-core/   /velocity-workflow-core/
COPY migrations/ /migrations/

COPY velocity-workflow-server/Cargo.toml velocity-workflow-server/Cargo.toml
COPY velocity-workflow-server/build.rs   velocity-workflow-server/build.rs
COPY velocity-workflow-server/src/       velocity-workflow-server/src/

RUN sed -i 's|../velocity-bench/proto/|/bench-proto/|g' velocity-workflow-server/build.rs && \
    sed -i 's|path = "../velocity-workflow-engine"|path = "/engine"|g' velocity-workflow-server/Cargo.toml && \
    sed -i 's|path = "../velocity-workflow-core"|path = "/velocity-workflow-core"|g' /engine/Cargo.toml

WORKDIR /app/velocity-workflow-server
RUN cargo build --release

EXPOSE 7234

VOLUME /data
ENV VELOCITY_DATA_DIR=/data

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD nc -z localhost 7234 || exit 1

CMD ["./target/release/velocity-server", \
     "--ip", "0.0.0.0", \
     "--grpc-port", "7234", \
     "--real-engine", \
     "--storage", "sqlite"]
