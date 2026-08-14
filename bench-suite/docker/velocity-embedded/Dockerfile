# Velocity Embedded — Rust HTTP server with PostgreSQL + WAL
#
# Builds velocity-embedded-server: HTTP API on port 18082, PostgreSQL persistence.
# This is the DBOS-compatible deployment flavor.
#
# Build context: repository root (velocity-workflow/)

FROM rust:1.88-slim-bookworm AS builder

RUN apt-get update && \
    apt-get install -y protobuf-compiler && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Minimal workspace manifest for the embedded server
RUN echo '[workspace]\nmembers = ["velocity-workflow-core", "velocity-workflow-engine", "velocity-embedded", "velocity-embedded-server"]\nresolver = "2"\n\n[profile.release]\nopt-level = 2\nlto = false\ncodegen-units = 16\n' > Cargo.toml

COPY velocity-workflow-core/    ./velocity-workflow-core/
COPY velocity-workflow-engine/  ./velocity-workflow-engine/
COPY velocity-embedded/         ./velocity-embedded/
COPY velocity-embedded-server/  ./velocity-embedded-server/
COPY migrations/                ./migrations/

RUN cargo build --release -p velocity-embedded-server

# ── Runtime image ────────────────────────────────────────────────────────────
FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y curl ca-certificates libssl3 && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/velocity-embedded-server /usr/local/bin/

EXPOSE 18082

HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=5 \
    CMD curl -f http://localhost:18082/health || exit 1

CMD ["velocity-embedded-server", "--bind", "0.0.0.0:18082"]
