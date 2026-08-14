# Velocity Classic — Rust HTTP server with WAL (Temporal-compatible)
#
# Builds velocity-classic-server: HTTP API on port 8083, WAL persistence.
# This is the Temporal-compatible deployment flavor.
#
# Build context: repository root (velocity-workflow/)

FROM rust:1.88-slim-bookworm AS builder

RUN apt-get update && \
    apt-get install -y protobuf-compiler && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Minimal workspace manifest for the classic server
RUN echo '[workspace]\nmembers = ["velocity-workflow-core", "velocity-workflow-engine", "velocity-classic", "velocity-classic-server"]\nresolver = "2"\n\n[profile.release]\nopt-level = 2\nlto = false\ncodegen-units = 16\n' > Cargo.toml

COPY velocity-workflow-core/ ./velocity-workflow-core/
COPY velocity-workflow-engine/ ./velocity-workflow-engine/
COPY velocity-classic/         ./velocity-classic/
COPY velocity-classic-server/  ./velocity-classic-server/
COPY migrations/               ./migrations/

RUN cargo build --release -p velocity-classic-server

# ── Runtime image ────────────────────────────────────────────────────────────
FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y curl ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/velocity-classic-server /usr/local/bin/

EXPOSE 8083

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8083/api/health || exit 1

CMD ["velocity-classic-server", "--bind", "0.0.0.0:8083"]
