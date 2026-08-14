# syntax=docker/dockerfile:1
FROM rust:1.88-slim-bookworm AS builder

WORKDIR /build

# Create a minimal workspace Cargo.toml with only the crates we need
RUN echo '[workspace]\nmembers = ["velocity-workflow-core", "velocity-workflow-engine", "velocity-classic", "velocity-classic-server"]\nresolver = "2"\n\n[profile.release]\nopt-level = 2\nlto = false\ncodegen-units = 16\n' > Cargo.toml

COPY velocity-workflow-core ./velocity-workflow-core
COPY velocity-workflow-engine ./velocity-workflow-engine
COPY velocity-classic ./velocity-classic
COPY velocity-classic-server ./velocity-classic-server

# Also need migrations for engine include_str!
COPY migrations ./migrations

RUN cargo build --release -p velocity-classic-server

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates libssl3 && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/velocity-classic-server /usr/local/bin/

EXPOSE 8083
CMD ["velocity-classic-server", "--bind", "0.0.0.0:8083"]
