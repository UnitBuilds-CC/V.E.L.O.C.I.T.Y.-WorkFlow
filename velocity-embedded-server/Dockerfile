# syntax=docker/dockerfile:1
FROM rust:1.78-slim AS builder

WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY velocity-workflow-core ./velocity-workflow-core
COPY velocity-workflow-engine ./velocity-workflow-engine
COPY velocity-embedded ./velocity-embedded
COPY velocity-embedded-server ./velocity-embedded-server

RUN cargo build --release -p velocity-embedded-server

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates libssl3 && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/velocity-embedded-server /usr/local/bin/

EXPOSE 8082
CMD ["velocity-embedded-server", "--bind", "0.0.0.0:8082"]
