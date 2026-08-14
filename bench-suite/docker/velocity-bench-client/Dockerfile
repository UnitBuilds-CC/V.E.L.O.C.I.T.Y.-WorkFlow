FROM rust:1.88-slim-bookworm AS build
RUN apt-get update && \
    apt-get install -y pkg-config libssl-dev protobuf-compiler && \
    rm -rf /var/lib/apt/lists/*
WORKDIR /build
# Copy workspace root and all workspace members needed for resolution
COPY Cargo.toml Cargo.lock ./
COPY velocity-bench/ velocity-bench/
COPY proto/ proto/
COPY velocity-workflow-core/ velocity-workflow-core/
COPY velocity-workflow-engine/ velocity-workflow-engine/
COPY velocity-workflow-daemon/ velocity-workflow-daemon/
COPY velocity-workflow-server/ velocity-workflow-server/
COPY velocity-dev-server/ velocity-dev-server/
COPY velocity-test-framework/ velocity-test-framework/
COPY velocity-embedded/ velocity-embedded/
COPY velocity-classic/ velocity-classic/
COPY migrations/ migrations/
RUN cargo build --release -p velocity-bench 2>&1 && \
    cp target/release/velocity-bench /usr/local/bin/ && \
    cp target/release/velocity-bench-http /usr/local/bin/
ENTRYPOINT ["velocity-bench-http"]
