FROM rust:1.88-slim-bookworm
WORKDIR /build
# Copy workspace files needed for velocity-bench
COPY Cargo.toml Cargo.lock ./
COPY velocity-bench/ velocity-bench/
COPY proto/ proto/
COPY velocity-workflow-core/ velocity-workflow-core/
COPY velocity-workflow-engine/ velocity-workflow-engine/
COPY velocity-sdk-rust/ velocity-sdk-rust/
COPY src/ src/
RUN cargo build --release -p velocity-bench 2>&1 && \
    cp target/release/velocity-bench /usr/local/bin/
ENTRYPOINT ["velocity-bench"]
