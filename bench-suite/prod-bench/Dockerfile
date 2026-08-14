FROM rust:1.88-slim-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy workspace files needed for prod-bench
COPY Cargo.toml Cargo.toml
COPY bench-suite/prod-bench/Cargo.toml bench-suite/prod-bench/Cargo.toml

# Create stub crates for workspace members (so cargo can resolve the workspace)
RUN for dir in velocity-workflow-core velocity-workflow-engine velocity-workflow-daemon \
    velocity-workflow-server velocity-bench velocity-dev-server velocity-test-framework \
    velocity-embedded velocity-classic; do \
    mkdir -p $dir/src && \
    echo "fn main() {}" > $dir/src/main.rs && \
    echo "[package]\nname = \"$dir\"\nversion = \"0.1.0\"\nedition = \"2021\"\n" > $dir/Cargo.toml; \
    done

# Copy actual prod-bench source
COPY bench-suite/prod-bench/src/ bench-suite/prod-bench/src/

# Build only prod-bench
RUN cargo build --release -p prod-bench

# Runtime image
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=0 /app/target/release/prod-bench /usr/local/bin/prod-bench

ENTRYPOINT ["prod-bench"]
