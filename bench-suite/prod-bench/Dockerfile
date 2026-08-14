FROM rust:1.88-slim-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Create a minimal workspace Cargo.toml with only prod-bench
RUN echo '[workspace]\nmembers = ["bench-suite/prod-bench"]\nresolver = "2"\n\n[profile.release]\nopt-level = 2\nlto = false\ncodegen-units = 16\n' > Cargo.toml

COPY bench-suite/prod-bench/Cargo.toml bench-suite/prod-bench/Cargo.toml
COPY bench-suite/prod-bench/build.rs bench-suite/prod-bench/build.rs

# Copy proto files (needed by build.rs)
COPY velocity-bench/proto/ /bench-proto/

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
