# =============================================================================
# Multi-stage Dockerfile for Velocity Workflow Server
# Stage 1: Build Rust engine (cdylib + rlib)
# Stage 2: Build .NET server (net10.0)
# Stage 3: Minimal runtime image
# =============================================================================

# ── Stage 1: Rust Builder ────────────────────────────────────────────────────
FROM rust:1.82-slim-bookworm AS rust-builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config libssl-dev protobuf-compiler && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /build/rust

# Cache dependencies first
COPY velocity-workflow-core/Cargo.toml velocity-workflow-core/Cargo.toml
COPY velocity-workflow-engine/Cargo.toml velocity-workflow-engine/Cargo.toml
COPY velocity-workflow-daemon/Cargo.toml velocity-workflow-daemon/Cargo.toml

# Create dummy lib.rs files to cache dependency builds
RUN mkdir -p velocity-workflow-core/src && echo "pub fn dummy(){}" > velocity-workflow-core/src/lib.rs && \
    mkdir -p velocity-workflow-engine/src && echo "pub fn dummy(){}" > velocity-workflow-engine/src/lib.rs && \
    mkdir -p velocity-workflow-daemon/src && echo "fn main(){}" > velocity-workflow-daemon/src/main.rs

RUN cargo build --release --manifest-path velocity-workflow-engine/Cargo.toml || true

# Copy actual source and build
COPY velocity-workflow-core/ velocity-workflow-core/
COPY velocity-workflow-engine/ velocity-workflow-engine/
COPY velocity-workflow-daemon/ velocity-workflow-daemon/

RUN cargo build --release --manifest-path velocity-workflow-engine/Cargo.toml && \
    cargo build --release --manifest-path velocity-workflow-core/Cargo.toml

# ── Stage 2: .NET Builder ────────────────────────────────────────────────────
FROM mcr.microsoft.com/dotnet/sdk:10.0-preview AS dotnet-builder

WORKDIR /build/dotnet

# Copy Rust native libraries to the expected location for .NET build
COPY --from=rust-builder /build/rust/target/release/libvelocity_workflow_core.so \
    /build/dotnet/src/Velocity.Workflow.Core/runtimes/linux-x64/native/velocity_workflow_core.so
COPY --from=rust-builder /build/rust/target/release/libvelocity_workflow_engine.so \
    /build/dotnet/src/Velocity.Workflow.Core/runtimes/linux-x64/native/velocity_workflow_engine.so

# Copy .NET solution
COPY src/ src/
COPY Velocity.Workflow.slnx ./

# Build the server project
RUN dotnet publish src/Velocity.Workflow.Server/Velocity.Workflow.Server.csproj \
    -c Release \
    -o /app/publish \
    --no-self-contained \
    -r linux-x64 \
    /p:UseAppHost=false

# ── Stage 3: Runtime ─────────────────────────────────────────────────────────
FROM mcr.microsoft.com/dotnet/aspnet:9.0 AS runtime

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates && \
    rm -rf /var/lib/apt/lists/* && \
    groupadd -r velocity && useradd -r -g velocity -m -s /bin/false velocity

WORKDIR /app

# Copy published .NET app
COPY --from=dotnet-builder /app/publish .

# Copy native Rust libraries to runtime location
COPY --from=rust-builder /build/rust/target/release/libvelocity_workflow_core.so /app/lib/
COPY --from=rust-builder /build/rust/target/release/libvelocity_workflow_engine.so /app/lib/

ENV LD_LIBRARY_PATH="/app/lib"
ENV ASPNETCORE_URLS="http://+:5000"
ENV ASPNETCORE_ENVIRONMENT="Production"

EXPOSE 5000
EXPOSE 50051

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:5000/health || exit 1

USER velocity

ENTRYPOINT ["dotnet", "Velocity.Workflow.Server.dll"]
