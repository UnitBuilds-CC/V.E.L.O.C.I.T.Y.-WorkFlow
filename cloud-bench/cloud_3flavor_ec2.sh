#!/usr/bin/env bash
# =============================================================================
# cloud-bench/cloud_3flavor_ec2.sh
#
# Runs ON each of the 6 VMs (uploaded by cloud_3flavor.sh).
# Detects which flavor to benchmark via FLAVOR env var.
#
# FLAVOR values:
#   velocity-classic    — Velocity Classic (gRPC :7234)
#   temporal            — Temporal (Docker, gRPC :7233)
#   velocity-runtime    — Velocity Runtime (HTTP :8080)
#   restate             — Restate (Docker, HTTP :8080)
#   velocity-embedded   — Velocity Embedded (HTTP :8080)
#   dbos                — DBOS + PostgreSQL (Docker)
#
# Environment:
#   PROFILE=standard    (quick | standard | stress)
#   WORKLOADS=all       (smoke | all)
# =============================================================================
set -euo pipefail

FLAVOR="${FLAVOR:-unknown}"
PROFILE="${PROFILE:-standard}"
WORKLOADS="${WORKLOADS:-all}"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
log()  { echo -e "${GREEN}[$FLAVOR]${NC} $*"; }
warn() { echo -e "${YELLOW}[$FLAVOR]${NC} $*"; }
info() { echo -e "${CYAN}[$FLAVOR]${NC} $*"; }
err()  { echo -e "${RED}[$FLAVOR]${NC} $*"; }

# ── Cleanup trap ────────────────────────────────────────────────────────────
cleanup() {
    log "Cleaning up background processes..."
    [[ -n "${SERVER_PID:-}" ]] && kill "$SERVER_PID" 2>/dev/null || true
}
trap cleanup EXIT

# ══════════════════════════════════════════════════════════════════════════════
# PHASE 1: System setup
# ══════════════════════════════════════════════════════════════════════════════
log "[1/5] Installing system packages..."
export DEBIAN_FRONTEND=noninteractive

for i in 1 2 3; do
    if sudo apt-get update; then break; fi
    warn "  apt-get update failed (attempt $i), retrying..."
    sleep 5
done

sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update

sudo apt-get install -y \
    build-essential pkg-config libssl-dev \
    protobuf-compiler \
    curl wget git unzip jq netcat-openbsd \
    docker-ce docker-ce-cli containerd.io docker-compose-plugin

sudo systemctl enable --now docker
sudo usermod -aG docker ubuntu 2>/dev/null || true

if ! sudo docker info >/dev/null 2>&1; then
    warn "  Docker daemon not ready yet, waiting..."
    sleep 5
    sudo systemctl restart docker
    sleep 3
fi
log "  Docker: $(sudo docker --version)"

# ══════════════════════════════════════════════════════════════════════════════
# PHASE 2: Rust toolchain
# ══════════════════════════════════════════════════════════════════════════════
if ! command -v cargo &>/dev/null; then
    log "[2/5] Installing Rust toolchain..."
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable
    source "$HOME/.cargo/env"
else
    log "[2/5] Rust already installed: $(cargo --version)"
fi
export PATH="$HOME/.cargo/bin:$PATH"

# ══════════════════════════════════════════════════════════════════════════════
# PHASE 3: Extract repository
# ══════════════════════════════════════════════════════════════════════════════
REPO_DIR="$HOME/VELOCITY-WorkFlow"
if [ -f "$HOME/velocity-repo.tar.gz" ]; then
    log "[3/5] Extracting uploaded repository..."
    rm -rf "$REPO_DIR"
    mkdir -p "$REPO_DIR"
    tar xzf "$HOME/velocity-repo.tar.gz" -C "$REPO_DIR"
    rm -f "$HOME/velocity-repo.tar.gz"
    cd "$REPO_DIR"
    if [ ! -f Cargo.toml ]; then
        err "ERROR: Cargo.toml not found after extraction!"
        exit 1
    fi
    log "  Repository extracted OK."
else
    log "[3/5] No tarball found — attempting git clone..."
    rm -rf "$REPO_DIR"
    git clone --depth 1 git@github.com:UnitBuilds-CC/V.E.L.O.C.I.T.Y.-WorkFlow.git "$REPO_DIR" 2>/dev/null || \
    git clone --depth 1 https://github.com/UnitBuilds-CC/V.E.L.O.C.I.T.Y.-WorkFlow.git "$REPO_DIR"
    cd "$REPO_DIR"
fi

# ══════════════════════════════════════════════════════════════════════════════
# PHASE 4: Build binaries (only for Velocity flavors)
# ══════════════════════════════════════════════════════════════════════════════
if [[ "$FLAVOR" == velocity-* ]]; then
    log "[4/5] Building Velocity binaries (release mode)..."
    log "  This takes ~3-5 minutes on first build..."

    case "$FLAVOR" in
        velocity-classic)
            cargo build --release \
                -p velocity-workflow-server \
                -p velocity-bench \
                2>&1 | grep -E '(Compiling|Finished|error)' || true
            ;;
        velocity-runtime)
            cargo build --release \
                -p velocity-bench \
                --bin velocity-bench-http \
                2>&1 | grep -E '(Compiling|Finished|error)' || true
            ;;
        velocity-embedded)
            cargo build --release \
                -p velocity-dev-server \
                2>&1 | grep -E '(Compiling|Finished|error)' || true
            ;;
    esac

    log "  Binaries:"
    ls -lh target/release/ 2>/dev/null | grep -E 'velocity|temporal' || true
else
    log "[4/5] Skipping build (competitor flavor: $FLAVOR)"
fi

# ══════════════════════════════════════════════════════════════════════════════
# PHASE 5: Start services and run benchmark
# ══════════════════════════════════════════════════════════════════════════════
log "[5/5] Starting services and running benchmark..."

case "$FLAVOR" in
    velocity-classic)
        # Start Velocity Classic (gRPC :7234)
        log "  Starting Velocity Classic (gRPC :7234)..."
        ./target/release/velocity-server --grpc-port 7234 > /tmp/velocity-classic.log 2>&1 &
        SERVER_PID=$!
        for i in $(seq 1 15); do
            nc -z localhost 7234 2>/dev/null && break
            sleep 1
        done
        log "  Velocity Classic ready (PID $SERVER_PID)"

        # Start Temporal competitor (Docker, gRPC :7233)
        log "  Starting Temporal (Docker)..."
        sudo docker compose -f velocity-bench/docker-compose.temporal.yml up -d 2>&1 || true
        for i in $(seq 1 40); do
            nc -z localhost 7233 2>/dev/null && break
            sleep 3
        done
        log "  Temporal ready (gRPC :7233)"

        # Run gRPC benchmark
        log "  Running gRPC benchmark..."
        ./target/release/velocity-bench \
            --workloads "$WORKLOADS" \
            --engine both \
            --format all \
            --profile "$PROFILE" \
            --velocity-address http://localhost:7234 \
            --temporal-address http://localhost:7233 \
            --output /tmp/bench/classic_results
        ;;

    temporal)
        # Start Temporal (Docker, gRPC :7233)
        log "  Starting Temporal (Docker)..."
        sudo docker compose -f velocity-bench/docker-compose.temporal.yml up -d 2>&1 || true
        for i in $(seq 1 40); do
            nc -z localhost 7233 2>/dev/null && break
            sleep 3
        done
        log "  Temporal ready (gRPC :7233)"

        # Build velocity-bench for comparison
        log "  Building velocity-bench for Temporal-only run..."
        cargo build --release -p velocity-bench 2>&1 | grep -E '(Compiling|Finished|error)' || true

        # Run Temporal-only benchmark (no Velocity comparison on this VM)
        log "  Running Temporal benchmark..."
        ./target/release/velocity-bench \
            --workloads "$WORKLOADS" \
            --engine temporal \
            --format all \
            --profile "$PROFILE" \
            --temporal-address http://localhost:7233 \
            --output /tmp/bench/temporal_results
        ;;

    velocity-runtime)
        # Start Velocity Runtime (HTTP :8080)
        log "  Starting Velocity Runtime (HTTP :8080)..."
        ./target/release/velocity-dev-server --http-port 8080 > /tmp/velocity-runtime.log 2>&1 &
        SERVER_PID=$!
        for i in $(seq 1 15); do
            curl -sf http://localhost:8080/health >/dev/null 2>&1 && break
            sleep 1
        done
        log "  Velocity Runtime ready (PID $SERVER_PID)"

        # Start Restate competitor (Docker, HTTP :8081)
        log "  Starting Restate (Docker)..."
        sudo docker compose -f velocity-bench/docker-compose.restate.yml up -d 2>&1 || true
        for i in $(seq 1 40); do
            nc -z localhost 8081 2>/dev/null && break
            sleep 3
        done
        log "  Restate ready (HTTP :8081)"

        # Run HTTP benchmark
        log "  Running HTTP benchmark..."
        ./target/release/velocity-bench-http \
            --workloads "$WORKLOADS" \
            --engine both \
            --format all \
            --profile "$PROFILE" \
            --velocity-address http://localhost:8080 \
            --restate-address http://localhost:8081 \
            --output /tmp/bench/runtime_results
        ;;

    restate)
        # Start Restate (Docker, HTTP :8081)
        log "  Starting Restate (Docker)..."
        sudo docker compose -f velocity-bench/docker-compose.restate.yml up -d 2>&1 || true
        for i in $(seq 1 40); do
            nc -z localhost 8081 2>/dev/null && break
            sleep 3
        done
        log "  Restate ready (HTTP :8081)"

        # Build velocity-bench-http for comparison
        log "  Building velocity-bench-http for Restate-only run..."
        cargo build --release --bin velocity-bench-http 2>&1 | grep -E '(Compiling|Finished|error)' || true

        # Run Restate-only benchmark
        log "  Running Restate benchmark..."
        ./target/release/velocity-bench-http \
            --workloads "$WORKLOADS" \
            --engine restate \
            --format all \
            --profile "$PROFILE" \
            --restate-address http://localhost:8081 \
            --output /tmp/bench/restate_results
        ;;

    velocity-embedded)
        # Start PostgreSQL for Velocity Embedded
        log "  Starting PostgreSQL (Docker)..."
        sudo docker compose -f velocity-bench/docker-compose.dbos.yml up -d 2>&1 || true
        for i in $(seq 1 30); do
            nc -z localhost 5432 2>/dev/null && break
            sleep 2
        done
        log "  PostgreSQL ready (:5432)"

        # Start Velocity Embedded (dev-server with Postgres, HTTP :8080)
        log "  Starting Velocity Embedded (HTTP :8080)..."
        ./target/release/velocity-dev-server \
            --http-port 8080 \
            --postgres-url postgresql://bench:bench@localhost:5432/benchdb \
            > /tmp/velocity-embedded.log 2>&1 &
        SERVER_PID=$!
        for i in $(seq 1 15); do
            curl -sf http://localhost:8080/health >/dev/null 2>&1 && break
            sleep 1
        done
        log "  Velocity Embedded ready (PID $SERVER_PID)"

        # Run embedded benchmark
        log "  Running embedded benchmark..."
        mkdir -p /tmp/bench
        FLAVOR=embedded bash velocity-bench/embedded_bench.sh
        ;;

    dbos)
        # Start PostgreSQL + DBOS (Docker)
        log "  Starting PostgreSQL (Docker)..."
        sudo docker compose -f velocity-bench/docker-compose.dbos.yml up -d 2>&1 || true
        for i in $(seq 1 30); do
            nc -z localhost 5432 2>/dev/null && break
            sleep 2
        done
        log "  PostgreSQL ready (:5432)"

        # Run DBOS benchmark (pgbench + DBOS HTTP API if available)
        log "  Running DBOS benchmark..."
        mkdir -p /tmp/bench
        FLAVOR=dbos bash velocity-bench/embedded_bench.sh
        ;;

    *)
        err "Unknown FLAVOR: $FLAVOR"
        err "Expected: velocity-classic | temporal | velocity-runtime | restate | velocity-embedded | dbos"
        exit 1
        ;;
esac

log ""
log "════════════════════════════════════════════════════════"
log "  Benchmark Complete ($FLAVOR)"
log "════════════════════════════════════════════════════════"
log "  Results:"
ls -lh /tmp/bench/ 2>/dev/null || true
log "════════════════════════════════════════════════════════"
