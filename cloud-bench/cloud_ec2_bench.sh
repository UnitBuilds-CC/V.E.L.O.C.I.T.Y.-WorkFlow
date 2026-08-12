#!/usr/bin/env bash
# =============================================================================
# cloud-bench/cloud_ec2_bench.sh
#
# Runs ON the EC2 instance (uploaded by cloud_master.sh).
# Installs all dependencies, builds binaries, starts servers, runs benchmarks.
#
# Can also be run manually after SSH-ing into any Ubuntu instance:
#   curl -sO https://raw.githubusercontent.com/.../cloud_ec2_bench.sh
#   chmod +x cloud_ec2_bench.sh && ./cloud_ec2_bench.sh
#
# Environment variables:
#   PROFILE=standard   (quick | standard | stress)
#   WORKLOADS=all      (smoke | all)
# =============================================================================
set -euo pipefail

PROFILE="${PROFILE:-standard}"
WORKLOADS="${WORKLOADS:-all}"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
log()  { echo -e "${GREEN}[ec2]${NC} $*"; }
warn() { echo -e "${YELLOW}[ec2]${NC} $*"; }
info() { echo -e "${CYAN}[ec2]${NC} $*"; }

# ── Cleanup trap ────────────────────────────────────────────────────────────
cleanup() {
    log "Cleaning up background processes..."
    [[ -n "${DEV_PID:-}" ]] && kill "$DEV_PID" 2>/dev/null || true
    [[ -n "${BRIDGE_PID:-}" ]] && kill "$BRIDGE_PID" 2>/dev/null || true
    sudo docker compose -f ~/VELOCITY-WorkFlow/velocity-bench/docker-compose.temporal.yml down 2>/dev/null || true
}
trap cleanup EXIT

# ── 1. System packages ─────────────────────────────────────────────────────
log "[1/6] Installing system packages..."
export DEBIAN_FRONTEND=noninteractive
sudo apt-get update -qq
sudo apt-get install -y \
    build-essential pkg-config libssl-dev \
    protobuf-compiler \
    curl wget git unzip jq netcat-openbsd \
    docker.io docker-compose-v2

sudo systemctl enable --now docker
sudo usermod -aG docker "$USER" 2>/dev/null || true

log "  System packages installed."

# ── 2. Rust toolchain ──────────────────────────────────────────────────────
if ! command -v cargo &>/dev/null; then
    log "[2/6] Installing Rust toolchain..."
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable
    source "$HOME/.cargo/env"
else
    log "[2/6] Rust already installed: $(cargo --version)"
fi
export PATH="$HOME/.cargo/bin:$PATH"

# ── 3. Clone repository ────────────────────────────────────────────────────
REPO_DIR="$HOME/VELOCITY-WorkFlow"
if [ -d "$REPO_DIR" ]; then
    log "[3/6] Repository exists — pulling latest..."
    cd "$REPO_DIR"
    git pull --ff-only 2>/dev/null || warn "Using existing checkout"
else
    log "[3/6] Cloning repository..."
    git clone --depth 1 https://github.com/UnitBuilds-CC/V.E.L.O.C.I.T.Y.-WorkFlow.git "$REPO_DIR"
    cd "$REPO_DIR"
fi

# ── 4. Build Rust binaries ────────────────────────────────────────────────
log "[4/6] Building binaries (release mode)..."
log "  This takes ~3-5 minutes on first build (dependency compilation)..."
cargo build --release \
    -p velocity-dev-server \
    -p velocity-bench \
    2>&1 | grep -E '(Compiling|Finished|error)' || true

if [ ! -f target/release/velocity-dev ] || [ ! -f target/release/velocity-bench ] || [ ! -f target/release/temporal-bridge ]; then
    # Try building temporal-bridge separately (it's a separate [[bin]])
    cargo build --release --bin temporal-bridge 2>&1 | grep -E '(Compiling|Finished|error)' || true
fi

log "  Binaries:"
ls -lh target/release/velocity-dev \
       target/release/temporal-bridge \
       target/release/velocity-bench 2>/dev/null || {
    err "Build failed — binaries not found"
    exit 1
}

# ── 5. Start services ──────────────────────────────────────────────────────
log "[5/6] Starting services..."

# 5a. Real Temporal server via Docker
log "  Starting Temporal (Docker: PostgreSQL + Temporal + Web UI)..."
sudo docker compose -f velocity-bench/docker-compose.temporal.yml up -d 2>/dev/null || \
    docker compose -f velocity-bench/docker-compose.temporal.yml up -d

log "  Waiting for Temporal to be healthy (up to 2 minutes)..."
TEMPORAL_READY=false
for i in $(seq 1 40); do
    if nc -z localhost 7233 2>/dev/null; then
        TEMPORAL_READY=true
        break
    fi
    sleep 3
    printf "."
done
echo ""

if [ "$TEMPORAL_READY" = false ]; then
    warn "  Temporal not ready on :7233 — checking Docker status..."
    sudo docker compose -f velocity-bench/docker-compose.temporal.yml ps 2>/dev/null || true
    sudo docker compose -f velocity-bench/docker-compose.temporal.yml logs --tail 10 temporal 2>/dev/null || true
    # Continue anyway — benchmark uses temporal-bridge, not real Temporal directly
    warn "  Continuing with temporal-bridge for benchmark comparison."
else
    log "  Temporal ready (gRPC :7233, Web UI :8233)"
fi

# 5b. VELOCITY dev-server
log "  Starting VELOCITY dev-server (gRPC :7234)..."
./target/release/velocity-dev --grpc-port 7234 > /tmp/velocity-dev.log 2>&1 &
DEV_PID=$!

for i in $(seq 1 15); do
    nc -z localhost 7234 2>/dev/null && break
    sleep 1
done
log "  VELOCITY dev-server ready (PID $DEV_PID)"

# 5c. temporal-bridge
log "  Starting temporal-bridge (gRPC :7235)..."
./target/release/temporal-bridge --grpc-port 7235 > /tmp/temporal-bridge.log 2>&1 &
BRIDGE_PID=$!

for i in $(seq 1 10); do
    nc -z localhost 7235 2>/dev/null && break
    sleep 1
done
log "  temporal-bridge ready (PID $BRIDGE_PID)"

# ── 6. Print environment & run benchmark ───────────────────────────────────
echo ""
info "════════════════════════════════════════════════════════"
info "  Benchmark Environment                                "
info "════════════════════════════════════════════════════════"
info "  Hostname:  $(hostname)"
info "  OS:        $(lsb_release -ds 2>/dev/null || grep PRETTY_NAME /etc/os-release | cut -d= -f2 | tr -d '"')"
info "  Kernel:    $(uname -r)"
info "  CPU:       $(lscpu | grep 'Model name' | sed 's/Model name:\s*//' | xargs)"
info "  vCPUs:     $(nproc)"
info "  RAM:       $(free -h | awk '/^Mem:/{print $2}')"
info "  Rust:      $(rustc --version)"
info "  Docker:    $(docker --version 2>/dev/null || echo 'N/A')"
info "  VELOCITY:  $(git rev-parse --short HEAD 2>/dev/null || echo 'unknown')"
info "  Profile:   $PROFILE"
info "  Workloads: $WORKLOADS"
info "────────────────────────────────────────────────────────"
info "  VELOCITY dev-server:  localhost:7234"
info "  temporal-bridge:      localhost:7235"
info "  Real Temporal:        localhost:7233"
info "════════════════════════════════════════════════════════"
echo ""

log "[6/6] Running benchmark suite ($WORKLOADS workloads, $PROFILE profile)..."
log "  Estimated time: "
case "$PROFILE" in
    quick)   log "    ~5 minutes" ;;
    standard) log "    ~15 minutes" ;;
    stress)  log "    ~45 minutes" ;;
    *)       log "    ~15 minutes" ;;
esac
echo ""

# Remove old results
rm -f bench_results.md bench_results.csv bench_results.json

# Run the benchmark
./target/release/velocity-bench \
    --workloads "$WORKLOADS" \
    --engine both \
    --format all \
    --profile "$PROFILE" \
    --velocity-address http://localhost:7234 \
    --temporal-address http://localhost:7235 \
    --output bench_results.md

echo ""
log "════════════════════════════════════════════════════════"
log "  Benchmark complete!                                    "
log "════════════════════════════════════════════════════════"
echo ""

# Print summary
if [ -f bench_results.md ]; then
    log "Results:"
    ls -lh bench_results.md bench_results.csv bench_results.json 2>/dev/null
    echo ""

    # Show the summary table
    info "── Summary ──"
    sed -n '/^## Summary/,/^## Detailed/p' bench_results.md | head -20
    echo ""

    # Show the detailed comparison table header + first few rows
    info "── Top Workloads ──"
    sed -n '/^## Detailed Comparison/,/^## Per/p' bench_results.md | head -10
fi
