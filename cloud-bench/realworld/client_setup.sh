#!/usr/bin/env bash
# =============================================================================
# cloud-bench/realworld/client_setup.sh
#
# Runs ON the client EC2 instance (t3.micro or similar).
# Connects to the server instance over VPC private IP and runs benchmarks.
#
# Usage:
#   ./client_setup.sh <server-private-ip>
#   ./client_setup.sh 172.31.42.100
# =============================================================================
set -euo pipefail

SERVER_IP="${1:?Usage: client_setup.sh <server-private-ip>}"
PROFILE="${PROFILE:-standard}"
WORKLOADS="${WORKLOADS:-all}"

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
log()  { echo -e "${GREEN}[client]${NC} $*"; }
warn() { echo -e "${YELLOW}[client]${NC} $*"; }
info() { echo -e "${CYAN}[client]${NC} $*"; }

log "════════════════════════════════════════════════════════"
log "  VELOCITY Real-World Benchmark — Client                  "
log "════════════════════════════════════════════════════════"
log "  Server:  $SERVER_IP"
log "  Profile: $PROFILE"
log "  Workloads: $WORKLOADS"
echo ""

# ── 1. System packages ─────────────────────────────────────────────────────
log "[1/4] Installing system packages..."
export DEBIAN_FRONTEND=noninteractive
sudo apt-get update -qq
sudo apt-get install -y -qq \
    build-essential pkg-config libssl-dev \
    protobuf-compiler \
    curl git jq netcat-openbsd \
    > /dev/null 2>&1
log "  Done."

# ── 2. Rust toolchain ──────────────────────────────────────────────────────
if ! command -v cargo &>/dev/null; then
    log "[2/4] Installing Rust toolchain..."
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable
    source "$HOME/.cargo/env"
else
    log "[2/4] Rust already installed: $(cargo --version)"
fi
export PATH="$HOME/.cargo/bin:$PATH"

# ── 3. Clone & build benchmark harness only ────────────────────────────────
REPO_DIR="$HOME/V.E.L.O.C.I.T.Y.-WorkFlow"
if [ -d "$REPO_DIR" ]; then
    log "[3/4] Repository exists — pulling latest..."
    cd "$REPO_DIR"
    git pull --ff-only 2>/dev/null || true
else
    log "[3/4] Cloning repository..."
    git clone --depth 1 git@github.com:UnitBuilds-CC/V.E.L.O.C.I.T.Y.-WorkFlow.git "$REPO_DIR"
    cd "$REPO_DIR"
fi

log "  Building benchmark harness (release)..."
cargo build --release -p velocity-bench 2>&1 | grep -E '(Compiling|Finished|error)' || true
log "  Benchmark harness ready."

# ── 4. Verify connectivity to server ───────────────────────────────────────
log "[4/4] Verifying connectivity to server ($SERVER_IP)..."

# Check Temporal
if nc -z -w5 "$SERVER_IP" 7233 2>/dev/null; then
    log "  Real Temporal (7233): REACHABLE"
else
    warn "  Real Temporal (7233): NOT REACHABLE (will skip)"
fi

# Check VELOCITY
if nc -z -w5 "$SERVER_IP" 7234 2>/dev/null; then
    log "  VELOCITY server (7234): REACHABLE"
else
    warn "  VELOCITY server (7234): NOT REACHABLE"
    echo "ERROR: Cannot reach VELOCITY server. Is the server script running?" >&2
    exit 1
fi

# Check temporal-bridge
if nc -z -w5 "$SERVER_IP" 7235 2>/dev/null; then
    log "  temporal-bridge (7235): REACHABLE"
else
    warn "  temporal-bridge (7235): NOT REACHABLE"
fi

# Measure baseline network latency
log ""
log "  Network latency to server (10 pings):"
ping -c 10 "$SERVER_IP" 2>/dev/null | tail -1 || warn "  ping not available"

# ── Print environment & run benchmark ──────────────────────────────────────
echo ""
info "════════════════════════════════════════════════════════"
info "  CLIENT INSTANCE (Benchmark Runner)                     "
info "════════════════════════════════════════════════════════"
info "  Hostname:  $(hostname)"
info "  CPU:       $(lscpu | grep 'Model name' | sed 's/Model name:\s*//' | xargs)"
info "  vCPUs:     $(nproc)"
info "  RAM:       $(free -h | awk '/^Mem:/{print $2}')"
info "  Rust:      $(rustc --version)"
info "────────────────────────────────────────────────────────"
info "  Server (VELOCITY):  grpc://$SERVER_IP:7234"
info "  Server (Temporal):  grpc://$SERVER_IP:7235"
info "  Network:            VPC private IP (consistent latency)"
info "════════════════════════════════════════════════════════"
echo ""

log "Running real-world benchmark (over VPC network)..."
log "Profile: $PROFILE | Workloads: $WORKLOADS"
echo ""

# Remove old results
rm -f bench_results.md bench_results.csv bench_results.json

# Run the benchmark — connecting to REMOTE server over VPC
./target/release/velocity-bench \
    --workloads "$WORKLOADS" \
    --engine both \
    --format all \
    --profile "$PROFILE" \
    --velocity-address "http://$SERVER_IP:7234" \
    --temporal-address "http://$SERVER_IP:7235" \
    --output bench_results.md

echo ""
log "════════════════════════════════════════════════════════"
log "  Real-World Benchmark Complete!                          "
log "════════════════════════════════════════════════════════"
echo ""

if [ -f bench_results.md ]; then
    log "Results:"
    ls -lh bench_results.md bench_results.csv bench_results.json 2>/dev/null
    echo ""

    # Show summary
    info "── Summary ──"
    sed -n '/^## Summary/,/^## Detailed/p' bench_results.md | head -20
    echo ""

    info "── Detailed Comparison ──"
    sed -n '/^## Detailed Comparison/,/^## Per/p' bench_results.md | head -25
fi
