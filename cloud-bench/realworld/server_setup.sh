#!/usr/bin/env bash
# =============================================================================
# cloud-bench/realworld/server_setup.sh
#
# Runs ON the server EC2 instance (m7i-flex.large or similar).
# Sets up: Real Temporal (Docker) + VELOCITY production server + Temporal worker.
# Both servers listen on 0.0.0.0 so the client instance can reach them over VPC.
# =============================================================================
set -euo pipefail

PROFILE="${PROFILE:-standard}"
WORKLOADS="${WORKLOADS:-all}"

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
log()  { echo -e "${GREEN}[server]${NC} $*"; }
warn() { echo -e "${YELLOW}[server]${NC} $*"; }
info() { echo -e "${CYAN}[server]${NC} $*"; }

# ── 1. System packages ─────────────────────────────────────────────────────
log "[1/5] Installing system packages..."
export DEBIAN_FRONTEND=noninteractive

log "  Updating package lists..."
for i in 1 2 3; do
    if sudo apt-get update; then break; fi
    warn "  apt-get update failed (attempt $i), retrying..."
    sleep 5
done

log "  Adding Docker repository..."
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update

log "  Installing packages..."
sudo apt-get install -y \
    build-essential pkg-config libssl-dev \
    protobuf-compiler \
    curl wget git unzip jq netcat-openbsd \
    docker-ce docker-ce-cli containerd.io docker-compose-plugin

sudo systemctl enable --now docker
sudo usermod -aG docker "$USER" 2>/dev/null || true
log "  Done."

# ── 2. Rust toolchain ──────────────────────────────────────────────────────
if ! command -v cargo &>/dev/null; then
    log "[2/5] Installing Rust toolchain..."
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable
    source "$HOME/.cargo/env"
else
    log "[2/5] Rust already installed: $(cargo --version)"
fi
export PATH="$HOME/.cargo/bin:$PATH"

# ── 3. Clone & build ──────────────────────────────────────────────────────
REPO_DIR="$HOME/V.E.L.O.C.I.T.Y.-WorkFlow"
if [ -d "$REPO_DIR" ]; then
    log "[3/5] Repository exists — pulling latest..."
    cd "$REPO_DIR"
    git pull --ff-only 2>/dev/null || true
else
    log "[3/5] Cloning repository..."
    git clone --depth 1 git@github.com:UnitBuilds-CC/V.E.L.O.C.I.T.Y.-WorkFlow.git "$REPO_DIR"
    cd "$REPO_DIR"
fi

log "  Building VELOCITY servers (release)..."
cargo build --release \
    -p velocity-dev-server \
    -p velocity-bench \
    2>&1 | grep -E '(Compiling|Finished|error)' || true

# Build temporal-bridge separately (it's a [[bin]] target)
cargo build --release --bin temporal-bridge 2>&1 | grep -E '(Compiling|Finished|error)' || true

log "  Binaries ready."

# ── 4. Start Real Temporal (Docker) ────────────────────────────────────────
log "[4/5] Starting Real Temporal server (Docker)..."

# Patch the docker-compose to bind on 0.0.0.0 (accessible from VPC)
# The default docker-compose.temporal.yml already binds to 0.0.0.0 by default
cd "$REPO_DIR"
docker compose -f velocity-bench/docker-compose.temporal.yml up -d 2>/dev/null || \
    sudo docker compose -f velocity-bench/docker-compose.temporal.yml up -d

log "  Waiting for Temporal to be healthy..."
for i in $(seq 1 60); do
    if nc -z 0.0.0.0 7233 2>/dev/null; then
        log "  Temporal ready (gRPC :7233, Web UI :8233)"
        break
    fi
    if [ "$i" -eq 60 ]; then
        warn "  Temporal not ready after 3 min — continuing anyway"
    fi
    sleep 3
done

# ── 5. Start VELOCITY production server ────────────────────────────────────
log "[5/5] Starting VELOCITY production server..."

# Start VELOCITY dev-server bound to 0.0.0.0 (accessible from VPC)
# Using --host 0.0.0.0 equivalent: the dev-server binds to all interfaces by default
./target/release/velocity-dev \
    --grpc-port 7234 \
    --port 7230 \
    > /tmp/velocity-server.log 2>&1 &
VEL_PID=$!

for i in $(seq 1 15); do
    nc -z 0.0.0.0 7234 2>/dev/null && break
    sleep 1
done
log "  VELOCITY server ready (gRPC :7234, HTTP :7230, PID $VEL_PID)"

# Also start temporal-bridge on 0.0.0.0:7235 for BenchmarkService proto access
./target/release/temporal-bridge --grpc-port 7235 > /tmp/temporal-bridge.log 2>&1 &
BRIDGE_PID=$!

for i in $(seq 1 10); do
    nc -z 0.0.0.0 7235 2>/dev/null && break
    sleep 1
done
log "  temporal-bridge ready (gRPC :7235, PID $BRIDGE_PID)"

# ── Print status ───────────────────────────────────────────────────────────
PRIVATE_IP=$(hostname -I | awk '{print $1}')
PUBLIC_IP=$(curl -s http://169.254.169.254/latest/meta-data/public-ipv4 2>/dev/null || echo "unknown")

echo ""
info "════════════════════════════════════════════════════════"
info "  SERVER INSTANCE READY                                  "
info "════════════════════════════════════════════════════════"
info "  Private IP:  $PRIVATE_IP  (use this for client)"
info "  Public IP:   $PUBLIC_IP"
info "  CPU:         $(lscpu | grep 'Model name' | sed 's/Model name:\s*//' | xargs)"
info "  vCPUs:       $(nproc)"
info "  RAM:         $(free -h | awk '/^Mem:/{print $2}')"
info "────────────────────────────────────────────────────────"
info "  Real Temporal:      grpc://$PRIVATE_IP:7233"
info "  Temporal Web UI:    http://$PUBLIC_IP:8233"
info "  VELOCITY server:    grpc://$PRIVATE_IP:7234"
info "  temporal-bridge:    grpc://$PRIVATE_IP:7235"
info "════════════════════════════════════════════════════════"
echo ""

# Save server info for the client script
cat > /tmp/server-info.json <<EOF
{
  "private_ip": "$PRIVATE_IP",
  "public_ip": "$PUBLIC_IP",
  "temporal_grpc": "$PRIVATE_IP:7233",
  "velocity_grpc": "$PRIVATE_IP:7234",
  "bridge_grpc": "$PRIVATE_IP:7235",
  "temporal_ui": "$PUBLIC_IP:8233"
}
EOF

log "Server info saved to /tmp/server-info.json"
log "Server is ready. Waiting for client benchmark to connect..."

# Keep script alive (servers are background processes)
wait
