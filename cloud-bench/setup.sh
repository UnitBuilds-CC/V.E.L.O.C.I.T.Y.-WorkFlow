#!/usr/bin/env bash
# =============================================================================
# cloud-bench/setup.sh — Provision an Ubuntu EC2 instance for benchmarking
#
# Tested on: Ubuntu 22.04 LTS (ami-0c7217cdde3efc8f2 — us-east-1)
# Instance:  t3.medium (2 vCPU, 4 GB RAM) or larger
#
# Usage:
#   chmod +x cloud-bench/setup.sh
#   ./cloud-bench/setup.sh
#
# This script is idempotent — safe to re-run.
# =============================================================================
set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log()  { echo -e "${GREEN}[setup]${NC} $*"; }
warn() { echo -e "${YELLOW}[warn]${NC} $*"; }
err()  { echo -e "${RED}[error]${NC} $*" >&2; }

# ── 1. System packages ──────────────────────────────────────────────────────
log "Updating system packages..."
export DEBIAN_FRONTEND=noninteractive
sudo apt-get update -qq
sudo apt-get install -y -qq \
    build-essential pkg-config libssl-dev \
    protobuf-compiler \
    curl wget git unzip jq \
    docker.io docker-compose-v2 \
    > /dev/null 2>&1

# Ensure Docker daemon is running
sudo systemctl enable --now docker
sudo usermod -aG docker "$USER" || warn "Could not add $USER to docker group (will need new login)"

log "System packages installed."

# ── 2. Rust toolchain ───────────────────────────────────────────────────────
if ! command -v cargo &> /dev/null; then
    log "Installing Rust toolchain..."
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable
    source "$HOME/.cargo/env"
else
    log "Rust already installed: $(cargo --version)"
fi

# Ensure cargo is in PATH for subsequent commands
export PATH="$HOME/.cargo/bin:$PATH"

# ── 3. Clone repository ─────────────────────────────────────────────────────
REPO_DIR="$HOME/VELOCITY-WorkFlow"
if [ -d "$REPO_DIR" ]; then
    log "Repository already exists at $REPO_DIR — pulling latest..."
    cd "$REPO_DIR"
    git pull --ff-only || warn "Git pull failed — using existing checkout"
else
    log "Cloning VELOCITY-WorkFlow repository..."
    git clone https://github.com/UnitBuilds-CC/V.E.L.O.C.I.T.Y.-WorkFlow.git "$REPO_DIR"
    cd "$REPO_DIR"
fi

# ── 4. Build Rust binaries (release) ────────────────────────────────────────
log "Building velocity-dev-server, temporal-bridge, and velocity-bench (release)..."
cargo build --release \
    -p velocity-dev-server \
    -p velocity-bench \
    2>&1 | tail -5

log "Binaries built:"
ls -lh target/release/velocity-dev \
       target/release/temporal-bridge \
       target/release/velocity-bench

# ── 5. Pull Docker images ───────────────────────────────────────────────────
log "Pulling Temporal Docker images (this may take a few minutes)..."
docker compose -f velocity-bench/docker-compose.temporal.yml pull

log "Docker images pulled."

# ── 6. Verify ───────────────────────────────────────────────────────────────
echo ""
log "════════════════════════════════════════════════════════"
log "  Setup complete!                                       "
log "════════════════════════════════════════════════════════"
echo ""
log "  Rust:        $(rustc --version)"
log "  Cargo:       $(cargo --version)"
log "  Docker:      $(docker --version)"
log "  protoc:      $(protoc --version 2>/dev/null || echo 'not found')"
log "  Bench:       $(file target/release/velocity-bench | cut -d: -f2)"
log "  Dev-server:  $(file target/release/velocity-dev | cut -d: -f2)"
log "  Bridge:      $(file target/release/temporal-bridge | cut -d: -f2)"
echo ""
log "  Next step:  ./cloud-bench/run.sh"
echo ""
