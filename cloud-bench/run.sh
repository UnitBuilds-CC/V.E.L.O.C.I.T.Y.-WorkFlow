#!/usr/bin/env bash
# =============================================================================
# cloud-bench/run.sh — Run the full benchmark suite on a provisioned instance
#
# Prerequisites: Run cloud-bench/setup.sh first.
#
# Usage:
#   ./cloud-bench/run.sh                    # Full suite (18 workloads)
#   ./cloud-bench/run.sh --profile stress   # Stress profile
#   ./cloud-bench/run.sh --workload simple_workflow  # Single workload
#   ./cloud-bench/run.sh --smoke            # Quick smoke test (3 workloads)
#
# All services run on localhost. Results are written to bench_results.md.
# =============================================================================
set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

log()  { echo -e "${GREEN}[bench]${NC} $*"; }
warn() { echo -e "${YELLOW}[warn]${NC} $*"; }
info() { echo -e "${CYAN}[info]${NC} $*"; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"
cd "$REPO_DIR"

export PATH="$HOME/.cargo/bin:$PATH"

# ── Parse arguments ─────────────────────────────────────────────────────────
BENCH_ARGS=("--workloads" "all" "--engine" "both" "--format" "all" "--profile" "standard" "--output" "bench_results.md")

while [[ $# -gt 0 ]]; do
    case "$1" in
        --smoke)
            BENCH_ARGS=("--workloads" "smoke" "--engine" "both" "--format" "all" "--output" "bench_results.md")
            shift ;;
        --profile)
            # Replace profile in args
            for i in "${!BENCH_ARGS[@]}"; do
                if [[ "${BENCH_ARGS[$i]}" == "--profile" ]]; then
                    BENCH_ARGS[$((i+1))]="$2"
                fi
            done
            shift 2 ;;
        --workload)
            BENCH_ARGS+=("--workload" "$2")
            shift 2 ;;
        *)
            BENCH_ARGS+=("$1")
            shift ;;
    esac
done

# ── Cleanup function ────────────────────────────────────────────────────────
cleanup() {
    log "Cleaning up..."
    # Kill background servers
    [[ -n "${DEV_PID:-}" ]] && kill "$DEV_PID" 2>/dev/null || true
    [[ -n "${BRIDGE_PID:-}" ]] && kill "$BRIDGE_PID" 2>/dev/null || true
    # Stop Temporal containers
    docker compose -f velocity-bench/docker-compose.temporal.yml down 2>/dev/null || true
    log "Cleanup complete."
}
trap cleanup EXIT

# ── 1. Start Real Temporal Server ───────────────────────────────────────────
log "Starting real Temporal server (Docker: PostgreSQL + Temporal + UI)..."
docker compose -f velocity-bench/docker-compose.temporal.yml up -d

log "Waiting for Temporal to become healthy..."
TEMPORAL_READY=false
for i in $(seq 1 60); do
    if docker compose -f velocity-bench/docker-compose.temporal.yml exec -T temporal \
        nc -z localhost 7233 2>/dev/null; then
        TEMPORAL_READY=true
        break
    fi
    sleep 2
done

if [ "$TEMPORAL_READY" = false ]; then
    err "Temporal server did not become ready within 120s"
    docker compose -f velocity-bench/docker-compose.temporal.yml logs temporal
    exit 1
fi
log "Real Temporal server is ready (gRPC on :7233, Web UI on :8233)"

# ── 2. Start VELOCITY dev-server ────────────────────────────────────────────
log "Starting VELOCITY dev-server (gRPC on :7234)..."
./target/release/velocity-dev --grpc-port 7234 > /tmp/velocity-dev.log 2>&1 &
DEV_PID=$!

# Wait for dev-server to be ready
for i in $(seq 1 30); do
    if nc -z localhost 7234 2>/dev/null; then
        break
    fi
    sleep 1
done
log "VELOCITY dev-server is ready (PID $DEV_PID)"

# ── 3. Start temporal-bridge ────────────────────────────────────────────────
log "Starting temporal-bridge (gRPC on :7235)..."
./target/release/temporal-bridge --grpc-port 7235 > /tmp/temporal-bridge.log 2>&1 &
BRIDGE_PID=$!

# Wait for bridge to be ready
for i in $(seq 1 10); do
    if nc -z localhost 7235 2>/dev/null; then
        break
    fi
    sleep 1
done
log "temporal-bridge is ready (PID $BRIDGE_PID)"

# ── 4. Print environment ───────────────────────────────────────────────────
echo ""
info "════════════════════════════════════════════════════════"
info "  Benchmark Environment                                "
info "════════════════════════════════════════════════════════"
info "  Hostname:    $(hostname)"
info "  OS:          $(lsb_release -ds 2>/dev/null || cat /etc/os-release | head -1)"
info "  Kernel:      $(uname -r)"
info "  CPU:         $(lscpu | grep 'Model name' | sed 's/Model name:\s*//')"
info "  vCPUs:       $(nproc)"
info "  RAM:         $(free -h | awk '/^Mem:/{print $2}')"
info "  Rust:        $(rustc --version)"
info "  Docker:      $(docker --version)"
info "  Temporal:    $(docker inspect temporalio/auto-setup:latest --format='{{.Id}}' 2>/dev/null | cut -c1-12 || echo 'unknown')"
info "  VELOCITY:    $(git rev-parse --short HEAD)"
info "────────────────────────────────────────────────────────"
info "  VELOCITY dev-server:  http://localhost:7234 (PID $DEV_PID)"
info "  temporal-bridge:      http://localhost:7235 (PID $BRIDGE_PID)"
info "  Real Temporal:        http://localhost:7233 (Docker)"
info "  Temporal Web UI:      http://localhost:8233"
info "════════════════════════════════════════════════════════"
echo ""

# ── 5. Run benchmark ────────────────────────────────────────────────────────
log "Running benchmark suite..."
log "Args: ${BENCH_ARGS[*]}"
echo ""

# The benchmark harness connects to:
#   --velocity-address http://localhost:7234  (VELOCITY dev-server)
#   --temporal-address http://localhost:7235  (temporal-bridge)
#
# Note: temporal-bridge implements the BenchmarkService proto and faithfully
# simulates Temporal's event-sourcing architecture (O(N) replay). It runs on
# port 7235 to avoid conflicting with real Temporal on 7233.
./target/release/velocity-bench \
    --velocity-address http://localhost:7234 \
    --temporal-address http://localhost:7235 \
    "${BENCH_ARGS[@]}"

# ── 6. Results ──────────────────────────────────────────────────────────────
echo ""
log "════════════════════════════════════════════════════════"
log "  Benchmark complete!                                    "
log "════════════════════════════════════════════════════════"
echo ""

if [ -f bench_results.md ]; then
    log "Results written to:"
    ls -lh bench_results.md bench_results.csv bench_results.json 2>/dev/null
    echo ""
    log "To copy results to your local machine:"
    echo "  scp -i <key.pem> ubuntu@<ec2-host>:~/VELOCITY-WorkFlow/bench_results.md ./"
    echo "  scp -i <key.pem> ubuntu@<ec2-host>:~/VELOCITY-WorkFlow/bench_results.csv ./"
    echo "  scp -i <key.pem> ubuntu@<ec2-host>:~/VELOCITY-WorkFlow/bench_results.json ./"
fi
