#!/usr/bin/env bash
# Velocity 3-Flavor Benchmark Suite
#
# Benchmarks all 3 server flavors in Docker:
#   1. workflow-server  — VCTP (UDP)     vs Temporal gRPC
#   2. classic-server   — NMCP (WS)      vs Restate HTTP
#   3. embedded-server  — NMCP (WS)      vs DBOS HTTP
#
# Usage:
#   ./deploy/bench-all-flavors.sh
#   ./deploy/bench-all-flavors.sh --quick    # 100 iterations
#   ./deploy/bench-all-flavors.sh --full     # 10000 iterations

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
COMPOSE_FILE="$PROJECT_DIR/docker-compose.flavors.yml"

ITERATIONS=${ITERATIONS:-1000}
if [[ "${1:-}" == "--quick" ]]; then ITERATIONS=100; fi
if [[ "${1:-}" == "--full" ]]; then ITERATIONS=10000; fi

echo "╔══════════════════════════════════════════════════════════════╗"
echo "║  Velocity 3-Flavor Benchmark Suite                          ║"
echo "║  Iterations: $ITERATIONS                                       ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

# ─── Build all images ────────────────────────────────────────────────────────
echo "▶ Building Docker images..."
docker compose -f "$COMPOSE_FILE" build --quiet workflow-server classic-server embedded-server
echo "  ✓ All images built"
echo ""

# ─── Start servers ───────────────────────────────────────────────────────────
echo "▶ Starting all 3 server flavors..."
docker compose -f "$COMPOSE_FILE" up -d workflow-server classic-server embedded-server
echo "  Waiting for servers to be ready..."
sleep 5
echo "  ✓ All servers running"
echo ""

# ─── Results directory ───────────────────────────────────────────────────────
RESULTS_DIR="$PROJECT_DIR/bench-results"
mkdir -p "$RESULTS_DIR"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)

# ─── Benchmark: Classic Server (NMCP WebSocket) ─────────────────────────────
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "▶ Benchmark: Classic Server (NMCP WebSocket) — $ITERATIONS workflows"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

docker compose -f "$COMPOSE_FILE" run --rm -e ITERATIONS=$ITERATIONS bench-classic 2>&1 | tee "$RESULTS_DIR/classic-${TIMESTAMP}.txt"

echo ""

# ─── Benchmark: Embedded Server (NMCP WebSocket) ────────────────────────────
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "▶ Benchmark: Embedded Server (NMCP WebSocket) — $ITERATIONS workflows"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

docker compose -f "$COMPOSE_FILE" run --rm -e ITERATIONS=$ITERATIONS bench-embedded 2>&1 | tee "$RESULTS_DIR/embedded-${TIMESTAMP}.txt"

echo ""

# ─── Summary ─────────────────────────────────────────────────────────────────
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║  Benchmark Summary                                          ║"
echo "╠══════════════════════════════════════════════════════════════╣"
echo "║  Results saved to: $RESULTS_DIR/                                ║"
echo "║  Timestamp: $TIMESTAMP                                          ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

# ─── Cleanup ─────────────────────────────────────────────────────────────────
echo "▶ Stopping servers..."
docker compose -f "$COMPOSE_FILE" down -v
echo "  ✓ Done"
