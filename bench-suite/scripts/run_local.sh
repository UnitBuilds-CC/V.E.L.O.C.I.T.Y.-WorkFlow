#!/usr/bin/env bash
# run_local.sh — Orchestrate local Docker benchmarks across all engines.
#
# Usage:
#   ./bench-suite/scripts/run_local.sh [profile]
#
# Profiles: smoke (default), short, standard
#
# Steps:
#   1. docker compose up -d  (start all engines)
#   2. Wait for health checks
#   3. Run smoke test on each engine
#   4. Run the selected profile on each engine
#   5. Collect JSON results
#   6. Print comparison table

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH_DIR="$(dirname "$SCRIPT_DIR")"
COMPOSE_FILE="$BENCH_DIR/docker-compose.yml"
PROFILE="${1:-smoke}"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULTS_DIR="$BENCH_DIR/results/${TIMESTAMP}"

mkdir -p "$RESULTS_DIR"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

log()  { echo -e "${CYAN}[bench]${NC} $*"; }
ok()   { echo -e "${GREEN}[  ok]${NC} $*"; }
warn() { echo -e "${YELLOW}[warn]${NC} $*"; }
err()  { echo -e "${RED}[ err]${NC} $*"; }

# ─── 1. Start all engines ────────────────────────────────────────────────────
log "Starting all engines via docker compose..."
cd "$BENCH_DIR"
docker compose -f "$COMPOSE_FILE" up -d --build

# ─── 2. Wait for health ─────────────────────────────────────────────────────
log "Waiting for all engines to become healthy..."
"$SCRIPT_DIR/wait_for_healthy.sh" "$COMPOSE_FILE"

# ─── 3. Register Restate service ────────────────────────────────────────────
log "Registering Restate service..."
docker exec bench-restate-server restate deployments register http://bench-restate-service:9080 2>/dev/null || warn "Restate registration may have failed"

# ─── 4. Run benchmarks ──────────────────────────────────────────────────────
# Engine → endpoint mapping
declare -A ENGINES=(
    ["velocity-classic"]="grpc://localhost:7234"
    ["velocity-runtime"]="grpc://localhost:7235"
    ["velocity-embedded"]="grpc://localhost:7236"
    ["dbos"]="http://localhost:8081"
    ["restate"]="http://localhost:8082"
    ["temporal"]="http://localhost:8083"
)

# Run Velocity engines via velocity-bench (Rust gRPC client)
for engine in velocity-classic velocity-runtime velocity-embedded; do
    endpoint="${ENGINES[$engine]}"
    log "Running $PROFILE profile on $engine ($endpoint)..."

    # Map profile to velocity-bench flags
    case "$PROFILE" in
        smoke)    runs=1;  bench_profile="quick" ;;
        short)    runs=1;  bench_profile="quick" ;;
        standard) runs=3;  bench_profile="standard" ;;
        stress)   runs=3;  bench_profile="stress" ;;
        *)        runs=1;  bench_profile="quick" ;;
    esac

    # Run velocity-bench
    if command -v velocity-bench &> /dev/null; then
        velocity-bench \
            --workloads smoke \
            --engine velocity \
            --velocity-address "$endpoint" \
            --profile "$bench_profile" \
            --runs "$runs" \
            --format json \
            --output "$RESULTS_DIR/${engine}_${PROFILE}.json" \
            2>&1 | tee "$RESULTS_DIR/${engine}_${PROFILE}.log" || warn "$engine benchmark had errors"
    else
        warn "velocity-bench not found in PATH, skipping $engine"
        echo '{"engine":"'"$engine"'","error":"velocity-bench not found"}' > "$RESULTS_DIR/${engine}_${PROFILE}.json"
    fi
done

# Run DBOS via Python client
log "Running $PROFILE profile on dbos (${ENGINES[dbos]})..."
if [ -f "$BENCH_DIR/../cloud-bench/production/dbos/client.py" ]; then
    python3 "$BENCH_DIR/../cloud-bench/production/dbos/client.py" \
        --base-url "${ENGINES[dbos]}" \
        --profile "$PROFILE" \
        --output "$RESULTS_DIR/dbos_${PROFILE}.json" \
        2>&1 | tee "$RESULTS_DIR/dbos_${PROFILE}.log" || warn "DBOS benchmark had errors"
else
    warn "DBOS client not found, skipping"
fi

# Run Restate via Node.js client
log "Running $PROFILE profile on restate (${ENGINES[restate]})..."
if [ -f "$BENCH_DIR/../cloud-bench/production/restate/client.js" ]; then
    node "$BENCH_DIR/../cloud-bench/production/restate/client.js" \
        --ingress "${ENGINES[restate]}" \
        --profile "$PROFILE" \
        --output "$RESULTS_DIR/restate_${PROFILE}.json" \
        2>&1 | tee "$RESULTS_DIR/restate_${PROFILE}.log" || warn "Restate benchmark had errors"
else
    warn "Restate client not found, skipping"
fi

# Run Temporal via Python client
log "Running $PROFILE profile on temporal (${ENGINES[temporal]})..."
if [ -f "$BENCH_DIR/../cloud-bench/production/temporal/client.py" ]; then
    python3 "$BENCH_DIR/../cloud-bench/production/temporal/client.py" \
        --base-url "${ENGINES[temporal]}" \
        --profile "$PROFILE" \
        --output "$RESULTS_DIR/temporal_${PROFILE}.json" \
        2>&1 | tee "$RESULTS_DIR/temporal_${PROFILE}.log" || warn "Temporal benchmark had errors"
else
    warn "Temporal client not found, skipping"
fi

# ─── 5. Merge results ───────────────────────────────────────────────────────
log "Merging results..."
if command -v python3 &> /dev/null; then
    python3 "$SCRIPT_DIR/merge_results.py" "$RESULTS_DIR" "${RESULTS_DIR}/merged_${PROFILE}.json"
fi

# ─── 6. Print summary ───────────────────────────────────────────────────────
log ""
log "═══════════════════════════════════════════════════════════════"
log "  Benchmark complete — results in $RESULTS_DIR"
log "═══════════════════════════════════════════════════════════════"
log ""

# Print summary table from merged results
if [ -f "${RESULTS_DIR}/merged_${PROFILE}.json" ]; then
    python3 -c "
import json, sys
with open('${RESULTS_DIR}/merged_${PROFILE}.json') as f:
    data = json.load(f)
print(f'{\"Engine\":<22} {\"Workload\":<20} {\"ops/sec\":>10} {\"p99(us)\":>10} {\"errors\":>8}')
print('-' * 72)
for engine, results in sorted(data.get('engines', {}).items()):
    for w in results.get('workloads', []):
        name = w.get('name', '?')
        ops = w.get('ops_per_sec', 0)
        p99 = w.get('p99_latency_us', 0)
        errs = w.get('error_count', 0)
        print(f'{engine:<22} {name:<20} {ops:>10.1f} {p99:>10.0f} {errs:>8}')
" 2>/dev/null || warn "Could not print summary table"
fi

log ""
log "To tear down:  docker compose -f $COMPOSE_FILE down -v"
