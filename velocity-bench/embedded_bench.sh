#!/usr/bin/env bash
# =============================================================================
# velocity-bench/embedded_bench.sh
#
# Runs ON Instance 5 (velocity-embedded) or Instance 6 (dbos-bench).
# Measures PostgreSQL-native throughput for Velocity Embedded vs DBOS.
#
# Both engines use PostgreSQL as their durable store, so pgbench gives
# a shared baseline.  Workflow-level operations are measured via HTTP.
#
# Environment:
#   FLAVOR=embedded        (embedded | dbos)
#   PG_HOST=localhost
#   PG_PORT=5432
#   PG_USER=bench
#   PG_DB=benchdb
#   PROFILE=standard       (quick | standard | stress)
#   OUTPUT_DIR=/tmp/bench
# =============================================================================
set -euo pipefail

FLAVOR="${FLAVOR:-embedded}"
PG_HOST="${PG_HOST:-localhost}"
PG_PORT="${PG_PORT:-5432}"
PG_USER="${PG_USER:-bench}"
PG_DB="${PG_DB:-benchdb}"
PROFILE="${PROFILE:-standard}"
OUTPUT_DIR="${OUTPUT_DIR:-/tmp/bench}"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
log()  { echo -e "${GREEN}[embedded-bench]${NC} $*"; }
warn() { echo -e "${YELLOW}[embedded-bench]${NC} $*"; }
info() { echo -e "${CYAN}[embedded-bench]${NC} $*"; }
err()  { echo -e "${RED}[embedded-bench]${NC} $*"; }

mkdir -p "$OUTPUT_DIR"

# Profile durations
case "$PROFILE" in
    quick)   PGBENCH_SECS=10; HTTP_OPS=100;  HTTP_CONCURRENCY=5;  HTTP_DURATION=10 ;;
    standard) PGBENCH_SECS=30; HTTP_OPS=500; HTTP_CONCURRENCY=10; HTTP_DURATION=30 ;;
    stress)  PGBENCH_SECS=60; HTTP_OPS=2000; HTTP_CONCURRENCY=50; HTTP_DURATION=120 ;;
    *)       PGBENCH_SECS=30; HTTP_OPS=500;  HTTP_CONCURRENCY=10; HTTP_DURATION=30 ;;
esac

log "════════════════════════════════════════════════════════"
log "  Embedded Benchmark: $FLAVOR"
log "════════════════════════════════════════════════════════"
log "  PostgreSQL:  $PG_HOST:$PG_PORT/$PG_DB"
log "  Profile:     $PROFILE"
log "  pgbench:     ${PGBENCH_SECS}s"
log "  HTTP ops:    $HTTP_OPS (concurrency $HTTP_CONCURRENCY)"
log "  HTTP dur:    ${HTTP_DURATION}s"
log "════════════════════════════════════════════════════════"

# ── Helper: get process memory (RSS) in MB ──────────────────────────────────
get_rss_mb() {
    local pid=$1
    if [ -f "/proc/$pid/status" ]; then
        awk '/VmRSS/{print $2/1024}' "/proc/$pid/status" 2>/dev/null || echo "0"
    else
        echo "0"
    fi
}

# ── Helper: get docker container memory in MB ───────────────────────────────
get_docker_mem_mb() {
    local name=$1
    sudo docker stats "$name" --no-stream --format "{{.MemUsage}}" 2>/dev/null \
        | awk '{gsub(/[a-zA-Z]/,"",$1); print $1}' || echo "0"
}

# ── Helper: get postgres process memory ─────────────────────────────────────
get_pg_mem_mb() {
    # Sum RSS of all postgres processes
    ps aux 2>/dev/null | awk '/[p]ostgres/ {sum += $6} END {printf "%.1f", sum/1024}' || echo "0"
}

# ══════════════════════════════════════════════════════════════════════════════
# PHASE 1: pgbench — Raw PostgreSQL TPS baseline
# ══════════════════════════════════════════════════════════════════════════════
log ""
log "[1/3] Running pgbench baseline (${PGBENCH_SECS}s)..."

# Initialize pgbench tables if needed
if command -v pgbench &>/dev/null; then
    PGBENCH_CMD="pgbench"
elif sudo docker exec velocity-bench-dbos-postgres pgbench --help &>/dev/null 2>&1; then
    PGBENCH_CMD="sudo docker exec velocity-bench-dbos-postgres pgbench"
else
    warn "pgbench not available — skipping raw Postgres baseline"
    PGBENCH_CMD=""
fi

PGBENCH_JSON="{}"
if [ -n "$PGBENCH_CMD" ]; then
    # Initialize benchmark tables
    $PGBENCH_CMD -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" -i 2>/dev/null || true

    # Run pgbench
    PGBENCH_OUT=$($PGBENCH_CMD -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" \
        -T "$PGBENCH_SECS" -j 2 -c 10 2>&1) || true

    PG_TPS=$(echo "$PGBENCH_OUT" | grep "tps =" | head -1 | awk '{print $3}')
    PG_LAT_AVG=$(echo "$PGBENCH_OUT" | grep "latency average" | awk '{print $4}')
    PG_LAT_P99=$(echo "$PGBENCH_OUT" | grep "99th" | awk '{print $NF}' || echo "0")
    PG_CONN=$(echo "$PGBENCH_OUT" | grep "connections" | awk '{print $1}' || echo "10")

    # Default values if parsing failed
    PG_TPS="${PG_TPS:-0}"
    PG_LAT_AVG="${PG_LAT_AVG:-0}"
    PG_LAT_P99="${PG_LAT_P99:-0}"

    log "  pgbench results:"
    log "    TPS:          $PG_TPS"
    log "    Lat avg:      ${PG_LAT_AVG}ms"
    log "    Lat p99:      ${PG_LAT_P99}ms"

    PGBENCH_JSON=$(cat <<EOF
{
    "tps": $PG_TPS,
    "latency_avg_ms": $PG_LAT_AVG,
    "latency_p99_ms": $PG_LAT_P99,
    "connections": ${PG_CONN:-10},
    "duration_secs": $PGBENCH_SECS
}
EOF
)
fi

# ══════════════════════════════════════════════════════════════════════════════
# PHASE 2: Workflow-level HTTP benchmark
# ══════════════════════════════════════════════════════════════════════════════
log ""
log "[2/3] Running workflow HTTP benchmark ($FLAVOR)..."

# Determine engine URL based on flavor
if [ "$FLAVOR" = "embedded" ]; then
    ENGINE_URL="http://localhost:7233"
    ENGINE_NAME="Velocity Embedded"
elif [ "$FLAVOR" = "dbos" ]; then
    ENGINE_URL="http://localhost:3000"
    ENGINE_NAME="DBOS"
else
    err "Unknown FLAVOR: $FLAVOR"
    exit 1
fi

# Wait for engine to be ready
log "  Waiting for $ENGINE_NAME at $ENGINE_URL..."
ENGINE_READY=false
for i in $(seq 1 30); do
    if curl -sf "$ENGINE_URL/health" >/dev/null 2>&1; then
        ENGINE_READY=true
        break
    fi
    sleep 2
    printf "."
done
echo ""

if [ "$ENGINE_READY" = false ]; then
    warn "  $ENGINE_NAME not ready — continuing with best-effort measurements"
fi

# Record pre-benchmark memory
if [ "$FLAVOR" = "embedded" ]; then
    ENGINE_PID=$(pgrep -f "velocity-dev-server\|velocity-server" | head -1 || echo "0")
    MEM_BEFORE=$(get_rss_mb "$ENGINE_PID")
else
    MEM_BEFORE=$(get_docker_mem_mb "dbos-test")
fi
PG_MEM_BEFORE=$(get_pg_mem_mb)

# ── 2a: Sequential workflow creation ────────────────────────────────────────
log "  2a: Sequential workflow creation ($HTTP_OPS ops)..."
SEQ_START=$(date +%s%N)
SEQ_SUCCESS=0
SEQ_FAIL=0
SEQ_LATENCIES=""

for i in $(seq 1 "$HTTP_OPS"); do
    OP_START=$(date +%s%N)
    HTTP_CODE=$(curl -sf -o /dev/null -w "%{http_code}" \
        -X POST "$ENGINE_URL/api/v1/namespaces/default/workflows" \
        -H "Content-Type: application/json" \
        -d "{\"workflowId\":\"bench-seq-$i\",\"workflowType\":\"bench_workflow\",\"input\":{\"iteration\":$i}}" \
        --max-time 5 2>/dev/null) || HTTP_CODE="000"
    OP_END=$(date +%s%N)
    OP_LAT=$(( (OP_END - OP_START) / 1000000 ))

    if [ "$HTTP_CODE" -ge 200 ] 2>/dev/null && [ "$HTTP_CODE" -lt 300 ] 2>/dev/null; then
        SEQ_SUCCESS=$((SEQ_SUCCESS + 1))
        SEQ_LATENCIES="$SEQ_LATENCIES $OP_LAT"
    else
        SEQ_FAIL=$((SEQ_FAIL + 1))
    fi
done
SEQ_END=$(date +%s%N)
SEQ_TOTAL_MS=$(( (SEQ_END - SEQ_START) / 1000000 ))

# Calculate latency stats
if [ -n "$SEQ_LATENCIES" ]; then
    SEQ_P50=$(echo "$SEQ_LATENCIES" | tr ' ' '\n' | sort -n | awk -v n=$(echo "$SEQ_LATENCIES" | wc -w) 'NR==int(n*0.5){print}')
    SEQ_P99=$(echo "$SEQ_LATENCIES" | tr ' ' '\n' | sort -n | awk -v n=$(echo "$SEQ_LATENCIES" | wc -w) 'NR==int(n*0.99){print}')
    SEQ_AVG=$(echo "$SEQ_LATENCIES" | tr ' ' '\n' | awk '{sum+=$1; n++} END{if(n>0) printf "%.0f", sum/n; else print 0}')
else
    SEQ_P50=0; SEQ_P99=0; SEQ_AVG=0
fi

SEQ_TPS=0
if [ "$SEQ_TOTAL_MS" -gt 0 ]; then
    SEQ_TPS=$(awk "BEGIN{printf \"%.1f\", $SEQ_SUCCESS * 1000.0 / $SEQ_TOTAL_MS}")
fi

log "    Success: $SEQ_SUCCESS/$HTTP_OPS, TPS: $SEQ_TPS, avg: ${SEQ_AVG}ms, p50: ${SEQ_P50}ms, p99: ${SEQ_P99}ms"

# ── 2b: Concurrent workflow creation ────────────────────────────────────────
log "  2b: Concurrent workflow creation ($HTTP_OPS ops, concurrency $HTTP_CONCURRENCY)..."
CONC_START=$(date +%s%N)
CONC_SUCCESS=0
CONC_FAIL=0
CONC_TMPDIR=$(mktemp -d)

conc_worker() {
    local idx=$1
    local success=0
    local fail=0
    for j in $(seq 1 $((HTTP_OPS / HTTP_CONCURRENCY))); do
        HTTP_CODE=$(curl -sf -o /dev/null -w "%{http_code}" \
            -X POST "$ENGINE_URL/api/v1/namespaces/default/workflows" \
            -H "Content-Type: application/json" \
            -d "{\"workflowId\":\"bench-conc-${idx}-${j}\",\"workflowType\":\"bench_workflow\",\"input\":{\"worker\":$idx,\"iteration\":$j}}" \
            --max-time 5 2>/dev/null) || HTTP_CODE="000"
        if [ "$HTTP_CODE" -ge 200 ] 2>/dev/null && [ "$HTTP_CODE" -lt 300 ] 2>/dev/null; then
            success=$((success + 1))
        else
            fail=$((fail + 1))
        fi
    done
    echo "$success $fail" > "$CONC_TMPDIR/worker_$idx"
}

for w in $(seq 1 "$HTTP_CONCURRENCY"); do
    conc_worker "$w" &
done
wait

for f in "$CONC_TMPDIR"/worker_*; do
    read -r s f_cnt < "$f" 2>/dev/null || true
    CONC_SUCCESS=$((CONC_SUCCESS + ${s:-0}))
    CONC_FAIL=$((CONC_FAIL + ${f_cnt:-0}))
done
rm -rf "$CONC_TMPDIR"

CONC_END=$(date +%s%N)
CONC_TOTAL_MS=$(( (CONC_END - CONC_START) / 1000000 ))
CONC_TPS=0
if [ "$CONC_TOTAL_MS" -gt 0 ]; then
    CONC_TPS=$(awk "BEGIN{printf \"%.1f\", $CONC_SUCCESS * 1000.0 / $CONC_TOTAL_MS}")
fi

log "    Success: $CONC_SUCCESS, TPS: $CONC_TPS"

# ── 2c: Sustained load ─────────────────────────────────────────────────────
log "  2c: Sustained load (${HTTP_DURATION}s)..."
SUST_START=$(date +%s)
SUST_OPS=0
SUST_SUCCESS=0
SUST_LATENCIES=""

while true; do
    ELAPSED=$(( $(date +%s) - SUST_START ))
    if [ "$ELAPSED" -ge "$HTTP_DURATION" ]; then break; fi

    # Fire a batch of concurrent requests
    for _ in $(seq 1 "$HTTP_CONCURRENCY"); do
        OP_START=$(date +%s%N)
        HTTP_CODE=$(curl -sf -o /dev/null -w "%{http_code}" \
            -X POST "$ENGINE_URL/api/v1/namespaces/default/workflows" \
            -H "Content-Type: application/json" \
            -d "{\"workflowId\":\"bench-sust-$SUST_OPS\",\"workflowType\":\"bench_workflow\",\"input\":{\"n\":$SUST_OPS}}" \
            --max-time 5 2>/dev/null) || HTTP_CODE="000"
        OP_END=$(date +%s%N)
        OP_LAT=$(( (OP_END - OP_START) / 1000000 ))
        SUST_OPS=$((SUST_OPS + 1))

        if [ "$HTTP_CODE" -ge 200 ] 2>/dev/null && [ "$HTTP_CODE" -lt 300 ] 2>/dev/null; then
            SUST_SUCCESS=$((SUST_SUCCESS + 1))
            SUST_LATENCIES="$SUST_LATENCIES $OP_LAT"
        fi
    done
done
SUST_END=$(date +%s)
SUST_TOTAL_SECS=$((SUST_END - SUST_START))
SUST_TPS=0
if [ "$SUST_TOTAL_SECS" -gt 0 ]; then
    SUST_TPS=$(awk "BEGIN{printf \"%.1f\", $SUST_SUCCESS * 1.0 / $SUST_TOTAL_SECS}")
fi

if [ -n "$SUST_LATENCIES" ]; then
    SUST_P50=$(echo "$SUST_LATENCIES" | tr ' ' '\n' | sort -n | awk -v n=$(echo "$SUST_LATENCIES" | wc -w) 'NR==int(n*0.5){print}')
    SUST_P99=$(echo "$SUST_LATENCIES" | tr ' ' '\n' | sort -n | awk -v n=$(echo "$SUST_LATENCIES" | wc -w) 'NR==int(n*0.99){print}')
else
    SUST_P50=0; SUST_P99=0
fi

log "    Ops: $SUST_OPS, Success: $SUST_SUCCESS, TPS: $SUST_TPS, p50: ${SUST_P50}ms, p99: ${SUST_P99}ms"

# Record post-benchmark memory
if [ "$FLAVOR" = "embedded" ]; then
    MEM_AFTER=$(get_rss_mb "$ENGINE_PID")
else
    MEM_AFTER=$(get_docker_mem_mb "dbos-test")
fi
PG_MEM_AFTER=$(get_pg_mem_mb)

# ══════════════════════════════════════════════════════════════════════════════
# PHASE 3: Write structured JSON results
# ══════════════════════════════════════════════════════════════════════════════
log ""
log "[3/3] Writing results..."

RESULTS_FILE="$OUTPUT_DIR/embedded_bench_${FLAVOR}_results.json"

cat > "$RESULTS_FILE" <<EOF
{
    "flavor": "$FLAVOR",
    "engine": "$ENGINE_NAME",
    "profile": "$PROFILE",
    "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
    "hostname": "$(hostname)",
    "pgbench": $PGBENCH_JSON,
    "sequential": {
        "total_operations": $HTTP_OPS,
        "successful_operations": $SEQ_SUCCESS,
        "failed_operations": $SEQ_FAIL,
        "total_duration_ms": $SEQ_TOTAL_MS,
        "operations_per_second": $SEQ_TPS,
        "latency_avg_ms": $SEQ_AVG,
        "latency_p50_ms": ${SEQ_P50:-0},
        "latency_p99_ms": ${SEQ_P99:-0}
    },
    "concurrent": {
        "total_operations": $HTTP_OPS,
        "successful_operations": $CONC_SUCCESS,
        "failed_operations": $CONC_FAIL,
        "concurrency": $HTTP_CONCURRENCY,
        "total_duration_ms": $CONC_TOTAL_MS,
        "operations_per_second": $CONC_TPS
    },
    "sustained": {
        "total_operations": $SUST_OPS,
        "successful_operations": $SUST_SUCCESS,
        "duration_secs": $SUST_TOTAL_SECS,
        "operations_per_second": $SUST_TPS,
        "latency_p50_ms": ${SUST_P50:-0},
        "latency_p99_ms": ${SUST_P99:-0}
    },
    "memory": {
        "engine_before_mb": $MEM_BEFORE,
        "engine_after_mb": $MEM_AFTER,
        "postgres_before_mb": $PG_MEM_BEFORE,
        "postgres_after_mb": $PG_MEM_AFTER
    }
}
EOF

log "  Results written to: $RESULTS_FILE"
log ""
log "════════════════════════════════════════════════════════"
log "  Embedded Benchmark Complete ($FLAVOR)"
log "════════════════════════════════════════════════════════"
log "  pgbench TPS:    $PG_TPS"
log "  Seq TPS:        $SEQ_TPS"
log "  Concurrent TPS: $CONC_TPS"
log "  Sustained TPS:  $SUST_TPS"
log "  Engine memory:  ${MEM_BEFORE}MB → ${MEM_AFTER}MB"
log "════════════════════════════════════════════════════════"
