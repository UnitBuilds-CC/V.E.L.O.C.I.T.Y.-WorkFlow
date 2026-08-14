#!/usr/bin/env bash
# =============================================================================
# Production Benchmark Runner
# =============================================================================
# Starts all 4 real engines via Docker Compose, runs the benchmark suite,
# and generates a comparison report.
#
# Usage:
#   ./run.sh                    # Full benchmark (all engines, standard profile)
#   ./run.sh --profile quick    # Quick benchmark (0.1x operations)
#   ./run.sh --profile stress   # Stress test (10x operations)
#   ./run.sh --engines velocity,dbos  # Only specific engines
#   ./run.sh --skip-teardown    # Keep engines running after benchmark
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Defaults
PROFILE="standard"
ENGINES="all"
FORMAT="markdown"
SKIP_TEARDOWN=false
OUTPUT_FILE=""
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --profile) PROFILE="$2"; shift 2 ;;
        --engines) ENGINES="$2"; shift 2 ;;
        --format) FORMAT="$2"; shift 2 ;;
        --output) OUTPUT_FILE="$2"; shift 2 ;;
        --skip-teardown) SKIP_TEARDOWN=true; shift ;;
        -h|--help)
            echo "Usage: $0 [--profile quick|standard|stress] [--engines velocity,dbos,restate|all] [--format markdown|json|csv] [--output FILE] [--skip-teardown]"
            exit 0
            ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

if [ -z "$OUTPUT_FILE" ]; then
    OUTPUT_FILE="results/prod_bench_${PROFILE}_${TIMESTAMP}.${FORMAT}"
    if [ "$FORMAT" = "markdown" ]; then
        OUTPUT_FILE="results/prod_bench_${PROFILE}_${TIMESTAMP}.md"
    fi
fi

mkdir -p results

echo "╔══════════════════════════════════════════════════════════╗"
echo "║  Production Benchmark Suite                              ║"
echo "║  Real engines. Real APIs. Real persistence.              ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""
echo "Profile:  $PROFILE"
echo "Engines:  $ENGINES"
echo "Format:   $FORMAT"
echo "Output:   $OUTPUT_FILE"
echo ""

# ─── Step 1: Build everything ────────────────────────────────────────────────
echo "━━━ Step 1/4: Building containers ━━━"
docker compose build 2>&1 | tail -5
echo "  Build complete."
echo ""

# ─── Step 2: Start engines ───────────────────────────────────────────────────
echo "━━━ Step 2/4: Starting engines ━━━"
docker compose up -d velocity dbos-postgres dbos restate restate-bench-svc temporal 2>&1

echo "  Waiting for engines to become healthy..."
MAX_WAIT=120
ELAPSED=0
while [ $ELAPSED -lt $MAX_WAIT ]; do
    HEALTHY=0
    TOTAL=0

    # Check Velocity (gRPC)
    if nc -z localhost 7234 2>/dev/null; then
        HEALTHY=$((HEALTHY + 1))
        echo "    ✓ Velocity (gRPC port 7234)"
    else
        echo "    ⏳ Velocity..."
    fi
    TOTAL=$((TOTAL + 1))

    # Check DBOS
    if curl -sf http://localhost:8081/health > /dev/null 2>&1; then
        HEALTHY=$((HEALTHY + 1))
        echo "    ✓ DBOS (port 8081)"
    else
        echo "    ⏳ DBOS..."
    fi
    TOTAL=$((TOTAL + 1))

    # Check Restate
    if curl -sf http://localhost:9070/health > /dev/null 2>&1; then
        HEALTHY=$((HEALTHY + 1))
        echo "    ✓ Restate (port 9070)"
    else
        echo "    ⏳ Restate..."
    fi
    TOTAL=$((TOTAL + 1))

    # Check Temporal
    if nc -z localhost 7233 2>/dev/null; then
        HEALTHY=$((HEALTHY + 1))
        echo "    ✓ Temporal (port 7233)"
    else
        echo "    ⏳ Temporal..."
    fi
    TOTAL=$((TOTAL + 1))

    if [ $HEALTHY -eq $TOTAL ]; then
        echo ""
        echo "  All engines healthy!"
        break
    fi

    sleep 5
    ELAPSED=$((ELAPSED + 5))
done

if [ $ELAPSED -ge $MAX_WAIT ]; then
    echo "  WARNING: Not all engines became healthy within ${MAX_WAIT}s"
    echo "  Continuing with available engines..."
fi
echo ""

# ─── Step 3: Register Restate service ────────────────────────────────────────
echo "━━━ Step 3/4: Registering Restate bench service ━━━"
# Register the bench service with Restate
docker compose run --rm restate-register 2>&1 || echo "  (Restate registration may have already been done)"
sleep 2

# Verify Restate service is registered
echo "  Testing Restate bench service..."
RESTATE_TEST=$(curl -sf -X POST http://localhost:9070/BenchmarkService/handler_invocation \
    -H "Content-Type: application/json" \
    -d '{"input": "test"}' 2>&1 || echo "FAILED")
if echo "$RESTATE_TEST" | grep -q "ok"; then
    echo "  ✓ Restate bench service registered and responding"
else
    echo "  ⚠ Restate bench service may not be registered yet"
    echo "  Response: $RESTATE_TEST"
fi
echo ""

# ─── Step 4: Run benchmarks ──────────────────────────────────────────────────
echo "━━━ Step 4/4: Running benchmarks ━━━"
echo ""

# Build the velocity URLs for the bench client
VELOCITY_URL="http://localhost:7234"
DBOS_URL="http://localhost:8081"
RESTATE_URL="http://localhost:9070"

# Run prod-bench (either via Docker or local binary)
if command -v cargo &> /dev/null && [ -f "Cargo.toml" ]; then
    echo "  Running prod-bench via cargo..."
    cd "$SCRIPT_DIR"
    cargo run --release -- \
        --engines "$ENGINES" \
        --profile "$PROFILE" \
        --velocity-url "$VELOCITY_URL" \
        --dbos-url "$DBOS_URL" \
        --restate-url "$RESTATE_URL" \
        --format "$FORMAT" \
        --output "$OUTPUT_FILE" \
        2>&1
elif docker compose run --rm prod-bench \
    --engines "$ENGINES" \
    --profile "$PROFILE" \
    --format "$FORMAT" \
    2>&1; then
    # Copy results from container
    echo "  (Results from Docker run)"
else
    echo "ERROR: Neither cargo nor Docker available for running prod-bench"
    exit 1
fi

# Run Temporal bench separately (Go SDK, different binary)
if [[ "$ENGINES" == *"temporal"* || "$ENGINES" == "all" ]]; then
    TEMPORAL_OUTPUT="results/temporal_bench_${PROFILE}_${TIMESTAMP}.${FORMAT}"
    if [ "$FORMAT" = "markdown" ]; then
        TEMPORAL_OUTPUT="results/temporal_bench_${PROFILE}_${TIMESTAMP}.md"
    fi
    echo ""
    echo "  Running Temporal bench (Go SDK)..."
    if command -v go &> /dev/null; then
        cd "$SCRIPT_DIR/temporal-bench"
        go run main.go \
            --temporal-url localhost:7233 \
            --profile "$PROFILE" \
            --format "$FORMAT" \
            --output "$TEMPORAL_OUTPUT" \
            2>&1
        cd "$SCRIPT_DIR"
    else
        docker compose run --rm temporal-bench \
            --temporal-url temporal:7233 \
            --profile "$PROFILE" \
            --format "$FORMAT" \
            2>&1 || echo "  (Temporal bench skipped — Go not available)"
    fi
fi

echo ""
echo "━━━ Results ━━━"
if [ -f "$OUTPUT_FILE" ]; then
    echo "Results written to: $OUTPUT_FILE"
    echo ""
    if [[ "$OUTPUT_FILE" == *.md ]]; then
        cat "$OUTPUT_FILE"
    fi
else
    echo "No output file found. Check logs above."
fi

# ─── Teardown ────────────────────────────────────────────────────────────────
if [ "$SKIP_TEARDOWN" = false ]; then
    echo ""
    echo "━━━ Tearing down ━━━"
    docker compose down -v 2>&1 | tail -3
    echo "  All engines stopped."
else
    echo ""
    echo "Engines still running. To stop: docker compose down -v"
fi

echo ""
echo "━━━ Done ━━━"
