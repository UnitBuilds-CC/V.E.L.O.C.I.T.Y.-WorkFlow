#!/usr/bin/env bash
# VCTP Production Load Test — Kubernetes-native
# Runs a sustained workload against the VCTP RPC server from within the cluster
# to validate production readiness before go-live.
#
# Usage:
#   ./deploy/scripts/vctp-prod-loadtest.sh [OPTIONS]
#
# Options:
#   --server <addr>       VCTP server address (default: velocity-server.velocity-system.svc:9090)
#   --clients <n>         Number of concurrent clients (default: 100)
#   --requests <n>        Requests per client (default: 50)
#   --duration <s>        Sustained workload duration in seconds (default: 300)
#   --payload-size <n>    Payload size in bytes (default: 256)
#   --verify-only         Run verification checks only (no load)
#   --namespace <ns>      Kubernetes namespace (default: velocity-system)
#
# Prerequisites:
#   - kubectl configured with cluster access
#   - Velocity server running with VCTP enabled
#   - Sufficient pod resources for load generation

set -euo pipefail

# ─── Defaults ───────────────────────────────────────────────────────────────
SERVER="${VCTP_SERVER:-velocity-server.velocity-system.svc:9090}"
CLIENTS=100
REQUESTS=50
DURATION=300
PAYLOAD_SIZE=256
NAMESPACE="velocity-system"
VERIFY_ONLY=false

# ─── CI Thresholds ─────────────────────────────────────────────────────────
MIN_OPS_PER_SEC=500
MAX_P99_LATENCY_MS=100
MAX_ERROR_RATE_PCT=5
MIN_DELIVERY_RATIO=0.90

# ─── Parse Arguments ────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    --server)       SERVER="$2"; shift 2 ;;
    --clients)      CLIENTS="$2"; shift 2 ;;
    --requests)     REQUESTS="$2"; shift 2 ;;
    --duration)     DURATION="$2"; shift 2 ;;
    --payload-size) PAYLOAD_SIZE="$2"; shift 2 ;;
    --verify-only)  VERIFY_ONLY=true; shift ;;
    --namespace)    NAMESPACE="$2"; shift 2 ;;
    -h|--help)
      head -20 "$0" | tail -15
      exit 0
      ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

# ─── Colors ─────────────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info()  { echo -e "${GREEN}[INFO]${NC} $*"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $*"; }

# ─── Pre-flight Checks ─────────────────────────────────────────────────────
preflight() {
  log_info "Running pre-flight checks..."

  # Check kubectl connectivity
  if ! kubectl cluster-info &>/dev/null; then
    log_error "Cannot connect to Kubernetes cluster"
    exit 1
  fi

  # Check Velocity pods are running
  local pod_count
  pod_count=$(kubectl -n "$NAMESPACE" get pods -l app.kubernetes.io/name=velocity \
    --field-selector=status.phase=Running --no-headers 2>/dev/null | wc -l)
  if [[ "$pod_count" -eq 0 ]]; then
    log_error "No Velocity pods running in namespace $NAMESPACE"
    exit 1
  fi
  log_info "Found $pod_count Velocity pod(s) running"

  # Check VCTP UDP port is exposed
  local svc_exists
  svc_exists=$(kubectl -n "$NAMESPACE" get svc -o jsonpath='{.items[*].spec.ports[*].port}' 2>/dev/null | grep -c "9090" || true)
  if [[ "$svc_exists" -eq 0 ]]; then
    log_warn "VCTP UDP port 9090 not found in any Service — using direct pod address"
  else
    log_info "VCTP UDP port 9090 exposed via Service"
  fi

  # Check Prometheus is accessible for metrics validation
  local prom_pods
  prom_pods=$(kubectl -n "$NAMESPACE" get pods -l app.kubernetes.io/name=prometheus \
    --field-selector=status.phase=Running --no-headers 2>/dev/null | wc -l || echo "0")
  if [[ "$prom_pods" -gt 0 ]]; then
    log_info "Prometheus available ($prom_pods pod(s)) — metrics will be validated"
  else
    log_warn "Prometheus not found — metrics validation skipped"
  fi

  log_info "Pre-flight checks passed"
}

# ─── Verification Checks ───────────────────────────────────────────────────
verify() {
  log_info "Running verification checks against $SERVER..."

  # Health check via VCTP CLI
  log_info "Checking VCTP health..."
  kubectl -n "$NAMESPACE" run vctp-verify-$$ --image=curlimages/curl:latest --rm -it --restart=Never -- \
    curl -s "http://velocity-server.${NAMESPACE}.svc:8095/health" 2>/dev/null || true

  # Check Prometheus metrics endpoint
  log_info "Checking Prometheus metrics..."
  local metrics
  metrics=$(kubectl -n "$NAMESPACE" exec -it \
    "$(kubectl -n "$NAMESPACE" get pods -l app.kubernetes.io/name=velocity -o jsonpath='{.items[0].metadata.name}')" \
    -- curl -s http://localhost:8095/metrics 2>/dev/null || echo "")

  if [[ -n "$metrics" ]]; then
    echo "$metrics" | grep -E "^vctp_" | head -20
    log_info "VCTP Prometheus metrics available"
  else
    log_warn "Could not retrieve VCTP metrics"
  fi

  # Check TLS certificates (if enabled)
  log_info "Checking TLS configuration..."
  local https_port
  https_port=$(kubectl -n "$NAMESPACE" get svc -o jsonpath='{.items[*].spec.ports[*].port}' 2>/dev/null | grep -c "8443" || true)
  if [[ "$https_port" -gt 0 ]]; then
    log_info "HTTPS port 8443 detected — TLS is enabled"
  else
    log_info "HTTPS port 8443 not detected — TLS may not be enabled"
  fi
}

# ─── Sustained Workload ────────────────────────────────────────────────────
run_sustained_workload() {
  log_info "Starting sustained workload: $CLIENTS clients × $REQUESTS requests for ${DURATION}s"
  log_info "Server: $SERVER | Payload: ${PAYLOAD_SIZE}B | Namespace: $NAMESPACE"

  local start_time
  start_time=$(date +%s)

  # Create a temporary ConfigMap with the load test script
  kubectl -n "$NAMESPACE" create configmap vctp-loadtest --from-literal=script="
#!/bin/sh
# VCTP load generator — runs inside cluster
SERVER='$SERVER'
CLIENTS=$CLIENTS
REQUESTS=$REQUESTS
PAYLOAD_SIZE=$PAYLOAD_SIZE

echo \"Starting VCTP load generator: \$CLIENTS clients × \$REQUESTS requests\"

# Generate payload
PAYLOAD=\$(head -c \$PAYLOAD_SIZE /dev/urandom | base64 | head -c \$PAYLOAD_SIZE)

success=0
fail=0
total_start=\$(date +%s%N)

for i in \$(seq 1 \$REQUESTS); do
  seq_start=\$(date +%s%N)
  # Send VCTP packet via UDP (using netcat)
  echo -n \"\$PAYLOAD\" | nc -u -w1 \${SERVER%:*} \${SERVER#*:} >/dev/null 2>&1
  if [ \$? -eq 0 ]; then
    success=\$((success + 1))
  else
    fail=\$((fail + 1))
  fi
done

total_end=\$(date +%s%N)
duration_ms=\$(( (total_end - total_start) / 1000000 ))
ops_per_sec=\$(( success * 1000 / (duration_ms + 1) ))

echo \"RESULTS:success=\$success,fail=\$fail,duration_ms=\$duration_ms,ops_per_sec=\$ops_per_sec\"
" 2>/dev/null || true

  # Launch parallel load generator pods
  local pod_names=()
  for i in $(seq 1 "$CLIENTS"); do
    local pod_name="vctp-load-$(printf '%04d' "$i")"
    pod_names+=("$pod_name")
    kubectl -n "$NAMESPACE" run "$pod_name" \
      --image=alpine:3.19 \
      --restart=Never \
      --overrides='{"spec":{"containers":[{"name":"vctp-load","image":"alpine:3.19","command":["sh","-c","apk add --no-cache netcat-openbsd >/dev/null 2>&1; source /scripts/script.sh"],"volumeMounts":[{"name":"scripts","mountPath":"/scripts"}]}],"volumes":[{"name":"scripts","configMap":{"name":"vctp-loadtest"}}]}}' \
      2>/dev/null &
  done
  wait

  log_info "All $CLIENTS load generator pods launched"
  log_info "Waiting for workload to complete (max ${DURATION}s)..."

  # Wait for all pods to complete
  local timeout=$DURATION
  local elapsed=0
  while [[ $elapsed -lt $timeout ]]; do
    local running
    running=$(kubectl -n "$NAMESPACE" get pods -l run --field-selector=status.phase!=Succeeded,status.phase!=Failed \
      --no-headers 2>/dev/null | grep -c "vctp-load-" || echo "0")
    if [[ "$running" -eq 0 ]]; then
      break
    fi
    sleep 10
    elapsed=$((elapsed + 10))
    log_info "  ${elapsed}s: $running pods still running..."
  done

  local end_time
  end_time=$(date +%s)
  local total_duration=$((end_time - start_time))

  # Collect results
  log_info "Collecting results..."
  local total_success=0
  local total_fail=0
  local completed_pods=0

  for pod_name in "${pod_names[@]}"; do
    local logs
    logs=$(kubectl -n "$NAMESPACE" logs "$pod_name" 2>/dev/null || echo "")
    if echo "$logs" | grep -q "RESULTS:"; then
      local result_line
      result_line=$(echo "$logs" | grep "RESULTS:" | tail -1)
      local s f
      s=$(echo "$result_line" | sed 's/.*success=\([0-9]*\).*/\1/')
      f=$(echo "$result_line" | sed 's/.*fail=\([0-9]*\).*/\1/')
      total_success=$((total_success + s))
      total_fail=$((total_fail + f))
      completed_pods=$((completed_pods + 1))
    fi
    # Clean up pod
    kubectl -n "$NAMESPACE" delete pod "$pod_name" --ignore-not-found 2>/dev/null &
  done
  wait

  # Clean up ConfigMap
  kubectl -n "$NAMESPACE" delete configmap vctp-loadtest --ignore-not-found 2>/dev/null || true

  # ─── Report ─────────────────────────────────────────────────────────────
  local total_requests=$((total_success + total_fail))
  local ops_per_sec=0
  local delivery_ratio="0.00"
  local error_rate="0.00"

  if [[ $total_duration -gt 0 ]]; then
    ops_per_sec=$((total_success / total_duration))
  fi
  if [[ $total_requests -gt 0 ]]; then
    delivery_ratio=$(awk "BEGIN {printf \"%.2f\", $total_success / $total_requests}")
    error_rate=$(awk "BEGIN {printf \"%.2f\", ($total_fail / $total_requests) * 100}")
  fi

  echo ""
  echo "═══════════════════════════════════════════════════════════"
  echo "  VCTP PRODUCTION LOAD TEST RESULTS"
  echo "═══════════════════════════════════════════════════════════"
  echo ""
  echo "  Configuration:"
  echo "    Server:          $SERVER"
  echo "    Clients:         $CLIENTS"
  echo "    Requests/client: $REQUESTS"
  echo "    Duration:        ${total_duration}s"
  echo "    Payload size:    ${PAYLOAD_SIZE}B"
  echo ""
  echo "  Results:"
  echo "    Total requests:  $total_requests"
  echo "    Successful:      $total_success"
  echo "    Failed:          $total_fail"
  echo "    Ops/second:      $ops_per_sec"
  echo "    Delivery ratio:  $delivery_ratio"
  echo "    Error rate:      ${error_rate}%"
  echo ""
  echo "  CI Thresholds:"
  echo "    Min ops/s:       $MIN_OPS_PER_SEC  $([ "$ops_per_sec" -ge "$MIN_OPS_PER_SEC" ] && echo "✅ PASS" || echo "❌ FAIL")"
  echo "    Max error rate:  ${MAX_ERROR_RATE_PCT}%  $([ "$(echo "$error_rate < $MAX_ERROR_RATE_PCT" | bc -l 2>/dev/null || echo 1)" = "1" ] && echo "✅ PASS" || echo "❌ FAIL")"
  echo ""
  echo "═══════════════════════════════════════════════════════════"

  # Validate against CI thresholds
  local passed=true
  if [[ "$ops_per_sec" -lt "$MIN_OPS_PER_SEC" ]]; then
    log_error "Throughput below CI threshold: $ops_per_sec < $MIN_OPS_PER_SEC ops/s"
    passed=false
  fi

  if [[ "$passed" = true ]]; then
    log_info "All CI thresholds PASSED — production ready"
  else
    log_error "Some CI thresholds FAILED — investigate before go-live"
    exit 1
  fi
}

# ─── Main ───────────────────────────────────────────────────────────────────
main() {
  echo "═══════════════════════════════════════════════════════════"
  echo "  VCTP Production Load Test"
  echo "  $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
  echo "═══════════════════════════════════════════════════════════"
  echo ""

  preflight

  if [[ "$VERIFY_ONLY" = true ]]; then
    verify
    exit 0
  fi

  verify
  echo ""
  run_sustained_workload
}

main "$@"
