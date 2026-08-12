#!/usr/bin/env bash
# =============================================================================
# cloud-bench/cloud_k8s_bench.sh — Suite B: Kubernetes (GKE) Benchmark
#
# Provisions a GKE cluster, deploys all 6 engines, runs benchmarks,
# collects results, and tears down.
#
# Measures Kubernetes orchestration overhead vs bare VM (Suite A).
#
# Prerequisites:
#   - gcloud with container component
#   - kubectl
#   - GKE cluster or auto-provisioning
#
# Usage:
#   chmod +x cloud_k8s_bench.sh && ./cloud_k8s_bench.sh
#
# Environment:
#   GCP_PROJECT=velocity-live-test-001
#   GCP_ZONE=us-east1-b
#   GKE_CLUSTER=velocity-bench-cluster
#   BENCH_PROFILE=standard     (quick | standard | stress)
#   SKIP_CLUSTER=false         (reuse existing cluster)
#   CLEANUP=true               (delete cluster after benchmark)
# =============================================================================
set -euo pipefail

GCP_PROJECT="${GCP_PROJECT:-velocity-live-test-001}"
GCP_ZONE="${GCP_ZONE:-us-east1-b}"
GKE_CLUSTER="${GKE_CLUSTER:-velocity-bench-cluster}"
PROFILE="${BENCH_PROFILE:-standard}"
SKIP_CLUSTER="${SKIP_CLUSTER:-false}"
CLEANUP="${CLEANUP:-true}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
log()  { echo -e "${GREEN}[k8s-bench]${NC} $*"; }
warn() { echo -e "${YELLOW}[k8s-bench]${NC} $*"; }
info() { echo -e "${CYAN}[k8s-bench]${NC} $*"; }
err()  { echo -e "${RED}[k8s-bench]${NC} $*"; }

# ── Cleanup ─────────────────────────────────────────────────────────────────
cleanup() {
    if [ "$CLEANUP" = "true" ]; then
        echo ""
        log "════════════════════════════════════════════════════════"
        log "  Cleaning up GKE resources..."
        log "════════════════════════════════════════════════════════"

        # Delete benchmark namespace (removes all deployments, services, jobs)
        kubectl delete namespace velocity-bench --ignore-not-found=true 2>/dev/null || true

        # Delete cluster if we created it
        if [ "$SKIP_CLUSTER" = "false" ]; then
            log "  Deleting GKE cluster $GKE_CLUSTER..."
            gcloud container clusters delete "$GKE_CLUSTER" \
                --zone="$GCP_ZONE" --quiet 2>/dev/null || true
        fi

        log "  Cleanup complete."
    fi
}
trap cleanup EXIT

# ══════════════════════════════════════════════════════════════════════════════
# PHASE 1: Preflight
# ══════════════════════════════════════════════════════════════════════════════
log "════════════════════════════════════════════════════════"
log "  Suite B: Kubernetes Benchmark (GKE)"
log "════════════════════════════════════════════════════════"
log "  Project:  $GCP_PROJECT"
log "  Zone:     $GCP_ZONE"
log "  Cluster:  $GKE_CLUSTER"
log "  Profile:  $PROFILE"
log "════════════════════════════════════════════════════════"
echo ""

# Check prerequisites
for cmd in gcloud kubectl; do
    if ! command -v "$cmd" &>/dev/null; then
        err "$cmd not found. Install it first."
        exit 1
    fi
done

gcloud config set project "$GCP_PROJECT" >/dev/null 2>&1

# ══════════════════════════════════════════════════════════════════════════════
# PHASE 2: Create GKE cluster
# ══════════════════════════════════════════════════════════════════════════════
if [ "$SKIP_CLUSTER" = "false" ]; then
    log "[1/6] Creating GKE cluster..."

    if gcloud container clusters describe "$GKE_CLUSTER" --zone="$GCP_ZONE" >/dev/null 2>&1; then
        log "  Cluster $GKE_CLUSTER already exists (reusing)"
    else
        gcloud container clusters create "$GKE_CLUSTER" \
            --zone="$GCP_ZONE" \
            --machine-type=e2-standard-4 \
            --num-nodes=3 \
            --disk-size=50 \
            --enable-autoscaling \
            --min-nodes=3 \
            --max-nodes=6 \
            --release-channel=regular

        log "  Cluster created."
    fi
else
    log "[1/6] Skipping cluster creation (SKIP_CLUSTER=true)"
fi

# Get credentials
log "  Getting cluster credentials..."
gcloud container clusters get-credentials "$GKE_CLUSTER" --zone="$GCP_ZONE"

# Verify kubectl
kubectl cluster-info
log "  kubectl connected."

# ══════════════════════════════════════════════════════════════════════════════
# PHASE 3: Build and push Docker images
# ══════════════════════════════════════════════════════════════════════════════
log ""
log "[2/6] Building and pushing Docker images..."

# Find repo root
if git rev-parse --show-toplevel >/dev/null 2>&1; then
    REPO_ROOT="$(git rev-parse --show-toplevel)"
else
    REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
fi

GCR_HOST="gcr.io"
IMAGE_PREFIX="$GCR_HOST/$GCP_PROJECT"

# Build Velocity server image
log "  Building velocity-workflow-server..."
cd "$REPO_ROOT"
docker build -t velocity-workflow-server:latest -f Dockerfile . 2>/dev/null || \
    docker build -t velocity-workflow-server:latest . 2>/dev/null || \
    warn "  Could not build velocity-workflow-server (Dockerfile not found)"

# Build Velocity dev-server image
log "  Building velocity-dev-server..."
docker build -t velocity-dev-server:latest -f Dockerfile.dev-server . 2>/dev/null || \
    warn "  Could not build velocity-dev-server"

# Build velocity-bench image
log "  Building velocity-bench..."
docker build -t velocity-bench:latest -f deploy/Dockerfile.bench . 2>/dev/null || \
    warn "  Could not build velocity-bench"

# Tag and push to GCR
for img in velocity-workflow-server velocity-dev-server velocity-bench; do
    if docker image inspect "$img:latest" >/dev/null 2>&1; then
        docker tag "$img:latest" "$IMAGE_PREFIX/$img:latest"
        docker push "$IMAGE_PREFIX/$img:latest" 2>/dev/null || \
            warn "  Could not push $img (auth issue? Run: gcloud auth configure-docker)"
    fi
done

# Update manifests to use GCR images
if [ -d "$SCRIPT_DIR/k8s" ]; then
    sed -i "s|velocity-workflow-server:latest|$IMAGE_PREFIX/velocity-workflow-server:latest|g" "$SCRIPT_DIR/k8s/k8s_bench_manifests.yaml"
    sed -i "s|velocity-dev-server:latest|$IMAGE_PREFIX/velocity-dev-server:latest|g" "$SCRIPT_DIR/k8s/k8s_bench_manifests.yaml"
    sed -i "s|velocity-bench:latest|$IMAGE_PREFIX/velocity-bench:latest|g" "$SCRIPT_DIR/k8s/k8s_bench_manifests.yaml"
fi

log "  Images pushed to GCR."

# ══════════════════════════════════════════════════════════════════════════════
# PHASE 4: Deploy engines to GKE
# ══════════════════════════════════════════════════════════════════════════════
log ""
log "[3/6] Deploying engines to GKE..."

kubectl apply -f "$SCRIPT_DIR/k8s/k8s_bench_manifests.yaml"

# Wait for all deployments to be ready
log "  Waiting for deployments to be ready..."

wait_for_deployment() {
    local name=$1
    local timeout=$2
    log "    Waiting for $name (timeout: ${timeout}s)..."
    if kubectl rollout status "deployment/$name" -n velocity-bench --timeout="${timeout}s" 2>/dev/null; then
        log "    $name ready"
        return 0
    else
        warn "    $name not ready (timeout)"
        return 1
    fi
}

# Wait for StatefulSet (postgres)
log "    Waiting for bench-postgres..."
kubectl rollout status "statefulset/bench-postgres" -n velocity-bench --timeout=120s 2>/dev/null || \
    warn "    bench-postgres not ready"

# Wait for Deployments
wait_for_deployment "velocity-classic" 120 || true
wait_for_deployment "temporal-bench" 180 || true
wait_for_deployment "velocity-runtime" 120 || true
wait_for_deployment "restate-bench" 120 || true
wait_for_deployment "velocity-embedded" 120 || true
wait_for_deployment "dbos-bench" 120 || true

# Print pod status
log ""
log "  Pod status:"
kubectl get pods -n velocity-bench -o wide

# ══════════════════════════════════════════════════════════════════════════════
# PHASE 5: Run benchmarks
# ══════════════════════════════════════════════════════════════════════════════
log ""
log "[4/6] Running Kubernetes benchmarks..."
log "  This will take 15-45 minutes..."
echo ""

# Option A: Use the Job manifest (if bench runner image is available)
if kubectl get job/velocity-bench-runner -n velocity-bench >/dev/null 2>&1; then
    log "  Benchmark Job submitted. Waiting for completion..."
    kubectl wait --for=condition=complete job/velocity-bench-runner \
        -n velocity-bench --timeout=3600s 2>/dev/null || true

    log "  Job logs:"
    kubectl logs job/velocity-bench-runner -n velocity-bench 2>/dev/null || true
fi

# Option B: Run benchmarks from local machine via port-forward (more reliable)
log ""
log "  Running benchmarks via port-forward..."

RESULTS_DIR="$HOME/velocity-k8s-bench-results-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$RESULTS_DIR"

# Port-forward and run gRPC benchmark
log "  gRPC: Velocity Classic vs Temporal..."
kubectl port-forward svc/velocity-classic 17234:7234 -n velocity-bench &
PF_CLASSIC=$!
kubectl port-forward svc/temporal-bench 17233:7233 -n velocity-bench &
PF_TEMPORAL=$!
sleep 3

# Run gRPC bench (if velocity-bench binary available locally)
if command -v velocity-bench &>/dev/null || [ -f "$REPO_ROOT/target/release/velocity-bench" ]; then
    BENCH_BIN="$REPO_ROOT/target/release/velocity-bench"
    [ -f "$BENCH_BIN" ] || BENCH_BIN="velocity-bench"

    "$BENCH_BIN" \
        --workloads all \
        --engine both \
        --format all \
        --profile "$PROFILE" \
        --velocity-address http://localhost:17234 \
        --temporal-address http://localhost:17233 \
        --output "$RESULTS_DIR/grpc_k8s_results" 2>/dev/null || \
        warn "  gRPC benchmark failed"
fi

kill $PF_CLASSIC $PF_TEMPORAL 2>/dev/null || true
wait 2>/dev/null || true

# Port-forward and run HTTP benchmark
log "  HTTP: Velocity Runtime vs Restate..."
kubectl port-forward svc/velocity-runtime 18080:8080 -n velocity-bench &
PF_RUNTIME=$!
kubectl port-forward svc/restate-bench 18081:8080 -n velocity-bench &
PF_RESTATE=$!
sleep 3

if command -v velocity-bench-http &>/dev/null || [ -f "$REPO_ROOT/target/release/velocity-bench-http" ]; then
    BENCH_HTTP_BIN="$REPO_ROOT/target/release/velocity-bench-http"
    [ -f "$BENCH_HTTP_BIN" ] || BENCH_HTTP_BIN="velocity-bench-http"

    "$BENCH_HTTP_BIN" \
        --workloads all \
        --engine both \
        --format all \
        --profile "$PROFILE" \
        --velocity-address http://localhost:18080 \
        --restate-address http://localhost:18081 \
        --output "$RESULTS_DIR/http_k8s_results" 2>/dev/null || \
        warn "  HTTP benchmark failed"
fi

kill $PF_RUNTIME $PF_RESTATE 2>/dev/null || true
wait 2>/dev/null || true

# Port-forward and run Embedded benchmark (Velocity Embedded vs DBOS)
log "  Embedded: Velocity Embedded vs DBOS..."
kubectl port-forward svc/velocity-embedded 18082:8082 -n velocity-bench &
PF_EMBEDDED=$!
kubectl port-forward svc/dbos-bench 13000:3000 -n velocity-bench &
PF_DBOS=$!
sleep 3

# Run embedded benchmark via HTTP workflow CRUD against both engines
EMBEDDED_OPS=500
log "    Sequential workflow creation ($EMBEDDED_OPS ops each)..."

# Velocity Embedded sequential
VEL_EMB_START=$(date +%s%N)
VEL_EMB_SUCCESS=0
for i in $(seq 1 "$EMBEDDED_OPS"); do
    HTTP_CODE=$(curl -sf -o /dev/null -w "%{http_code}" \
        -X POST "http://localhost:18082/api/v1/namespaces/default/workflows" \
        -H "Content-Type: application/json" \
        -d "{\"workflowId\":\"k8s-vel-$i\",\"workflowType\":\"bench\",\"input\":{\"n\":$i}}" \
        --max-time 5 2>/dev/null) || HTTP_CODE="000"
    if [ "$HTTP_CODE" -ge 200 ] 2>/dev/null && [ "$HTTP_CODE" -lt 300 ] 2>/dev/null; then
        VEL_EMB_SUCCESS=$((VEL_EMB_SUCCESS + 1))
    fi
done
VEL_EMB_END=$(date +%s%N)
VEL_EMB_MS=$(( (VEL_EMB_END - VEL_EMB_START) / 1000000 ))
VEL_EMB_TPS=0
if [ "$VEL_EMB_MS" -gt 0 ]; then
    VEL_EMB_TPS=$(awk "BEGIN{printf \"%.1f\", $VEL_EMB_SUCCESS * 1000.0 / $VEL_EMB_MS}")
fi
log "    Velocity Embedded: $VEL_EMB_SUCCESS/$EMBEDDED_OPS, ${VEL_EMB_TPS} ops/sec"

# DBOS sequential
DBOS_K8S_START=$(date +%s%N)
DBOS_K8S_SUCCESS=0
for i in $(seq 1 "$EMBEDDED_OPS"); do
    HTTP_CODE=$(curl -sf -o /dev/null -w "%{http_code}" \
        -X POST "http://localhost:13000/api/v1/namespaces/default/workflows" \
        -H "Content-Type: application/json" \
        -d "{\"workflowId\":\"k8s-dbos-$i\",\"workflowType\":\"bench\",\"input\":{\"n\":$i}}" \
        --max-time 5 2>/dev/null) || HTTP_CODE="000"
    if [ "$HTTP_CODE" -ge 200 ] 2>/dev/null && [ "$HTTP_CODE" -lt 300 ] 2>/dev/null; then
        DBOS_K8S_SUCCESS=$((DBOS_K8S_SUCCESS + 1))
    fi
done
DBOS_K8S_END=$(date +%s%N)
DBOS_K8S_MS=$(( (DBOS_K8S_END - DBOS_K8S_START) / 1000000 ))
DBOS_K8S_TPS=0
if [ "$DBOS_K8S_MS" -gt 0 ]; then
    DBOS_K8S_TPS=$(awk "BEGIN{printf \"%.1f\", $DBOS_K8S_SUCCESS * 1000.0 / $DBOS_K8S_MS}")
fi
log "    DBOS: $DBOS_K8S_SUCCESS/$EMBEDDED_OPS, ${DBOS_K8S_TPS} ops/sec"

# Write embedded results
cat > "$RESULTS_DIR/embedded_k8s_results.json" <<EOF
{
    "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
    "benchmark": "embedded_k8s",
    "velocity_embedded": {
        "sequential_ops": $VEL_EMB_SUCCESS,
        "sequential_total_ms": $VEL_EMB_MS,
        "sequential_tps": $VEL_EMB_TPS
    },
    "dbos": {
        "sequential_ops": $DBOS_K8S_SUCCESS,
        "sequential_total_ms": $DBOS_K8S_MS,
        "sequential_tps": $DBOS_K8S_TPS
    }
}
EOF

kill $PF_EMBEDDED $PF_DBOS 2>/dev/null || true
wait 2>/dev/null || true

# ══════════════════════════════════════════════════════════════════════════════
# PHASE 6: Collect results
# ══════════════════════════════════════════════════════════════════════════════
log ""
log "[5/6] Collecting results..."

# Try to get results from the Job pods
if kubectl get job/velocity-bench-runner -n velocity-bench >/dev/null 2>&1; then
    kubectl cp "velocity-bench/$(kubectl get pods -n velocity-bench -l app=velocity-bench-runner -o jsonpath='{.items[0].metadata.name}'):results/" \
        "$RESULTS_DIR/job_results/" 2>/dev/null || true
fi
if kubectl get job/embedded-bench-runner -n velocity-bench >/dev/null 2>&1; then
    mkdir -p "$RESULTS_DIR/embedded_job_results"
    kubectl cp "velocity-bench/$(kubectl get pods -n velocity-bench -l app=embedded-bench-runner -o jsonpath='{.items[0].metadata.name}'):results/" \
        "$RESULTS_DIR/embedded_job_results/" 2>/dev/null || true
fi

# Aggregate if script available
if [ -f "$SCRIPT_DIR/aggregate_results.py" ]; then
    log "  Aggregating results..."
    python3 "$SCRIPT_DIR/aggregate_results.py" \
        --input-dir "$RESULTS_DIR" \
        --output "$RESULTS_DIR/aggregated" 2>/dev/null || true
fi

log "  Results: $RESULTS_DIR"
ls -lh "$RESULTS_DIR/" 2>/dev/null || true

# ══════════════════════════════════════════════════════════════════════════════
# PHASE 7: Summary
# ══════════════════════════════════════════════════════════════════════════════
log ""
log "[6/6] Kubernetes benchmark summary..."
echo ""
log "════════════════════════════════════════════════════════"
log "  Suite B: Kubernetes Benchmark Complete"
log "════════════════════════════════════════════════════════"
log "  Cluster:  $GKE_CLUSTER"
log "  Zone:     $GCP_ZONE"
log "  Results:  $RESULTS_DIR"
log ""
log "  Engines deployed (6 + PostgreSQL):"
log "    velocity-classic   (gRPC :7234)  vs  temporal-bench (gRPC :7233)"
log "    velocity-runtime   (HTTP :8080)  vs  restate-bench  (HTTP :8080)"
log "    velocity-embedded  (HTTP :8082)  vs  dbos-bench     (HTTP :3000)"
log ""
log "  Benchmark pairs:"
log "    gRPC:      Velocity Classic vs Temporal"
log "    HTTP:      Velocity Runtime vs Restate"
log "    Embedded:  Velocity Embedded vs DBOS"
log ""
log "  This measures Kubernetes orchestration overhead."
log "  Compare with Suite A (bare VM) for overhead analysis."
log "════════════════════════════════════════════════════════"

if [ "$CLEANUP" = "true" ]; then
    log "  Cleanup enabled — cluster will be deleted."
else
    log "  Cleanup disabled — cluster remains running."
    log "  To delete: gcloud container clusters delete $GKE_CLUSTER --zone=$GCP_ZONE"
fi
