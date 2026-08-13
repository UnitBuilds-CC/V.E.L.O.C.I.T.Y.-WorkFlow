#!/usr/bin/env bash
# run_k8s.sh — Orchestrate Kubernetes benchmarks.
#
# Usage:
#   ./bench-suite/scripts/run_k8s.sh [profile] [overlay]
#
# Profiles: smoke (default), short, standard, stress
# Overlays: local-docker-desktop (default), gke-standard, gke-stress

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH_DIR="$(dirname "$SCRIPT_DIR")"
PROFILE="${1:-smoke}"
OVERLAY="${2:-local-docker-desktop}"
KUSTOMIZE_DIR="$BENCH_DIR/kustomize/overlays/$OVERLAY"

echo "[k8s] Profile: $PROFILE, Overlay: $OVERLAY"
echo "[k8s] Kustomize dir: $KUSTOMIZE_DIR"

# ─── 1. Apply manifests ─────────────────────────────────────────────────────
echo "[k8s] Applying Kustomize overlay..."
kubectl apply -k "$KUSTOMIZE_DIR"

# ─── 2. Wait for all pods ───────────────────────────────────────────────────
echo "[k8s] Waiting for all pods to be Ready..."
kubectl -n benchmark wait --for=condition=Ready pod --all --timeout=300s

# ─── 3. Register Restate service ────────────────────────────────────────────
echo "[k8s] Registering Restate service..."
RESTATE_POD=$(kubectl -n benchmark get pod -l app=restate-server -o jsonpath='{.items[0].metadata.name}')
kubectl -n benchmark exec "$RESTATE_POD" -- \
    restate deployments register http://restate-service:9080 || echo "[k8s] Restate registration may have failed"

# ─── 4. Run benchmark Job ───────────────────────────────────────────────────
echo "[k8s] Starting benchmark Job with profile=$PROFILE..."
kubectl -n benchmark delete job bench-job --ignore-not-found=true
kubectl -n benchmark set env job/bench-job BENCH_PROFILE="$PROFILE" 2>/dev/null || true
kubectl -n benchmark create -f "$BENCH_DIR/k8s/bench-job.yaml" 2>/dev/null || \
    kubectl -n benchmark apply -f "$BENCH_DIR/k8s/bench-job.yaml"

# Wait for Job to complete
echo "[k8s] Waiting for benchmark Job to complete..."
kubectl -n benchmark wait --for=condition=Complete job/bench-job --timeout=3600s

# ─── 5. Collect results ─────────────────────────────────────────────────────
echo "[k8s] Collecting results..."
kubectl -n benchmark logs job/bench-job

echo ""
echo "[k8s] ═══════════════════════════════════════════════════════════════"
echo "[k8s]   Benchmark Job complete"
echo "[k8s] ═══════════════════════════════════════════════════════════════"
echo ""
echo "[k8s] To clean up:  kubectl delete -k $KUSTOMIZE_DIR"
