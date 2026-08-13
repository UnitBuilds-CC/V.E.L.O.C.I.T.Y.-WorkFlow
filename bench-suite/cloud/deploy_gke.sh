#!/usr/bin/env bash
# deploy_gke.sh — Deploy benchmark engines to GKE.
#
# Creates a GKE cluster, deploys all 6 engines using Kustomize,
# and runs the benchmark suite.
#
# Prerequisites:
#   - gcloud CLI authenticated
#   - kubectl installed
#   - Artifact Registry images pushed
#
# Usage:
#   ./deploy_gke.sh [overlay]
#
# Overlays: gke-standard (default), gke-stress

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH_DIR="$(dirname "$SCRIPT_DIR")"

PROJECT="${GCP_PROJECT:-$(gcloud config get-value project)}"
REGION="${GCP_REGION:-us-central1}"
ZONE="${GCP_ZONE:-us-central1-a}"
CLUSTER_NAME="${GKE_CLUSTER:-velocity-bench}"
MACHINE_TYPE="${GKE_MACHINE_TYPE:-e2-standard-4}"
NUM_NODES="${GKE_NUM_NODES:-3}"
OVERLAY="${1:-gke-standard}"
KUSTOMIZE_DIR="$BENCH_DIR/kustomize/overlays/$OVERLAY"

log() { echo "[gke] $*"; }

# ─── Destroy ─────────────────────────────────────────────────────────────────
if [ "${1:-}" = "--destroy" ]; then
    log "Destroying GKE cluster $CLUSTER_NAME..."
    gcloud container clusters delete "$CLUSTER_NAME" --zone "$ZONE" --quiet
    log "Cluster destroyed."
    exit 0
fi

# ─── Create GKE Cluster ─────────────────────────────────────────────────────
log "Creating GKE cluster: $CLUSTER_NAME"
log "  Region: $ZONE, Nodes: $NUM_NODES, Machine: $MACHINE_TYPE"

gcloud container clusters create "$CLUSTER_NAME" \
    --zone "$ZONE" \
    --num-nodes "$NUM_NODES" \
    --machine-type "$MACHINE_TYPE" \
    --enable-autoupgrade \
    --quiet 2>/dev/null || log "Cluster already exists"

# Get credentials
gcloud container clusters get-credentials "$CLUSTER_NAME" --zone "$ZONE"

# ─── Configure Artifact Registry access ─────────────────────────────────────
log "Configuring node access to Artifact Registry..."
# Nodes should already have read access via default service account

# ─── Deploy with Kustomize ───────────────────────────────────────────────────
log "Deploying benchmark suite with overlay: $OVERLAY"
kubectl apply -k "$KUSTOMIZE_DIR"

# ─── Wait for all pods ───────────────────────────────────────────────────────
log "Waiting for all pods to be Ready..."
kubectl -n benchmark wait --for=condition=Ready pod --all --timeout=600s

# ─── Register Restate ───────────────────────────────────────────────────────
log "Registering Restate service..."
RESTATE_POD=$(kubectl -n benchmark get pod -l app=restate-server -o jsonpath='{.items[0].metadata.name}')
kubectl -n benchmark exec "$RESTATE_POD" -- \
    restate deployments register http://restate-service:9080

# ─── Smoke test ──────────────────────────────────────────────────────────────
log "Running smoke test..."
kubectl -n benchmark delete job bench-job --ignore-not-found=true
kubectl -n benchmark set env job/bench-job BENCH_PROFILE=smoke 2>/dev/null || true
kubectl -n benchmark create -f "$BENCH_DIR/k8s/bench-job.yaml" 2>/dev/null || true
kubectl -n benchmark wait --for=condition=Complete job/bench-job --timeout=300s

log ""
log "═══════════════════════════════════════════════════════════════"
log "  GKE benchmark suite deployed and smoke test passed"
log "═══════════════════════════════════════════════════════════════"
log ""
log "Cluster: $CLUSTER_NAME"
log "Overlay: $OVERLAY"
log ""
log "Next steps:"
log "  Run standard benchmark:  kubectl -n benchmark set env job/bench-job BENCH_PROFILE=standard"
log "  View results:            kubectl -n benchmark logs job/bench-job"
log "  Destroy cluster:         $0 --destroy"
