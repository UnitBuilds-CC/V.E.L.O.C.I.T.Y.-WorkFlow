#!/usr/bin/env bash
# collect_results.sh — Collect benchmark results from GCE VMs and GKE.
#
# Usage:
#   ./collect_results.sh [output-dir]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH_DIR="$(dirname "$SCRIPT_DIR")"
OUTPUT_DIR="${1:-$BENCH_DIR/results/cloud_$(date +%Y%m%d_%H%M%S)}"

PROJECT="${GCP_PROJECT:-$(gcloud config get-value project)}"
ZONE="${GCP_ZONE:-us-central1-a}"

mkdir -p "$OUTPUT_DIR"

log() { echo "[collect] $*"; }

# ─── Collect from GCE VMs ────────────────────────────────────────────────────
declare -A VM_ENGINES=(
    ["velocity-classic-vm"]="velocity-classic"
    ["velocity-runtime-vm"]="velocity-runtime"
    ["velocity-embedded-vm"]="velocity-embedded"
    ["dbos-vm"]="dbos"
    ["restate-vm"]="restate"
    ["temporal-vm"]="temporal"
)

for vm in "${!VM_ENGINES[@]}"; do
    engine="${VM_ENGINES[$vm]}"
    log "Collecting results from $vm ($engine)..."

    # Get VM IP
    ip=$(gcloud compute instances describe "$vm" --zone "$ZONE" \
        --format='get(networkInterfaces[0].accessConfigs[0].natIP)' 2>/dev/null || echo "")

    if [ -z "$ip" ]; then
        log "  WARNING: Could not get IP for $vm"
        continue
    fi

    # SCP results
    scp -o StrictHostKeyChecking=no \
        "$vm:~/bench-results/*.json" \
        "$OUTPUT_DIR/${engine}_" 2>/dev/null || \
        log "  No results found on $vm"
done

# ─── Collect from GKE ────────────────────────────────────────────────────────
log "Collecting results from GKE benchmark Job..."
kubectl -n benchmark logs job/bench-job > "$OUTPUT_DIR/gke_bench_job.log" 2>/dev/null || \
    log "  No GKE Job found"

# ─── Merge results ───────────────────────────────────────────────────────────
log "Merging results..."
python3 "$BENCH_DIR/scripts/merge_results.py" "$OUTPUT_DIR" "$OUTPUT_DIR/merged_results.json"

log ""
log "═══════════════════════════════════════════════════════════════"
log "  Results collected in: $OUTPUT_DIR"
log "═══════════════════════════════════════════════════════════════"
