#!/usr/bin/env bash
# =============================================================================
# cloud-bench/cloud_3flavor.sh — Master orchestration for 3-Flavor Benchmark
#
# Run from local machine or CloudShell. Provisions 6 GCE VMs (one per engine),
# uploads repo, runs benchmarks in parallel, collects results, generates report.
#
# Architecture (6 dedicated VMs, zero resource contention):
#   VM 1: velocity-classic     — Velocity Classic (gRPC :7234)
#   VM 2: temporal-bench       — Temporal (Docker, gRPC :7233)
#   VM 3: velocity-runtime     — Velocity Runtime (HTTP :8080)
#   VM 4: restate-bench        — Restate (Docker, HTTP :8080)
#   VM 5: velocity-embedded    — Velocity Embedded (HTTP :8080)
#   VM 6: dbos-bench           — DBOS + PostgreSQL (Docker)
#
# Usage:
#   chmod +x cloud_3flavor.sh && ./cloud_3flavor.sh
#
# Environment:
#   GCP_PROJECT=velocity-live-test-001
#   GCP_ZONE=us-east1-b
#   BENCH_PROFILE=standard     (quick | standard | stress)
#   BENCH_WORKLOADS=all        (smoke | all)
#   SKIP_PROVISION=false       (set true to reuse existing VMs)
#   CLEANUP=false              (set true to delete VMs after benchmark)
# =============================================================================
set -euo pipefail

GCP_PROJECT="${GCP_PROJECT:-velocity-live-test-001}"
GCP_ZONE="${GCP_ZONE:-us-east1-b}"
PROFILE="${BENCH_PROFILE:-standard}"
WORKLOADS="${BENCH_WORKLOADS:-all}"
SKIP_PROVISION="${SKIP_PROVISION:-false}"
CLEANUP="${CLEANUP:-false}"

# VM names
VM_CLASSIC="velocity-classic"
VM_TEMPORAL="temporal-bench"
VM_RUNTIME="velocity-runtime"
VM_RESTATE="restate-bench"
VM_EMBEDDED="velocity-embedded"
VM_DBOS="dbos-bench"

ALL_VMS=("$VM_CLASSIC" "$VM_TEMPORAL" "$VM_RUNTIME" "$VM_RESTATE" "$VM_EMBEDDED" "$VM_DBOS")
NEW_VMS=("$VM_TEMPORAL" "$VM_RUNTIME" "$VM_RESTATE" "$VM_EMBEDDED" "$VM_DBOS")

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
log()  { echo -e "${GREEN}[master]${NC} $*"; }
warn() { echo -e "${YELLOW}[master]${NC} $*"; }
info() { echo -e "${CYAN}[master]${NC} $*"; }
err()  { echo -e "${RED}[master]${NC} $*"; }

# ── Cleanup trap ────────────────────────────────────────────────────────────
cleanup() {
    if [ "$CLEANUP" = "true" ]; then
        echo ""
        log "════════════════════════════════════════════════════════"
        log "  Cleaning up GCE VMs..."
        log "════════════════════════════════════════════════════════"
        for vm in "${NEW_VMS[@]}"; do
            log "  Deleting $vm..."
            gcloud compute instances delete "$vm" --zone="$GCP_ZONE" --quiet 2>/dev/null || true
        done
        log "  Cleanup complete."
    fi
}
trap cleanup EXIT

# ══════════════════════════════════════════════════════════════════════════════
# PHASE 1: Preflight checks
# ══════════════════════════════════════════════════════════════════════════════
log "════════════════════════════════════════════════════════"
log "  3-Flavor Cloud Benchmark — Master Orchestration"
log "════════════════════════════════════════════════════════"
log "  Project:     $GCP_PROJECT"
log "  Zone:        $GCP_ZONE"
log "  Profile:     $PROFILE"
log "  Workloads:   $WORKLOADS"
log "  VMs:         ${#ALL_VMS[@]} (6 dedicated)"
log "════════════════════════════════════════════════════════"
echo ""

# Check gcloud auth
if ! gcloud auth print-access-token >/dev/null 2>&1; then
    err "gcloud not authenticated. Run: gcloud auth login"
    exit 1
fi

# Set project
gcloud config set project "$GCP_PROJECT" >/dev/null 2>&1

# Find repo root
if git rev-parse --show-toplevel >/dev/null 2>&1; then
    REPO_ROOT="$(git rev-parse --show-toplevel)"
else
    _dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
    REPO_ROOT="$_dir"
fi

if [ ! -f "$REPO_ROOT/Cargo.toml" ]; then
    err "Could not find repo root (no Cargo.toml). Run from inside the repo."
    exit 1
fi

log "  Repo root: $REPO_ROOT"

# ══════════════════════════════════════════════════════════════════════════════
# PHASE 2: Provision VMs
# ══════════════════════════════════════════════════════════════════════════════
if [ "$SKIP_PROVISION" = "false" ]; then
    log ""
    log "[1/7] Provisioning GCE VMs..."

    # Check if velocity-classic exists (reuse existing VM)
    if gcloud compute instances describe "$VM_CLASSIC" --zone="$GCP_ZONE" >/dev/null 2>&1; then
        log "  $VM_CLASSIC already exists (reusing)"
    else
        log "  Creating $VM_CLASSIC..."
        gcloud compute instances create "$VM_CLASSIC" \
            --zone="$GCP_ZONE" \
            --machine-type=e2-standard-4 \
            --image-family=ubuntu-2404-lts-amd64 \
            --image-project=ubuntu-os-cloud \
            --boot-disk-size=50GB \
            --boot-disk-type=pd-balanced \
            --tags=http-server,https-server \
            --metadata=startup-script="#!/bin/bash
sudo apt-get update
sudo apt-get install -y docker.io
sudo systemctl enable --now docker"
    fi

    # Create 5 new VMs in parallel
    for vm in "${NEW_VMS[@]}"; do
        if gcloud compute instances describe "$vm" --zone="$GCP_ZONE" >/dev/null 2>&1; then
            log "  $vm already exists (reusing)"
        else
            log "  Creating $vm..."
            gcloud compute instances create "$vm" \
                --zone="$GCP_ZONE" \
                --machine-type=e2-standard-4 \
                --image-family=ubuntu-2404-lts-amd64 \
                --image-project=ubuntu-os-cloud \
                --boot-disk-size=50GB \
                --boot-disk-type=pd-balanced \
                --tags=http-server,https-server \
                --metadata=startup-script="#!/bin/bash
sudo apt-get update
sudo apt-get install -y docker.io
sudo systemctl enable --now docker" &
        fi
    done
    wait
    log "  All VMs created."
else
    log ""
    log "[1/7] Skipping provisioning (SKIP_PROVISION=true)"
fi

# ══════════════════════════════════════════════════════════════════════════════
# PHASE 3: Wait for SSH
# ══════════════════════════════════════════════════════════════════════════════
log ""
log "[2/7] Waiting for SSH on all VMs..."

wait_for_ssh() {
    local vm=$1
    for i in $(seq 1 60); do
        if gcloud compute ssh "$vm" --zone="$GCP_ZONE" --command="echo ok" 2>/dev/null | grep -q ok; then
            return 0
        fi
        sleep 3
    done
    return 1
}

for vm in "${ALL_VMS[@]}"; do
    log "  Waiting for $vm..."
    if wait_for_ssh "$vm"; then
        log "    $vm ready"
    else
        err "    $vm SSH timeout"
        exit 1
    fi
done

# ══════════════════════════════════════════════════════════════════════════════
# PHASE 4: Upload repo tarball
# ══════════════════════════════════════════════════════════════════════════════
log ""
log "[3/7] Uploading repository to all VMs..."

cd "$REPO_ROOT"
tar czf /tmp/velocity-repo.tar.gz \
    --exclude='.git' --exclude='target' --exclude='node_modules' --exclude='*.log' \
    --exclude='.ssh' --exclude='.zshrc' --exclude='.lesshst' --exclude='.bash_history' .

log "  Tarball: $(ls -lh /tmp/velocity-repo.tar.gz | awk '{print $5}')"

for vm in "${ALL_VMS[@]}"; do
    log "  Uploading to $vm..."
    gcloud compute scp /tmp/velocity-repo.tar.gz "$vm:~/velocity-repo.tar.gz" --zone="$GCP_ZONE" 2>/dev/null &
done
wait

rm -f /tmp/velocity-repo.tar.gz
log "  Upload complete."

# ══════════════════════════════════════════════════════════════════════════════
# PHASE 5: Upload per-VM benchmark script
# ══════════════════════════════════════════════════════════════════════════════
log ""
log "[4/7] Uploading benchmark scripts..."

for vm in "${ALL_VMS[@]}"; do
    gcloud compute scp "$REPO_ROOT/cloud-bench/cloud_3flavor_ec2.sh" "$vm:~/cloud_3flavor_ec2.sh" --zone="$GCP_ZONE" 2>/dev/null &
done
wait

log "  Scripts uploaded."

# ══════════════════════════════════════════════════════════════════════════════
# PHASE 6: Run benchmarks in parallel
# ══════════════════════════════════════════════════════════════════════════════
log ""
log "[5/7] Running benchmarks on all 6 VMs (parallel)..."
log "  This will take 15-45 minutes depending on profile..."
echo ""

run_bench() {
    local vm=$1
    local flavor=$2
    log "  Starting benchmark on $vm ($flavor)..."
    gcloud compute ssh "$vm" --zone="$GCP_ZONE" --command="
        chmod +x ~/cloud_3flavor_ec2.sh
        FLAVOR=$flavor PROFILE=$PROFILE WORKLOADS=$WORKLOADS bash ~/cloud_3flavor_ec2.sh
    " 2>&1 | sed "s/^/[$vm] /" &
}

run_bench "$VM_CLASSIC" "velocity-classic"
run_bench "$VM_TEMPORAL" "temporal"
run_bench "$VM_RUNTIME" "velocity-runtime"
run_bench "$VM_RESTATE" "restate"
run_bench "$VM_EMBEDDED" "velocity-embedded"
run_bench "$VM_DBOS" "dbos"

wait
log ""
log "  All benchmarks complete."

# ══════════════════════════════════════════════════════════════════════════════
# PHASE 7: Download results
# ══════════════════════════════════════════════════════════════════════════════
log ""
log "[6/7] Downloading results from all VMs..."

RESULTS_DIR="$HOME/velocity-bench-results-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$RESULTS_DIR"

download_results() {
    local vm=$1
    local flavor=$2
    log "  Downloading from $vm..."
    mkdir -p "$RESULTS_DIR/$flavor"
    gcloud compute scp "$vm:/tmp/bench/*" "$RESULTS_DIR/$flavor/" --zone="$GCP_ZONE" --recurse 2>/dev/null || true
}

download_results "$VM_CLASSIC" "classic"
download_results "$VM_TEMPORAL" "temporal"
download_results "$VM_RUNTIME" "runtime"
download_results "$VM_RESTATE" "restate"
download_results "$VM_EMBEDDED" "embedded"
download_results "$VM_DBOS" "dbos"

log "  Results downloaded to: $RESULTS_DIR"

# ══════════════════════════════════════════════════════════════════════════════
# PHASE 8: Aggregate results
# ══════════════════════════════════════════════════════════════════════════════
log ""
log "[7/7] Aggregating results..."

if [ -f "$REPO_ROOT/cloud-bench/aggregate_results.py" ]; then
    python3 "$REPO_ROOT/cloud-bench/aggregate_results.py" \
        --input-dir "$RESULTS_DIR" \
        --output "$RESULTS_DIR/aggregated"
else
    warn "  aggregate_results.py not found — skipping aggregation"
fi

# ══════════════════════════════════════════════════════════════════════════════
# Summary
# ══════════════════════════════════════════════════════════════════════════════
echo ""
log "════════════════════════════════════════════════════════"
log "  3-FLAVOR BENCHMARK COMPLETE"
log "════════════════════════════════════════════════════════"
log "  Results: $RESULTS_DIR"
ls -lh "$RESULTS_DIR/" 2>/dev/null || true
echo ""

if [ -f "$RESULTS_DIR/aggregated/benchmark_comparison.md" ]; then
    info "── Summary ──"
    head -50 "$RESULTS_DIR/aggregated/benchmark_comparison.md"
    echo ""
fi

log "════════════════════════════════════════════════════════"
log "  VMs (6 dedicated):"
log "    $VM_CLASSIC     — Velocity Classic"
log "    $VM_TEMPORAL    — Temporal"
log "    $VM_RUNTIME     — Velocity Runtime"
log "    $VM_RESTATE     — Restate"
log "    $VM_EMBEDDED    — Velocity Embedded"
log "    $VM_DBOS        — DBOS"
log "════════════════════════════════════════════════════════"

if [ "$CLEANUP" = "true" ]; then
    log "  Cleanup enabled — VMs will be deleted."
else
    log "  Cleanup disabled — VMs remain running."
    log "  To delete: CLEANUP=true ./cloud_3flavor.sh"
fi
