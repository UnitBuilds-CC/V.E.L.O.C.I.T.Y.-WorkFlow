#!/bin/bash
# Production Benchmark — Deploy all 3 competitors and run benchmarks.
#
# Usage:
#   ./run_all.sh [profile]
#   profile: smoke, standard, stress (default: standard)
#
# Prerequisites:
#   - SSH access to all 3 competitor VMs
#   - VM IPs configured below
set -e

PROFILE=${1:-"standard"}
SSH_KEY="$HOME/.ssh/google_compute_engine"
SSH_USER="ian_unitbuilds_com"
SSH_OPTS="-i $SSH_KEY -o StrictHostKeyChecking=no -o ConnectTimeout=10"

# VM IPs (update these to match your GCE instances)
TEMPORAL_VM="34.139.181.220"
RESTATE_VM="35.227.44.141"
DBOS_VM="34.26.33.56"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "============================================"
echo "  Production Benchmark — All Competitors"
echo "  Profile: $PROFILE"
echo "============================================"
echo ""

# ─── Deploy to each VM ───────────────────────────────────────────────────────

deploy_to_vm() {
    local name=$1
    local ip=$2
    local subdir=$3
    local remote_dir=$4

    echo "━━━ Deploying $name to $ip ━━━"

    # Create tar of production files
    tar cf /tmp/${subdir}_bench.tar -C "$SCRIPT_DIR" "$subdir"

    # SCP to VM
    scp $SSH_OPTS /tmp/${subdir}_bench.tar ${SSH_USER}@${ip}:/tmp/ 2>&1

    # Extract and run deploy
    ssh $SSH_OPTS ${SSH_USER}@${ip} "
        rm -rf ~/${remote_dir}
        mkdir -p ~/${remote_dir}
        tar xf /tmp/${subdir}_bench.tar -C ~/${remote_dir} --strip-components=1
        cd ~/${remote_dir}
        chmod +x deploy.sh
        bash deploy.sh
    " 2>&1

    echo "✓ $name deployed"
    echo ""
}

# Deploy all three
deploy_to_vm "Temporal" "$TEMPORAL_VM" "temporal" "temporal-production"
deploy_to_vm "Restate" "$RESTATE_VM" "restate" "restate-production"
deploy_to_vm "DBOS"    "$DBOS_VM"    "dbos"    "dbos-production"

echo "============================================"
echo "  All competitors deployed!"
echo "============================================"
echo ""

# ─── Run Benchmarks ──────────────────────────────────────────────────────────

run_bench() {
    local name=$1
    local ip=$2
    local cmd=$3
    local output=$4

    echo "━━━ Benchmarking $name on $ip ━━━"
    ssh $SSH_OPTS ${SSH_USER}@${ip} "$cmd" 2>&1 | tee "/tmp/${output}_results.log"
    echo ""
    echo "✓ $name benchmark complete"
    echo ""
}

# Run benchmarks sequentially (to avoid resource contention)
run_bench "Temporal" "$TEMPORAL_VM" \
    "cd ~/temporal-production && python3 client.py $PROFILE" \
    "temporal"

run_bench "Restate" "$RESTATE_VM" \
    "cd ~/restate-production && node client.js $PROFILE" \
    "restate"

run_bench "DBOS" "$DBOS_VM" \
    "cd ~/dbos-production && python3 client.py $PROFILE" \
    "dbos"

# ─── Collect Results ─────────────────────────────────────────────────────────

echo "============================================"
echo "  Collecting Results"
echo "============================================"

RESULTS_DIR="$SCRIPT_DIR/results"
mkdir -p "$RESULTS_DIR"

for vm_ip in "$TEMPORAL_VM" "$RESTATE_VM" "$DBOS_VM"; do
    echo "Collecting from $vm_ip..."
    scp $SSH_OPTS ${SSH_USER}@${vm_ip}:/tmp/*_bench_results.json "$RESULTS_DIR/" 2>/dev/null || \
        echo "  No results from $vm_ip"
done

echo ""
echo "Results saved to: $RESULTS_DIR/"
ls -la "$RESULTS_DIR/"

echo ""
echo "============================================"
echo "  All benchmarks complete!"
echo "============================================"
