#!/bin/bash
# Deploy latest code to all GCE VMs and rebuild Docker images.
# Usage: ./bench-suite/cloud/update_vm.sh [vm-name]
#   If no vm-name given, updates all 6 VMs.

set -euo pipefail

ZONE="us-east1-b"
REPO_DIR="$HOME/V.E.L.O.C.I.T.Y.-WorkFlow"

ALL_VMS=(velocity-classic velocity-runtime velocity-embedded dbos-bench restate-bench temporal-bench)

update_vm() {
  local VM="$1"
  echo "═══════════════════════════════════════════════════"
  echo "  Updating $VM"
  echo "═══════════════════════════════════════════════════"

  # Pull latest code
  gcloud compute ssh "$VM" --zone="$ZONE" --quiet --command="
    set -e
    cd $REPO_DIR
    echo 'Pulling latest code...'
    git fetch origin main
    git reset --hard origin/main
    echo 'Latest commit:'
    git log --oneline -1

    echo ''
    echo 'Stopping old containers...'
    cd bench-suite
    docker compose down 2>/dev/null || true

    echo ''
    echo 'Rebuilding and starting...'
    docker compose up -d --build
    echo ''
    echo 'Container status:'
    docker compose ps
    echo ''
    echo 'Done updating $VM'
  "
  echo ""
}

if [ $# -gt 0 ]; then
  update_vm "$1"
else
  for vm in "${ALL_VMS[@]}"; do
    update_vm "$vm"
  done
fi

echo "All VMs updated!"
