#!/usr/bin/env bash
# deploy_gce.sh — Deploy benchmark engines to GCE VMs.
#
# Creates 6 GCE VMs (one per engine), installs Docker, pulls images from
# Artifact Registry, and starts the benchmark engines.
#
# Prerequisites:
#   - gcloud CLI authenticated
#   - Artifact Registry images pushed (see deploy_gce.sh --push first)
#
# Usage:
#   ./deploy_gce.sh              # Create VMs + deploy engines
#   ./deploy_gce.sh --destroy    # Tear down all VMs

set -euo pipefail

PROJECT="${GCP_PROJECT:-$(gcloud config get-value project)}"
REGION="${GCP_REGION:-us-central1}"
ZONE="${GCP_ZONE:-us-central1-a}"
MACHINE_TYPE="${GCP_MACHINE_TYPE:-e2-standard-4}"
REPO="${GCP_ARTIFACT_REPO:-us-central1-docker.pkg.dev/${PROJECT}/velocity-bench}"

VMs=(
    "velocity-classic-vm"
    "velocity-runtime-vm"
    "velocity-embedded-vm"
    "dbos-vm"
    "restate-vm"
    "temporal-vm"
)

log() { echo "[gce] $*"; }

# ─── Destroy ─────────────────────────────────────────────────────────────────
if [ "${1:-}" = "--destroy" ]; then
    log "Destroying all benchmark VMs..."
    for vm in "${VMs[@]}"; do
        log "  Deleting $vm..."
        gcloud compute instances delete "$vm" --zone "$ZONE" --quiet 2>/dev/null || true
    done
    log "All VMs destroyed."
    exit 0
fi

# ─── Create VMs ──────────────────────────────────────────────────────────────
log "Creating 6 GCE VMs in $ZONE (machine-type: $MACHINE_TYPE)..."

for vm in "${VMs[@]}"; do
    log "  Creating $vm..."
    gcloud compute instances create "$vm" \
        --zone "$ZONE" \
        --machine-type "$MACHINE_TYPE" \
        --image-family debian-12 \
        --image-project debian-cloud \
        --scopes cloud-platform \
        --quiet 2>/dev/null || log "  $vm already exists"
done

# ─── Install Docker + Deploy ─────────────────────────────────────────────────
deploy_to_vm() {
    local vm="$1"
    local engine="$2"
    local docker_cmd="$3"

    log "Deploying $engine to $vm..."

    # Install Docker
    gcloud compute ssh "$vm" --zone "$ZONE" --command "
        if ! command -v docker &> /dev/null; then
            curl -fsSL https://get.docker.com | sh
            sudo usermod -aG docker \$USER
        fi
        sudo systemctl enable docker
        sudo systemctl start docker

        # Configure Docker to use Artifact Registry
        sudo mkdir -p /etc/docker
        echo '{\"credsStore\":\"gcloud\"}' | sudo tee /etc/docker/config.json > /dev/null

        # Pull and start
        $docker_cmd
    "
    log "  $engine deployed to $vm"
}

# Velocity Classic
deploy_to_vm "velocity-classic-vm" "Velocity Classic" "
    sudo docker rm -f velocity-classic 2>/dev/null || true
    sudo docker run -d --name velocity-classic --network host --restart unless-stopped \\
        ${REPO}/velocity-classic:latest
    echo 'Waiting for gRPC...'
    for i in \$(seq 1 30); do
        nc -z localhost 7234 && echo 'Ready!' && break
        sleep 1
    done
"

# Velocity Runtime
deploy_to_vm "velocity-runtime-vm" "Velocity Runtime" "
    sudo docker rm -f velocity-runtime 2>/dev/null || true
    sudo docker run -d --name velocity-runtime --network host --restart unless-stopped \\
        ${REPO}/velocity-runtime:latest
    echo 'Waiting for gRPC...'
    for i in \$(seq 1 30); do
        nc -z localhost 7234 && echo 'Ready!' && break
        sleep 1
    done
"

# Velocity Embedded
deploy_to_vm "velocity-embedded-vm" "Velocity Embedded" "
    sudo docker rm -f velocity-embedded 2>/dev/null || true
    sudo mkdir -p /data
    sudo docker run -d --name velocity-embedded --network host --restart unless-stopped \\
        -v /data:/data \\
        ${REPO}/velocity-embedded:latest
    echo 'Waiting for gRPC...'
    for i in \$(seq 1 30); do
        nc -z localhost 7234 && echo 'Ready!' && break
        sleep 1
    done
"

# DBOS
deploy_to_vm "dbos-vm" "DBOS" "
    sudo docker rm -f dbos-postgres dbos-service 2>/dev/null || true
    sudo docker network create bench-net 2>/dev/null || true
    sudo docker run -d --name dbos-postgres --network bench-net \\
        -e POSTGRES_USER=dbos -e POSTGRES_PASSWORD=dbos_bench -e POSTGRES_DB=dbos_bench \\
        postgres:16-alpine
    sleep 5
    sudo docker run -d --name dbos-service --network bench-net --network host \\
        -e DBOS_DATABASE_URL=postgresql://dbos:dbos_bench@dbos-postgres:5432/dbos_bench \\
        ${REPO}/dbos:latest
"

# Restate
deploy_to_vm "restate-vm" "Restate" "
    sudo docker rm -f restate-server restate-service 2>/dev/null || true
    sudo docker network create bench-net 2>/dev/null || true
    sudo docker run -d --name restate-server --network bench-net --network host \\
        restatedev/restate:latest
    sleep 5
    sudo docker run -d --name restate-service --network bench-net \\
        -p 9080:9080 \\
        ${REPO}/restate:latest
    sleep 3
    sudo docker exec restate-server restate deployments register http://restate-service:9080
"

# Temporal
deploy_to_vm "temporal-vm" "Temporal" "
    sudo docker rm -f temporal-server temporal-service 2>/dev/null || true
    sudo docker network create bench-net 2>/dev/null || true
    sudo docker run -d --name temporal-server --network bench-net --network host \\
        -e DB=sqlite \\
        temporalio/auto-setup:latest
    sleep 15
    sudo docker run -d --name temporal-service --network bench-net \\
        -e TEMPORAL_ADDRESS=temporal-server:7233 \\
        -p 8080:8080 \\
        ${REPO}/temporal:latest
"

log ""
log "═══════════════════════════════════════════════════════════════"
log "  All 6 engines deployed to GCE VMs"
log "═══════════════════════════════════════════════════════════════"
log ""
log "VM IPs:"
for vm in "${VMs[@]}"; do
    ip=$(gcloud compute instances describe "$vm" --zone "$ZONE" \
        --format='get(networkInterfaces[0].accessConfigs[0].natIP)' 2>/dev/null || echo "unknown")
    log "  $vm: $ip"
done
