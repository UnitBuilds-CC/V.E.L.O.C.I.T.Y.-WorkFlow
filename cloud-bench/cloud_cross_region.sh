#!/usr/bin/env bash
# =============================================================================
# cloud-bench/cloud_cross_region.sh — CROSS-REGION 2-VM BENCHMARK
#
# Launches 2 instances in SEPARATE AWS regions:
#   SERVER in REGION_A: Real Temporal (PostgreSQL) + VELOCITY dev-server
#   CLIENT in REGION_B: Benchmark harness → connects to server over internet
#
# Cross-region latency is FIXED and DETERMINISTIC (~20-80ms depending on regions).
# This amplifies VELOCITY's fewer round-trips vs Temporal's DB-heavy protocol.
#
# Usage:
#   chmod +x cloud_cross_region.sh && ./cloud_cross_region.sh
#
# Optional env vars:
#   REGION_A=us-east-2  REGION_B=us-east-1   (nearby, ~10-15ms)
#   REGION_A=us-east-2  REGION_B=us-west-1   (cross-country, ~60ms)
#   SERVER_INSTANCE=m7i-flex.large  CLIENT_INSTANCE=t3.small
# =============================================================================
set -euo pipefail

REGION_A="${REGION_A:-us-east-2}"    # Server region
REGION_B="${REGION_B:-us-east-1}"    # Client region
SERVER_TYPE="${SERVER_INSTANCE:-m7i-flex.large}"
CLIENT_TYPE="${CLIENT_INSTANCE:-t3.small}"
PROFILE="${BENCH_PROFILE:-standard}"

KEY_NAME_A="velocity-xr-a-$$"
KEY_NAME_B="velocity-xr-b-$$"
SG_NAME_A="velocity-xr-sg-a-$$"
SG_NAME_B="velocity-xr-sg-b-$$"
SERVER_ID="" CLIENT_ID="" SG_A="" SG_B=""
PEM_A="" PEM_B=""

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; RED='\033[0;31m'; NC='\033[0m'
log()  { echo -e "${GREEN}[bench]${NC} $*"; }
warn() { echo -e "${YELLOW}[bench]${NC} $*"; }
info() { echo -e "${CYAN}[bench]${NC} $*"; }
err()  { echo -e "${RED}[bench]${NC} $*"; }

# ── Cleanup ───────────────────────────────────────────────────────────────
cleanup() {
    echo ""
    log "════════════════════════════════════════════════════════"
    log "  Cleaning up ALL resources in both regions..."
    log "════════════════════════════════════════════════════════"
    [ -n "$SERVER_ID" ] && aws ec2 terminate-instances --instance-ids "$SERVER_ID" --region "$REGION_A" --output text >/dev/null 2>&1 || true
    [ -n "$CLIENT_ID" ] && aws ec2 terminate-instances --instance-ids "$CLIENT_ID" --region "$REGION_B" --output text >/dev/null 2>&1 || true
    [ -n "$KEY_NAME_A" ] && aws ec2 delete-key-pair --key-name "$KEY_NAME_A" --region "$REGION_A" >/dev/null 2>&1 || true
    [ -n "$KEY_NAME_B" ] && aws ec2 delete-key-pair --key-name "$KEY_NAME_B" --region "$REGION_B" >/dev/null 2>&1 || true
    # Wait for instances before deleting SGs
    [ -n "$SERVER_ID" ] && aws ec2 wait instance-terminated --instance-ids "$SERVER_ID" --region "$REGION_A" 2>/dev/null || true
    [ -n "$CLIENT_ID" ] && aws ec2 wait instance-terminated --instance-ids "$CLIENT_ID" --region "$REGION_B" 2>/dev/null || true
    [ -n "$SG_A" ] && aws ec2 delete-security-group --group-name "$SG_NAME_A" --region "$REGION_A" >/dev/null 2>&1 || true
    [ -n "$SG_B" ] && aws ec2 delete-security-group --group-name "$SG_NAME_B" --region "$REGION_B" >/dev/null 2>&1 || true
    log "  Cleanup complete."
}
trap cleanup EXIT

# ── 0. Preflight ──────────────────────────────────────────────────────────
log "════════════════════════════════════════════════════════"
log "  VELOCITY Cross-Region Benchmark                        "
log "════════════════════════════════════════════════════════"
log "  Server region:  $REGION_A ($SERVER_TYPE)"
log "  Client region:  $REGION_B ($CLIENT_TYPE)"
log "  Profile:        $PROFILE"
log "  Expected latency: ~20-80ms cross-region"
echo ""

aws sts get-caller-identity --query 'Account' --output text >/dev/null 2>&1 || {
    err "AWS CLI not configured."; exit 1
}

# ── Helper: lookup AMI for a region ──────────────────────────────────────
lookup_ami() {
    local region="$1"
    aws ec2 describe-images \
        --owners 099720109477 \
        --filters "Name=name,Values=ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*" \
                  "Name=state,Values=available" \
        --query 'sort_by(Images,&CreationDate)[-1].ImageId' \
        --output text --region "$region"
}

# ── 1. Look up AMIs ──────────────────────────────────────────────────────
log "[1/8] Looking up AMIs..."
AMI_A=$(lookup_ami "$REGION_A")
AMI_B=$(lookup_ami "$REGION_B")
log "  $REGION_A AMI: $AMI_A"
log "  $REGION_B AMI: $AMI_B"

# ── 2. Create SSH keys (one per region) ──────────────────────────────────
log "[2/8] Creating SSH key pairs..."
aws ec2 create-key-pair --key-name "$KEY_NAME_A" --key-type ed25519 --key-format pem \
    --query 'KeyMaterial' --output text --region "$REGION_A" > "$HOME/.ssh/$KEY_NAME_A.pem" 2>/dev/null
PEM_A="$HOME/.ssh/$KEY_NAME_A.pem"
chmod 400 "$PEM_A"

aws ec2 create-key-pair --key-name "$KEY_NAME_B" --key-type ed25519 --key-format pem \
    --query 'KeyMaterial' --output text --region "$REGION_B" > "$HOME/.ssh/$KEY_NAME_B.pem" 2>/dev/null
PEM_B="$HOME/.ssh/$KEY_NAME_B.pem"
chmod 400 "$PEM_B"

# ── 3. Security groups ───────────────────────────────────────────────────
log "[3/8] Creating security groups..."
MY_IP=$(curl -s https://checkip.amazonaws.com | tr -d '\n')

# Server SG: SSH from CloudShell + ports 7233-7235 open to client region
SG_A=$(aws ec2 create-security-group \
    --group-name "$SG_NAME_A" --description "VELOCITY cross-region server" \
    --query 'GroupId' --output text --region "$REGION_A")
aws ec2 authorize-security-group-ingress \
    --group-id "$SG_A" --protocol tcp --port 22 --cidr "${MY_IP}/32" \
    --region "$REGION_A" --output json >/dev/null
# Open benchmark ports to the world (client comes from region B's IP range)
aws ec2 authorize-security-group-ingress \
    --group-id "$SG_A" --protocol tcp --port "7233-7235" --cidr "0.0.0.0/0" \
    --region "$REGION_A" --output json >/dev/null
log "  Server SG: $SG_A (ports 7233-7235 open)"

# Client SG: SSH only from CloudShell
SG_B=$(aws ec2 create-security-group \
    --group-name "$SG_NAME_B" --description "VELOCITY cross-region client" \
    --query 'GroupId' --output text --region "$REGION_B")
aws ec2 authorize-security-group-ingress \
    --group-id "$SG_B" --protocol tcp --port 22 --cidr "${MY_IP}/32" \
    --region "$REGION_B" --output json >/dev/null
log "  Client SG: $SG_B"

# ── 4. Launch instances ──────────────────────────────────────────────────
log "[4/8] Launching SERVER in $REGION_A..."
SERVER_ID=$(aws ec2 run-instances \
    --image-id "$AMI_A" --instance-type "$SERVER_TYPE" \
    --key-name "$KEY_NAME_A" --security-group-ids "$SG_A" \
    --block-device-mappings '[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":30,"VolumeType":"gp3","DeleteOnTermination":true}}]' \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=velocity-xr-server}]" \
    --query 'Instances[0].InstanceId' --output text --region "$REGION_A")

log "  Launching CLIENT in $REGION_B..."
CLIENT_ID=$(aws ec2 run-instances \
    --image-id "$AMI_B" --instance-type "$CLIENT_TYPE" \
    --key-name "$KEY_NAME_B" --security-group-ids "$SG_B" \
    --block-device-mappings '[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":20,"VolumeType":"gp3","DeleteOnTermination":true}}]' \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=velocity-xr-client}]" \
    --query 'Instances[0].InstanceId' --output text --region "$REGION_B")

# ── 5. Wait for both ─────────────────────────────────────────────────────
log "[5/8] Waiting for instances (both regions)..."
aws ec2 wait instance-running --instance-ids "$SERVER_ID" --region "$REGION_A"
aws ec2 wait instance-running --instance-ids "$CLIENT_ID" --region "$REGION_B"

SERVER_IP=$(aws ec2 describe-instances --instance-ids "$SERVER_ID" \
    --query 'Reservations[0].Instances[0].PublicIpAddress' --output text --region "$REGION_A")
CLIENT_IP=$(aws ec2 describe-instances --instance-ids "$CLIENT_ID" \
    --query 'Reservations[0].Instances[0].PublicIpAddress' --output text --region "$REGION_B")

log "  Server ($REGION_A): $SERVER_IP"
log "  Client ($REGION_B): $CLIENT_IP"

SSH_A="-i $PEM_A -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10 -o LogLevel=ERROR"
SSH_B="-i $PEM_B -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10 -o LogLevel=ERROR"

log "  Waiting for SSH..."
for i in $(seq 1 60); do
    S_OK=$(ssh $SSH_A "ubuntu@$SERVER_IP" "echo ok" 2>/dev/null | grep -c ok || true)
    C_OK=$(ssh $SSH_B "ubuntu@$CLIENT_IP" "echo ok" 2>/dev/null | grep -c ok || true)
    if [ "$S_OK" -ge 1 ] && [ "$C_OK" -ge 1 ]; then break; fi
    printf "."; sleep 3
done
echo ""
log "  Both instances ready."

# ── 6. Upload repo to both ───────────────────────────────────────────────
log "[6/8] Uploading repository to both regions..."
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if git rev-parse --show-toplevel >/dev/null 2>&1; then
    REPO_ROOT="$(git rev-parse --show-toplevel)"
else
    REPO_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"
fi

cd "$REPO_ROOT"
tar czf /tmp/velocity-repo.tar.gz \
    --exclude='.git' --exclude='target' --exclude='node_modules' --exclude='*.log' \
    --exclude='.ssh' --exclude='.zshrc' --exclude='.lesshst' .
log "  Tarball: $(ls -lh /tmp/velocity-repo.tar.gz | awk '{print $5}')"

# Upload to both in parallel
for target in "$SSH_A ubuntu@$SERVER_IP" "$SSH_B ubuntu@$CLIENT_IP"; do
    # shellcheck disable=SC2086
    scp $target /tmp/velocity-repo.tar.gz:~/velocity-repo.tar.gz &
done
wait

# Extract on both in parallel
for ssh_opts_ip in "$SSH_A $SERVER_IP" "$SSH_B $CLIENT_IP"; do
    # shellcheck disable=SC2086
    ssh $ssh_opts_ip "mkdir -p ~/VELOCITY-WorkFlow && tar xzf ~/velocity-repo.tar.gz -C ~/VELOCITY-WorkFlow && rm ~/velocity-repo.tar.gz" &
done
wait
rm -f /tmp/velocity-repo.tar.gz
log "  Repo uploaded to both regions."

# ── 7. Setup SERVER ──────────────────────────────────────────────────────
log "[7/8] Setting up SERVER in $REGION_A (~5 min)..."
info "  Installing deps, building, starting engines..."

ssh $SSH_A "ubuntu@$SERVER_IP" bash -s <<'SERVER_SCRIPT'
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive

sudo apt-get update -qq
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update -qq
sudo apt-get install -y -qq build-essential pkg-config libssl-dev protobuf-compiler \
    curl wget git unzip jq netcat-openbsd \
    docker-ce docker-ce-cli containerd.io docker-compose-plugin

sudo systemctl enable --now docker
sudo usermod -aG docker ubuntu 2>/dev/null || true
if ! sudo docker info >/dev/null 2>&1; then sleep 5; sudo systemctl restart docker; sleep 3; fi
echo "[server] Docker: $(sudo docker --version)"

curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.org | sh -s -- -y --default-toolchain stable 2>/dev/null
export PATH="$HOME/.cargo/bin:$PATH"

cd ~/VELOCITY-WorkFlow
echo "[server] Building..."
cargo build --release -p velocity-workflow-server -p velocity-bench 2>&1 | grep -E '(Compiling|Finished|error)' || true
echo "[server] Binaries ready."

# Start Real Temporal
echo "[server] Starting Real Temporal..."
sudo docker compose -f ~/VELOCITY-WorkFlow/velocity-bench/docker-compose.temporal.yml up -d 2>&1 || {
    echo "[server] Docker compose output above"
}

echo "[server] Waiting for Temporal (up to 3 min)..."
for i in $(seq 1 60); do
    nc -z localhost 7233 2>/dev/null && { echo "[server] Real Temporal READY on :7233"; break; }
    sleep 3; printf "."
done
echo ""
if ! nc -z localhost 7233 2>/dev/null; then
    echo "[server] Temporal logs:"
    sudo docker logs velocity-bench-temporal --tail 10 2>&1 || true
fi

# Start VELOCITY production server on 0.0.0.0
echo "[server] Starting VELOCITY production server on 0.0.0.0:7234..."
cd ~/VELOCITY-WorkFlow
nohup ./target/release/velocity-server --grpc-port 7234 --ip 0.0.0.0 > /tmp/velocity-server.log 2>&1 &
sleep 2
nc -z localhost 7234 2>/dev/null && echo "[server] VELOCITY READY on :7234" || echo "[server] VELOCITY not ready"

# Start temporal-bridge on 0.0.0.0
echo "[server] Starting temporal-bridge on 0.0.0.0:7235..."
nohup ./target/release/temporal-bridge --grpc-port 7235 --ip 0.0.0.0 > /tmp/temporal-bridge.log 2>&1 &
sleep 2
nc -z localhost 7235 2>/dev/null && echo "[server] temporal-bridge READY on :7235" || echo "[server] temporal-bridge not ready"

echo "[server] ═══ ALL ENGINES STARTED ═══"
SERVER_SCRIPT

# ── 8. Run benchmark on CLIENT → SERVER (cross-region!) ──────────────────
log "[8/8] Running CROSS-REGION benchmark..."
log "  Client ($REGION_B) → Server ($REGION_A) via $SERVER_IP"
log "  Streaming live output:"
echo ""
info "════════════════════════════════════════════════════════"
info "  Cross-Region Benchmark: $REGION_B → $REGION_A"
info "  Server: $SERVER_IP"
info "════════════════════════════════════════════════════════"

ssh $SSH_B "ubuntu@$CLIENT_IP" bash -s "$SERVER_IP" "$PROFILE" <<'CLIENT_SCRIPT'
set -euo pipefail
SERVER_IP="$1"
PROFILE="$2"

export DEBIAN_FRONTEND=noninteractive
sudo apt-get update -qq
sudo apt-get install -y -qq build-essential pkg-config libssl-dev protobuf-compiler curl netcat-openbsd

curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.org | sh -s -- -y --default-toolchain stable 2>/dev/null
export PATH="$HOME/.cargo/bin:$PATH"

cd ~/VELOCITY-WorkFlow
echo "[client] Building velocity-bench..."
cargo build --release -p velocity-bench 2>&1 | grep -E '(Compiling|Finished|error)' || true

# Measure cross-region latency
echo "[client] ═══ Cross-Region Latency ═══"
echo "[client] Ping to server $SERVER_IP:"
ping -c 20 "$SERVER_IP" 2>/dev/null | tail -2 || echo "  ping failed (ICMP may be blocked)"
echo ""

# Test port connectivity
echo "[client] Port connectivity:"
for port in 7233 7234 7235; do
    if nc -z -w10 "$SERVER_IP" "$port" 2>/dev/null; then
        # Measure TCP connect latency
        LATENCY=$(nc -z -w10 "$SERVER_IP" "$port" 2>&1; echo $?)
        echo "[client]   Port $port: OPEN"
    else
        echo "[client]   Port $port: CLOSED"
    fi
done
echo ""

# Run benchmark
echo "[client] ═══ Starting Benchmark (profile: $PROFILE) ═══"
./target/release/velocity-bench \
    --workloads all \
    --profile "$PROFILE" \
    --velocity-address "http://$SERVER_IP:7234" \
    --temporal-address "http://$SERVER_IP:7235" \
    --format all \
    --output bench_results

echo ""
echo "[client] ═══ BENCHMARK COMPLETE ═══"
ls -lh bench_results.* 2>/dev/null
CLIENT_SCRIPT

# ── Download results ─────────────────────────────────────────────────────
log ""
log "Downloading results from client ($REGION_B)..."
RESULTS_DIR="$HOME/velocity-bench-crossregion-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$RESULTS_DIR"
for f in bench_results.md bench_results.csv bench_results.json; do
    scp $SSH_B "ubuntu@$CLIENT_IP:~/VELOCITY-WorkFlow/$f" "$RESULTS_DIR/$f" 2>/dev/null || true
done

echo ""
log "════════════════════════════════════════════════════════"
log "  CROSS-REGION BENCHMARK COMPLETE                        "
log "════════════════════════════════════════════════════════"
log "  Server: $REGION_A ($SERVER_IP)"
log "  Client: $REGION_B ($CLIENT_IP)"
log "  Results: $RESULTS_DIR"
ls -lh "$RESULTS_DIR/" 2>/dev/null || true
echo ""

if [ -f "$RESULTS_DIR/bench_results.md" ]; then
    info "── Summary ──"
    sed -n '/^## Summary/,/^## Detailed/p' "$RESULTS_DIR/bench_results.md" | head -20
    echo ""
fi

# Cleanup runs automatically
