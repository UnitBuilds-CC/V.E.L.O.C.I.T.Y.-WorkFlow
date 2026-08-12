#!/usr/bin/env bash
# =============================================================================
# cloud-bench/cloud_2instance.sh — 2-VPS BENCHMARK (ONE SCRIPT DOES EVERYTHING)
#
# Launches 2 EC2 instances in the same VPC:
#   SERVER (m7i-flex.large): Real Temporal (PostgreSQL) + VELOCITY dev-server
#   CLIENT (t3.small):       Benchmark harness connecting over VPC network
#
# Network latency between instances (~0.5-2ms) is the KEY differentiator:
# VELOCITY needs fewer round-trips than Temporal, so network amplifies the gap.
#
# Run in AWS CloudShell:
#   chmod +x cloud_2instance.sh && ./cloud_2instance.sh
#
# Optional: SERVER_INSTANCE=t3.medium CLIENT_INSTANCE=t3.medium ./cloud_2instance.sh
# =============================================================================
set -euo pipefail

REGION="${AWS_DEFAULT_REGION:-us-east-2}"
SERVER_TYPE="${SERVER_INSTANCE:-m7i-flex.large}"
CLIENT_TYPE="${CLIENT_INSTANCE:-t3.small}"
PROFILE="${BENCH_PROFILE:-standard}"
KEY_NAME="velocity-2inst-$$"
SG_NAME="velocity-2inst-sg-$$"
SERVER_ID=""
CLIENT_ID=""
SG_ID=""
PEM=""

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; RED='\033[0;31m'; NC='\033[0m'
log()  { echo -e "${GREEN}[bench]${NC} $*"; }
warn() { echo -e "${YELLOW}[bench]${NC} $*"; }
info() { echo -e "${CYAN}[bench]${NC} $*"; }
err()  { echo -e "${RED}[bench]${NC} $*"; }

# ── Cleanup: ALWAYS runs ──────────────────────────────────────────────────
cleanup() {
    echo ""
    log "════════════════════════════════════════════════════════"
    log "  Cleaning up ALL AWS resources..."
    log "════════════════════════════════════════════════════════"
    for id in $SERVER_ID $CLIENT_ID; do
        if [ -n "$id" ]; then
            log "  Terminating $id..."
            aws ec2 terminate-instances --instance-ids "$id" --output text >/dev/null 2>&1 || true
        fi
    done
    if [ -n "$KEY_NAME" ]; then
        log "  Deleting key pair $KEY_NAME..."
        aws ec2 delete-key-pair --key-name "$KEY_NAME" >/dev/null 2>&1 || true
    fi
    # Wait for instances to terminate before deleting SG
    for id in $SERVER_ID $CLIENT_ID; do
        [ -n "$id" ] && aws ec2 wait instance-terminated --instance-ids "$id" 2>/dev/null || true
    done
    if [ -n "$SG_ID" ]; then
        log "  Deleting security group..."
        aws ec2 delete-security-group --group-name "$SG_NAME" >/dev/null 2>&1 || true
    fi
    log "  Cleanup complete."
}
trap cleanup EXIT

# ── 0. Preflight ──────────────────────────────────────────────────────────
log "════════════════════════════════════════════════════════"
log "  VELOCITY 2-Instance Cloud Benchmark                    "
log "════════════════════════════════════════════════════════"
log "  Region:         $REGION"
log "  Server:         $SERVER_TYPE (engines)"
log "  Client:         $CLIENT_TYPE (benchmark harness)"
log "  Profile:        $PROFILE"
log "  Network:        VPC private (~0.5-2ms latency)"
echo ""

aws sts get-caller-identity --query 'Account' --output text >/dev/null 2>&1 || {
    err "AWS CLI not configured."; exit 1
}

# ── 1. AMI ────────────────────────────────────────────────────────────────
log "[1/8] Looking up Ubuntu 22.04 AMI..."
AMI_ID=$(aws ec2 describe-images \
    --owners 099720109477 \
    --filters "Name=name,Values=ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*" \
              "Name=state,Values=available" \
    --query 'sort_by(Images,&CreationDate)[-1].ImageId' \
    --output text --region "$REGION")
[ -z "$AMI_ID" ] || [ "$AMI_ID" = "None" ] && { err "AMI not found"; exit 1; }
log "  AMI: $AMI_ID"

# ── 2. SSH key ────────────────────────────────────────────────────────────
log "[2/8] Creating SSH key pair..."
aws ec2 create-key-pair --key-name "$KEY_NAME" --key-type ed25519 --key-format pem \
    --query 'KeyMaterial' --output text > "$HOME/.ssh/$KEY_NAME.pem" 2>/dev/null
PEM="$HOME/.ssh/$KEY_NAME.pem"
chmod 400 "$PEM"

# ── 3. Security group (allows ALL internal traffic) ───────────────────────
log "[3/8] Creating security group (all internal traffic allowed)..."
MY_IP=$(curl -s https://checkip.amazonaws.com | tr -d '\n')
SG_ID=$(aws ec2 create-security-group \
    --group-name "$SG_NAME" \
    --description "VELOCITY 2-instance benchmark" \
    --query 'GroupId' --output text)
# SSH from CloudShell
aws ec2 authorize-security-group-ingress \
    --group-id "$SG_ID" --protocol tcp --port 22 --cidr "${MY_IP}/32" \
    --output json >/dev/null
# ALL traffic within group (VPC internal)
aws ec2 authorize-security-group-ingress \
    --group-id "$SG_ID" --protocol all --source-group "$SG_ID" \
    --output json >/dev/null
log "  SG: $SG_ID"

# ── 4. Launch both instances ─────────────────────────────────────────────
log "[4/8] Launching SERVER ($SERVER_TYPE)..."
SERVER_ID=$(aws ec2 run-instances \
    --image-id "$AMI_ID" --instance-type "$SERVER_TYPE" \
    --key-name "$KEY_NAME" --security-group-ids "$SG_ID" \
    --block-device-mappings '[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":30,"VolumeType":"gp3","DeleteOnTermination":true}}]' \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=velocity-bench-server}]" \
    --query 'Instances[0].InstanceId' --output text)
log "  Server: $SERVER_ID"

log "  Launching CLIENT ($CLIENT_TYPE)..."
CLIENT_ID=$(aws ec2 run-instances \
    --image-id "$AMI_ID" --instance-type "$CLIENT_TYPE" \
    --key-name "$KEY_NAME" --security-group-ids "$SG_ID" \
    --block-device-mappings '[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":20,"VolumeType":"gp3","DeleteOnTermination":true}}]' \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=velocity-bench-client}]" \
    --query 'Instances[0].InstanceId' --output text)
log "  Client: $CLIENT_ID"

# ── 5. Wait for both ─────────────────────────────────────────────────────
log "[5/8] Waiting for instances to start..."
aws ec2 wait instance-running --instance-ids "$SERVER_ID" "$CLIENT_ID"

SERVER_IP=$(aws ec2 describe-instances --instance-ids "$SERVER_ID" \
    --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
CLIENT_IP=$(aws ec2 describe-instances --instance-ids "$CLIENT_ID" \
    --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
SERVER_PRIV=$(aws ec2 describe-instances --instance-ids "$SERVER_ID" \
    --query 'Reservations[0].Instances[0].PrivateIpAddress' --output text)

log "  Server: $SERVER_IP (private: $SERVER_PRIV)"
log "  Client: $CLIENT_IP"

SSH_OPTS="-i $PEM -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=5 -o LogLevel=ERROR"

log "  Waiting for SSH on both instances..."
for i in $(seq 1 60); do
    S_OK=$(ssh $SSH_OPTS "ubuntu@$SERVER_IP" "echo ok" 2>/dev/null | grep -c ok || true)
    C_OK=$(ssh $SSH_OPTS "ubuntu@$CLIENT_IP" "echo ok" 2>/dev/null | grep -c ok || true)
    if [ "$S_OK" -ge 1 ] && [ "$C_OK" -ge 1 ]; then break; fi
    printf "."; sleep 3
done
echo ""
log "  Both instances ready."

# ── 6. Upload repo to both ───────────────────────────────────────────────
log "[6/8] Uploading repository..."
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

for HOST in "$SERVER_IP" "$CLIENT_IP"; do
    scp $SSH_OPTS /tmp/velocity-repo.tar.gz "ubuntu@$HOST:~/velocity-repo.tar.gz"
    ssh $SSH_OPTS "ubuntu@$HOST" "mkdir -p ~/VELOCITY-WorkFlow && tar xzf ~/velocity-repo.tar.gz -C ~/VELOCITY-WorkFlow && rm ~/velocity-repo.tar.gz" &
done
wait
rm -f /tmp/velocity-repo.tar.gz
log "  Repo uploaded to both instances."

# ── 7. Setup SERVER (install deps, build, start engines) ─────────────────
log "[7/8] Setting up SERVER (installing deps, building, starting engines)..."
info "  This takes ~5 minutes (apt + Rust + cargo build + Docker)..."

ssh $SSH_OPTS "ubuntu@$SERVER_IP" bash -s <<'SERVER_SCRIPT'
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive

# Install system packages
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

# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.org | sh -s -- -y --default-toolchain stable 2>/dev/null
export PATH="$HOME/.cargo/bin:$PATH"

# Build
cd ~/VELOCITY-WorkFlow
echo "[server] Building (release mode)..."
cargo build --release -p velocity-workflow-server -p velocity-bench 2>&1 | grep -E '(Compiling|Finished|error)' || true
echo "[server] Binaries:"
ls -lh target/release/velocity-server target/release/velocity-bench target/release/temporal-bridge 2>/dev/null || echo "BUILD FAILED"

# Start Real Temporal
echo "[server] Starting Real Temporal (PostgreSQL)..."
sudo docker compose -f ~/VELOCITY-WorkFlow/velocity-bench/docker-compose.temporal.yml up -d 2>&1 || {
    echo "[server] Docker compose failed, showing logs:"
    sudo docker logs velocity-bench-temporal --tail 20 2>&1 || true
}

# Wait for Temporal
echo "[server] Waiting for Temporal (up to 3 minutes)..."
TEMPORAL_READY=false
for i in $(seq 1 60); do
    if nc -z localhost 7233 2>/dev/null; then TEMPORAL_READY=true; break; fi
    sleep 3; printf "."
done
echo ""
if [ "$TEMPORAL_READY" = true ]; then
    echo "[server] Real Temporal READY on :7233"
else
    echo "[server] WARNING: Temporal not ready. Logs:"
    sudo docker logs velocity-bench-temporal --tail 15 2>&1 || true
fi

# Start VELOCITY production server on 0.0.0.0 (accessible from client VPC)
echo "[server] Starting VELOCITY production server on 0.0.0.0:7234..."
cd ~/VELOCITY-WorkFlow
nohup ./target/release/velocity-server --grpc-port 7234 --ip 0.0.0.0 > /tmp/velocity-server.log 2>&1 &
sleep 2
if nc -z localhost 7234 2>/dev/null; then
    echo "[server] VELOCITY production server READY on :7234"
else
    echo "[server] WARNING: VELOCITY production server not ready"
fi

# Start temporal-bridge on 0.0.0.0
echo "[server] Starting temporal-bridge on 0.0.0.0:7235..."
nohup ./target/release/temporal-bridge --grpc-port 7235 --ip 0.0.0.0 > /tmp/temporal-bridge.log 2>&1 &
sleep 2
if nc -z localhost 7235 2>/dev/null; then
    echo "[server] temporal-bridge READY on :7235"
else
    echo "[server] WARNING: temporal-bridge not ready"
fi

echo "[server] ═══ SERVER READY ═══"
echo "[server] Engines available:"
echo "[server]   Real Temporal:    0.0.0.0:7233"
echo "[server]   VELOCITY prod:    0.0.0.0:7234"
echo "[server]   temporal-bridge:  0.0.0.0:7235"
SERVER_SCRIPT

# ── 8. Run benchmark on CLIENT (connects to SERVER over VPC) ─────────────
log "[8/8] Running benchmark on CLIENT → SERVER over VPC network..."
log "  Server private IP: $SERVER_PRIV"
log "  Streaming live output:"
echo ""
info "════════════════════════════════════════════════════════"
info "  Benchmark: CLIENT ($CLIENT_IP) → SERVER ($SERVER_PRIV)"
info "  All traffic goes over VPC network (real latency)"
info "════════════════════════════════════════════════════════"

ssh $SSH_OPTS "ubuntu@$CLIENT_IP" bash -s "$SERVER_PRIV" "$PROFILE" <<'CLIENT_SCRIPT'
set -euo pipefail
SERVER_IP="$1"
PROFILE="$2"

# Install minimal deps (just Rust + protobuf)
export DEBIAN_FRONTEND=noninteractive
sudo apt-get update -qq
sudo apt-get install -y -qq build-essential pkg-config libssl-dev protobuf-compiler curl netcat-openbsd

# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.org | sh -s -- -y --default-toolchain stable 2>/dev/null
export PATH="$HOME/.cargo/bin:$PATH"

# Build benchmark harness only
cd ~/VELOCITY-WorkFlow
echo "[client] Building velocity-bench..."
cargo build --release -p velocity-bench 2>&1 | grep -E '(Compiling|Finished|error)' || true

# Verify connectivity to server
echo "[client] Testing connectivity to server $SERVER_IP..."
for port in 7233 7234 7235; do
    if nc -z -w5 "$SERVER_IP" "$port" 2>/dev/null; then
        echo "[client]   Port $port: OPEN"
    else
        echo "[client]   Port $port: CLOSED"
    fi
done

# Measure baseline network latency
echo "[client] Network latency to server (ping):"
ping -c 10 "$SERVER_IP" 2>/dev/null | tail -1 || echo "  ping failed"

# Run benchmark — connect to REMOTE server over VPC
echo "[client] ═══ Starting benchmark (profile: $PROFILE) ═══"
cd ~/VELOCITY-WorkFlow
./target/release/velocity-bench \
    --workloads all \
    --profile "$PROFILE" \
    --velocity-address "http://$SERVER_IP:7234" \
    --temporal-address "http://$SERVER_IP:7235" \
    --format all \
    --output bench_results

echo ""
echo "[client] ═══ BENCHMARK COMPLETE ═══"
echo "[client] Results:"
ls -lh bench_results.* 2>/dev/null
CLIENT_SCRIPT

# ── 9. Download results ──────────────────────────────────────────────────
log ""
log "Downloading results from client..."
RESULTS_DIR="$HOME/velocity-bench-2inst-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$RESULTS_DIR"

for f in bench_results.md bench_results.csv bench_results.json; do
    scp $SSH_OPTS "ubuntu@$CLIENT_IP:~/VELOCITY-WorkFlow/$f" "$RESULTS_DIR/$f" 2>/dev/null || true
done

echo ""
log "════════════════════════════════════════════════════════"
log "  2-INSTANCE BENCHMARK COMPLETE                          "
log "════════════════════════════════════════════════════════"
log "  Results: $RESULTS_DIR"
ls -lh "$RESULTS_DIR/" 2>/dev/null || true
echo ""

if [ -f "$RESULTS_DIR/bench_results.md" ]; then
    info "── Summary ──"
    sed -n '/^## Summary/,/^## Detailed/p' "$RESULTS_DIR/bench_results.md" | head -20
    echo ""
fi

# Cleanup runs automatically via trap
