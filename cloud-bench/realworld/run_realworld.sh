#!/usr/bin/env bash
# =============================================================================
# cloud-bench/realworld/run_realworld.sh
#
# Run this ENTIRELY inside AWS CloudShell.
# Launches TWO EC2 instances in the same VPC:
#   - SERVER: m7i-flex.large (2 vCPU, 8GB) — runs Temporal + VELOCITY
#   - CLIENT: t3.micro (1 vCPU, 1GB) — runs benchmark harness over VPC
#
# The benchmark runs over the VPC private network (consistent ~0.5ms latency),
# so the only variable between users is their own internet latency to CloudShell.
#
# Usage:
#   ./cloud-bench/realworld/run_realworld.sh
#   BENCH_PROFILE=stress ./cloud-bench/realworld/run_realworld.sh
# =============================================================================
set -euo pipefail

REGION="${AWS_DEFAULT_REGION:-us-east-1}"
SERVER_TYPE="${BENCH_SERVER_INSTANCE:-m7i-flex.large}"
CLIENT_TYPE="${BENCH_CLIENT_INSTANCE:-t3.micro}"
KEY_NAME="velocity-rw-$$"
SG_NAME="velocity-rw-sg-$$"
PROFILE="${BENCH_PROFILE:-standard}"
WORKLOADS="${BENCH_WORKLOADS:-all}"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
log()  { echo -e "${GREEN}[realworld]${NC} $*"; }
warn() { echo -e "${YELLOW}[realworld]${NC} $*"; }
info() { echo -e "${CYAN}[realworld]${NC} $*"; }

cleanup_on_error() {
    warn "Script interrupted. Resources may still be running:"
    warn "  Server: ${SERVER_ID:-not launched}  Client: ${CLIENT_ID:-not launched}"
    warn "  Key: $KEY_NAME  SG: $SG_NAME"
    warn "Cleanup:"
    warn "  aws ec2 terminate-instances --instance-ids ${SERVER_ID:-dummy} ${CLIENT_ID:-dummy} 2>/dev/null"
    warn "  aws ec2 delete-key-pair --key-name $KEY_NAME"
    warn "  aws ec2 delete-security-group --group-name $SG_NAME"
}
trap cleanup_on_error ERR

log "════════════════════════════════════════════════════════"
log "  VELOCITY Real-World Benchmark (2-Instance)              "
log "════════════════════════════════════════════════════════"
log "  Region:       $REGION"
log "  Server:       $SERVER_TYPE (engines)"
log "  Client:       $CLIENT_TYPE (benchmark harness)"
log "  Profile:      $PROFILE"
log "  Workloads:    $WORKLOADS"
echo ""

# ── Preflight ──────────────────────────────────────────────────────────────
aws sts get-caller-identity --query 'Account' --output text >/dev/null

# ── 1. AMI lookup ─────────────────────────────────────────────────────────
log "[1/10] Looking up Ubuntu 22.04 AMI for $REGION..."
AMI_ID=$(aws ec2 describe-images \
    --owners 099720109477 \
    --filters "Name=name,Values=ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*" \
              "Name=state,Values=available" \
    --query 'sort_by(Images,&CreationDate)[-1].ImageId' \
    --output text --region "$REGION")
log "  AMI: $AMI_ID"

# ── 2. SSH key ────────────────────────────────────────────────────────────
log "[2/10] Creating SSH key pair..."
aws ec2 create-key-pair --key-name "$KEY_NAME" --key-type ed25519 --key-format pem \
    --query 'KeyMaterial' --output text > "$HOME/.ssh/$KEY_NAME.pem" 2>/dev/null
chmod 400 "$HOME/.ssh/$KEY_NAME.pem"
PEM="$HOME/.ssh/$KEY_NAME.pem"

# ── 3. Security group (allow SSH + all internal VPC traffic) ──────────────
log "[3/10] Creating security group..."
MY_IP=$(curl -s https://checkip.amazonaws.com | tr -d '\n')
SG_ID=$(aws ec2 create-security-group \
    --group-name "$SG_NAME" \
    --description "VELOCITY real-world benchmark" \
    --query 'GroupId' --output text)

# SSH from CloudShell
aws ec2 authorize-security-group-ingress \
    --group-id "$SG_ID" --protocol tcp --port 22 --cidr "${MY_IP}/32" \
    --output json > /dev/null

# All traffic within the SG (VPC internal)
aws ec2 authorize-security-group-ingress \
    --group-id "$SG_ID" --protocol tcp --port 0 --cidr "172.16.0.0/12" \
    --output json > /dev/null 2>&1 || true
aws ec2 authorize-security-group-ingress \
    --group-id "$SG_ID" --protocol tcp --port 0 --cidr "10.0.0.0/8" \
    --output json > /dev/null 2>&1 || true

# Allow all ports from same SG (instances talk to each other on all ports)
aws ec2 authorize-security-group-ingress \
    --group-id "$SG_ID" --protocol -1 --port 0 \
    --source-group "$SG_ID" \
    --output json > /dev/null

log "  SG: $SG_ID (SSH from $MY_IP, all VPC internal)"

# ── 4. Launch SERVER instance ─────────────────────────────────────────────
log "[4/10] Launching SERVER instance ($SERVER_TYPE)..."
SERVER_ID=$(aws ec2 run-instances \
    --image-id "$AMI_ID" --instance-type "$SERVER_TYPE" \
    --key-name "$KEY_NAME" --security-group-ids "$SG_ID" \
    --block-device-mappings '[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":30,"VolumeType":"gp3","DeleteOnTermination":true}}]' \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=velocity-rw-server}]" \
    --query 'Instances[0].InstanceId' --output text)
log "  Server: $SERVER_ID"

# ── 5. Launch CLIENT instance ─────────────────────────────────────────────
log "[5/10] Launching CLIENT instance ($CLIENT_TYPE)..."
CLIENT_ID=$(aws ec2 run-instances \
    --image-id "$AMI_ID" --instance-type "$CLIENT_TYPE" \
    --key-name "$KEY_NAME" --security-group-ids "$SG_ID" \
    --block-device-mappings '[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":20,"VolumeType":"gp3","DeleteOnTermination":true}}]' \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=velocity-rw-client}]" \
    --query 'Instances[0].InstanceId' --output text)
log "  Client: $CLIENT_ID"

# ── 6. Wait for both instances ────────────────────────────────────────────
log "[6/10] Waiting for both instances to be running..."
aws ec2 wait instance-running --instance-ids "$SERVER_ID" "$CLIENT_ID"

SERVER_PUB=$(aws ec2 describe-instances --instance-ids "$SERVER_ID" \
    --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
SERVER_PRIV=$(aws ec2 describe-instances --instance-ids "$SERVER_ID" \
    --query 'Reservations[0].Instances[0].PrivateIpAddress' --output text)
CLIENT_PUB=$(aws ec2 describe-instances --instance-ids "$CLIENT_ID" \
    --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)

log "  Server: public=$SERVER_PUB  private=$SERVER_PRIV"
log "  Client: public=$CLIENT_PUB"

# ── 7. Wait for SSH on both ───────────────────────────────────────────────
log "[7/10] Waiting for SSH on both instances..."
SSH_OPTS="-i $PEM -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=5 -o LogLevel=ERROR"

for target in "$SERVER_PUB" "$CLIENT_PUB"; do
    log "  Waiting for $target..."
    for i in $(seq 1 60); do
        if ssh $SSH_OPTS "ubuntu@$target" "echo ok" 2>/dev/null | grep -q ok; then break; fi
        printf "."; sleep 3
    done
    echo ""
done
log "  Both instances ready."

# ── 8. Upload & run server setup ──────────────────────────────────────────
log "[8/10] Setting up SERVER (install deps, build, start engines)..."
log "  This takes ~5-8 minutes (building Rust + pulling Docker images)..."

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"

scp $SSH_OPTS "$SCRIPT_DIR/server_setup.sh" "ubuntu@$SERVER_PUB:~/server_setup.sh"
ssh $SSH_OPTS "ubuntu@$SERVER_PUB" "chmod +x ~/server_setup.sh" &

scp $SSH_OPTS "$SCRIPT_DIR/client_setup.sh" "ubuntu@$CLIENT_PUB:~/client_setup.sh"
ssh $SSH_OPTS "ubuntu@$CLIENT_PUB" "chmod +x ~/client_setup.sh" &

wait

# Start server setup in background (it takes a while)
info "  Running server setup (streamed live)..."
ssh $SSH_OPTS "ubuntu@$SERVER_PUB" \
    "PROFILE=$PROFILE WORKLOADS=$WORKLOADS nohup bash ~/server_setup.sh > ~/setup.log 2>&1 &"

# Wait for server to be ready (check for the "ready" marker in the log)
log "  Waiting for server engines to start..."
for i in $(seq 1 120); do
    READY=$(ssh $SSH_OPTS "ubuntu@$SERVER_PUB" "grep -c 'SERVER INSTANCE READY' ~/setup.log 2>/dev/null || echo 0")
    if [ "$READY" -ge 1 ]; then break; fi
    if [ $((i % 10)) -eq 0 ]; then log "  Still setting up... ($((i*5))s elapsed)"; fi
    sleep 5
done

# Get the actual server private IP from the setup log
ACTUAL_PRIV=$(ssh $SSH_OPTS "ubuntu@$SERVER_PUB" "grep 'Private IP' ~/setup.log | awk '{print \$NF}' | head -1")
if [ -n "$ACTUAL_PRIV" ]; then SERVER_PRIV="$ACTUAL_PRIV"; fi
log "  Server engines ready. Private IP: $SERVER_PRIV"

# ── 9. Run client benchmark ───────────────────────────────────────────────
log "[9/10] Running benchmark from CLIENT → SERVER over VPC..."
log "  This streams live from the client instance."
echo ""
info "════════════════════════════════════════════════════════"
info "  Real-World Benchmark (live output)                     "
info "════════════════════════════════════════════════════════"

ssh $SSH_OPTS "ubuntu@$CLIENT_PUB" \
    "PROFILE=$PROFILE WORKLOADS=$WORKLOADS bash ~/client_setup.sh $SERVER_PRIV" 2>&1

# ── 10. Download results ──────────────────────────────────────────────────
log "[10/10] Downloading results from client..."
RESULTS_DIR="$HOME/velocity-rw-results-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$RESULTS_DIR"

for f in bench_results.md bench_results.csv bench_results.json; do
    scp $SSH_OPTS "ubuntu@$CLIENT_PUB:~/V.E.L.O.C.I.T.Y.-WorkFlow/$f" "$RESULTS_DIR/$f" 2>/dev/null || true
done

echo ""
log "════════════════════════════════════════════════════════"
log "  REAL-WORLD BENCHMARK COMPLETE                          "
log "════════════════════════════════════════════════════════"
echo ""
log "Results: $RESULTS_DIR"
ls -lh "$RESULTS_DIR/" 2>/dev/null
echo ""

if [ -f "$RESULTS_DIR/bench_results.md" ]; then
    info "── Summary ──"
    head -20 "$RESULTS_DIR/bench_results.md" | grep -E '^\|' || true
    echo ""
fi

log "════════════════════════════════════════════════════════"
log "  INSTANCES STILL RUNNING (terminate when done)           "
log "════════════════════════════════════════════════════════"
echo ""
log "  Server:  ssh -i $PEM ubuntu@$SERVER_PUB"
log "  Client:  ssh -i $PEM ubuntu@$CLIENT_PUB"
log ""
log "  TERMINATE when done:"
log "    aws ec2 terminate-instances --instance-ids $SERVER_ID $CLIENT_ID"
log "    aws ec2 delete-key-pair --key-name $KEY_NAME"
log "    aws ec2 delete-security-group --group-name $SG_NAME"

# Save connection info
cat > "$RESULTS_DIR/connection-info.txt" <<EOF
=== Real-World Benchmark Infrastructure ===
Server Instance:  $SERVER_ID  (public: $SERVER_PUB, private: $SERVER_PRIV)
Client Instance:  $CLIENT_ID  (public: $CLIENT_PUB)
Key Pair:         $KEY_NAME
PEM:              $PEM
Security Group:   $SG_ID
Region:           $REGION

SSH:
  ssh -i $PEM ubuntu@$SERVER_PUB   # server
  ssh -i $PEM ubuntu@$CLIENT_PUB   # client

Terminate:
  aws ec2 terminate-instances --instance-ids $SERVER_ID $CLIENT_ID
  aws ec2 delete-key-pair --key-name $KEY_NAME
  aws ec2 delete-security-group --group-name $SG_NAME
EOF

log "Connection info: $RESULTS_DIR/connection-info.txt"
