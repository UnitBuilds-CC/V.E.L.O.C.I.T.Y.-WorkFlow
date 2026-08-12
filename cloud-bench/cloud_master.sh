#!/usr/bin/env bash
# =============================================================================
# cloud-bench/cloud_master.sh
#
# Run this ENTIRELY inside AWS CloudShell (https://console.aws.amazon.com/cloudshell)
# It provisions an EC2 t3.medium, deploys everything, runs the full benchmark,
# and downloads results — all automatically.
#
# Usage (in CloudShell):
#   curl -sO https://raw.githubusercontent.com/UnitBuilds-CC/V.E.L.O.C.I.T.Y.-WorkFlow/main/cloud-bench/cloud_master.sh
#   chmod +x cloud_master.sh
#   ./cloud_master.sh
#
# Or paste directly into the CloudShell terminal.
#
# Prerequisites: AWS CLI v2 (pre-installed in CloudShell).
# Cost: ~$0.05/hour for a t3.medium. Remember to terminate when done.
# =============================================================================
set -euo pipefail

# ── Configuration ────────────────────────────────────────────────────────────
REGION="${AWS_DEFAULT_REGION:-us-east-1}"
INSTANCE_TYPE="${BENCH_INSTANCE:-t3.medium}"
KEY_NAME="velocity-bench-$$"
SG_NAME="velocity-bench-sg-$$"
PROFILE="${BENCH_PROFILE:-standard}"  # quick | standard | stress
WORKLOADS="${BENCH_WORKLOADS:-all}"   # smoke | all

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
log()  { echo -e "${GREEN}[cloud]${NC} $*"; }
warn() { echo -e "${YELLOW}[cloud]${NC} $*"; }
info() { echo -e "${CYAN}[cloud]${NC} $*"; }

cleanup_on_error() {
    warn "Script interrupted. Resources may still be running:"
    warn "  Instance: ${INSTANCE_ID:-not yet launched}"
    warn "  Key pair: $KEY_NAME"
    warn "  SG:       $SG_NAME"
    warn "Terminate manually:"
    warn "  aws ec2 terminate-instances --instance-ids <id>"
    warn "  aws ec2 delete-key-pair --key-name $KEY_NAME"
    warn "  aws ec2 delete-security-group --group-name $SG_NAME"
}
trap cleanup_on_error ERR

# ── 0. Preflight checks ────────────────────────────────────────────────────
log "════════════════════════════════════════════════════════"
log "  VELOCITY Cloud Benchmark — AWS CloudShell              "
log "════════════════════════════════════════════════════════"
echo ""

if ! command -v aws &>/dev/null; then
    echo "ERROR: AWS CLI not found. This script must run in AWS CloudShell." >&2
    exit 1
fi

# Look up latest Ubuntu 22.04 AMI for current region (Canonical official)
log "Looking up Ubuntu 22.04 AMI for region $REGION..."
AMI_ID=$(aws ec2 describe-images \
    --owners 099720109477 \
    --filters "Name=name,Values=ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*" \
              "Name=state,Values=available" \
    --query 'sort_by(Images,&CreationDate)[-1].ImageId' \
    --output text \
    --region "$REGION")

if [ -z "$AMI_ID" ] || [ "$AMI_ID" = "None" ]; then
    echo "ERROR: Could not find Ubuntu 22.04 AMI in $REGION" >&2
    exit 1
fi

log "Region:        $REGION"
log "Instance type: $INSTANCE_TYPE"
log "AMI:           $AMI_ID (auto-detected)"
log "Profile:       $PROFILE"
log "Workloads:     $WORKLOADS"
echo ""

# Verify AWS credentials work
aws sts get-caller-identity --query 'Account' --output text >/dev/null || {
    echo "ERROR: AWS credentials not configured." >&2; exit 1
}

# ── 1. Create SSH key pair ──────────────────────────────────────────────────
log "[1/8] Creating SSH key pair ($KEY_NAME)..."
aws ec2 create-key-pair \
    --key-name "$KEY_NAME" \
    --key-type ed25519 \
    --key-format pem \
    --query 'KeyMaterial' \
    --output text > "$HOME/.ssh/$KEY_NAME.pem" 2>/dev/null
chmod 400 "$HOME/.ssh/$KEY_NAME.pem"
PEM="$HOME/.ssh/$KEY_NAME.pem"
log "  Key saved to $PEM"

# ── 2. Create security group ────────────────────────────────────────────────
log "[2/8] Creating security group ($SG_NAME)..."
MY_IP=$(curl -s https://checkip.amazonaws.com | tr -d '\n')
log "  Your public IP: $MY_IP"

SG_ID=$(aws ec2 create-security-group \
    --group-name "$SG_NAME" \
    --description "VELOCITY benchmark SSH access" \
    --query 'GroupId' \
    --output text)

aws ec2 authorize-security-group-ingress \
    --group-id "$SG_ID" \
    --protocol tcp \
    --port 22 \
    --cidr "${MY_IP}/32" \
    --output json > /dev/null

log "  SG created: $SG_ID (SSH from $MY_IP only)"

# ── 3. Launch EC2 instance ──────────────────────────────────────────────────
log "[3/8] Launching $INSTANCE_TYPE EC2 instance..."
INSTANCE_ID=$(aws ec2 run-instances \
    --image-id "$AMI_ID" \
    --instance-type "$INSTANCE_TYPE" \
    --key-name "$KEY_NAME" \
    --security-group-ids "$SG_ID" \
    --block-device-mappings '[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":30,"VolumeType":"gp3","DeleteOnTermination":true}}]' \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=velocity-bench},{Key=purpose,Value=benchmark}]" \
    --query 'Instances[0].InstanceId' \
    --output text)

log "  Instance: $INSTANCE_ID"

# ── 4. Wait for running + public IP ────────────────────────────────────────
log "[4/8] Waiting for instance to start running..."
aws ec2 wait instance-running --instance-ids "$INSTANCE_ID"
PUBLIC_IP=$(aws ec2 describe-instances \
    --instance-ids "$INSTANCE_ID" \
    --query 'Reservations[0].Instances[0].PublicIpAddress' \
    --output text)
log "  Instance running at: $PUBLIC_IP"

# ── 5. Wait for SSH ────────────────────────────────────────────────────────
log "[5/8] Waiting for SSH to become available..."
SSH_OPTS="-i $PEM -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=5 -o LogLevel=ERROR"
for i in $(seq 1 60); do
    if ssh $SSH_OPTS "ubuntu@$PUBLIC_IP" "echo ok" 2>/dev/null | grep -q ok; then
        break
    fi
    printf "."
    sleep 3
done
echo ""
log "  SSH is ready."

# ── 6. Upload remote benchmark script ──────────────────────────────────────
log "[6/8] Uploading benchmark script to instance..."
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Try to find cloud_ec2_bench.sh next to this script
REMOTE_SCRIPT=""
for candidate in \
    "$SCRIPT_DIR/cloud_ec2_bench.sh" \
    "$HOME/VELOCITY-WorkFlow/cloud-bench/cloud_ec2_bench.sh" \
    "$HOME/cloud_ec2_bench.sh"; do
    if [ -f "$candidate" ]; then
        REMOTE_SCRIPT="$candidate"
        break
    fi
done

if [ -z "$REMOTE_SCRIPT" ]; then
    log "  Script not found locally — downloading from GitHub..."
    curl -sSfL "https://raw.githubusercontent.com/UnitBuilds-CC/V.E.L.O.C.I.T.Y.-WorkFlow/main/cloud-bench/cloud_ec2_bench.sh" \
        -o /tmp/cloud_ec2_bench.sh
    REMOTE_SCRIPT=/tmp/cloud_ec2_bench.sh
fi

scp $SSH_OPTS "$REMOTE_SCRIPT" "ubuntu@$PUBLIC_IP:~/cloud_ec2_bench.sh"
log "  Uploaded."

# ── 7. Run benchmark on instance ───────────────────────────────────────────
log "[7/8] Running benchmark on EC2 instance..."
log "  This will take 15-45 minutes depending on profile."
log "  Progress is streamed live below."
echo ""
info "════════════════════════════════════════════════════════"
info "  Remote benchmark output (live)                        "
info "════════════════════════════════════════════════════════"

ssh $SSH_OPTS "ubuntu@$PUBLIC_IP" \
    "chmod +x ~/cloud_ec2_bench.sh && PROFILE=$PROFILE WORKLOADS=$WORKLOADS bash ~/cloud_ec2_bench.sh" \
    2>&1

info "════════════════════════════════════════════════════════"
info "  Remote benchmark complete                             "
info "════════════════════════════════════════════════════════"
echo ""

# ── 8. Download results ────────────────────────────────────────────────────
log "[8/8] Downloading results..."
RESULTS_DIR="$HOME/velocity-bench-results-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$RESULTS_DIR"

for f in bench_results.md bench_results.csv bench_results.json; do
    scp $SSH_OPTS "ubuntu@$PUBLIC_IP:~/VELOCITY-WorkFlow/$f" "$RESULTS_DIR/$f" 2>/dev/null || true
done

echo ""
log "════════════════════════════════════════════════════════"
log "  DONE!                                                  "
log "════════════════════════════════════════════════════════"
echo ""
log "Results saved to: $RESULTS_DIR"
ls -lh "$RESULTS_DIR/" 2>/dev/null
echo ""

# Print summary from markdown
if [ -f "$RESULTS_DIR/bench_results.md" ]; then
    info "── Summary ──"
    head -20 "$RESULTS_DIR/bench_results.md" | grep -E '^\|' || true
    echo ""
fi

log "════════════════════════════════════════════════════════"
log "  Instance is STILL RUNNING (for manual inspection)      "
log "════════════════════════════════════════════════════════"
echo ""
log "  SSH in manually:    ssh -i $PEM ubuntu@$PUBLIC_IP"
log "  Temporal Web UI:    http://$PUBLIC_IP:8233  (need to open SG port)"
log ""
log "  When done, TERMINATE to stop billing:"
log "    aws ec2 terminate-instances --instance-ids $INSTANCE_ID"
log "    aws ec2 delete-key-pair --key-name $KEY_NAME"
log "    aws ec2 delete-security-group --group-name $SG_NAME"
echo ""

# Save connection info for later reference
cat > "$RESULTS_DIR/connection-info.txt" <<EOF
Instance ID:  $INSTANCE_ID
Public IP:    $PUBLIC_IP
Key pair:     $KEY_NAME
PEM file:     $PEM
Security GrP: $SG_ID
Region:       $REGION

SSH:  ssh -i $PEM ubuntu@$PUBLIC_IP

Terminate:
  aws ec2 terminate-instances --instance-ids $INSTANCE_ID
  aws ec2 delete-key-pair --key-name $KEY_NAME
  aws ec2 delete-security-group --group-name $SG_NAME
EOF

log "Connection info saved to: $RESULTS_DIR/connection-info.txt"
