#!/usr/bin/env bash
# =============================================================================
# cloud-bench/cloud_master.sh — ONE SCRIPT DOES EVERYTHING
#
# Run in AWS CloudShell. It will:
#   1. Launch EC2 instance
#   2. Install deps, build, start engines
#   3. Run benchmark
#   4. Download results
#   5. Terminate instance & clean up ALL resources
#
# Usage:
#   chmod +x cloud_master.sh && ./cloud_master.sh
#
# Optional env vars:
#   BENCH_INSTANCE=m7i-flex.large  (default: t3.medium)
#   BENCH_PROFILE=standard         (quick | standard | stress)
# =============================================================================
set -euo pipefail

REGION="${AWS_DEFAULT_REGION:-us-east-1}"
INSTANCE_TYPE="${BENCH_INSTANCE:-t3.medium}"
PROFILE="${BENCH_PROFILE:-standard}"
KEY_NAME="velocity-bench-$$"
SG_NAME="velocity-bench-sg-$$"
INSTANCE_ID=""
SG_ID=""
PEM=""

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; RED='\033[0;31m'; NC='\033[0m'
log()  { echo -e "${GREEN}[bench]${NC} $*"; }
warn() { echo -e "${YELLOW}[bench]${NC} $*"; }
info() { echo -e "${CYAN}[bench]${NC} $*"; }
err()  { echo -e "${RED}[bench]${NC} $*"; }

# ── Cleanup: ALWAYS runs (success or failure) ──────────────────────────────
cleanup() {
    echo ""
    log "════════════════════════════════════════════════════════"
    log "  Cleaning up AWS resources..."
    log "════════════════════════════════════════════════════════"

    if [ -n "$INSTANCE_ID" ]; then
        log "  Terminating instance $INSTANCE_ID..."
        aws ec2 terminate-instances --instance-ids "$INSTANCE_ID" --output text >/dev/null 2>&1 || true
    fi
    if [ -n "$KEY_NAME" ]; then
        log "  Deleting key pair $KEY_NAME..."
        aws ec2 delete-key-pair --key-name "$KEY_NAME" >/dev/null 2>&1 || true
    fi
    if [ -n "$SG_ID" ]; then
        # Wait for instance to fully terminate before deleting SG
        if [ -n "$INSTANCE_ID" ]; then
            log "  Waiting for instance to terminate..."
            aws ec2 wait instance-terminated --instance-ids "$INSTANCE_ID" 2>/dev/null || true
        fi
        log "  Deleting security group $SG_NAME..."
        aws ec2 delete-security-group --group-name "$SG_NAME" >/dev/null 2>&1 || true
    fi

    log "  Cleanup complete."
}
trap cleanup EXIT

# ── 0. Preflight ───────────────────────────────────────────────────────────
log "════════════════════════════════════════════════════════"
log "  VELOCITY Cloud Benchmark — One-Shot                    "
log "════════════════════════════════════════════════════════"
log "  Region:       $REGION"
log "  Instance:     $INSTANCE_TYPE"
log "  Profile:      $PROFILE"
echo ""

aws sts get-caller-identity --query 'Account' --output text >/dev/null 2>&1 || {
    err "AWS CLI not configured. Run 'aws configure' first."
    exit 1
}

# ── 1. Look up AMI ────────────────────────────────────────────────────────
log "[1/7] Looking up Ubuntu 22.04 AMI for $REGION..."
AMI_ID=$(aws ec2 describe-images \
    --owners 099720109477 \
    --filters "Name=name,Values=ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*" \
              "Name=state,Values=available" \
    --query 'sort_by(Images,&CreationDate)[-1].ImageId' \
    --output text --region "$REGION")

if [ -z "$AMI_ID" ] || [ "$AMI_ID" = "None" ]; then
    err "Could not find Ubuntu 22.04 AMI in $REGION"
    exit 1
fi
log "  AMI: $AMI_ID"

# ── 2. Create SSH key ─────────────────────────────────────────────────────
log "[2/7] Creating SSH key pair..."
aws ec2 create-key-pair --key-name "$KEY_NAME" --key-type ed25519 --key-format pem \
    --query 'KeyMaterial' --output text > "$HOME/.ssh/$KEY_NAME.pem" 2>/dev/null
PEM="$HOME/.ssh/$KEY_NAME.pem"
chmod 400 "$PEM"
log "  Key: $KEY_NAME"

# ── 3. Create security group ──────────────────────────────────────────────
log "[3/7] Creating security group..."
MY_IP=$(curl -s https://checkip.amazonaws.com | tr -d '\n')
SG_ID=$(aws ec2 create-security-group \
    --group-name "$SG_NAME" \
    --description "VELOCITY benchmark" \
    --query 'GroupId' --output text)

aws ec2 authorize-security-group-ingress \
    --group-id "$SG_ID" --protocol tcp --port 22 --cidr "${MY_IP}/32" \
    --output json >/dev/null
log "  SG: $SG_ID (SSH from $MY_IP)"

# ── 4. Launch instance ────────────────────────────────────────────────────
log "[4/7] Launching $INSTANCE_TYPE..."
INSTANCE_ID=$(aws ec2 run-instances \
    --image-id "$AMI_ID" --instance-type "$INSTANCE_TYPE" \
    --key-name "$KEY_NAME" --security-group-ids "$SG_ID" \
    --block-device-mappings '[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":30,"VolumeType":"gp3","DeleteOnTermination":true}}]' \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=velocity-bench}]" \
    --query 'Instances[0].InstanceId' --output text)
log "  Instance: $INSTANCE_ID"

# ── 5. Wait for running + SSH ─────────────────────────────────────────────
log "[5/7] Waiting for instance to start..."
aws ec2 wait instance-running --instance-ids "$INSTANCE_ID"
PUBLIC_IP=$(aws ec2 describe-instances --instance-ids "$INSTANCE_ID" \
    --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
log "  Running at: $PUBLIC_IP"

SSH_OPTS="-i $PEM -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=5 -o LogLevel=ERROR"
log "  Waiting for SSH..."
for i in $(seq 1 60); do
    if ssh $SSH_OPTS "ubuntu@$PUBLIC_IP" "echo ok" 2>/dev/null | grep -q ok; then break; fi
    printf "."; sleep 3
done
echo ""
log "  SSH ready."

# ── 6. Upload repo + scripts ──────────────────────────────────────────────
log "[6/7] Uploading repository and scripts..."
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"

# Tar repo (exclude heavy dirs) and upload
cd "$REPO_ROOT"
log "  Tarring from: $(pwd)"
tar czf /tmp/velocity-repo.tar.gz \
    --exclude='.git' --exclude='target' --exclude='node_modules' --exclude='*.log' .
log "  Tarball: $(ls -lh /tmp/velocity-repo.tar.gz | awk '{print $5}')"
log "  Contains Cargo.toml: $(tar tzf /tmp/velocity-repo.tar.gz | grep -c 'Cargo.toml') files"
scp $SSH_OPTS /tmp/velocity-repo.tar.gz "ubuntu@$PUBLIC_IP:~/velocity-repo.tar.gz"
rm -f /tmp/velocity-repo.tar.gz

# Upload the EC2 benchmark script
scp $SSH_OPTS "$SCRIPT_DIR/cloud_ec2_bench.sh" "ubuntu@$PUBLIC_IP:~/cloud_ec2_bench.sh"
log "  Uploaded."

# ── 7. Run benchmark ──────────────────────────────────────────────────────
log "[7/7] Running benchmark on EC2 (this takes 10-30 minutes)..."
log "  Streaming live output:"
echo ""
info "════════════════════════════════════════════════════════"
info "  Remote benchmark output (live)                         "
info "════════════════════════════════════════════════════════"

ssh $SSH_OPTS "ubuntu@$PUBLIC_IP" \
    "chmod +x ~/cloud_ec2_bench.sh && PROFILE=$PROFILE bash ~/cloud_ec2_bench.sh" 2>&1 || true

# ── 8. Download results ───────────────────────────────────────────────────
log ""
log "Downloading results..."
RESULTS_DIR="$HOME/velocity-bench-results-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$RESULTS_DIR"

for f in bench_results.md bench_results.csv bench_results.json; do
    scp $SSH_OPTS "ubuntu@$PUBLIC_IP:~/VELOCITY-WorkFlow/$f" "$RESULTS_DIR/$f" 2>/dev/null || true
done

echo ""
log "════════════════════════════════════════════════════════"
log "  BENCHMARK COMPLETE                                     "
log "════════════════════════════════════════════════════════"
log "  Results: $RESULTS_DIR"
ls -lh "$RESULTS_DIR/" 2>/dev/null || true
echo ""

if [ -f "$RESULTS_DIR/bench_results.md" ]; then
    info "── Summary ──"
    sed -n '/^## Summary/,/^## Detailed/p' "$RESULTS_DIR/bench_results.md" | head -15
    echo ""
fi

# Cleanup runs automatically via trap
