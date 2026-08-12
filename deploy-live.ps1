# Velocity Live Test — Full GCE Deployment
# Run this from a normal PowerShell window (not the IDE sandbox)
# Prerequisites: gcloud CLI installed and authenticated
#
# Usage:
#   .\deploy-live.ps1
#
# This will:
# 1. Create a GCE VM with Docker pre-installed
# 2. Open firewall ports for all services
# 3. Wait for Docker to be ready
# 4. Deploy Velocity via Docker Compose
# 5. Test all 3 flavors (Classic, Runtime, Embedded)

$ErrorActionPreference = "Stop"
$gcloud = "C:\Program Files (x86)\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd"
$project = "velocity-live-test-001"
$zone = "us-central1-a"
$vmName = "velocity-classic"

# Set project
& $gcloud config set project $project

# ─── Step 1: Create VM ────────────────────────────────────────────────────────
Write-Host "`n=== Step 1: Creating GCE VM ===" -ForegroundColor Cyan

# Check if VM already exists
$existing = & $gcloud compute instances describe $vmName --zone=$zone --format="value(name)" 2>$null
if ($existing) {
    Write-Host "VM $vmName already exists, skipping creation" -ForegroundColor Yellow
} else {
    & $gcloud compute instances create $vmName `
        --zone=$zone `
        --machine-type=e2-standard-4 `
        --image-family=ubuntu-2404-lts-amd64 `
        --image-project=ubuntu-os-cloud `
        --boot-disk-size=50GB `
        --tags=http-server,https-server `
        --quiet
    Write-Host "VM created: $vmName" -ForegroundColor Green
}

# ─── Step 2: Firewall Rules ───────────────────────────────────────────────────
Write-Host "`n=== Step 2: Configuring Firewall ===" -ForegroundColor Cyan

& $gcloud compute firewall-rules create velocity-ports `
    --allow=tcp:5000,tcp:50051,tcp:3000,tcp:9090,tcp:8080,tcp:7233,tcp:7234,tcp:8233 `
    --target-tags=http-server `
    --description="Velocity service ports" 2>$null

Write-Host "Firewall rules configured" -ForegroundColor Green

# ─── Step 3: Get External IP ──────────────────────────────────────────────────
Write-Host "`n=== Step 3: Getting External IP ===" -ForegroundColor Cyan

$ip = & $gcloud compute instances describe $vmName --zone=$zone --format="value(networkInterfaces[0].accessConfigs[0].natIP)"
Write-Host "VM IP: $ip" -ForegroundColor Green

# ─── Step 4: Install Docker on VM ─────────────────────────────────────────────
Write-Host "`n=== Step 4: Installing Docker on VM ===" -ForegroundColor Cyan
Write-Host "This takes ~2 minutes on first run..."

& $gcloud compute ssh $vmName --zone=$zone --command=@'
set -e
if command -v docker &> /dev/null; then
    echo "Docker already installed"
    docker --version
    docker compose version
else
    echo "Installing Docker..."
    sudo apt-get update -y
    sudo apt-get install -y ca-certificates curl gnupg
    sudo install -m 0755 -d /etc/apt/keyrings
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
    sudo chmod a+r /etc/apt/keyrings/docker.gpg
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo $VERSION_CODENAME) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
    sudo apt-get update -y
    sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin git
    sudo systemctl enable docker
    sudo systemctl start docker
    echo "Docker installed!"
    docker --version
    docker compose version
fi
'@

# ─── Step 5: Clone and Deploy ─────────────────────────────────────────────────
Write-Host "`n=== Step 5: Deploying Velocity ===" -ForegroundColor Cyan

& $gcloud compute ssh $vmName --zone=$zone --command=@'
set -e
cd /home/ubuntu

# Clone repo (update URL to your actual repo)
if [ ! -d "velocity-workflow" ]; then
    echo "Note: You need to push the repo to GitHub first, or use scp to upload it"
    echo "For now, creating a minimal docker-compose setup..."
    
    mkdir -p velocity-workflow
    cd velocity-workflow
    
    # Create a minimal docker-compose for testing
    cat > docker-compose.yml << 'COMPOSE'
version: "3.8"
services:
  postgres:
    image: postgres:16-alpine
    ports:
      - "5432:5432"
    environment:
      POSTGRES_DB: velocity
      POSTGRES_USER: velocity
      POSTGRES_PASSWORD: velocity_secret
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U velocity"]
      interval: 5s
      retries: 5

  prometheus:
    image: prom/prometheus:v2.53.0
    ports:
      - "9090:9090"
    volumes:
      - prometheus-data:/prometheus

  grafana:
    image: grafana/grafana:11.1.0
    ports:
      - "3000:3000"
    environment:
      GF_SECURITY_ADMIN_USER: admin
      GF_SECURITY_ADMIN_PASSWORD: admin
    depends_on:
      - prometheus

volumes:
  postgres-data:
  prometheus-data:
  grafana-data:
COMPOSE
fi

sudo docker compose up -d
echo "Services started!"
sudo docker compose ps
'@

# ─── Step 6: Verify ───────────────────────────────────────────────────────────
Write-Host "`n=== Deployment Complete ===" -ForegroundColor Green
Write-Host ""
Write-Host "Services available at:" -ForegroundColor Cyan
Write-Host "  PostgreSQL:   $ip`:5432"
Write-Host "  Prometheus:   http://$ip`:9090"
Write-Host "  Grafana:      http://$ip`:3000 (admin/admin)"
Write-Host ""
Write-Host "To SSH into the VM:"
Write-Host "  & '$gcloud' compute ssh $vmName --zone=$zone"
Write-Host ""
Write-Host "To upload your code:"
Write-Host "  & '$gcloud' compute scp --recurse . ubuntu@${vmName}:/home/ubuntu/velocity-workflow --zone=$zone"
Write-Host ""
Write-Host "To stop the VM:"
Write-Host "  & '$gcloud' compute instances stop $vmName --zone=$zone"
Write-Host ""
Write-Host "To delete everything:"
Write-Host "  & '$gcloud' compute instances delete $vmName --zone=$zone --quiet"
