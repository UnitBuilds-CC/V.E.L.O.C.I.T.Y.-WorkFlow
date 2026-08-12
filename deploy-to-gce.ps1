# Velocity Live Test Environment — Google Cloud Deployment
# Prerequisites: gcloud CLI installed and authenticated
# Usage: .\deploy-to-gce.ps1

param(
    [string]$ProjectId = "velocity-test",
    [string]$Zone = "us-central1-a",
    [string]$MachineType = "e2-standard-4",
    [string]$ImageFamily = "ubuntu-2404-lts-amd64"
)

$ErrorActionPreference = "Stop"

Write-Host "=== Velocity Live Test Deployment ===" -ForegroundColor Cyan
Write-Host "Project: $ProjectId"
Write-Host "Zone: $Zone"
Write-Host ""

# Set project
Write-Host "Setting active project..."
gcloud config set project $ProjectId

# Create VM
Write-Host "Creating GCE VM..."
$vmName = "velocity-test-$(Get-Date -Format 'yyyyMMdd-HHmmss')"
gcloud compute instances create $vmName `
    --zone=$Zone `
    --machine-type=$MachineType `
    --image-family=$ImageFamily `
    --image-project=ubuntu-os-cloud `
    --boot-disk-size=50GB `
    --tags=http-server,https-server `
    --scopes=cloud-platform `
    --metadata=startup-script="#!/bin/bash
apt-get update
apt-get install -y docker.io docker-compose-v2 git
systemctl enable docker
systemctl start docker
usermod -aG docker ubuntu
echo 'Docker installed successfully'"

if ($LASTEXITCODE -ne 0) {
    Write-Error "Failed to create VM"
    exit 1
}

Write-Host "VM created: $vmName" -ForegroundColor Green

# Create firewall rules
Write-Host "Creating firewall rules..."
gcloud compute firewall-rules create velocity-ports `
    --allow=tcp:5000,tcp:50051,tcp:3000,tcp:9090,tcp:8080 `
    --target-tags=http-server `
    --description="Velocity service ports" 2>$null

if ($LASTEXITCODE -ne 0) {
    Write-Host "Firewall rule may already exist, continuing..." -ForegroundColor Yellow
}

# Wait for VM to be ready
Write-Host "Waiting for VM to be ready..."
Start-Sleep -Seconds 30

# Get external IP
$ip = gcloud compute instances describe $vmName --zone=$Zone --format="value(networkInterfaces[0].accessConfigs[0].natIP)"
Write-Host "VM external IP: $ip" -ForegroundColor Green

# SSH and deploy Velocity
Write-Host "Deploying Velocity to VM..."
$sshCommands = @"
cd /home/ubuntu
git clone https://github.com/YOUR_USERNAME/Velocity-workflow.git velocity-workflow || echo 'Repo clone failed - you may need to push first'
cd velocity-workflow
docker compose up -d --build
echo 'Deployment complete!'
"@

# Write SSH commands to temp file
$tempFile = [System.IO.Path]::GetTempFileName()
Set-Content -Path $tempFile -Value $sshCommands

# Execute via SSH
gcloud compute ssh $vmName --zone=$Zone --command="bash -s" < $tempFile

Remove-Item $tempFile -Force

Write-Host ""
Write-Host "=== Deployment Complete ===" -ForegroundColor Green
Write-Host ""
Write-Host "Services available at:"
Write-Host "  Velocity HTTP:  http://$ip`:5000"
Write-Host "  Velocity gRPC:  http://$ip`:50051"
Write-Host "  Grafana:        http://$ip`:3000 (admin/admin)"
Write-Host "  Prometheus:     http://$ip`:9090"
Write-Host "  Web UI:         http://$ip`:8080"
Write-Host ""
Write-Host "To SSH into the VM:"
Write-Host "  gcloud compute ssh $vmName --zone=$Zone"
Write-Host ""
Write-Host "To stop the VM when done:"
Write-Host "  gcloud compute instances stop $vmName --zone=$Zone"
Write-Host ""
Write-Host "To delete everything:"
Write-Host "  gcloud compute instances delete $vmName --zone=$Zone"
