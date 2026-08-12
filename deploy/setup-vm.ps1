$token = (& 'C:\Program Files (x86)\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd' auth print-access-token 2>&1 | Where-Object { $_ -notmatch "^WARNING" } | Select-Object -Last 1).Trim()
$project = "velocity-live-test-001"
$zone = "us-east1-b"
$vmName = "velocity-classic"
$ip = "34.26.15.38"

Write-Host "=== Installing Docker on $vmName ($ip) ===" -ForegroundColor Cyan

# Use OS Login to SSH via the API
# First, enable OS Login on the project
$osLoginUri = "https://compute.googleapis.com/compute/v1/projects/$project/setCommonInstanceMetadata"
$headers = @{ Authorization = "Bearer $token"; "Content-Type" = "application/json" }

# Get current metadata
$projUri = "https://compute.googleapis.com/compute/v1/projects/$project"
$proj = Invoke-RestMethod -Uri $projUri -Method Get -Headers $headers
$existingItems = @($proj.commonInstanceMetadata.items)
$fingerprint = $proj.commonInstanceMetadata.fingerprint

# Enable OS Login
$newItems = @($existingItems | Where-Object { $_.key -ne "enable-oslogin" })
$newItems += @{ key = "enable-oslogin"; value = "TRUE" }

$metaBody = @{
    fingerprint = $fingerprint
    items = $newItems
} | ConvertTo-Json -Depth 5

try {
    Invoke-RestMethod -Uri $osLoginUri -Method Post -Headers $headers -Body $metaBody | Out-Null
    Write-Host "OS Login enabled" -ForegroundColor Green
} catch {
    Write-Host "OS Login may already be enabled" -ForegroundColor Yellow
}

# SSH commands to install Docker and deploy
$sshScript = @'
#!/bin/bash
set -e
echo "=== Checking Docker ==="
if command -v docker &> /dev/null; then
    echo "Docker already installed: $(docker --version)"
else
    echo "Installing Docker..."
    apt-get update -y
    apt-get install -y ca-certificates curl gnupg git
    install -m 0755 -d /etc/apt/keyrings
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
    chmod a+r /etc/apt/keyrings/docker.gpg
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo $VERSION_CODENAME) stable" > /etc/apt/sources.list.d/docker.list
    apt-get update -y
    apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
    systemctl enable docker
    systemctl start docker
    echo "Docker installed: $(docker --version)"
fi

echo ""
echo "=== Setting up Velocity ==="
cd /home
mkdir -p velocity-workflow
cd velocity-workflow

# Create docker-compose.yml
cat > docker-compose.yml << 'COMPOSE_EOF'
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
    restart: unless-stopped

  prometheus:
    image: prom/prometheus:v2.53.0
    ports:
      - "9090:9090"
    volumes:
      - prometheus-data:/prometheus
    restart: unless-stopped

  grafana:
    image: grafana/grafana:11.1.0
    ports:
      - "3000:3000"
    environment:
      GF_SECURITY_ADMIN_USER: admin
      GF_SECURITY_ADMIN_PASSWORD: admin
    depends_on:
      - prometheus
    restart: unless-stopped

volumes:
  postgres-data:
  prometheus-data:
  grafana-data:
COMPOSE_EOF

echo "Starting services..."
docker compose up -d
echo ""
echo "=== Service Status ==="
docker compose ps
echo ""
echo "=== All Services Running ==="
'@

# Write SSH script to temp file and execute via gcloud
$tempScript = [System.IO.Path]::GetTempFileName()
Set-Content -Path $tempScript -Value $sshScript -NoNewline

Write-Host "SSH into VM to install Docker and deploy..." -ForegroundColor Cyan
Get-Content $tempScript | & 'C:\Program Files (x86)\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd' compute ssh $vmName --zone=$zone --command="bash -s" 2>&1

Remove-Item $tempScript -Force -ErrorAction SilentlyContinue

Write-Host ""
Write-Host "=== Deployment Complete ===" -ForegroundColor Green
Write-Host ""
Write-Host "Services at:"
Write-Host "  PostgreSQL:  $ip`:5432"
Write-Host "  Prometheus:  http://$ip`:9090"
Write-Host "  Grafana:     http://$ip`:3000 (admin/admin)"
Write-Host ""
Write-Host "Next steps:"
Write-Host "  1. Upload your code: gcloud compute scp --recurse . ubuntu@${ip}:/home/velocity-workflow --zone=$zone"
Write-Host "  2. SSH in: gcloud compute ssh $vmName --zone=$zone"
