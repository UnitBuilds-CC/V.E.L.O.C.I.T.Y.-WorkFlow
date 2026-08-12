$project = "velocity-live-test-001"
$zone = "us-east1-b"
$vmName = "velocity-classic"
$ip = "34.26.15.38"
$gcloud = "C:\Program Files (x86)\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd"

# Accept the SSH key first
Write-Host "Accepting SSH host key..." -ForegroundColor Cyan
echo "y" | & $gcloud compute ssh $vmName --zone=$zone --command="echo connected" --quiet 2>&1

# Now run the setup script
Write-Host "Running setup..." -ForegroundColor Cyan

$setupCmds = @'
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
mkdir -p /home/velocity-workflow
cd /home/velocity-workflow

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

$tempFile = [System.IO.Path]::GetTempFileName()
Set-Content -Path $tempFile -Value $setupCmds -NoNewline

Get-Content $tempFile | & $gcloud compute ssh $vmName --zone=$zone --command="bash -s" --quiet 2>&1

Remove-Item $tempFile -Force -ErrorAction SilentlyContinue

Write-Host ""
Write-Host "=== Deployment Complete ===" -ForegroundColor Green
Write-Host ""
Write-Host "Services at http://${ip}:"
Write-Host "  PostgreSQL:  ${ip}:5432"
Write-Host "  Prometheus:  http://${ip}:9090"
Write-Host "  Grafana:     http://${ip}:3000 (admin/admin)"
