$Zone = "us-east1-b"
$VMs = @("velocity-classic", "velocity-runtime", "velocity-embedded", "dbos-bench", "restate-bench", "temporal-bench")

foreach ($vm in $VMs) {
    Write-Host "`n=== $vm ===" -ForegroundColor Cyan
    gcloud compute ssh $vm --zone=$Zone --quiet --command "docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}' 2>/dev/null || echo NO_DOCKER" 2>&1
}
