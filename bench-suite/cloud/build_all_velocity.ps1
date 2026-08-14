# Build Velocity on all 3 Velocity VMs
$VMs = @("velocity-classic", "velocity-runtime", "velocity-embedded")
$Zone = "us-east1-b"

foreach ($vm in $VMs) {
    Write-Host "`n========================================" -ForegroundColor Cyan
    Write-Host "  Building on $vm" -ForegroundColor Cyan
    Write-Host "========================================" -ForegroundColor Cyan
    
    gcloud compute scp bench-suite/cloud/build_velocity.sh "${vm}:/tmp/build.sh" --zone=$Zone --quiet 2>&1
    gcloud compute ssh $vm --zone=$Zone --quiet --command "bash /tmp/build.sh" 2>&1
}

Write-Host "`n=== All builds complete ===" -ForegroundColor Green
