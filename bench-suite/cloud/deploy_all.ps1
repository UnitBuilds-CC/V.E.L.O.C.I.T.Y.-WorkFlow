$VMs = @("velocity-classic", "velocity-runtime", "velocity-embedded", "dbos-bench", "restate-bench", "temporal-bench")
$Zone = "us-east1-b"
$Tarball = "bench-suite-deploy.tar.gz"
$DeployScript = "bench-suite/cloud/deploy_from_tar.sh"

foreach ($vm in $VMs) {
    Write-Host "`n========================================" -ForegroundColor Cyan
    Write-Host "  Deploying to $vm" -ForegroundColor Cyan
    Write-Host "========================================" -ForegroundColor Cyan
    
    # Upload tarball
    Write-Host "  Uploading tarball..."
    gcloud compute scp $Tarball "${vm}:/tmp/velocity-repo.tar.gz" --zone=$Zone --quiet 2>&1
    
    # Upload deploy script
    gcloud compute scp $DeployScript "${vm}:/tmp/deploy.sh" --zone=$Zone --quiet 2>&1
    
    # Run deploy
    Write-Host "  Extracting..."
    gcloud compute ssh $vm --zone=$Zone --quiet --command "bash /tmp/deploy.sh" 2>&1
}

Write-Host "`n=== All VMs deployed ===" -ForegroundColor Green
