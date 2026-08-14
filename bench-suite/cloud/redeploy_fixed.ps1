$Zone = "us-east1-b"
$VMs = @("velocity-classic", "velocity-runtime", "velocity-embedded", "dbos-bench", "restate-bench", "temporal-bench")
$DeployScript = "c:\Users\visse\OneDrive\Documents\Velocity-workflow\bench-suite\cloud\deploy_from_tar.sh"

foreach ($vm in $VMs) {
    Write-Host "`n=== Deploying to $vm ===" -ForegroundColor Cyan
    
    # Upload fixed deploy script
    gcloud compute scp $DeployScript "${vm}:/tmp/deploy.sh" --zone=$Zone --quiet 2>&1
    
    # Run it (tarball already at /tmp/velocity-repo.tar.gz from previous deploy)
    gcloud compute ssh $vm --zone=$Zone --quiet --command "bash /tmp/deploy.sh" 2>&1
    
    Write-Host "Done with $vm" -ForegroundColor Green
}

Write-Host "`n=== All VMs deployed ===" -ForegroundColor Green
