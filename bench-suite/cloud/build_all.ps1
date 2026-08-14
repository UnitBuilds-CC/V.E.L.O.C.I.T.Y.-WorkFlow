$Zone = "us-east1-b"
$VMs = @("velocity-classic", "velocity-runtime", "velocity-embedded")
$BuildScript = "c:\Users\visse\OneDrive\Documents\Velocity-workflow\bench-suite\cloud\build_velocity.sh"

foreach ($vm in $VMs) {
    Write-Host "`n=== Building on $vm ===" -ForegroundColor Cyan
    
    # Upload build script
    gcloud compute scp $BuildScript "${vm}:/tmp/build.sh" --zone=$Zone --quiet 2>&1
    
    # Run build (takes ~5-10 min per VM)
    gcloud compute ssh $vm --zone=$Zone --quiet --command "bash /tmp/build.sh" 2>&1
    
    Write-Host "Done building on $vm" -ForegroundColor Green
}

Write-Host "`n=== All builds complete ===" -ForegroundColor Green
