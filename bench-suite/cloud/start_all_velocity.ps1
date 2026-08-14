$Zone = "us-east1-b"
$StartScript = "c:\Users\visse\OneDrive\Documents\Velocity-workflow\bench-suite\cloud\start_velocity.sh"

# Start Velocity on each VM with appropriate mode
$modes = @{
    "velocity-classic" = "classic"
    "velocity-runtime" = "runtime"
    "velocity-embedded" = "embedded"
}

foreach ($vm in $modes.Keys) {
    $mode = $modes[$vm]
    Write-Host "`n=== Starting Velocity ($mode) on $vm ===" -ForegroundColor Cyan
    
    # Upload start script
    gcloud compute scp $StartScript "${vm}:/tmp/start_velocity.sh" --zone=$Zone --quiet 2>&1
    
    # Run it
    gcloud compute ssh $vm --zone=$Zone --quiet --command "bash /tmp/start_velocity.sh $mode" 2>&1
    
    Write-Host "Done starting $mode on $vm" -ForegroundColor Green
}

Write-Host "`n=== All Velocity servers started ===" -ForegroundColor Green
