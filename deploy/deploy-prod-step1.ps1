$gcloud = 'C:\Program Files (x86)\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd'

# Create build context directories
& $gcloud compute ssh velocity-classic --zone=us-east1-b --quiet --command="sudo mkdir -p /home/prod-build/velocity-bench/proto /home/prod-build/velocity-workflow-server/src /home/prod-build/velocity-workflow-engine/src; sudo chown -R ian_unitbuilds_com:ian_unitbuilds_com /home/prod-build"
Start-Sleep -Seconds 3

# Upload proto
& $gcloud compute scp "c:\Users\visse\OneDrive\Documents\Velocity-workflow\velocity-bench\proto\benchmark.proto" "velocity-classic:/home/prod-build/velocity-bench/proto/benchmark.proto" --zone=us-east1-b --quiet

# Upload server source
& $gcloud compute scp "c:\Users\visse\OneDrive\Documents\Velocity-workflow\velocity-workflow-server\Cargo.toml" "velocity-classic:/home/prod-build/velocity-workflow-server/Cargo.toml" --zone=us-east1-b --quiet
& $gcloud compute scp "c:\Users\visse\OneDrive\Documents\Velocity-workflow\velocity-workflow-server\build.rs" "velocity-classic:/home/prod-build/velocity-workflow-server/build.rs" --zone=us-east1-b --quiet
$serverFiles = @("main.rs")
foreach ($f in $serverFiles) {
    & $gcloud compute scp "c:\Users\visse\OneDrive\Documents\Velocity-workflow\velocity-workflow-server\src\$f" "velocity-classic:/home/prod-build/velocity-workflow-server/src/$f" --zone=us-east1-b --quiet
}

# Upload Dockerfile
& $gcloud compute scp "c:\Users\visse\OneDrive\Documents\Velocity-workflow\deploy\Dockerfile.production-server" "velocity-classic:/home/prod-build/Dockerfile.production-server" --zone=us-east1-b --quiet

Write-Host "Proto + server + Dockerfile uploaded. Now uploading engine..."
