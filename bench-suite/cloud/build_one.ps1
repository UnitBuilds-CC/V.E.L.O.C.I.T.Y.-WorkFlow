$Zone = "us-east1-b"
$VM = "velocity-classic"

gcloud compute scp bench-suite/cloud/build_velocity.sh "${VM}:/tmp/build.sh" --zone=$Zone --quiet 2>&1
Write-Host "Building on $VM (this takes 5-10 min)..."
gcloud compute ssh $VM --zone=$Zone --quiet --command "bash /tmp/build.sh" 2>&1
