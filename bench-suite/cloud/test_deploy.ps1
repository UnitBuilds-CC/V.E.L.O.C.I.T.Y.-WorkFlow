# Test deploy to velocity-classic
$Zone = "us-east1-b"
$VM = "velocity-classic"

Write-Host "Copying deploy script to $VM..."
gcloud compute scp bench-suite/cloud/deploy_to_vm.sh "${VM}:/tmp/deploy.sh" --zone=$Zone --quiet 2>&1

Write-Host "Running deploy on $VM..."
gcloud compute ssh $VM --zone=$Zone --quiet --command "bash /tmp/deploy.sh" 2>&1
