$gcloud = 'C:\Program Files (x86)\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd'
$workDir = "c:\Users\visse\OneDrive\Documents\Velocity-workflow"

# Create tar with correct structure: velocity/v1/*.proto (so it extracts to /proto/velocity/v1/)
Set-Location "$workDir\proto"
tar -cf "$workDir\protos.tar" velocity
Write-Host "Protos tar created: $(Get-Item "$workDir\protos.tar" | Select-Object -ExpandProperty Length) bytes"

# Upload
& $gcloud compute ssh velocity-classic --zone=us-east1-b --quiet --command="sudo touch /home/prod-build/protos.tar; sudo chown ian_unitbuilds_com:ian_unitbuilds_com /home/prod-build/protos.tar"
Start-Sleep -Seconds 2
& $gcloud compute scp "$workDir\protos.tar" "velocity-classic:/home/prod-build/protos.tar" --zone=us-east1-b --quiet

# Extract to /proto/ so it creates /proto/velocity/v1/*.proto
& $gcloud compute ssh velocity-classic --zone=us-east1-b --quiet --command="sudo rm -rf /proto; sudo mkdir -p /proto; sudo tar xf /home/prod-build/protos.tar -C /proto/; sudo chown -R ian_unitbuilds_com:ian_unitbuilds_com /proto; ls /proto/velocity/v1/ | head -5; echo 'OK'"

# Clean up
Remove-Item "$workDir\protos.tar" -Force
