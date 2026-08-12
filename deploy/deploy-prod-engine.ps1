$gcloud = 'C:\Program Files (x86)\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd'
$workDir = "c:\Users\visse\OneDrive\Documents\Velocity-workflow"

# Create tar of engine source locally
Set-Location $workDir
tar -cf "$workDir\engine.tar" -C "$workDir" velocity-workflow-engine/src velocity-workflow-engine/Cargo.toml velocity-workflow-engine/build.rs
Write-Host "Engine tar created: $(Get-Item "$workDir\engine.tar" | Select-Object -ExpandProperty Length) bytes"

# Upload tar
& $gcloud compute ssh velocity-classic --zone=us-east1-b --quiet --command="sudo touch /home/prod-build/engine.tar; sudo chown ian_unitbuilds_com:ian_unitbuilds_com /home/prod-build/engine.tar"
Start-Sleep -Seconds 2
& $gcloud compute scp "$workDir\engine.tar" "velocity-classic:/home/prod-build/engine.tar" --zone=us-east1-b --quiet
Write-Host "Engine tar uploaded"

# Extract on VM
& $gcloud compute ssh velocity-classic --zone=us-east1-b --quiet --command="cd /home/prod-build; tar xf engine.tar; sudo chown -R ian_unitbuilds_com:ian_unitbuilds_com velocity-workflow-engine; ls -la velocity-workflow-engine/; rm engine.tar"
Write-Host "Engine extracted on VM"

# Clean up local tar
Remove-Item "$workDir\engine.tar" -Force
