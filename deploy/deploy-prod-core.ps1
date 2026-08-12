$gcloud = 'C:\Program Files (x86)\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd'
$workDir = "c:\Users\visse\OneDrive\Documents\Velocity-workflow"

# Create tar of core source
Set-Location $workDir
tar -cf "$workDir\core.tar" -C "$workDir" velocity-workflow-core/src velocity-workflow-core/Cargo.toml
Write-Host "Core tar created: $(Get-Item "$workDir\core.tar" | Select-Object -ExpandProperty Length) bytes"

# Upload
& $gcloud compute ssh velocity-classic --zone=us-east1-b --quiet --command="sudo touch /home/prod-build/core.tar; sudo chown ian_unitbuilds_com:ian_unitbuilds_com /home/prod-build/core.tar"
Start-Sleep -Seconds 2
& $gcloud compute scp "$workDir\core.tar" "velocity-classic:/home/prod-build/core.tar" --zone=us-east1-b --quiet

# Extract and fix paths
& $gcloud compute ssh velocity-classic --zone=us-east1-b --quiet --command="cd /home/prod-build; tar xf core.tar; sudo chown -R ian_unitbuilds_com:ian_unitbuilds_com velocity-workflow-core; rm core.tar; echo 'Core extracted'"

# Clean up local tar
Remove-Item "$workDir\core.tar" -Force
