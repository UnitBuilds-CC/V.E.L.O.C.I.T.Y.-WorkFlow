$gcloud = 'C:\Program Files (x86)\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd'
$workDir = "c:\Users\visse\OneDrive\Documents\Velocity-workflow"

# Create tar of migrations
Set-Location $workDir
tar -cf "$workDir\migrations.tar" migrations
Write-Host "Migrations tar created: $(Get-Item "$workDir\migrations.tar" | Select-Object -ExpandProperty Length) bytes"

# Upload
& $gcloud compute ssh velocity-classic --zone=us-east1-b --quiet --command="sudo touch /home/prod-build/migrations.tar; sudo chown ian_unitbuilds_com:ian_unitbuilds_com /home/prod-build/migrations.tar"
Start-Sleep -Seconds 2
& $gcloud compute scp "$workDir\migrations.tar" "velocity-classic:/home/prod-build/migrations.tar" --zone=us-east1-b --quiet

# Extract to /migrations (engine include_str! expects /engine/src/../../migrations/ = /migrations/)
& $gcloud compute ssh velocity-classic --zone=us-east1-b --quiet --command="sudo rm -rf /migrations; sudo tar xf /home/prod-build/migrations.tar -C /; sudo chown -R ian_unitbuilds_com:ian_unitbuilds_com /migrations; ls /migrations/ | head -5; echo 'OK'"

# Also copy into build context for Docker COPY
& $gcloud compute ssh velocity-classic --zone=us-east1-b --quiet --command="sudo cp -r /migrations /home/prod-build/migrations; sudo chown -R ian_unitbuilds_com:ian_unitbuilds_com /home/prod-build/migrations"

# Clean up
Remove-Item "$workDir\migrations.tar" -Force

# Rebuild
& $gcloud compute ssh velocity-classic --zone=us-east1-b --quiet --command="cd /home/prod-build; sudo docker build -f Dockerfile.production-server -t velocity-prod-server . 2>&1 | tail -15"
