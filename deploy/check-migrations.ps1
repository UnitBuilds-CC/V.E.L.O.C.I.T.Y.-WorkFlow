$gcloud = 'C:\Program Files (x86)\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd'
# Check what's in /migrations on VM
& $gcloud compute ssh velocity-classic --zone=us-east1-b --quiet --command="ls -la /migrations/; ls -la /migrations/rollback/ 2>/dev/null || echo 'rollback dir missing'"
