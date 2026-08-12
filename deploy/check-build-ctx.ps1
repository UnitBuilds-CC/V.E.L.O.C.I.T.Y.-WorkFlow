$gcloud = 'C:\Program Files (x86)\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd'
& $gcloud compute ssh velocity-classic --zone=us-east1-b --quiet --command="ls /home/prod-build/migrations/rollback/ 2>/dev/null || echo 'rollback missing from build context'"
