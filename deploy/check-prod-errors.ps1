$gcloud = 'C:\Program Files (x86)\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd'
& $gcloud compute ssh velocity-classic --zone=us-east1-b --quiet --command="sudo docker logs velocity-prod 2>&1 | grep -i 'error\|panic\|fail\|warn' | tail -20"
