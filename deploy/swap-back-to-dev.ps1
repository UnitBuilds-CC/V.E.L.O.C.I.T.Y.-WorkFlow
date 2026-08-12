$gcloud = 'C:\Program Files (x86)\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd'
& $gcloud compute ssh velocity-classic --zone=us-east1-b --quiet --command="sudo docker rm velocity-dev 2>/dev/null; sudo docker run -d --name velocity-dev --network velocity-workflow_default velocity-dev-server; sleep 3; sudo docker ps --filter name=velocity-dev --format '{{.Names}} {{.Status}}'"
