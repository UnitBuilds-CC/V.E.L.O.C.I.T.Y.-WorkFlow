$gcloud = 'C:\Program Files (x86)\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd'
# Stop dev server, start production server
& $gcloud compute ssh velocity-classic --zone=us-east1-b --quiet --command="sudo docker stop velocity-dev; sudo docker rm velocity-dev; sudo docker run -d --name velocity-prod --network velocity-workflow_default velocity-prod-server; sleep 3; sudo docker ps --filter name=velocity-prod --format '{{.Names}} {{.Status}}'; sudo docker logs velocity-prod 2>&1 | head -5"
