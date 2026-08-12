$gcloud = 'C:\Program Files (x86)\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd'
& $gcloud compute ssh velocity-classic --zone=us-east1-b --quiet --command="sudo docker run -d --name dbos-test --network velocity-workflow_default node:20-slim sleep 3600; sleep 3; sudo docker stats --no-stream --format 'table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}'"
