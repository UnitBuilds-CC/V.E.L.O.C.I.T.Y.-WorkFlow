$gcloud = 'C:\Program Files (x86)\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd'
& $gcloud compute ssh velocity-classic --zone=us-east1-b --quiet --command="sudo docker images | grep velocity; echo '---'; sudo docker ps -a --filter name=velocity --format '{{.Names}} {{.Image}} {{.Status}}'"
