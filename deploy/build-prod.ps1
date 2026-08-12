$gcloud = 'C:\Program Files (x86)\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd'
# Upload updated Dockerfile
& $gcloud compute scp "c:\Users\visse\OneDrive\Documents\Velocity-workflow\deploy\Dockerfile.production-server" "velocity-classic:/home/prod-build/Dockerfile.production-server" --zone=us-east1-b --quiet
Start-Sleep -Seconds 2
# Rebuild
& $gcloud compute ssh velocity-classic --zone=us-east1-b --quiet --command="cd /home/prod-build; sudo docker build -f Dockerfile.production-server -t velocity-prod-server . 2>&1 | tail -15"
