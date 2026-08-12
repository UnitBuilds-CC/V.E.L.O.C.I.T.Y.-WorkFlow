$gcloud = 'C:\Program Files (x86)\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd'
& $gcloud compute ssh velocity-classic --zone=us-east1-b --quiet --command="sudo docker run --rm --network host -v /home/bench-all-engines.js:/bench.js node:20-slim node /bench.js 2000"
