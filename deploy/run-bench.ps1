$gcloud = 'C:\Program Files (x86)\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd'
& $gcloud compute ssh velocity-classic --zone=us-east1-b --quiet --command="sudo docker run --rm --name velocity-bench-json --network velocity-workflow_default velocity-bench --workloads all --engine velocity --velocity-address http://velocity-dev:7234 --profile standard --format json"
