$gcloud = 'C:\Program Files (x86)\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd'
# Quick verification - run smoke test against production server
& $gcloud compute ssh velocity-classic --zone=us-east1-b --quiet --command="sudo docker run --rm --network velocity-workflow_default velocity-bench --workloads smoke --engine both --velocity-address http://velocity-prod:7234 --temporal-address http://temporal-bridge:7234 --profile quick --format markdown 2>&1 | tail -30"
