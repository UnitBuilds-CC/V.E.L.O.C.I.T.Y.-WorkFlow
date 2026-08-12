$gcloud = 'C:\Program Files (x86)\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd'
& $gcloud compute ssh velocity-classic --zone=us-east1-b --quiet --command="sudo touch /home/dbos-bench.js; sudo chown ian_unitbuilds_com:ian_unitbuilds_com /home/dbos-bench.js"
Start-Sleep -Seconds 2
& $gcloud compute scp "c:\Users\visse\OneDrive\Documents\Velocity-workflow\deploy\dbos-bench.js" velocity-classic:/home/dbos-bench.js --zone=us-east1-b --quiet
Start-Sleep -Seconds 2
& $gcloud compute ssh velocity-classic --zone=us-east1-b --quiet --command="sudo docker cp /home/dbos-bench.js dbos-test:/dbos-bench.js; sudo docker exec dbos-test node /dbos-bench.js"
