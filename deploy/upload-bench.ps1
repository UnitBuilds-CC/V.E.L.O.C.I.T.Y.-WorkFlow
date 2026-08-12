$gcloud = 'C:\Program Files (x86)\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd'
& $gcloud compute ssh velocity-classic --zone=us-east1-b --quiet --command="sudo touch /home/bench-all-engines.js; sudo chown ian_unitbuilds_com:ian_unitbuilds_com /home/bench-all-engines.js"
Start-Sleep -Seconds 2
& $gcloud compute scp "c:\Users\visse\OneDrive\Documents\Velocity-workflow\deploy\bench-all-engines.js" velocity-classic:/home/bench-all-engines.js --zone=us-east1-b --quiet
