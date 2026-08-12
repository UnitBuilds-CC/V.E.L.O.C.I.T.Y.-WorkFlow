$gcloud = 'C:\Program Files (x86)\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd'
# Upload updated velocity-bench main.rs (with sustained mode) to the existing build context
& $gcloud compute scp "c:\Users\visse\OneDrive\Documents\Velocity-workflow\velocity-bench\src\main.rs" "velocity-classic:/home/bench-context/velocity-bench/src/main.rs" --zone=us-east1-b --quiet
Start-Sleep -Seconds 2
# Rebuild velocity-bench image
& $gcloud compute ssh velocity-classic --zone=us-east1-b --quiet --command="cd /home/bench-context; sudo docker build -f velocity-bench/Dockerfile -t velocity-bench . 2>&1 | tail -10"
