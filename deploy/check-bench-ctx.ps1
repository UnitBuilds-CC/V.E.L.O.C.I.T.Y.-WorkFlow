$gcloud = 'C:\Program Files (x86)\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd'
& $gcloud compute ssh velocity-classic --zone=us-east1-b --quiet --command="ls -la /home/bench-context/velocity-bench/src/ | head -5; echo '---'; ls -la /home/bench-context/velocity-bench/proto/ | head -5"
