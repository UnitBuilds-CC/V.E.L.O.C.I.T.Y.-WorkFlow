$gcloud = 'C:\Program Files (x86)\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd'
& $gcloud compute ssh velocity-classic --zone=us-east1-b --quiet --command="sudo rm -rf /proto; sudo mkdir -p /proto/velocity; sudo tar xf /home/prod-build/protos.tar -C /proto/ 2>/dev/null; ls /proto/; ls /proto/velocity/ 2>/dev/null; ls /proto/proto/ 2>/dev/null"
