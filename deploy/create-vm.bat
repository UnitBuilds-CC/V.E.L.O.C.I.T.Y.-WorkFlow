@echo off
"C:\Program Files (x86)\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd" compute instances create velocity-classic ^
  --zone=us-central1-a ^
  --machine-type=e2-standard-4 ^
  --image-family=ubuntu-2404-lts-amd64 ^
  --image-project=ubuntu-os-cloud ^
  --boot-disk-size=50GB ^
  --tags=http-server,https-server ^
  --quiet
