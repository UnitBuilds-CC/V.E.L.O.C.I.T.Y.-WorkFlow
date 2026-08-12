@echo off
set GCLOUD="C:\Program Files (x86)\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd"
set PROJECT=velocity-live-test-001
set ZONE=us-east1-b

echo Creating velocity-runtime...
%GCLOUD% compute instances create velocity-runtime --zone=%ZONE% --machine-type=e2-standard-4 --image-family=ubuntu-2204-lts --image-project=ubuntu-os-cloud --boot-disk-size=50GB --tags=velocity-bench --project=%PROJECT% --quiet

echo Creating velocity-embedded...
%GCLOUD% compute instances create velocity-embedded --zone=%ZONE% --machine-type=e2-standard-4 --image-family=ubuntu-2204-lts --image-project=ubuntu-os-cloud --boot-disk-size=50GB --tags=velocity-bench --project=%PROJECT% --quiet

echo Creating temporal-bench...
%GCLOUD% compute instances create temporal-bench --zone=%ZONE% --machine-type=e2-standard-4 --image-family=ubuntu-2204-lts --image-project=ubuntu-os-cloud --boot-disk-size=50GB --tags=velocity-bench --project=%PROJECT% --quiet

echo Creating restate-bench...
%GCLOUD% compute instances create restate-bench --zone=%ZONE% --machine-type=e2-standard-4 --image-family=ubuntu-2204-lts --image-project=ubuntu-os-cloud --boot-disk-size=50GB --tags=velocity-bench --project=%PROJECT% --quiet

echo Creating dbos-bench...
%GCLOUD% compute instances create dbos-bench --zone=%ZONE% --machine-type=e2-standard-4 --image-family=ubuntu-2204-lts --image-project=ubuntu-os-cloud --boot-disk-size=50GB --tags=velocity-bench --project=%PROJECT% --quiet

echo All VMs created!
%GCLOUD% compute instances list --zones=%ZONE% --project=%PROJECT%
