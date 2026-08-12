@echo off
"C:\Program Files (x86)\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd" container clusters create velocity-bench-k8s --project=velocity-live-test-001 --zone=us-east1-b --num-nodes=1 --machine-type=e2-standard-4 --disk-size=50
echo EXIT_CODE=%ERRORLEVEL%
