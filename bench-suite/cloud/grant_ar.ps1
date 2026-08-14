$projNum = gcloud projects describe velocity-live-test-001 --format="value(projectNumber)" 2>&1
Write-Host "Project number: $projNum"
$sa = "${projNum}-compute@developer.gserviceaccount.com"
Write-Host "Service account: $sa"
gcloud projects add-iam-policy-binding velocity-live-test-001 --member="serviceAccount:$sa" --role="roles/artifactregistry.reader"
