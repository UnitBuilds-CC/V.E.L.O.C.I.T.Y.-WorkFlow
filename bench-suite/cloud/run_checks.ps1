$Zone = "us-east1-b"

Write-Host "=== Checking Temporal VM ===" -ForegroundColor Cyan
gcloud compute ssh temporal-bench "--zone=$Zone" --quiet --command "bash /tmp/check.sh" 2>&1

Write-Host "`n=== Checking DBOS VM ===" -ForegroundColor Cyan
gcloud compute ssh dbos-bench "--zone=$Zone" --quiet --command "bash /tmp/check.sh" 2>&1

Write-Host "`n=== Checking Restate VM ===" -ForegroundColor Cyan
gcloud compute ssh restate-bench "--zone=$Zone" --quiet --command "bash /tmp/check.sh" 2>&1

Write-Host "`n=== All checks done ===" -ForegroundColor Green
