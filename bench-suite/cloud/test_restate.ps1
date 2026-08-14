Write-Host "=== Restate Smoke Test (correct URL) ===" -ForegroundColor Cyan
gcloud compute ssh restate-bench "--zone=us-east1-b" --quiet --command "curl -sv -X POST http://localhost:8080/bench/test-key/simple -H 'Content-Type: application/json' -d '{}' --max-time 30 2>&1" 2>&1
Write-Host "`n=== Done ===" -ForegroundColor Green
