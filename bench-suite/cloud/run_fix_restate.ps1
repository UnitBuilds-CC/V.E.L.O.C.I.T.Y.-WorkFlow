Write-Host "=== Fixing Restate VM ===" -ForegroundColor Cyan
gcloud compute ssh restate-bench "--zone=us-east1-b" --quiet --command "bash /tmp/fix_restate.sh" 2>&1
Write-Host "`n=== Done ===" -ForegroundColor Green
