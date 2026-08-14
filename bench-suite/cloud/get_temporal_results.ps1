$Zone = "us-east1-b"

# Get Temporal results
Write-Host "=== TEMPORAL RESULTS ===" -ForegroundColor Cyan
gcloud compute ssh temporal-bench --zone=$Zone --quiet --command "cat /tmp/bench_results.md" 2>&1
