$Zone = "us-east1-b"

# Get runtime results
Write-Host "=== VELOCITY-RUNTIME RESULTS ===" -ForegroundColor Cyan
gcloud compute ssh velocity-runtime --zone=$Zone --quiet --command "cat /tmp/bench_results.md" 2>&1

Write-Host "`n`n=== TEMPORAL RESULTS ===" -ForegroundColor Cyan
gcloud compute ssh temporal-bench --zone=$Zone --quiet --command "cat /tmp/bench_results.md" 2>&1

Write-Host "`n`n=== Classic bench detail ===" -ForegroundColor Yellow
gcloud compute ssh velocity-classic --zone=$Zone --quiet --command "tail -10 /tmp/bench_stdout.log" 2>&1

Write-Host "`n`n=== Embedded bench detail ===" -ForegroundColor Yellow
gcloud compute ssh velocity-embedded --zone=$Zone --quiet --command "tail -10 /tmp/bench_stdout.log" 2>&1
