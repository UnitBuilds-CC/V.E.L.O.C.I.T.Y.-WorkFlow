$Zone = "us-east1-b"

Write-Host "=== VELOCITY-CLASSIC (smoke) ===" -ForegroundColor Cyan
gcloud compute ssh velocity-classic --zone=$Zone --quiet --command "cat /tmp/bench_smoke.md" 2>&1

Write-Host "`n`n=== VELOCITY-RUNTIME (full) ===" -ForegroundColor Cyan
gcloud compute ssh velocity-runtime --zone=$Zone --quiet --command "cat /tmp/bench_results.md" 2>&1

Write-Host "`n`n=== VELOCITY-EMBEDDED (smoke) ===" -ForegroundColor Cyan
gcloud compute ssh velocity-embedded --zone=$Zone --quiet --command "cat /tmp/bench_smoke.md" 2>&1

Write-Host "`n`n=== TEMPORAL ===" -ForegroundColor Cyan
gcloud compute ssh temporal-bench --zone=$Zone --quiet --command "cat /tmp/bench_results.json" 2>&1
