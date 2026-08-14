$Zone = "us-east1-b"

Write-Host "=== Starting Temporal Smoke Benchmark ===" -ForegroundColor Cyan
Start-Process -NoNewWindow -FilePath "gcloud" -ArgumentList "compute","ssh","temporal-bench","--zone=$Zone","--quiet","--command","nohup bash /tmp/bench.sh > /tmp/bench_output.log 2>&1 &"

Write-Host "=== Starting DBOS Smoke Benchmark ===" -ForegroundColor Cyan
Start-Process -NoNewWindow -FilePath "gcloud" -ArgumentList "compute","ssh","dbos-bench","--zone=$Zone","--quiet","--command","nohup bash /tmp/bench.sh > /tmp/bench_output.log 2>&1 &"

Write-Host "=== Starting Restate Smoke Benchmark ===" -ForegroundColor Cyan
Start-Process -NoNewWindow -FilePath "gcloud" -ArgumentList "compute","ssh","restate-bench","--zone=$Zone","--quiet","--command","nohup bash /tmp/bench.sh > /tmp/bench_output.log 2>&1 &"

Write-Host "`nAll benchmarks launched. Waiting 60 seconds..." -ForegroundColor Yellow
Start-Sleep -Seconds 60

Write-Host "`n=== Checking Results ===" -ForegroundColor Cyan

Write-Host "`n--- Temporal ---" -ForegroundColor Yellow
gcloud compute ssh temporal-bench "--zone=$Zone" --quiet --command "tail -50 /tmp/bench_output.log 2>/dev/null; echo '---JSON---'; cat /tmp/temporal_smoke.json 2>/dev/null || echo NO_RESULTS" 2>&1

Write-Host "`n--- DBOS ---" -ForegroundColor Yellow
gcloud compute ssh dbos-bench "--zone=$Zone" --quiet --command "tail -50 /tmp/bench_output.log 2>/dev/null; echo '---JSON---'; cat /tmp/dbos_smoke.json 2>/dev/null || echo NO_RESULTS" 2>&1

Write-Host "`n--- Restate ---" -ForegroundColor Yellow
gcloud compute ssh restate-bench "--zone=$Zone" --quiet --command "tail -50 /tmp/bench_output.log 2>/dev/null; echo '---JSON---'; cat /tmp/restate_smoke.json 2>/dev/null || echo NO_RESULTS" 2>&1

Write-Host "`n=== Done ===" -ForegroundColor Green
