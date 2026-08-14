$Zone = "us-east1-b"

Write-Host "=== Checking Velocity VM Results ===" -ForegroundColor Cyan

Write-Host "`n--- velocity-classic ---" -ForegroundColor Yellow
gcloud compute ssh velocity-classic "--zone=$Zone" --quiet --command "ls -la /tmp/bench_results* /tmp/bench_smoke* 2>/dev/null; echo '==='; cat /tmp/bench_smoke.md 2>/dev/null || echo NO_SMOKE; echo '===FULL==='; cat /tmp/bench_results.md 2>/dev/null || echo NO_FULL" 2>&1

Write-Host "`n--- velocity-runtime ---" -ForegroundColor Yellow
gcloud compute ssh velocity-runtime "--zone=$Zone" --quiet --command "ls -la /tmp/bench_results* /tmp/bench_smoke* 2>/dev/null; echo '==='; cat /tmp/bench_smoke.md 2>/dev/null || echo NO_SMOKE; echo '===FULL==='; cat /tmp/bench_results.md 2>/dev/null || echo NO_FULL" 2>&1

Write-Host "`n--- velocity-embedded ---" -ForegroundColor Yellow
gcloud compute ssh velocity-embedded "--zone=$Zone" --quiet --command "ls -la /tmp/bench_results* /tmp/bench_smoke* 2>/dev/null; echo '==='; cat /tmp/bench_smoke.md 2>/dev/null || echo NO_SMOKE; echo '===FULL==='; cat /tmp/bench_results.md 2>/dev/null || echo NO_FULL" 2>&1

Write-Host "`n=== Done ===" -ForegroundColor Green
