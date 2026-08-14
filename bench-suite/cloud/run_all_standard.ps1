$Zone = "us-east1-b"

Write-Host "=== Running DBOS Standard Benchmark ===" -ForegroundColor Cyan
gcloud compute ssh dbos-bench "--zone=$Zone" --quiet --command "bash /tmp/bench_std.sh" 2>&1

Write-Host "`n=== Running Restate Standard Benchmark ===" -ForegroundColor Cyan
gcloud compute ssh restate-bench "--zone=$Zone" --quiet --command "bash /tmp/bench_std.sh" 2>&1

Write-Host "`n=== Running Temporal Standard Benchmark (this may take 10+ min) ===" -ForegroundColor Cyan
gcloud compute ssh temporal-bench "--zone=$Zone" --quiet --command "bash /tmp/bench_std.sh" 2>&1

Write-Host "`n=== All standard benchmarks complete ===" -ForegroundColor Green
