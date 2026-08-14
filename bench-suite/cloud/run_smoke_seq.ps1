$Zone = "us-east1-b"

Write-Host "=== Running Temporal Smoke Benchmark ===" -ForegroundColor Cyan
gcloud compute ssh temporal-bench "--zone=$Zone" --quiet --command "bash /tmp/bench.sh" 2>&1

Write-Host "`n=== Running DBOS Smoke Benchmark ===" -ForegroundColor Cyan
gcloud compute ssh dbos-bench "--zone=$Zone" --quiet --command "bash /tmp/bench.sh" 2>&1

Write-Host "`n=== Running Restate Smoke Benchmark ===" -ForegroundColor Cyan
gcloud compute ssh restate-bench "--zone=$Zone" --quiet --command "bash /tmp/bench.sh" 2>&1

Write-Host "`n=== All smoke benchmarks complete ===" -ForegroundColor Green
