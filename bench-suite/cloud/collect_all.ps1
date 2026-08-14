$Zone = "us-east1-b"

# Collect JSON results from all VMs
$OutputDir = "C:\Users\visse\OneDrive\Documents\Velocity-workflow\bench-suite\cloud\results"
New-Item -ItemType Directory -Force -Path $OutputDir | Out-Null

Write-Host "=== Collecting Results from All VMs ===" -ForegroundColor Cyan

# Velocity Runtime (has full 21-workload results)
Write-Host "`n--- velocity-runtime ---" -ForegroundColor Yellow
gcloud compute scp "velocity-runtime:/tmp/bench_results.json" "$OutputDir\velocity_runtime_full.json" "--zone=$Zone" --quiet 2>&1
gcloud compute scp "velocity-runtime:/tmp/bench_results.md" "$OutputDir\velocity_runtime_full.md" "--zone=$Zone" --quiet 2>&1

# Velocity Classic (smoke results)
Write-Host "--- velocity-classic ---" -ForegroundColor Yellow
gcloud compute scp "velocity-classic:/tmp/bench_smoke.json" "$OutputDir\velocity_classic_smoke.json" "--zone=$Zone" --quiet 2>&1
gcloud compute scp "velocity-classic:/tmp/bench_smoke.md" "$OutputDir\velocity_classic_smoke.md" "--zone=$Zone" --quiet 2>&1

# Velocity Embedded (smoke results)
Write-Host "--- velocity-embedded ---" -ForegroundColor Yellow
gcloud compute scp "velocity-embedded:/tmp/bench_smoke.json" "$OutputDir\velocity_embedded_smoke.json" "--zone=$Zone" --quiet 2>&1
gcloud compute scp "velocity-embedded:/tmp/bench_smoke.md" "$OutputDir\velocity_embedded_smoke.md" "--zone=$Zone" --quiet 2>&1

# Temporal (standard results)
Write-Host "--- temporal ---" -ForegroundColor Yellow
gcloud compute scp "temporal-bench:/tmp/temporal_bench_results.json" "$OutputDir\temporal_standard.json" "--zone=$Zone" --quiet 2>&1

# DBOS (standard results)
Write-Host "--- dbos ---" -ForegroundColor Yellow
gcloud compute scp "dbos-bench:/tmp/dbos_bench_results.json" "$OutputDir\dbos_standard.json" "--zone=$Zone" --quiet 2>&1

# Restate (standard results)
Write-Host "--- restate ---" -ForegroundColor Yellow
gcloud compute scp "restate-bench:/tmp/restate_bench_results.json" "$OutputDir\restate_standard.json" "--zone=$Zone" --quiet 2>&1

Write-Host "`n=== Collection Complete ===" -ForegroundColor Green
Get-ChildItem $OutputDir | Format-Table Name, Length -AutoSize
