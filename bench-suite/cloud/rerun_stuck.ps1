$Zone = "us-east1-b"
$SmokeScript = "c:\Users\visse\OneDrive\Documents\Velocity-workflow\bench-suite\cloud\run_bench_smoke.sh"

# Kill stuck benchmarks on classic and embedded
Write-Host "=== Killing stuck benchmarks ===" -ForegroundColor Yellow
gcloud compute ssh velocity-classic --zone=$Zone --quiet --command "pkill -f velocity-bench 2>/dev/null; echo killed" 2>&1
gcloud compute ssh velocity-embedded --zone=$Zone --quiet --command "pkill -f velocity-bench 2>/dev/null; echo killed" 2>&1

# Re-run with smoke workloads
Write-Host "`n=== Launching smoke benchmarks ===" -ForegroundColor Cyan
foreach ($vm in @("velocity-classic", "velocity-embedded")) {
    Write-Host "Launching smoke bench on $vm..." -ForegroundColor Cyan
    gcloud compute scp $SmokeScript "${vm}:/tmp/run_bench_smoke.sh" --zone=$Zone --quiet 2>&1
    gcloud compute ssh $vm --zone=$Zone --quiet --command "bash /tmp/run_bench_smoke.sh velocity http://localhost:7234" 2>&1
}

Write-Host "`n=== Smoke benchmarks launched ===" -ForegroundColor Green
