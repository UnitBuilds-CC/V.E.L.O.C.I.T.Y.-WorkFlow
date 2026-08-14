$Zone = "us-east1-b"
$BenchScript = "c:\Users\visse\OneDrive\Documents\Velocity-workflow\bench-suite\cloud\run_bench.sh"
$TemporalBenchScript = "c:\Users\visse\OneDrive\Documents\Velocity-workflow\bench-suite\cloud\run_temporal_bench.sh"
$Profile = "quick"

# Run Velocity benchmarks on 3 VMs
$VelocityVMs = @("velocity-classic", "velocity-runtime", "velocity-embedded")

foreach ($vm in $VelocityVMs) {
    Write-Host "`n=== Running Velocity bench on $vm ===" -ForegroundColor Cyan
    gcloud compute scp $BenchScript "${vm}:/tmp/run_bench.sh" --zone=$Zone --quiet 2>&1
    gcloud compute ssh $vm --zone=$Zone --quiet --command "bash /tmp/run_bench.sh $Profile" 2>&1
    Write-Host "Done with $vm" -ForegroundColor Green
}

# Run Temporal benchmark on temporal-bench VM
Write-Host "`n=== Running Temporal bench on temporal-bench ===" -ForegroundColor Cyan
gcloud compute scp $TemporalBenchScript "temporal-bench:/tmp/run_temporal_bench.sh" --zone=$Zone --quiet 2>&1
gcloud compute ssh temporal-bench --zone=$Zone --quiet --command "bash /tmp/run_temporal_bench.sh $Profile" 2>&1

Write-Host "`n=== All quick benchmarks complete ===" -ForegroundColor Green
