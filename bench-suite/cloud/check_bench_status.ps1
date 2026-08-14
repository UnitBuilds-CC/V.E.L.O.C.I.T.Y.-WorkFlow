$Zone = "us-east1-b"
$VMs = @("velocity-classic", "velocity-runtime", "velocity-embedded", "temporal-bench")

foreach ($vm in $VMs) {
    Write-Host "`n=== $vm ===" -ForegroundColor Cyan
    gcloud compute ssh $vm --zone=$Zone --quiet --command "echo 'Bench running:'; ps aux | grep velocity-bench | grep -v grep | wc -l; echo '---'; echo 'Last 3 lines of bench log:'; tail -3 /tmp/bench_stdout.log 2>/dev/null || echo NO_LOG; echo '---'; echo 'Results:'; ls /tmp/bench_results.md 2>/dev/null && echo EXISTS || echo MISSING" 2>&1
}
