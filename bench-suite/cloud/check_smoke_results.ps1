$Zone = "us-east1-b"
$VMs = @("velocity-classic", "velocity-embedded")

foreach ($vm in $VMs) {
    Write-Host "`n=== $vm (smoke results) ===" -ForegroundColor Cyan
    gcloud compute ssh $vm --zone=$Zone --quiet --command "echo 'Running:'; ps aux | grep velocity-bench | grep -v grep | wc -l; echo '---'; echo 'Smoke log tail:'; tail -5 /tmp/bench_smoke_stdout.log 2>/dev/null || echo NO_LOG; echo '---'; echo 'Smoke results:'; ls -la /tmp/bench_smoke* 2>/dev/null || echo NO_FILES; echo '---'; test -f /tmp/bench_smoke.md && echo RESULTS_EXIST || echo NO_RESULTS" 2>&1
}
