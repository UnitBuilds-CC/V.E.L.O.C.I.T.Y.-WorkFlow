$Zone = "us-east1-b"
$VM = "velocity-classic"

gcloud compute ssh $VM --zone=$Zone --quiet --command "ls -la /tmp/bench* /tmp/run_bench* 2>/dev/null; echo '---'; cat /tmp/bench_results.md 2>/dev/null || echo NO_MD; echo '---'; cat /tmp/bench_results.json 2>/dev/null || echo NO_JSON; echo '---'; cat /tmp/bench_results.csv 2>/dev/null || echo NO_CSV" 2>&1
