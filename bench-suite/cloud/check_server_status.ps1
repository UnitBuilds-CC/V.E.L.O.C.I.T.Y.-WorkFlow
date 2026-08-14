$Zone = "us-east1-b"
$VM = "velocity-classic"

# Check server status and recent logs
gcloud compute ssh $VM --zone=$Zone --quiet --command "echo '=== Server status ==='; ps aux | grep velocity-server | grep -v grep; echo; echo '=== Server log tail ==='; tail -30 /tmp/velocity-classic.log 2>/dev/null; echo; echo '=== Memory ==='; free -h" 2>&1
