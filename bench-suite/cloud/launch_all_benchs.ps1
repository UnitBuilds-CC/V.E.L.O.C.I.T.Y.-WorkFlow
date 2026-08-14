$Zone = "us-east1-b"
$BgScript = "c:\Users\visse\OneDrive\Documents\Velocity-workflow\bench-suite\cloud\run_bench_bg.sh"

# Launch Velocity benchmarks on 3 VMs simultaneously
$tasks = @(
    @{ VM = "velocity-classic";  Engine = "velocity"; Address = "http://localhost:7234" },
    @{ VM = "velocity-runtime";  Engine = "velocity"; Address = "http://localhost:7234" },
    @{ VM = "velocity-embedded"; Engine = "velocity"; Address = "http://localhost:7234" },
    @{ VM = "temporal-bench";    Engine = "temporal"; Address = "http://localhost:7233" }
)

foreach ($task in $tasks) {
    $vm = $task.VM
    $engine = $task.Engine
    $address = $task.Address
    Write-Host "Launching $engine bench on $vm..." -ForegroundColor Cyan
    
    gcloud compute scp $BgScript "${vm}:/tmp/run_bench_bg.sh" --zone=$Zone --quiet 2>&1
    gcloud compute ssh $vm --zone=$Zone --quiet --command "bash /tmp/run_bench_bg.sh quick $engine $address" 2>&1
}

Write-Host "`n=== All benchmarks launched ===" -ForegroundColor Green
Write-Host "Monitor with: check_bench_status.ps1" -ForegroundColor Yellow
