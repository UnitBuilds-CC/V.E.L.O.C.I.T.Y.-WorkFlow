$ErrorActionPreference = "Continue"
$server = ".\target\release\velocity-bench-server.exe"
$client = ".\target\release\velocity-bench-universal.exe"
$results_dir = "bench-suite\benchmark-results"

$sync_values = @(1, 5, 10, 50, 100)

foreach ($steps in $sync_values) {
    Write-Output "`n=== Starting bench server with sync_steps=$steps ==="
    
    # Start server
    $proc = Start-Process -FilePath $server -ArgumentList "--sync-steps",$steps,"--flush-interval-ms","5","--wal-path","bench-sync${steps}.wal" -NoNewWindow -PassThru
    Start-Sleep -Seconds 2
    
    # Run benchmark
    $outfile = "$results_dir/durability_sync${steps}"
    & $client --engines velocity-runtime --output $outfile --profile quick --runs 3 2>&1 | Select-String "ops/s|UNIVERSAL|Workload"
    
    # Stop server
    Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue
    Start-Sleep -Seconds 1
    Write-Output "=== Completed sync_steps=$steps ==="
}

Write-Output "`nAll configurations benchmarked!"
