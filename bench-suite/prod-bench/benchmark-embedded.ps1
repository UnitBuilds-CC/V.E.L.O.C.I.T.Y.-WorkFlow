# Velocity Embedded Server Benchmark Script
# Tests throughput and latency with real PostgreSQL persistence

param(
    [int]$Requests = 100,
    [int]$Concurrency = 5,
    [string]$Endpoint = "http://localhost:18082/api/v1/workflows"
)

Write-Host "=== Velocity Embedded Server Benchmark ===" -ForegroundColor Cyan
Write-Host "Endpoint: $Endpoint"
Write-Host "Requests: $Requests"
Write-Host "Concurrency: $Concurrency"
Write-Host ""

# Verify server is up
Write-Host "Checking server health..." -ForegroundColor Yellow
$health = curl.exe -s http://localhost:18082/health 2>$null
if (-not $health) {
    Write-Host "ERROR: Server not reachable" -ForegroundColor Red
    exit 1
}
Write-Host "Server healthy: $health" -ForegroundColor Green
Write-Host ""

# Create temp file for request body
[System.IO.File]::WriteAllText("bench_request.json", '{"workflowType":"benchmark_workflow","input":{"test":"data"}}')

Write-Host "Starting benchmark..." -ForegroundColor Yellow
Write-Host ""

# Run benchmark
$latencies = @()
$successCount = 0
$failedCount = 0
$startTime = [DateTime]::UtcNow

for ($i = 0; $i -lt $Requests; $i++) {
    $requestStart = [DateTime]::UtcNow
    
    # Execute request
    $response = curl.exe -s -o NUL -w "%{http_code}" -X POST $Endpoint -H "Content-Type: application/json" -d "@bench_request.json" 2>$null
    
    $latency = ([DateTime]::UtcNow - $requestStart).TotalMilliseconds
    $latencies += $latency
    
    if ($response -eq "200") {
        $successCount++
    } else {
        $failedCount++
    }
    
    # Progress
    if (($i + 1) % 10 -eq 0 -or $i -eq $Requests - 1) {
        $elapsed = ([DateTime]::UtcNow - $startTime).TotalSeconds
        $rps = ($i + 1) / $elapsed
        $percent = [math]::Round((($i + 1) / $Requests) * 100)
        Write-Host "`r[$percent%] Progress: $($i + 1)/$Requests | RPS: $([math]::Round($rps, 1)) | Success: $successCount | Failed: $failedCount" -NoNewline
    }
}

$totalTime = ([DateTime]::UtcNow - $startTime).TotalSeconds

Write-Host ""
Write-Host ""
Write-Host "=== Results ===" -ForegroundColor Cyan
Write-Host "Total Time: $([math]::Round($totalTime, 2))s"
Write-Host "Total Requests: $Requests"
Write-Host "Successful (200): $successCount"
Write-Host "Failed: $failedCount"

if ($Requests -gt 0) {
    $successRate = [math]::Round(($successCount / $Requests) * 100, 2)
    $overallRps = [math]::Round($Requests / $totalTime, 2)
    
    Write-Host "Success Rate: $successRate%"
    Write-Host ""
    Write-Host "Throughput: $overallRps ops/s" -ForegroundColor Green
    
    if ($latencies.Count -gt 0) {
        $sortedLatencies = $latencies | Sort-Object
        $avg = ($sortedLatencies | Measure-Object -Average).Average
        $min = [math]::Round(($sortedLatencies | Measure-Object -Minimum).Minimum, 2)
        $max = [math]::Round(($sortedLatencies | Measure-Object -Maximum).Maximum, 2)
        $p50 = [math]::Round($sortedLatencies[[math]::Floor($sortedLatencies.Count * 0.50)], 2)
        $p95 = [math]::Round($sortedLatencies[[math]::Floor($sortedLatencies.Count * 0.95)], 2)
        $p99 = [math]::Round($sortedLatencies[[math]::Floor($sortedLatencies.Count * 0.99)], 2)
        
        Write-Host ""
        Write-Host "Latency (ms):" -ForegroundColor Cyan
        Write-Host "  Avg: $([math]::Round($avg, 2))"
        Write-Host "  Min: $min"
        Write-Host "  Max: $max"
        Write-Host "  P50: $p50"
        Write-Host "  P95: $p95"
        Write-Host "  P99: $p99"
    }
}

Remove-Item "bench_request.json" -Force -ErrorAction SilentlyContinue

Write-Host ""
Write-Host "=== Benchmark Complete ===" -ForegroundColor Green
