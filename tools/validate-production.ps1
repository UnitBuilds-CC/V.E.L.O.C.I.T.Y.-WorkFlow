# VELOCITY Production Validation - Full-Stack Server Test
# Tests all 3 flavors through their actual server binaries.
# Usage: .\tools\validate-production.ps1

$ErrorActionPreference = "Stop"
$base = "c:\Users\visse\OneDrive\Documents\Velocity-workflow"
$bin = "$base\target\release"
$script:results = @()

function Test-Server {
    param([string]$Name, [string]$Binary, [string[]]$ServerArgs, [int]$HealthPort)
    Write-Host ""
    Write-Host "=== Testing: $Name ===" -ForegroundColor Cyan
    $proc = Start-Process -FilePath $Binary -ArgumentList $ServerArgs -PassThru -NoNewWindow
    Start-Sleep -Seconds 3
    try {
        $health = Invoke-RestMethod -Uri "http://127.0.0.1:$HealthPort/health" -Method GET -TimeoutSec 5
        Write-Host "  /health: $($health.status) ($($health.engine))" -ForegroundColor Green
        if ($health.status -ne "ok") { throw "Health check failed" }
        $ready = Invoke-RestMethod -Uri "http://127.0.0.1:$HealthPort/ready" -Method GET -TimeoutSec 5
        Write-Host "  /ready:  $($ready.status) ($($ready.engine))" -ForegroundColor Green
        if ($ready.status -ne "ready") { throw "Readiness check failed" }
        $metricsRaw = (Invoke-WebRequest -Uri "http://127.0.0.1:$HealthPort/metrics" -Method GET -UseBasicParsing -TimeoutSec 5).Content
        $hasUp = $metricsRaw -match "velocity_up 1"
        $metricLines = ($metricsRaw -split "`n").Count
        Write-Host "  /metrics: $metricLines lines, velocity_up=1 ($hasUp)" -ForegroundColor Green
        if (-not $hasUp) { throw "Metrics missing velocity_up" }
        $script:results += [PSCustomObject]@{ Server=$Name; Health="OK"; Ready="OK"; Metrics="OK ($metricLines lines)"; Status="PASS" }
        Write-Host "  PASS" -ForegroundColor Green
    } catch {
        Write-Host "  FAIL: $_" -ForegroundColor Red
        $script:results += [PSCustomObject]@{ Server=$Name; Health="FAIL"; Ready="FAIL"; Metrics="FAIL"; Status="FAIL" }
    } finally {
        Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue
        Start-Sleep -Seconds 1
    }
}

Test-Server -Name "VCTP (UDP)" -Binary "$bin\velocity-server.exe" -ServerArgs @("--health-bind","127.0.0.1:19095","--vctp-port","19234","--http-bench-port","0","--wal-path","$base\target\val-vctp.wal") -HealthPort 19095
Test-Server -Name "Classic (NMCP)" -Binary "$bin\velocity-classic-server.exe" -ServerArgs @("--health-bind","127.0.0.1:19093","--ws-bind","127.0.0.1:19083","--shmem-path","$base\target\val-classic.nmcp","--wal-path","$base\target\val-classic.wal") -HealthPort 19093
Test-Server -Name "Embedded (NMCP)" -Binary "$bin\velocity-embedded-server.exe" -ServerArgs @("--health-bind","127.0.0.1:19094","--ws-bind","127.0.0.1:19084","--shmem-path","$base\target\val-embedded.nmcp","--wal-path","$base\target\val-embedded.wal") -HealthPort 19094

Write-Host ""
Write-Host "=== Production Validation Summary ===" -ForegroundColor Cyan
$script:results | Format-Table -AutoSize
$allPass = ($script:results | Where-Object { $_.Status -ne "PASS" }).Count -eq 0
if ($allPass) { Write-Host "ALL 3 FLAVORS PASSED" -ForegroundColor Green } else { Write-Host "SOME FLAVORS FAILED" -ForegroundColor Red }
Remove-Item "$base\target\val-*.wal" -Force -ErrorAction SilentlyContinue
Remove-Item "$base\target\val-*.nmcp" -Force -ErrorAction SilentlyContinue
