# VELOCITY Production Hardening Validation
# Tests all operational hardening features across all 3 flavors.
# Usage: .\tools\validate-production-hardening.ps1

$ErrorActionPreference = "Stop"
$base = "c:\Users\visse\OneDrive\Documents\Velocity-workflow"
$bin = "$base\target\release"
$script:results = @()
$script:testsPassed = 0
$script:testsFailed = 0

function Test-Feature {
    param([string]$Name, [scriptblock]$Test)
    try {
        $result = & $Test
        if ($result) {
            Write-Host "  [PASS] $Name" -ForegroundColor Green
            $script:testsPassed++
            return $true
        } else {
            Write-Host "  [FAIL] $Name" -ForegroundColor Red
            $script:testsFailed++
            return $false
        }
    } catch {
        Write-Host "  [FAIL] $Name - $_" -ForegroundColor Red
        $script:testsFailed++
        return $false
    }
}

function Test-Server {
    param([string]$Name, [string]$Binary, [string[]]$ServerArgs, [int]$HealthPort, [string]$Token)
    
    Write-Host ""
    Write-Host "=== Testing: $Name ===" -ForegroundColor Cyan
    
    $proc = Start-Process -FilePath $Binary -ArgumentList $ServerArgs -PassThru -NoNewWindow
    Start-Sleep -Seconds 3
    
    try {
        # Health endpoint
        Test-Feature "Health endpoint" {
            $h = Invoke-RestMethod -Uri "http://127.0.0.1:$HealthPort/health" -TimeoutSec 5
            $h.status -eq "ok"
        }
        
        # Readiness endpoint
        Test-Feature "Readiness endpoint" {
            $r = Invoke-RestMethod -Uri "http://127.0.0.1:$HealthPort/ready" -TimeoutSec 5
            $r.status -eq "ready"
        }
        
        # Metrics without auth (should fail)
        Test-Feature "Metrics requires auth" {
            try {
                $null = Invoke-WebRequest -Uri "http://127.0.0.1:$HealthPort/metrics" -UseBasicParsing -TimeoutSec 5
                $false
            } catch {
                $_.Exception.Response.StatusCode.value__ -eq 401
            }
        }
        
        # Metrics with wrong token (should fail)
        Test-Feature "Metrics rejects wrong token" {
            try {
                $headers = @{ "Authorization" = "Bearer wrong-token" }
                $null = Invoke-WebRequest -Uri "http://127.0.0.1:$HealthPort/metrics" -UseBasicParsing -Headers $headers -TimeoutSec 5
                $false
            } catch {
                $_.Exception.Response.StatusCode.value__ -eq 401
            }
        }
        
        # Metrics with correct token (should succeed)
        Test-Feature "Metrics accepts correct token" {
            $headers = @{ "Authorization" = "Bearer $Token" }
            $resp = Invoke-WebRequest -Uri "http://127.0.0.1:$HealthPort/metrics" -UseBasicParsing -Headers $headers -TimeoutSec 5
            $resp.Content -match "velocity_up 1"
        }
        
        $script:results += [PSCustomObject]@{ Server=$Name; Status="PASS" }
    } catch {
        Write-Host "  [FAIL] Server test failed: $_" -ForegroundColor Red
        $script:results += [PSCustomObject]@{ Server=$Name; Status="FAIL" }
    } finally {
        Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue
        Start-Sleep -Seconds 1
    }
}

# Test VCTP Server
Test-Server -Name "VCTP (UDP)" -Binary "$bin\velocity-server.exe" -ServerArgs @("--health-bind","127.0.0.1:19295","--vctp-port","19334","--http-bench-port","0","--wal-path","$base\target\hard-vctp.wal","--metrics-token","vctp-token") -HealthPort 19295 -Token "vctp-token"

# Test Classic Server
Test-Server -Name "Classic (NMCP)" -Binary "$bin\velocity-classic-server.exe" -ServerArgs @("--health-bind","127.0.0.1:19293","--ws-bind","127.0.0.1:19283","--shmem-path","$base\target\hard-classic.nmcp","--wal-path","$base\target\hard-classic.wal","--metrics-token","classic-token") -HealthPort 19293 -Token "classic-token"

# Test Embedded Server
Test-Server -Name "Embedded (NMCP)" -Binary "$bin\velocity-embedded-server.exe" -ServerArgs @("--health-bind","127.0.0.1:19294","--ws-bind","127.0.0.1:19284","--shmem-path","$base\target\hard-embedded.nmcp","--wal-path","$base\target\hard-embedded.wal","--metrics-token","embedded-token") -HealthPort 19294 -Token "embedded-token"

# Summary
Write-Host ""
Write-Host "=== Production Hardening Summary ===" -ForegroundColor Cyan
Write-Host "Tests passed: $script:testsPassed" -ForegroundColor Green
Write-Host "Tests failed: $script:testsFailed" -ForegroundColor $(if ($script:testsFailed -eq 0) { "Green" } else { "Red" })
$script:results | Format-Table -AutoSize

if ($script:testsFailed -eq 0) {
    Write-Host "ALL PRODUCTION HARDENING TESTS PASSED" -ForegroundColor Green
} else {
    Write-Host "SOME TESTS FAILED" -ForegroundColor Red
}

Remove-Item "$base\target\hard-*.wal" -Force -ErrorAction SilentlyContinue
Remove-Item "$base\target\hard-*.nmcp" -Force -ErrorAction SilentlyContinue
