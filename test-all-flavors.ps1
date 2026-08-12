# Quick Test All 3 Velocity Flavors (Local)
# Prerequisites: Rust toolchain, Node.js, npm
# Usage: .\test-all-flavors.ps1

param(
    [switch]$SkipBuild,
    [switch]$ClassicOnly,
    [switch]$RuntimeOnly,
    [switch]$EmbeddedOnly
)

$ErrorActionPreference = "Continue"
$testResults = @()

function Write-Section($name) {
    Write-Host ""
    Write-Host "=== $name ===" -ForegroundColor Cyan
    Write-Host ""
}

function Test-Flavor($name, $path, $testCmd) {
    Write-Section "Testing $name"
    Push-Location $path
    try {
        if (-not $SkipBuild) {
            Write-Host "Installing dependencies..."
            npm install --silent 2>$null
        }
        Write-Host "Running tests..."
        & $testCmd
        if ($LASTEXITCODE -eq 0) {
            $script:testResults += [PSCustomObject]@{Flavor=$name; Status="PASS"; Path=$path}
            Write-Host "$name: PASS" -ForegroundColor Green
        } else {
            $script:testResults += [PSCustomObject]@{Flavor=$name; Status="FAIL"; Path=$path}
            Write-Host "$name: FAIL" -ForegroundColor Red
        }
    } catch {
        $script:testResults += [PSCustomObject]@{Flavor=$name; Status="ERROR"; Path=$path; Error=$_.Exception.Message}
        Write-Host "$name: ERROR - $($_.Exception.Message)" -ForegroundColor Red
    }
    Pop-Location
}

# ─── Rust Engine ──────────────────────────────────────────────────────────────
if (-not ($ClassicOnly -or $RuntimeOnly -or $EmbeddedOnly)) {
    Write-Section "Rust Engine (All Flavors)"
    Push-Location $PSScriptRoot
    cargo test --workspace --quiet 2>&1 | Select-String "test result" | Select-Object -Last 5
    if ($LASTEXITCODE -eq 0) {
        $testResults += [PSCustomObject]@{Flavor="Rust Engine"; Status="PASS"}
    } else {
        $testResults += [PSCustomObject]@{Flavor="Rust Engine"; Status="FAIL"}
    }
    Pop-Location
}

# ─── Classic (Temporal-compatible) ────────────────────────────────────────────
if (-not ($RuntimeOnly -or $EmbeddedOnly)) {
    Test-Flavor "Classic (Temporal)" "$PSScriptRoot\velocity-classic-ts" { npm test --silent }
}

# ─── Runtime (Restate-compatible) ─────────────────────────────────────────────
if (-not ($ClassicOnly -or $EmbeddedOnly)) {
    Test-Flavor "Runtime (Restate)" "$PSScriptRoot\velocity-runtime-typescript" { npm test --silent }
}

# ─── Embedded (DBOS-compatible) ───────────────────────────────────────────────
if (-not ($ClassicOnly -or $RuntimeOnly)) {
    Test-Flavor "Embedded (DBOS)" "$PSScriptRoot\velocity-embedded-ts" { npm test --silent }
}

# ─── Summary ──────────────────────────────────────────────────────────────────
Write-Section "Test Summary"
$testResults | Format-Table -AutoSize

$passed = ($testResults | Where-Object Status -eq "PASS").Count
$failed = ($testResults | Where-Object Status -ne "PASS").Count

Write-Host "Passed: $passed" -ForegroundColor Green
if ($failed -gt 0) {
    Write-Host "Failed: $failed" -ForegroundColor Red
}

Write-Host ""
Write-Host "To start a live dev server for manual testing:"
Write-Host "  cargo run --bin velocity-dev -- --port 7233 --grpc-port 7234 --ui-port 8233"
Write-Host ""
Write-Host "Then access:"
Write-Host "  HTTP API:  http://localhost:7233"
Write-Host "  gRPC:      http://localhost:7234"
Write-Host "  Web UI:    http://localhost:8233"
