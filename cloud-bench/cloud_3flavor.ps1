# =============================================================================
# cloud-bench\cloud_3flavor.ps1 — PowerShell wrapper for GCP 3-Flavor Benchmark
#
# Run from Windows workstation. Wraps cloud_3flavor.sh with GCP auth and config.
#
# Usage:
#   .\cloud-bench\cloud_3flavor.ps1
#   .\cloud-bench\cloud_3flavor.ps1 -Profile quick -Workloads smoke
#   .\cloud-bench\cloud_3flavor.ps1 -Cleanup  # delete VMs after benchmark
# =============================================================================

param(
    [ValidateSet("quick", "standard", "stress")]
    [string]$Profile = "standard",

    [ValidateSet("smoke", "all")]
    [string]$Workloads = "all",

    [string]$Project = "velocity-live-test-001",
    [string]$Zone = "us-east1-b",
    [switch]$SkipProvision,
    [switch]$Cleanup
)

$ErrorActionPreference = "Stop"

Write-Host ""
Write-Host "==================================================" -ForegroundColor Cyan
Write-Host "  3-Flavor Cloud Benchmark — PowerShell Wrapper" -ForegroundColor Cyan
Write-Host "==================================================" -ForegroundColor Cyan
Write-Host "  Project:   $Project"
Write-Host "  Zone:      $Zone"
Write-Host "  Profile:   $Profile"
Write-Host "  Workloads: $Workloads"
Write-Host ""

# ── Check gcloud ─────────────────────────────────────────────────────────────
$gcloud = Get-Command gcloud -ErrorAction SilentlyContinue
if (-not $gcloud) {
    $gcloud = Get-Command "C:\Program Files (x86)\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd" -ErrorAction SilentlyContinue
}
if (-not $gcloud) {
    Write-Host "ERROR: gcloud not found. Install Google Cloud SDK." -ForegroundColor Red
    exit 1
}
$gcloudCmd = $gcloud.Source

# ── Check auth ───────────────────────────────────────────────────────────────
Write-Host "Checking GCP authentication..." -ForegroundColor Yellow
$token = & $gcloudCmd auth print-access-token 2>$null
if (-not $token -or $LASTEXITCODE -ne 0) {
    Write-Host "ERROR: gcloud not authenticated. Run: gcloud auth login" -ForegroundColor Red
    exit 1
}
Write-Host "  Authenticated." -ForegroundColor Green

# ── Set project ──────────────────────────────────────────────────────────────
& $gcloudCmd config set project $Project 2>$null

# ── Check if WSL/bash available ─────────────────────────────────────────────
$bashCmd = $null
if (Get-Command bash -ErrorAction SilentlyContinue) {
    $bashCmd = "bash"
} elseif (Get-Command wsl -ErrorAction SilentlyContinue) {
    $bashCmd = "wsl"
} else {
    Write-Host "ERROR: bash/WSL not found. This script requires bash to run the benchmark." -ForegroundColor Red
    Write-Host "  Install WSL: wsl --install" -ForegroundColor Yellow
    exit 1
}

# ── Find repo root ──────────────────────────────────────────────────────────
$repoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
if (-not (Test-Path "$repoRoot\Cargo.toml")) {
    $repoRoot = (Get-Location).Path
}
Write-Host "  Repo root: $repoRoot"

# ── Build environment variables ──────────────────────────────────────────────
$env:GCP_PROJECT = $Project
$env:GCP_ZONE = $Zone
$env:BENCH_PROFILE = $Profile
$env:BENCH_WORKLOADS = $Workloads
$env:SKIP_PROVISION = if ($SkipProvision) { "true" } else { "false" }
$env:CLEANUP = if ($Cleanup) { "true" } else { "false" }

# ── Run the master script ───────────────────────────────────────────────────
$scriptPath = Join-Path $PSScriptRoot "cloud_3flavor.sh"
if (-not (Test-Path $scriptPath)) {
    Write-Host "ERROR: cloud_3flavor.sh not found at $scriptPath" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "Starting benchmark (this takes 15-45 minutes)..." -ForegroundColor Yellow
Write-Host ""

# Convert Windows path to WSL path if needed
$wslScriptPath = $scriptPath
if ($bashCmd -eq "wsl") {
    $wslScriptPath = & wsl wslpath -u ($scriptPath -replace '\\', '/')
}

& $bashCmd -c "cd '$repoRoot' && bash '$wslScriptPath'"

$exitCode = $LASTEXITCODE

Write-Host ""
if ($exitCode -eq 0) {
    Write-Host "==================================================" -ForegroundColor Green
    Write-Host "  Benchmark complete!" -ForegroundColor Green
    Write-Host "==================================================" -ForegroundColor Green
} else {
    Write-Host "==================================================" -ForegroundColor Red
    Write-Host "  Benchmark failed with exit code $exitCode" -ForegroundColor Red
    Write-Host "==================================================" -ForegroundColor Red
}

# ── List result files ────────────────────────────────────────────────────────
$resultsBase = if ($env:USERPROFILE) { $env:USERPROFILE } else { $env:HOME }
$recentResults = Get-ChildItem "$resultsBase/velocity-bench-results-*" -Directory -ErrorAction SilentlyContinue |
    Sort-Object LastWriteTime -Descending |
    Select-Object -First 1

if ($recentResults) {
    Write-Host ""
    Write-Host "Results directory: $($recentResults.FullName)" -ForegroundColor Cyan
    Get-ChildItem $recentResults.FullName -Recurse | ForEach-Object {
        Write-Host "  $($_.FullName) ($($_.Length) bytes)"
    }
}
