# V.E.L.O.C.I.T.Y.-WorkFlow Reproducible Benchmark Suite Engine
# Usage: powershell -ExecutionPolicy Bypass -File ./benchmarks/run_reproducible_benchmarks.ps1

$ErrorActionPreference = "Stop"

Write-Host "=========================================================" -ForegroundColor Cyan
Write-Host " V.E.L.O.C.I.T.Y.-WorkFlow Reproducible Benchmark Engine " -ForegroundColor Cyan
Write-Host "=========================================================" -ForegroundColor Cyan

# 1. Build Rust FFI Core
Write-Host "`n[1/4] Building Rust velocity-workflow-core in Release mode..." -ForegroundColor Yellow
Set-Location "$PSScriptRoot/../velocity-workflow-core"
cargo build --release
if ($LASTEXITCODE -ne 0) { throw "Rust build failed." }

# 2. Build .NET Benchmarks Solution
Write-Host "`n[2/4] Building .NET 10.0 Benchmarks Solution in Release mode..." -ForegroundColor Yellow
Set-Location "$PSScriptRoot/.."
dotnet build -c Release
if ($LASTEXITCODE -ne 0) { throw ".NET build failed." }

# 3. Copy Native DLL to Output
Write-Host "`n[3/4] Packaging native DLL to executable output path..." -ForegroundColor Yellow
Copy-Item "$PSScriptRoot/../velocity-workflow-core/target/release/velocity_workflow_core.dll" "$PSScriptRoot/Velocity.Workflow.Benchmarks/bin/Release/net10.0/velocity_workflow_core.dll" -Force

# 4. Execute Crash Recovery Fuzzing Benchmark
Write-Host "`n[4/4] Executing 1,000-pass Process Crash & State Resumption Fuzzing Suite..." -ForegroundColor Yellow
$fuzzOutput = & "$PSScriptRoot/Velocity.Workflow.Benchmarks/bin/Release/net10.0/Velocity.Workflow.Benchmarks.exe" --fuzz
Write-Host $fuzzOutput -ForegroundColor Green

Write-Host "`n[SUCCESS] All benchmark tests completed successfully!" -ForegroundColor Green
