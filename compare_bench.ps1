$v = Get-Content "velocity_final.json" | ConvertFrom-Json
$t = Get-Content "temporal_final.json" | ConvertFrom-Json

Write-Host "=== FAIR COMPARISON: VELOCITY vs TEMPORAL (Both direct-state HashMap mocks) ==="
Write-Host ""
Write-Host ("{0,-25} {1,12} {2,12} {3,12} {4,12} {5,10}" -f "Workload", "V ops/s", "T ops/s", "V p99(us)", "T p99(us)", "Winner")
Write-Host ("=" * 85)

$vWins = 0; $tWins = 0

# Build a map of temporal results by workload name
$temporalMap = @{}
foreach ($row in $t.rows) {
    $temporalMap[$row.workload_name] = $row
}

foreach ($vRow in $v.rows) {
    $tRow = $temporalMap[$vRow.workload_name]
    if ($tRow) {
        $vOps = [math]::Round($vRow.velocity_ops_per_sec, 0)
        $tOps = [math]::Round($tRow.temporal_ops_per_sec, 0)
        $vLat = $vRow.velocity_p99_us
        $tLat = $tRow.temporal_p99_us
        
        # For velocity-only rows, temporal is 0 — use velocity's temporal column
        # For temporal-only rows, velocity is 0 — use temporal's velocity column
        $vOpsDisplay = if ($vRow.velocity_ops_per_sec -gt 0) { [math]::Round($vRow.velocity_ops_per_sec, 0) } else { "-" }
        $tOpsDisplay = if ($tRow.temporal_ops_per_sec -gt 0) { [math]::Round($tRow.temporal_ops_per_sec, 0) } else { "-" }
        $vLatDisplay = if ($vRow.velocity_p99_us -gt 0) { $vRow.velocity_p99_us } else { "-" }
        $tLatDisplay = if ($tRow.temporal_p99_us -gt 0) { $tRow.temporal_p99_us } else { "-" }
        
        # Determine winner
        $vActual = if ($vRow.velocity_ops_per_sec -gt 0) { $vRow.velocity_ops_per_sec } else { 0 }
        $tActual = if ($tRow.temporal_ops_per_sec -gt 0) { $tRow.temporal_ops_per_sec } else { 0 }
        
        if ($vActual -gt $tActual -and $tActual -gt 0) { $winner = "VELOCITY"; $vWins++ }
        elseif ($tActual -gt $vActual -and $vActual -gt 0) { $winner = "Temporal"; $tWins++ }
        elseif ($vActual -gt 0 -and $tActual -eq 0) { $winner = "V-only" }
        elseif ($tActual -gt 0 -and $vActual -eq 0) { $winner = "T-only" }
        else { $winner = "TIE" }
        
        Write-Host ("{0,-25} {1,12} {2,12} {3,12} {4,12} {5,10}" -f $vRow.workload_name, $vOpsDisplay, $tOpsDisplay, $vLatDisplay, $tLatDisplay, $winner)
    }
}

Write-Host ""
Write-Host "Velocity wins: $vWins | Temporal wins: $tWins"
