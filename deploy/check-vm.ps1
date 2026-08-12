$token = (& 'C:\Program Files (x86)\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd' auth print-access-token 2>&1 | Where-Object { $_ -notmatch "^WARNING" } | Select-Object -Last 1).Trim()
$headers = @{ Authorization = "Bearer $token" }

# Check operation status
$opUri = "https://compute.googleapis.com/compute/v1/projects/velocity-live-test-001/zones/us-central1-a/operations/operation-1786386816529-658b597a17485-d607c474-290a2772"
$op = Invoke-RestMethod -Uri $opUri -Method Get -Headers $headers
Write-Host "Operation status: $($op.status)"

if ($op.error) {
    Write-Host "Errors:" -ForegroundColor Red
    $op.error.errors | ForEach-Object { Write-Host "  $($_.code): $($_.message)" -ForegroundColor Red }
}

# Check if VM exists
Start-Sleep -Seconds 5
$vmUri = "https://compute.googleapis.com/compute/v1/projects/velocity-live-test-001/zones/us-central1-a/instances/velocity-classic"
try {
    $vm = Invoke-RestMethod -Uri $vmUri -Method Get -Headers $headers
    Write-Host "VM status: $($vm.status)" -ForegroundColor Green
    $ip = $vm.networkInterfaces[0].accessConfigs[0].natIP
    Write-Host "External IP: $ip" -ForegroundColor Green
} catch {
    Write-Host "VM not ready yet, waiting 20 more seconds..."
    Start-Sleep -Seconds 20
    try {
        $vm = Invoke-RestMethod -Uri $vmUri -Method Get -Headers $headers
        Write-Host "VM status: $($vm.status)" -ForegroundColor Green
        $ip = $vm.networkInterfaces[0].accessConfigs[0].natIP
        Write-Host "External IP: $ip" -ForegroundColor Green
    } catch {
        Write-Host "VM still not available. Check console." -ForegroundColor Yellow
    }
}
