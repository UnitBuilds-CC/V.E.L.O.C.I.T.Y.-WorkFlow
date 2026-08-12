$token = (& 'C:\Program Files (x86)\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd' auth print-access-token 2>&1 | Where-Object { $_ -notmatch "^WARNING" } | Select-Object -Last 1).Trim()
$project = "velocity-live-test-001"
$zone = "us-central1-a"
$uri = "https://compute.googleapis.com/compute/v1/projects/$project/zones/$zone/instances"

$body = @"
{
  "name": "velocity-classic",
  "machineType": "zones/$zone/machineTypes/e2-standard-4",
  "disks": [{
    "boot": true,
    "autoDelete": true,
    "initializeParams": {
      "sourceImage": "projects/ubuntu-os-cloud/global/images/family/ubuntu-2404-lts-amd64",
      "diskSizeGb": "50",
      "diskType": "zones/$zone/diskTypes/pd-balanced"
    }
  }],
  "networkInterfaces": [{
    "accessConfigs": [{
      "type": "ONE_TO_ONE_NAT",
      "name": "External NAT"
    }]
  }],
  "tags": {
    "items": ["http-server", "https-server"]
  }
}
"@

$headers = @{
    Authorization = "Bearer $token"
    "Content-Type" = "application/json"
}

Write-Host "Creating VM velocity-classic in $project/$zone..."
try {
    $response = Invoke-RestMethod -Uri $uri -Method Post -Headers $headers -Body $body
    Write-Host "VM creation status: $($response.status)" -ForegroundColor Green
    Write-Host "Operation: $($response.selfLink)"
} catch {
    Write-Host "Error: $($_.Exception.Message)" -ForegroundColor Red
    if ($_.ErrorDetails.Message) {
        Write-Host "Details: $($_.ErrorDetails.Message)"
    }
}
