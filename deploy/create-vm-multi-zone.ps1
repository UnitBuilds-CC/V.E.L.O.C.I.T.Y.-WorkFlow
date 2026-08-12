$token = (& 'C:\Program Files (x86)\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd' auth print-access-token 2>&1 | Where-Object { $_ -notmatch "^WARNING" } | Select-Object -Last 1).Trim()
$headers = @{ Authorization = "Bearer $token"; "Content-Type" = "application/json" }
$project = "velocity-live-test-001"

# Try multiple zones
$zones = @("us-central1-b", "us-central1-c", "us-central1-f", "us-east1-b", "us-east1-c", "us-east4-a")

foreach ($zone in $zones) {
    Write-Host "Trying zone: $zone..." -ForegroundColor Cyan
    
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

    $uri = "https://compute.googleapis.com/compute/v1/projects/$project/zones/$zone/instances"
    
    try {
        $response = Invoke-RestMethod -Uri $uri -Method Post -Headers $headers -Body $body
        Write-Host "VM creation PENDING in $zone!" -ForegroundColor Green
        
        # Wait for it to be ready
        Write-Host "Waiting for VM to start..."
        Start-Sleep -Seconds 20
        
        $vmUri = "https://compute.googleapis.com/compute/v1/projects/$project/zones/$zone/instances/velocity-classic"
        $vm = Invoke-RestMethod -Uri $vmUri -Method Get -Headers $headers
        $ip = $vm.networkInterfaces[0].accessConfigs[0].natIP
        Write-Host ""
        Write-Host "=== VM Ready ===" -ForegroundColor Green
        Write-Host "Zone: $zone"
        Write-Host "External IP: $ip"
        Write-Host "Status: $($vm.status)"
        
        # Save zone and IP for后续 scripts
        $vm | ConvertTo-Json | Set-Content "c:\Users\visse\OneDrive\Documents\Velocity-workflow\deploy\vm-info.json"
        Write-Host "VM info saved to deploy\vm-info.json"
        
        # Create firewall rule
        Write-Host ""
        Write-Host "Creating firewall rules..." -ForegroundColor Cyan
        $fwUri = "https://compute.googleapis.com/compute/v1/projects/$project/global/firewalls"
        $fwBody = @"
{
  "name": "velocity-ports",
  "allowed": [{
    "IPProtocol": "tcp",
    "ports": ["5000", "50051", "3000", "9090", "8080", "7233", "7234", "8233"]
  }],
  "targetTags": ["http-server"],
  "description": "Velocity service ports"
}
"@
        try {
            Invoke-RestMethod -Uri $fwUri -Method Post -Headers $headers -Body $fwBody | Out-Null
            Write-Host "Firewall rule created" -ForegroundColor Green
        } catch {
            Write-Host "Firewall rule may already exist" -ForegroundColor Yellow
        }
        
        break  # Success, exit the loop
    } catch {
        $errMsg = $_.Exception.Message
        if ($errMsg -match "ZONE_RESOURCE_POOL_EXHAUSTED" -or $errMsg -match "does not have enough resources") {
            Write-Host "  Zone $zone exhausted, trying next..." -ForegroundColor Yellow
            continue
        } elseif ($errMsg -match "alreadyExists") {
            Write-Host "  VM already exists in this zone, checking..." -ForegroundColor Yellow
            $vmUri = "https://compute.googleapis.com/compute/v1/projects/$project/zones/$zone/instances/velocity-classic"
            try {
                $vm = Invoke-RestMethod -Uri $vmUri -Method Get -Headers $headers
                $ip = $vm.networkInterfaces[0].accessConfigs[0].natIP
                Write-Host "VM found: $ip in $zone" -ForegroundColor Green
                $vm | ConvertTo-Json | Set-Content "c:\Users\visse\OneDrive\Documents\Velocity-workflow\deploy\vm-info.json"
                break
            } catch {
                continue
            }
        } else {
            Write-Host "  Error: $errMsg" -ForegroundColor Red
            continue
        }
    }
}
