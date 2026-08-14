$token = gcloud auth print-access-token 2>&1
Write-Host "Got fresh token"
$kubeconfigPath = "c:\Users\visse\OneDrive\Documents\Velocity-workflow\.kube\config"
$content = Get-Content $kubeconfigPath -Raw
$content = $content -replace 'token: ya29\.[A-Za-z0-9_-]+', "token: $token"
Set-Content $kubeconfigPath $content -NoNewline
Write-Host "Updated kubeconfig with fresh token"
