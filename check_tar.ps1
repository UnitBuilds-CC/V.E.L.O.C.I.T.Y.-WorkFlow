$tarb = "c:\Users\visse\OneDrive\Documents\Velocity-workflow\bench-suite-deploy.tar.gz"
if (Test-Path $tarb) {
    Write-Host "EXISTS"
    $item = Get-Item $tarb
    Write-Host "Size: $($item.Length) bytes"
    Write-Host "Listing first 30 entries..."
    tar tzf $tarb 2>$null | Select-Object -First 30
} else {
    Write-Host "NOT_FOUND"
}
