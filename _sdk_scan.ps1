$root = 'E:\Temporal-V2\VELOCITY-WorkFlow\sdk'
$exts = @('*.go','*.py','*.ts','*.java','*.gradle','*.json','*.mod','*.txt')
$files = Get-ChildItem -Path $root -Recurse -File -Include $exts | Where-Object { $_.FullName -notmatch '__pycache__|node_modules|\.pytest_cache' }
$totalLoc = 0
$fileCount = 0
foreach ($f in $files) {
    $lines = (Get-Content $f.FullName | Measure-Object -Line).Lines
    $totalLoc += $lines
    $fileCount++
    Write-Output "$($f.FullName) | $lines LOC"
}
Write-Output "---"
Write-Output "SDK TOTAL: $fileCount files, $totalLoc LOC"
