$root = 'E:\Temporal-V2\VELOCITY-WorkFlow'
$exts = @('*.cs','*.rs','*.fs','*.toml','*.csproj','*.fsproj','*.slnx','*.ps1','*.md')
$files = Get-ChildItem -Path $root -Recurse -File -Include $exts | Where-Object { $_.FullName -notmatch 'target\\|bin\\|obj\\|\.git\\|node_modules' }
$totalLoc = 0
$fileCount = 0
foreach ($f in $files) {
    $lines = (Get-Content $f.FullName | Measure-Object -Line).Lines
    $totalLoc += $lines
    $fileCount++
    Write-Output "$($f.FullName) | $lines LOC"
}
Write-Output "---"
Write-Output "TOTAL: $fileCount files, $totalLoc LOC"
