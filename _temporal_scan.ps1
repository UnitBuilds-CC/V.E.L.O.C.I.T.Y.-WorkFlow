$files = Get-ChildItem -Path 'E:\Temporal-V2\temporal' -Recurse -File -Include '*.go','*.proto' | Where-Object { $_.FullName -notmatch 'vendor|third_party|\.git' }
$loc = 0
$count = 0
foreach ($f in $files) {
    $loc += (Get-Content $f.FullName | Measure-Object -Line).Lines
    $count++
}
Write-Output "Temporal Go+Proto: $count files, $loc LOC"

# Also count Temporal SDK repos if present
$sdkDirs = @('client','service','common','api')
foreach ($d in $sdkDirs) {
    $path = "E:\Temporal-V2\temporal\$d"
    if (Test-Path $path) {
        $sub = Get-ChildItem -Path $path -Recurse -File -Include '*.go' | Where-Object { $_.FullName -notmatch 'vendor|third_party' }
        $subLoc = 0; $subCount = 0
        foreach ($f in $sub) { $subLoc += (Get-Content $f.FullName | Measure-Object -Line).Lines; $subCount++ }
        Write-Output "  $d/: $subCount files, $subLoc LOC"
    }
}
