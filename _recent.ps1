$root = 'E:\Temporal-V2\VELOCITY-WorkFlow'
$exts = @('*.cs','*.rs','*.md','*.fs')
$files = Get-ChildItem -Path $root -Recurse -File -Include $exts | Where-Object { $_.FullName -notmatch 'target\\|bin\\|obj\\|\.git\\|node_modules' }
$sorted = $files | Sort-Object LastWriteTime -Descending | Select-Object -First 30
foreach ($f in $sorted) {
    $ts = $f.LastWriteTime.ToString('yyyy-MM-dd HH:mm')
    Write-Output "$ts  $($f.Length)  $($f.FullName)"
}
