$root = "E:\Temporal-V2\VELOCITY-WorkFlow"
$files = Get-ChildItem -Path $root -Recurse -File -Include *.rs,*.cs,*.fs,*.proto,*.py,*.go,*.ts,*.js,*.toml,*.csproj,*.fsproj,*.slnx | Where-Object { $_.FullName -notmatch '\\(target|bin|obj|node_modules|\.git)\\' }
$total = 0
$count = 0
foreach ($f in $files) {
    $lines = (Get-Content $f.FullName -ErrorAction SilentlyContinue | Measure-Object -Line).Lines
    $rel = $f.FullName.Replace("$root\", "")
    if ($lines -gt 20) {
        Write-Output "$lines`t$rel"
    }
    $total += $lines
    $count++
}
Write-Output "---"
Write-Output "TOTAL: $count files, $total LOC"
