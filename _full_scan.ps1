$root = "E:\Temporal-V2\VELOCITY-WorkFlow"
$exts = @("*.cs","*.rs","*.fs","*.toml","*.csproj","*.fsproj","*.slnx","*.ps1","*.md","*.yaml","*.yml","*.html","*.css","*.js","*.tpl","*.json","*.sql","*.go","*.py","*.ts","*.java","*.gradle","*.mod","*.txt","*.sh","*.rb","*.php")
$exclude = 'node_modules|\.git\\|target\\|bin\\|obj\\|\.pytest_cache|package-lock|BenchmarkDotNet'

$files = Get-ChildItem -Path $root -Recurse -File -Include $exts | Where-Object { $_.FullName -notmatch $exclude }
$totalLoc = 0
$totalFiles = 0
foreach ($f in $files) {
    $lines = (Get-Content $f.FullName -ErrorAction SilentlyContinue | Measure-Object -Line).Lines
    $totalLoc += $lines
    $totalFiles++
}
Write-Host "---"
Write-Host "TOTAL: $totalFiles files, $totalLoc LOC"
