# Storage density audit - measure persistence footprint per engine
Write-Host "=== VELOCITY WAL FILES ===" -ForegroundColor Cyan

# Velocity Classic WAL
$wal = docker exec bench-velocity-classic find / -name "*.wal" -type f 2>$null | Select-Object -First 1
if ($wal) {
    $sz = docker exec bench-velocity-classic ls -l $wal 2>$null
    Write-Host "  Classic: $sz"
} else {
    Write-Host "  Classic: WAL in container working dir (no volume mount)"
    docker exec bench-velocity-classic ls -lh /velocity.wal 2>$null
    docker exec bench-velocity-classic ls -lh /app/velocity.wal 2>$null
}

# Velocity Runtime WAL  
$sz = docker exec bench-velocity-runtime ls -lh /data/runtime.wal 2>$null
Write-Host "  Runtime: $sz"

# Velocity Embedded WAL
$sz = docker exec bench-velocity-embedded ls -lh /data/embedded.wal 2>$null
Write-Host "  Embedded: $sz"

Write-Host ""
Write-Host "=== DBOS POSTGRESQL ===" -ForegroundColor Cyan
docker exec bench-dbos-postgres sh -c "psql -U dbos -d dbos_bench -t -c ""SELECT pg_size_pretty(pg_database_size('dbos_bench'));""" 2>$null
docker exec bench-dbos-postgres sh -c "psql -U dbos -d dbos_bench -t -c ""SELECT relname, pg_size_pretty(pg_total_relation_size(oid)) as size FROM pg_class WHERE relkind='r' ORDER BY pg_total_relation_size(oid) DESC LIMIT 10;""" 2>$null

Write-Host ""
Write-Host "=== TEMPORAL POSTGRESQL ===" -ForegroundColor Cyan
docker exec bench-temporal-postgres sh -c "psql -U temporal -d temporal -t -c ""SELECT pg_size_pretty(pg_database_size('temporal'));""" 2>$null
docker exec bench-temporal-postgres sh -c "psql -U temporal -d temporal -t -c ""SELECT relname, pg_size_pretty(pg_total_relation_size(oid)) as size FROM pg_class WHERE relkind='r' ORDER BY pg_total_relation_size(oid) DESC LIMIT 10;""" 2>$null

Write-Host ""
Write-Host "=== RESTATE SERVER DATA ===" -ForegroundColor Cyan
docker exec bench-restate-server du -sh /var/lib/restate 2>$null
docker exec bench-restate-server du -sh /tmp/restate* 2>$null

Write-Host ""
Write-Host "=== CONTAINER WRITABLE LAYERS ===" -ForegroundColor Cyan
$containers = @("bench-velocity-classic","bench-velocity-runtime","bench-velocity-embedded","bench-dbos-service","bench-dbos-postgres","bench-restate-server","bench-restate-service","bench-temporal-server","bench-temporal-service","bench-temporal-postgres")
foreach ($c in $containers) {
    $rw = docker inspect --format='{{.SizeRw}}' $c 2>$null
    if ($rw -and $rw -ne "0") {
        $mb = [math]::Round([long]$rw / 1MB, 2)
        Write-Host "  ${c}: ${mb} MB"
    }
}

Write-Host ""
Write-Host "=== DOCKER VOLUMES ===" -ForegroundColor Cyan
docker volume ls 2>$null | Select-String "bench"
