#!/bin/sh
# Storage density audit - measure persistence footprint per engine
# Run from host, uses docker exec

echo "============================================="
echo "  STORAGE DENSITY AUDIT"
echo "  Persistence footprint per workflow engine"
echo "============================================="
echo ""

# --- Velocity WAL sizes ---
echo "=== VELOCITY WAL FILES ==="

echo "--- Classic (working dir, no volume) ---"
docker exec bench-velocity-classic sh -c 'find / -name "*.wal" -type f -exec ls -lh {} \; 2>/dev/null' | head -5
docker exec bench-velocity-classic sh -c 'ls -lh /app/velocity-workflow-server/*.wal 2>/dev/null || ls -lh /velocity.wal 2>/dev/null || echo "No WAL in CWD"'

echo ""
echo "--- Runtime (/data/runtime.wal on volume) ---"
docker exec bench-velocity-runtime ls -lh /data/runtime.wal 2>/dev/null || echo "No runtime WAL found"

echo ""
echo "--- Embedded (/data/embedded.wal on volume) ---"
docker exec bench-velocity-embedded ls -lh /data/embedded.wal 2>/dev/null || echo "No embedded WAL found"

echo ""
echo "=== VELOCITY VOLUME SIZES ==="
# Get actual disk usage of the volumes
docker run --rm -v bench-suite_runtime-data:/data:ro -v bench-suite_embedded-data:/embedded:ro alpine sh -c 'echo "Runtime volume:"; du -sh /data; echo "Embedded volume:"; du -sh /embedded'

echo ""
echo "=== DBOS POSTGRESQL ==="
docker exec bench-dbos-postgres sh -c 'psql -U dbos -d dbos_bench -t -c "SELECT pg_size_pretty(pg_database_size('"'"'dbos_bench'"'"'));"'
echo "DBOS top tables:"
docker exec bench-dbos-postgres sh -c 'psql -U dbos -d dbos_bench -t -c "SELECT relname, pg_size_pretty(pg_total_relation_size(oid)) FROM pg_class WHERE relkind='"'"'r'"'"' ORDER BY pg_total_relation_size(oid) DESC LIMIT 10;"'
echo "DBOS volume:"
docker run --rm -v bench-suite_dbos-pg-data:/pgdata:ro alpine du -sh /pgdata 2>/dev/null

echo ""
echo "=== TEMPORAL POSTGRESQL ==="
docker exec bench-temporal-postgres sh -c 'psql -U temporal -d temporal -t -c "SELECT pg_size_pretty(pg_database_size('"'"'temporal'"'"'));"'
echo "Temporal top tables:"
docker exec bench-temporal-postgres sh -c 'psql -U temporal -d temporal -t -c "SELECT relname, pg_size_pretty(pg_total_relation_size(oid)) FROM pg_class WHERE relkind='"'"'r'"'"' ORDER BY pg_total_relation_size(oid) DESC LIMIT 10;"'
echo "Temporal volume:"
docker run --rm -v bench-suite_temporal-pg-data:/pgdata:ro alpine du -sh /pgdata 2>/dev/null

echo ""
echo "=== RESTATE DATA ==="
docker exec bench-restate-server sh -c 'find / -maxdepth 5 -type d -name "restate*" 2>/dev/null | head -5'
docker exec bench-restate-server sh -c 'du -sh /var/lib/restate 2>/dev/null || du -sh /tmp/restate* 2>/dev/null || echo "Checking data dir..."'
docker exec bench-restate-server sh -c 'ls -la /var/lib/restate/ 2>/dev/null; du -sh /var/lib/restate/ 2>/dev/null'

echo ""
echo "=== CONTAINER LAYER SIZES ==="
# Approximate container writable layer sizes
docker system df -v 2>/dev/null | head -30
