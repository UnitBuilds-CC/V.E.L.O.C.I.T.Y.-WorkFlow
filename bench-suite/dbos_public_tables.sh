#!/bin/sh
echo "=== DBOS PUBLIC TABLES ==="
psql -U dbos -d dbos_bench -t -A -c "SELECT relname || ':' || pg_total_relation_size(oid) FROM pg_class WHERE relkind='r' AND relnamespace=(SELECT oid FROM pg_namespace WHERE nspname='public') ORDER BY pg_total_relation_size(oid) DESC;"
echo "=== DBOS PUBLIC ROW COUNTS ==="
for t in $(psql -U dbos -d dbos_bench -t -A -c "SELECT relname FROM pg_class WHERE relkind='r' AND relnamespace=(SELECT oid FROM pg_namespace WHERE nspname='public') ORDER BY relname;"); do
  cnt=$(psql -U dbos -d dbos_bench -t -A -c "SELECT COUNT(*) FROM \"$t\";")
  echo "  $t: $cnt rows"
done
echo "=== DBOS VOLUME ON DISK ==="
du -sh /var/lib/postgresql/data 2>/dev/null
