#!/bin/sh
echo "=== DBOS SCHEMA TABLES ==="
psql -U dbos -d dbos_bench -t -A -c "SELECT relname || ':' || pg_total_relation_size(oid) FROM pg_class WHERE relkind='r' AND relnamespace=(SELECT oid FROM pg_namespace WHERE nspname='dbos') ORDER BY pg_total_relation_size(oid) DESC;"
echo "=== DBOS ROW COUNTS ==="
for t in $(psql -U dbos -d dbos_bench -t -A -c "SELECT relname FROM pg_class WHERE relkind='r' AND relnamespace=(SELECT oid FROM pg_namespace WHERE nspname='dbos');"); do
  cnt=$(psql -U dbos -d dbos_bench -t -A -c "SELECT COUNT(*) FROM dbos.\"$t\";")
  sz=$(psql -U dbos -d dbos_bench -t -A -c "SELECT pg_size_pretty(pg_total_relation_size('dbos.\"$t\"'::regclass));")
  echo "  dbos.$t: $cnt rows ($sz)"
done
