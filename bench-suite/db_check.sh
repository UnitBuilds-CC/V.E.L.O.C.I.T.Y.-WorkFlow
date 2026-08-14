#!/bin/sh
echo "DB_SIZE:$(psql -U dbos -d dbos_bench -t -A -c "SELECT pg_database_size('dbos_bench')")"
echo "DB_SIZE_PRETTY:$(psql -U dbos -d dbos_bench -t -A -c "SELECT pg_size_pretty(pg_database_size('dbos_bench'))")"
echo "=== TABLE SIZES ==="
psql -U dbos -d dbos_bench -t -A -c "SELECT relname || ':' || pg_total_relation_size(oid) FROM pg_class WHERE relkind='r' ORDER BY pg_total_relation_size(oid) DESC LIMIT 10;"
echo "=== ROW COUNTS ==="
for t in $(psql -U dbos -d dbos_bench -t -A -c "SELECT relname FROM pg_class WHERE relkind='r' ORDER BY relname;"); do
  cnt=$(psql -U dbos -d dbos_bench -t -A -c "SELECT COUNT(*) FROM $t;")
  echo "  $t: $cnt rows"
done
