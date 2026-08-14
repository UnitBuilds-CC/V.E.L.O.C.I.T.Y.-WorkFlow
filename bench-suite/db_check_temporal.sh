#!/bin/sh
echo "DB_SIZE:$(psql -U temporal -d temporal -t -A -c "SELECT pg_database_size('temporal')")"
echo "DB_SIZE_PRETTY:$(psql -U temporal -d temporal -t -A -c "SELECT pg_size_pretty(pg_database_size('temporal'))")"
echo "=== TABLE SIZES (top 15) ==="
psql -U temporal -d temporal -t -A -c "SELECT relname || ':' || pg_total_relation_size(oid) FROM pg_class WHERE relkind='r' AND relnamespace=(SELECT oid FROM pg_namespace WHERE nspname='public') ORDER BY pg_total_relation_size(oid) DESC LIMIT 15;"
echo "=== USER TABLE ROW COUNTS ==="
for t in $(psql -U temporal -d temporal -t -A -c "SELECT relname FROM pg_class WHERE relkind='r' AND relnamespace=(SELECT oid FROM pg_namespace WHERE nspname='public') ORDER BY relname;"); do
  cnt=$(psql -U temporal -d temporal -t -A -c "SELECT COUNT(*) FROM \"$t\";")
  echo "  $t: $cnt rows"
done
