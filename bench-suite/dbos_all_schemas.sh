#!/bin/sh
echo "=== ALL SCHEMAS ==="
psql -U dbos -d dbos_bench -t -A -c "SELECT schema_name FROM information_schema.schemata;"
echo "=== ALL USER TABLES (all schemas) ==="
psql -U dbos -d dbos_bench -t -A -c "SELECT table_schema || '.' || table_name FROM information_schema.tables WHERE table_type='BASE TABLE' AND table_schema NOT IN ('pg_catalog', 'information_schema') ORDER BY table_schema, table_name;"
echo "=== DBOS PROCEDURE/FUNCTION COUNT ==="
psql -U dbos -d dbos_bench -t -A -c "SELECT COUNT(*) FROM pg_proc WHERE pronamespace=(SELECT oid FROM pg_namespace WHERE nspname='public');"
echo "=== VOLUME SIZE ==="
du -sh /var/lib/postgresql/data
du -sh /var/lib/postgresql/data/base/ 2>/dev/null
