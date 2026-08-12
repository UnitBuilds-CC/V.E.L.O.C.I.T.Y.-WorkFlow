#!/usr/bin/env bash
# run_migrations.sh — Apply all VELOCITY-WorkFlow migrations in order.
#
# Usage:
#   ./run_migrations.sh [PGHOST] [PGPORT] [PGDATABASE] [PGUSER]
#
# Environment variables (or positional args):
#   PGHOST      PostgreSQL host       (default: localhost)
#   PGPORT      PostgreSQL port       (default: 5432)
#   PGDATABASE  Database name         (default: velocity_workflow)
#   PGUSER      Database user         (default: velocity)
#   PGPASSWORD  Database password     (default: <empty>)
#
# The script runs each migration inside a transaction. If any migration
# fails, the script stops and reports the failing migration number.

set -euo pipefail

PGHOST="${1:-${PGHOST:-localhost}}"
PGPORT="${2:-${PGPORT:-5432}}"
PGDATABASE="${3:-${PGDATABASE:-velocity_workflow}}"
PGUSER="${4:-${PGUSER:-velocity}}"

export PGHOST PGPORT PGDATABASE PGUSER PGPASSWORD

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MIGRATION_FILES=("${SCRIPT_DIR}"/[0-9]*_*.sql)

if [ ${#MIGRATION_FILES[@]} -eq 0 ]; then
    echo "ERROR: No migration files found in ${SCRIPT_DIR}"
    exit 1
fi

echo "=== VELOCITY-WorkFlow Migration Runner ==="
echo "Host: ${PGHOST}:${PGPORT}  Database: ${PGDATABASE}  User: ${PGUSER}"
echo "Found ${#MIGRATION_FILES[@]} migration(s)"
echo ""

APPLIED=0
SKIPPED=0
FAILED=0

for migration in "${MIGRATION_FILES[@]}"; do
    BASENAME=$(basename "$migration")
    VERSION=$(echo "$BASENAME" | grep -oE '^[0-9]+')

    # Check if already applied
    ALREADY=$(psql -tAc "SELECT COUNT(*) FROM schema_version WHERE version = ${VERSION};" 2>/dev/null || echo "0")

    if [ "$ALREADY" -gt 0 ]; then
        echo "[SKIP] ${BASENAME} (already applied)"
        SKIPPED=$((SKIPPED + 1))
        continue
    fi

    echo -n "[APPLYING] ${BASENAME} ... "
    START_MS=$(date +%s%3N 2>/dev/null || date +%s)

    if psql -v ON_ERROR_STOP=1 -f "$migration" > /dev/null 2>&1; then
        END_MS=$(date +%s%3N 2>/dev/null || date +%s)
        DURATION=$((END_MS - START_MS))
        echo "OK (${DURATION}ms)"
        APPLIED=$((APPLIED + 1))
    else
        echo "FAILED"
        FAILED=$((FAILED + 1))
        echo "ERROR: Migration ${BASENAME} failed. Stopping."
        exit 1
    fi
done

echo ""
echo "=== Migration Summary ==="
echo "Applied: ${APPLIED}  Skipped: ${SKIPPED}  Failed: ${FAILED}"

if [ "$FAILED" -gt 0 ]; then
    exit 1
fi

echo "All migrations completed successfully."
