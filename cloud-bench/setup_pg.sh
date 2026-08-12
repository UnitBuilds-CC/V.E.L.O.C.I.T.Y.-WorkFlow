#!/bin/bash
set -e
sudo systemctl start postgresql
sleep 2
sudo -u postgres psql -c "CREATE USER velbench WITH PASSWORD 'velbench' SUPERUSER;" 2>/dev/null || true
sudo -u postgres psql -c "CREATE DATABASE velocity_bench OWNER velbench;" 2>/dev/null || true
echo "PG_READY"
pg_isready
