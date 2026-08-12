#!/bin/bash
set -e

echo "=== Setting up DBOS on DBOS VM ==="

# Install DBOS Python SDK
echo "Installing DBOS..."
pip3 install dbos 2>&1 | tail -5

echo "---"
python3 -c "import dbos; print('DBOS version:', dbos.__version__)" 2>&1 || echo "DBOS import failed"

# Check PostgreSQL
echo "---"
echo "Checking PostgreSQL..."
sudo systemctl status postgresql 2>&1 | head -5 || pg_lsclusters 2>/dev/null || echo "PostgreSQL status unknown"
sudo -u postgres psql -c "SELECT 1" 2>&1 || echo "PostgreSQL connection failed"

# Create DBOS database
echo "---"
echo "Creating DBOS database..."
sudo -u postgres psql -c "CREATE DATABASE dbos_bench;" 2>/dev/null || echo "Database may already exist"
sudo -u postgres psql -c "CREATE USER dbos WITH PASSWORD 'dbos_bench';" 2>/dev/null || echo "User may already exist"
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE dbos_bench TO dbos;" 2>/dev/null || true
sudo -u postgres psql -d dbos_bench -c "GRANT ALL ON SCHEMA public TO dbos;" 2>/dev/null || true

echo "=== DBOS setup complete ==="
