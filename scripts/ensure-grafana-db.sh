#!/bin/bash
# Ensure Grafana database and user exist
# This script runs on every container startup to handle cases where
# the PostgreSQL volume already exists (init scripts don't run)

set -e

POSTGRES_USER="${POSTGRES_GRAFANA_USER:-grafana}"
POSTGRES_PASSWORD="${POSTGRES_GRAFANA_PASSWORD:-grafana}"
POSTGRES_DB="${POSTGRES_GRAFANA_DB:-grafana}"
PGHOST="${PGHOST:-postgres-timescale}"
PGPORT="${PGPORT:-5432}"
PGPASSWORD="${POSTGRES_PASSWORD}"

export PGHOST
export PGPORT
export PGPASSWORD

# Install postgresql-client if not available (for Grafana container)
if ! command -v psql >/dev/null 2>&1; then
    echo "Installing postgresql-client..."
    apt-get update && apt-get install -y postgresql-client && rm -rf /var/lib/apt/lists/*
fi

# Wait for PostgreSQL to be ready
until pg_isready -h "$PGHOST" -p "$PGPORT" -U "$POSTGRES_USER" 2>/dev/null; do
  echo "Waiting for PostgreSQL to be ready at $PGHOST:$PGPORT..."
  sleep 1
done

echo "PostgreSQL is ready. Ensuring grafana database exists..."

# Since the init scripts don't run when data directory exists, we need to create the database manually
# Try different approaches to create the database

echo "Attempting to create grafana database..."

# Method 1: Try to connect without specifying a database (connects to default)
if psql -h "$PGHOST" -p "$PGPORT" --username "$POSTGRES_USER" -c "CREATE DATABASE $POSTGRES_DB OWNER $POSTGRES_USER;" 2>/dev/null; then
    echo "Grafana database created successfully (method 1)."
elif psql -h "$PGHOST" -p "$PGPORT" --username "$POSTGRES_USER" --dbname "template1" -c "CREATE DATABASE $POSTGRES_DB OWNER $POSTGRES_USER;" 2>/dev/null; then
    echo "Grafana database created successfully (method 2)."
else
    echo "Database creation failed, checking if it already exists..."
fi

# Wait for the grafana database to be available
echo "Waiting for grafana database to be available..."
for i in {1..30}; do
    if psql -h "$PGHOST" -p "$PGPORT" --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -c "SELECT 1;" >/dev/null 2>&1; then
        echo "Grafana database is available."
        break
    else
        echo "Attempt $i: Grafana database not yet available, waiting..."
        sleep 2
    fi
done

# Final check - if we can't connect after 30 attempts, exit
if ! psql -h "$PGHOST" -p "$PGPORT" --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -c "SELECT 1;" >/dev/null 2>&1; then
    echo "ERROR: Cannot connect to grafana database after 60 seconds. Exiting."
    exit 1
fi

# Now connect to the grafana database to set permissions
psql -v ON_ERROR_STOP=1 -h "$PGHOST" -p "$PGPORT" --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    -- Verify we're connected to the correct database
    SELECT current_database(), current_user;
    
    -- Grant schema privileges (in case they're not set)
    GRANT ALL ON SCHEMA public TO $POSTGRES_USER;
    
    -- Grant privileges on existing tables and sequences
    GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO $POSTGRES_USER;
    GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO $POSTGRES_USER;
    
    -- Set default privileges for future objects
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO $POSTGRES_USER;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO $POSTGRES_USER;
EOSQL

echo "✅ Grafana database verified and permissions ensured successfully!"

