#!/bin/bash
# Create stockdata database for TimescaleDB
# This must be run separately because CREATE DATABASE cannot be in a DO block

set -e

POSTGRES_USER="${POSTGRES_USER:-postgres}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-postgres}"
PGHOST="${PGHOST:-localhost}"
PGPORT="${PGPORT:-5432}"

export PGPASSWORD="${POSTGRES_PASSWORD}"

# Wait for PostgreSQL to be ready
until pg_isready -h "$PGHOST" -p "$PGPORT" -U "$POSTGRES_USER" 2>/dev/null; do
  echo "Waiting for PostgreSQL to be ready at $PGHOST:$PGPORT..."
  sleep 1
done

echo "Creating stockdata database..."

# Create database if it doesn't exist
psql -h "$PGHOST" -p "$PGPORT" -U "$POSTGRES_USER" -d postgres <<-EOSQL
    SELECT 'CREATE DATABASE stockdata'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'stockdata')\gexec
EOSQL

echo "✅ stockdata database created/verified successfully!"

