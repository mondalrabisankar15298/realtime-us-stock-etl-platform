#!/bin/bash

# Stop Real-Time Stock ETL Platform
# This script gracefully stops all services

set -e

echo "=========================================="
echo "Stopping Real-Time Stock ETL Platform"
echo "=========================================="
echo ""

echo "⚠️  Please manually stop Spark streaming jobs first (Ctrl+C in each terminal)"
echo "Waiting 10 seconds for you to stop them..."
sleep 10

echo ""
echo "Stopping Docker services..."
docker compose down

echo ""
echo "=========================================="
echo "System Stopped Successfully"
echo "=========================================="
echo ""
echo "Data is preserved in Docker volumes:"
echo "  - redpanda-data"
echo "  - spark-delta-tables"
echo "  - spark-checkpoints"
echo "  - postgres-airflow-data"
echo "  - postgres-grafana-data"
echo "  - grafana-data"
echo ""
echo "To remove all data, run: docker compose down -v"
echo ""

