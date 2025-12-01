#!/bin/bash
# Trigger ETL Pipeline Orchestration
# This script manually triggers the ETL pipeline DAG to coordinate Bronze → Silver → Gold

set -e

echo "🚀 Triggering ETL Pipeline Orchestration..."
echo "This will coordinate automatic data flow through all layers"
echo ""

# Check if Airflow is running
echo "Checking Airflow status..."
if ! docker compose ps airflow-webserver | grep -q "Up"; then
    echo "❌ Airflow is not running. Please start the services first:"
    echo "   docker compose up -d"
    exit 1
fi

# Wait for Airflow to be ready
echo "Waiting for Airflow to be ready..."
sleep 10

# Trigger the ETL pipeline DAG
echo "Triggering ETL pipeline DAG..."
docker compose exec airflow-webserver airflow dags trigger etl_pipeline_orchestration

echo ""
echo "✅ ETL Pipeline orchestration triggered!"
echo ""
echo "Monitor the pipeline:"
echo "1. Airflow UI: http://localhost:6009 (admin/admin)"
echo "2. Check DAG: etl_pipeline_orchestration"
echo "3. View logs for each layer:"
echo "   - Bronze: docker compose logs spark-bronze --follow"
echo "   - Silver: docker compose logs spark-silver --follow"
echo "   - Gold: docker compose logs spark-gold --follow"
echo ""
echo "The pipeline will now automatically:"
echo "1. Monitor Bronze layer data flow"
echo "2. Start Silver layer when Bronze is active"
echo "3. Start Gold layer when Silver is active"
echo "4. Collect and report pipeline metrics"
