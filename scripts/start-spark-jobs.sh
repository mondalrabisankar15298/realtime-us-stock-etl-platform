#!/bin/bash

# Start All Spark Streaming Jobs Automatically
# This script starts all 3 Spark streaming jobs in the background

set -e

echo "=========================================="
echo "Starting Spark Streaming Jobs"
echo "=========================================="
echo ""

SPARK_MASTER="spark://spark-master:7077"
DELTA_PACKAGE="io.delta:delta-core_2.12:2.4.0"
KAFKA_PACKAGE="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"

# Check if Spark master is ready
echo "Waiting for Spark master to be ready..."
for i in {1..30}; do
    if docker exec spark-master curl -s http://localhost:7004 > /dev/null 2>&1; then
        echo "✓ Spark master is ready"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "✗ Spark master not ready after 30 attempts"
        exit 1
    fi
    sleep 2
done

echo ""
echo "Starting Bronze Layer..."
docker exec -d spark-master spark-submit \
    --master ${SPARK_MASTER} \
    --packages ${DELTA_PACKAGE},${KAFKA_PACKAGE} \
    --conf spark.sql.streaming.checkpointLocation.checkpointSchema=true \
    /opt/spark-jobs/jobs/bronze_ingestion.py

sleep 5

echo "Starting Silver Layer..."
docker exec -d spark-master spark-submit \
    --master ${SPARK_MASTER} \
    --packages ${DELTA_PACKAGE} \
    --conf spark.sql.streaming.checkpointLocation.checkpointSchema=true \
    /opt/spark-jobs/jobs/silver_cleaning.py

sleep 5

echo "Starting Gold Layer..."
docker exec -d spark-master spark-submit \
    --master ${SPARK_MASTER} \
    --packages ${DELTA_PACKAGE} \
    --conf spark.sql.streaming.checkpointLocation.checkpointSchema=true \
    /opt/spark-jobs/jobs/gold_kpis.py

sleep 5

echo ""
echo "=========================================="
echo "Spark Jobs Started"
echo "=========================================="
echo ""
echo "Check job status:"
echo "  docker exec spark-master curl http://localhost:7004/api/v1/applications"
echo ""
echo "View logs:"
echo "  docker logs spark-master"
echo ""
echo "Or check Spark UI: http://localhost:7004"
echo ""

