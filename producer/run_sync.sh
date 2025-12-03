#!/bin/bash
# Script to run Delta Lake to TimescaleDB sync
# This runs from the host and uses docker exec

set -e

echo "=========================================="
echo "RUNNING DELTA LAKE → TIMESCALEDB SYNC"
echo "=========================================="

# Run the sync script in the spark-master container
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages io.delta:delta-core_2.12:2.4.0,org.postgresql:postgresql:42.6.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  /opt/spark-jobs/sync-delta-to-timescale.py \
  --batch

echo "=========================================="
echo "SYNC COMPLETED SUCCESSFULLY"
echo "=========================================="
