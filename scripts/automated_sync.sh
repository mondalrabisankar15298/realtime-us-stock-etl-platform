#!/bin/bash
# Automated Sync Script - Run this as a cron job for automatic syncing
# Example cron job (run every hour): 0 * * * * /path/to/automated_sync.sh

set -e

LOG_FILE="/tmp/stock_etl_sync.log"
echo "$(date): Starting automated Delta → TimescaleDB sync" >> "$LOG_FILE"

# Run the sync with batch mode (all data)
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages io.delta:delta-core_2.12:2.4.0,org.postgresql:postgresql:42.6.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  /opt/spark-jobs/sync-delta-to-timescale.py \
  --batch >> "$LOG_FILE" 2>&1

# Check if sync was successful
if [ $? -eq 0 ]; then
    echo "$(date): ✓ Sync completed successfully" >> "$LOG_FILE"

    # Optional: Send notification or alert here
    # curl -X POST -H 'Content-type: application/json' \
    #   --data '{"text":"Stock ETL Sync: SUCCESS"}' \
    #   YOUR_SLACK_WEBHOOK_URL

else
    echo "$(date): ✗ Sync failed with exit code $?" >> "$LOG_FILE"

    # Optional: Send alert on failure
    # curl -X POST -H 'Content-type: application/json' \
    #   --data '{"text":"Stock ETL Sync: FAILED"}' \
    #   YOUR_SLACK_WEBHOOK_URL
fi

echo "$(date): Automated sync process complete" >> "$LOG_FILE"
echo "----------------------------------------" >> "$LOG_FILE"
