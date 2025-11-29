#!/bin/bash

# Stop All Spark Streaming Jobs

echo "Stopping Spark streaming jobs..."

# Get running Spark application IDs
APP_IDS=$(docker exec spark-master curl -s http://localhost:7004/api/v1/applications | \
    python3 -c "import sys, json; apps = json.load(sys.stdin); print(' '.join([app['id'] for app in apps if 'Bronze' in app['name'] or 'Silver' in app['name'] or 'Gold' in app['name']]))" 2>/dev/null || echo "")

if [ -z "$APP_IDS" ]; then
    echo "No Spark streaming jobs found running"
    exit 0
fi

for APP_ID in $APP_IDS; do
    echo "Stopping application: $APP_ID"
    docker exec spark-master curl -X POST "http://localhost:7004/api/v1/applications/$APP_ID/stop" 2>/dev/null || true
done

echo "Spark jobs stopped"

