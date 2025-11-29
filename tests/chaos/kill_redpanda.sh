#!/bin/bash
# Chaos Test: Kill Redpanda and Observe Recovery
# Tests system resilience when message broker goes down

echo "=========================================="
echo "CHAOS TEST: Redpanda Downtime"
echo "=========================================="
echo ""

echo "1. Stopping Redpanda container..."
docker stop redpanda

echo "2. Waiting 30 seconds to observe system behavior..."
echo "   - Producer should retry connection"
echo "   - Spark streaming should buffer"
sleep 30

echo ""
echo "3. Checking producer logs for retry attempts..."
docker logs --tail 20 stock-producer | grep -i "retry\|error\|connection"

echo ""
echo "4. Restarting Redpanda..."
docker start redpanda

echo "5. Waiting for Redpanda to be healthy..."
sleep 15

echo ""
echo "6. Verifying recovery..."
docker ps --filter name=redpanda --format "{{.Names}}: {{.Status}}"

echo ""
echo "7. Checking if producer reconnected..."
docker logs --tail 10 stock-producer | grep -i "connected\|success"

echo ""
echo "=========================================="
echo "TEST COMPLETE"
echo "=========================================="
echo ""
echo "Expected outcomes:"
echo "  ✓ Producer retries connection with exponential backoff"
echo "  ✓ No data loss (buffered in Kafka after reconnect)"
echo "  ✓ Spark streaming resumes from checkpoint"
echo "  ✓ System fully recovers automatically"
echo ""

