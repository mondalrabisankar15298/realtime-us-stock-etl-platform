#!/bin/bash
# ==========================================
# Kafka/Redpanda Topic Cleanup Script
# ==========================================
# Deletes and recreates all Kafka topics for a fresh start

set -e

# Color codes
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# Configuration
BROKER="redpanda:9092"
TOPICS=(
    "stock-raw-data"
    "stock-dlq"
    "stock-backfill-dlq"
)

# Topic configurations
PARTITIONS=3
REPLICATION_FACTOR=1

echo "   Cleaning Kafka topics..."

# Delete existing topics
for topic in "${TOPICS[@]}"; do
    echo "   - Checking topic: ${topic}"
    
    # Check if topic exists
    if rpk topic list --brokers "${BROKER}" 2>/dev/null | grep -q "^${topic}"; then
        echo "     Deleting topic: ${topic}"
        rpk topic delete "${topic}" --brokers "${BROKER}" 2>/dev/null || {
            echo -e "     ${YELLOW}⚠ Failed to delete ${topic} (may not exist)${NC}"
        }
    else
        echo "     ${YELLOW}⚠ Topic ${topic} does not exist (already clean)${NC}"
    fi
done

# Wait a moment for deletions to complete
sleep 2

# Recreate topics
for topic in "${TOPICS[@]}"; do
    echo "   - Creating topic: ${topic}"
    rpk topic create "${topic}" \
        --brokers "${BROKER}" \
        --partitions "${PARTITIONS}" \
        --replicas "${REPLICATION_FACTOR}" \
        --config retention.ms=604800000 \
        --config cleanup.policy=delete 2>/dev/null || {
        echo -e "     ${YELLOW}⚠ Topic ${topic} may already exist${NC}"
    }
done

# List all consumer groups and delete them
echo "   - Cleaning consumer groups..."
CONSUMER_GROUPS=$(rpk group list --brokers "${BROKER}" 2>/dev/null | tail -n +2 | awk '{print $1}' || echo "")

if [ -n "${CONSUMER_GROUPS}" ]; then
    for group in ${CONSUMER_GROUPS}; do
        echo "     Deleting consumer group: ${group}"
        rpk group delete "${group}" --brokers "${BROKER}" 2>/dev/null || {
            echo -e "     ${YELLOW}⚠ Failed to delete group ${group}${NC}"
        }
    done
else
    echo "     No consumer groups found"
fi

# Verify topics were created
echo "   - Verifying topics..."
for topic in "${TOPICS[@]}"; do
    if rpk topic list --brokers "${BROKER}" 2>/dev/null | grep -q "^${topic}"; then
        echo -e "     ${GREEN}✓ Topic ${topic} ready${NC}"
    else
        echo -e "     ${RED}✗ Topic ${topic} not found${NC}"
        exit 1
    fi
done

echo -e "   ${GREEN}✓ All Kafka topics cleaned and recreated${NC}"

