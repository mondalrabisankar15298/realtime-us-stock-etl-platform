#!/bin/bash
# ==========================================
# Cleanup Flag Reset Script
# ==========================================
# This script removes the cleanup flag file
# It's run by cleanup-init BEFORE the flag check
# This ensures cleanup runs fresh in each docker-compose session

FLAG_FILE="/app/state/.cleanup_done"
STATE_DIR="/app/state"

echo "Resetting cleanup flag for fresh session..."

# Create state directory if it doesn't exist
mkdir -p "${STATE_DIR}"

# Remove the cleanup flag file so cleanup will run fresh
if [ -f "${FLAG_FILE}" ]; then
    rm -f "${FLAG_FILE}"
    echo "✓ Removed stale cleanup flag file"
else
    echo "✓ No stale cleanup flag found"
fi

echo "Ready to run cleanup"

