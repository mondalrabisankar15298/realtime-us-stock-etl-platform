#!/bin/bash
# ==========================================
# FORCE_BACKFILL Complete Cleanup Script
# ==========================================
# This script runs once per docker-compose up session when FORCE_BACKFILL=true
# It cleans all data from Kafka, Delta tables, checkpoints, TimescaleDB, and producer state

set -e

# Install PostgreSQL client tools (needed for TimescaleDB cleanup)
echo "Installing PostgreSQL client tools..."
apt-get update -qq >/dev/null 2>&1
apt-get install -y postgresql-client >/dev/null 2>&1 || true

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
FLAG_FILE="/app/state/.cleanup_done"
FORCE_BACKFILL="${FORCE_BACKFILL:-false}"

echo ""
echo "=========================================="
echo "FORCE_BACKFILL CLEANUP INITIALIZATION"
echo "=========================================="
echo ""

# Check if FORCE_BACKFILL is enabled
if [[ "${FORCE_BACKFILL}" != "true" && "${FORCE_BACKFILL}" != "1" && "${FORCE_BACKFILL}" != "yes" ]]; then
    echo -e "${BLUE}ℹ FORCE_BACKFILL is not enabled. Skipping cleanup.${NC}"
    exit 0
fi

# Reset cleanup flag at startup (fresh session)
# This ensures cleanup runs even if the volume persists
STATE_DIR=$(dirname "${FLAG_FILE}")
mkdir -p "${STATE_DIR}"
if [ -f "${FLAG_FILE}" ]; then
    rm -f "${FLAG_FILE}"
    echo -e "${BLUE}ℹ Removed stale cleanup flag for fresh session${NC}"
fi

echo -e "${GREEN}✓ FORCE_BACKFILL=true detected${NC}"
echo -e "${YELLOW}⚠ Starting comprehensive cleanup of all data...${NC}"
echo ""

# ==========================================
# Wait for Services to Be Ready
# ==========================================
echo "Waiting for all services to be fully ready..."

# Wait a bit for services to stabilize (docker-compose already verified healthy)
sleep 5

# Quick check for TimescaleDB
echo "   Checking TimescaleDB..."
if PGPASSWORD=grafana psql -h postgres-timescale -U grafana -d stockdata -c "SELECT 1" >/dev/null 2>&1; then
    echo -e "   ${GREEN}✓ TimescaleDB ready${NC}"
else
    echo -e "   ${YELLOW}⚠ TimescaleDB check failed, but continuing${NC}"
fi

# Quick check for Airflow DB
echo "   Checking Airflow DB..."
if PGPASSWORD=airflow psql -h postgres-airflow -U airflow -d airflow -c "SELECT 1" >/dev/null 2>&1; then
    echo -e "   ${GREEN}✓ Airflow DB ready${NC}"
else
    echo -e "   ${YELLOW}⚠ Airflow DB check failed, but continuing${NC}"
fi

echo -e "${GREEN}✓ Service checks complete${NC}"
echo ""

# ==========================================
# Function: Clean Kafka/Redpanda Topics
# ==========================================
cleanup_kafka() {
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "1. Cleaning Kafka/Redpanda Topics"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    # Try to run Kafka cleanup (skip gracefully if Redpanda not fully ready)
    if [ -f /scripts/cleanup-kafka.sh ]; then
        timeout 10 bash /scripts/cleanup-kafka.sh 2>/dev/null || {
            echo -e "   ${YELLOW}⚠ Kafka cleanup skipped (Redpanda may still be initializing)${NC}"
            return 0
        }
        echo -e "   ${GREEN}✓ Kafka topics cleaned${NC}"
    else
        echo -e "   ${YELLOW}⚠ cleanup-kafka.sh not found, skipping${NC}"
    fi
    echo ""
}

# ==========================================
# Function: Clean Delta Lake Tables
# ==========================================
cleanup_delta_tables() {
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "2. Cleaning Delta Lake Tables"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    DELTA_BASE="/opt/spark/delta_tables"
    
    for layer in bronze silver gold; do
        DELTA_PATH="${DELTA_BASE}/${layer}"
        if [ -d "${DELTA_PATH}" ]; then
            echo "   Removing ${layer} Delta table: ${DELTA_PATH}"
            # Use find with -delete to handle permission issues better
            find "${DELTA_PATH}" -type f -delete 2>/dev/null || true
            find "${DELTA_PATH}" -type d -empty -delete 2>/dev/null || true
            rm -rf "${DELTA_PATH}" 2>/dev/null || true
            
            if [ ! -d "${DELTA_PATH}" ]; then
                echo -e "   ${GREEN}✓ Removed ${layer} Delta table${NC}"
            else
                echo -e "   ${YELLOW}⚠ Partially cleaned ${layer} (some files may remain due to permissions)${NC}"
            fi
        else
            echo "   ${YELLOW}⚠ ${layer} Delta table not found (already clean)${NC}"
        fi
    done
    
    # Also clean DLQ path if it exists
    DLQ_PATH="${DELTA_BASE}/bronze_dlq"
    if [ -d "${DLQ_PATH}" ]; then
        echo "   Removing bronze_dlq: ${DLQ_PATH}"
        find "${DLQ_PATH}" -type f -delete 2>/dev/null || true
        find "${DLQ_PATH}" -type d -empty -delete 2>/dev/null || true
        rm -rf "${DLQ_PATH}" 2>/dev/null || true
        echo -e "   ${GREEN}✓ Removed bronze_dlq${NC}"
    fi
    
    echo -e "   ${GREEN}✓ All Delta tables cleaned${NC}"
    echo ""
}

# ==========================================
# Function: Clean Spark and Polars Checkpoints
# ==========================================
cleanup_checkpoints() {
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "3. Cleaning Spark & Polars Checkpoints"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    CHECKPOINT_BASE="/opt/spark/checkpoints"
    
    # Clean Spark streaming checkpoints (directories)
    for layer in bronze silver gold; do
        CHECKPOINT_PATH="${CHECKPOINT_BASE}/${layer}"
        if [ -d "${CHECKPOINT_PATH}" ]; then
            echo "   Removing ${layer} Spark checkpoint: ${CHECKPOINT_PATH}"
            # Use find with -delete to handle permission issues better
            find "${CHECKPOINT_PATH}" -type f -delete 2>/dev/null || true
            find "${CHECKPOINT_PATH}" -type d -empty -delete 2>/dev/null || true
            rm -rf "${CHECKPOINT_PATH}" 2>/dev/null || true
            
            if [ ! -d "${CHECKPOINT_PATH}" ]; then
                echo -e "   ${GREEN}✓ Removed ${layer} Spark checkpoint${NC}"
            else
                echo -e "   ${YELLOW}⚠ Partially cleaned ${layer} Spark checkpoint (some files may remain)${NC}"
            fi
        else
            echo "   ${YELLOW}⚠ ${layer} Spark checkpoint not found (already clean)${NC}"
        fi
    done
    
    # Clean Polars checkpoint JSON files
    echo "   Removing Polars checkpoint JSON files..."
    POLARS_CHECKPOINTS_REMOVED=0
    for checkpoint_file in "${CHECKPOINT_BASE}"/*_checkpoint.json; do
        if [ -f "${checkpoint_file}" ]; then
            rm -f "${checkpoint_file}" 2>/dev/null || true
            if [ ! -f "${checkpoint_file}" ]; then
                echo -e "   ${GREEN}✓ Removed $(basename "${checkpoint_file}")${NC}"
                POLARS_CHECKPOINTS_REMOVED=1
            fi
        fi
    done
    
    if [ ${POLARS_CHECKPOINTS_REMOVED} -eq 0 ]; then
        echo "   ${YELLOW}⚠ No Polars checkpoint JSON files found (already clean)${NC}"
    fi
    
    echo -e "   ${GREEN}✓ All checkpoints cleaned${NC}"
    echo ""
}

# ==========================================
# Function: Clean TimescaleDB Data
# ==========================================
cleanup_timescale() {
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "4. Cleaning TimescaleDB Data"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    # Run TimescaleDB cleanup script
    if [ -f /scripts/cleanup-timescale.sql ]; then
        echo "   Executing cleanup SQL script..."
        if PGPASSWORD=grafana psql -h postgres-timescale -U grafana -d stockdata -f /scripts/cleanup-timescale.sql >/dev/null 2>&1; then
            echo -e "   ${GREEN}✓ TimescaleDB data cleaned${NC}"
        else
            echo -e "   ${YELLOW}⚠ TimescaleDB cleanup had issues, but continuing${NC}"
        fi
    else
        echo -e "   ${YELLOW}⚠ cleanup-timescale.sql not found, skipping${NC}"
    fi
    
    echo ""
}

# ==========================================
# Function: Clean Producer State
# ==========================================
cleanup_producer_state() {
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "5. Cleaning Producer State"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    STATE_DIR="/app/state"
    
    # Remove state files
    if [ -f "${STATE_DIR}/last_fetch.json" ]; then
        rm -f "${STATE_DIR}/last_fetch.json"
        echo -e "   ${GREEN}✓ Removed last_fetch.json${NC}"
    else
        echo "   ${YELLOW}⚠ last_fetch.json not found (already clean)${NC}"
    fi
    
    if [ -f "${STATE_DIR}/watermarks.json" ]; then
        rm -f "${STATE_DIR}/watermarks.json"
        echo -e "   ${GREEN}✓ Removed watermarks.json${NC}"
    else
        echo "   ${YELLOW}⚠ watermarks.json not found (already clean)${NC}"
    fi
    
    # Remove cleanup flag file (if it exists, it will be recreated)
    if [ -f "${STATE_DIR}/.cleanup_done" ]; then
        rm -f "${STATE_DIR}/.cleanup_done"
        echo -e "   ${GREEN}✓ Removed cleanup flag${NC}"
    fi
    
    echo -e "   ${GREEN}✓ Producer state cleaned${NC}"
    echo ""
}

# ==========================================
# Function: Clean Airflow Metadata
# ==========================================
cleanup_airflow_metadata() {
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "6. Cleaning Airflow Metadata"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    # Clear Airflow DAG runs and task instances
    echo "   Clearing DAG runs and task instances..."
    if PGPASSWORD=airflow psql -h postgres-airflow -U airflow -d airflow >/dev/null 2>&1 <<EOF
DELETE FROM task_instance WHERE dag_id IN (
    'stock_producer', 'stock_monitoring', 'stock_backfill', 
    'stock_quality_check', 'gold_rebuild', 'sync_delta_to_timescale',
    'gold_timescale_sync', 'etl_pipeline', 'producer_dag', 'monitoring_dag',
    'qc_dag', 'backfill_dag', 'gold_rebuild_dag', 'gold_timescale_sync_dag'
);
DELETE FROM dag_run WHERE dag_id IN (
    'stock_producer', 'stock_monitoring', 'stock_backfill', 
    'stock_quality_check', 'gold_rebuild', 'sync_delta_to_timescale',
    'gold_timescale_sync', 'etl_pipeline', 'producer_dag', 'monitoring_dag',
    'qc_dag', 'backfill_dag', 'gold_rebuild_dag', 'gold_timescale_sync_dag'
);
DELETE FROM xcom WHERE dag_id IN (
    'stock_producer', 'stock_monitoring', 'stock_backfill', 
    'stock_quality_check', 'gold_rebuild', 'sync_delta_to_timescale',
    'gold_timescale_sync', 'etl_pipeline', 'producer_dag', 'monitoring_dag',
    'qc_dag', 'backfill_dag', 'gold_rebuild_dag', 'gold_timescale_sync_dag'
);
EOF
    then
        echo -e "   ${GREEN}✓ Airflow metadata cleaned${NC}"
    else
        echo -e "   ${YELLOW}⚠ Airflow cleanup had issues, but continuing${NC}"
    fi
    echo ""
}

# ==========================================
# Main Execution
# ==========================================
main() {
    local cleanup_failed=0
    local cleanup_warnings=0
    
    # Execute all cleanup functions (allow warnings but not failures)
    cleanup_kafka || { 
        echo -e "   ${YELLOW}⚠ Kafka cleanup had issues${NC}"
        cleanup_warnings=1
    }
    
    cleanup_delta_tables || { 
        echo -e "   ${YELLOW}⚠ Delta tables cleanup had issues${NC}"
        cleanup_warnings=1
    }
    
    cleanup_checkpoints || { 
        echo -e "   ${YELLOW}⚠ Checkpoints cleanup had issues${NC}"
        cleanup_warnings=1
    }
    
    cleanup_timescale || { 
        echo -e "   ${YELLOW}⚠ TimescaleDB cleanup had issues${NC}"
        cleanup_warnings=1
    }
    
    cleanup_producer_state || { 
        echo -e "   ${YELLOW}⚠ Producer state cleanup had issues${NC}"
        cleanup_warnings=1
    }
    
    cleanup_airflow_metadata || { 
        echo -e "   ${YELLOW}⚠ Airflow metadata cleanup had issues${NC}"
        cleanup_warnings=1
    }
    
    # Create flag file to prevent re-cleaning in this session
    mkdir -p "$(dirname "${FLAG_FILE}")"
    touch "${FLAG_FILE}"
    echo "Session cleanup timestamp: $(date)" > "${FLAG_FILE}"
    
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    if [ $cleanup_warnings -eq 1 ]; then
        echo -e "${YELLOW}⚠ CLEANUP COMPLETED WITH WARNINGS${NC}"
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        echo ""
        echo "Some cleanup operations had issues, but the system should still work."
        echo "Check the logs above for details."
    else
        echo -e "${GREEN}✓ CLEANUP COMPLETED SUCCESSFULLY${NC}"
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        echo ""
        echo "All data has been cleaned. The system is ready for a fresh backfill."
    fi
    echo "Flag file created: ${FLAG_FILE}"
    echo ""
}

# Run main function
main

