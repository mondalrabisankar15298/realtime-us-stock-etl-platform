-- ==========================================
-- TimescaleDB Cleanup Script
-- ==========================================
-- Truncates all tables and refreshes continuous aggregates
-- for a fresh start when FORCE_BACKFILL=true

\set ON_ERROR_STOP on

-- Connect to stockdata database
\c stockdata;

-- Display cleanup start message
\echo ''
\echo '   Truncating TimescaleDB tables...'

-- ==========================================
-- Truncate Main Tables
-- ==========================================

-- Truncate gold_stocks table
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'gold_stocks') THEN
        TRUNCATE TABLE gold_stocks CASCADE;
        RAISE NOTICE '   ✓ Truncated gold_stocks table';
    ELSE
        RAISE NOTICE '   ⚠ gold_stocks table does not exist';
    END IF;
END $$;

-- Truncate silver_stocks table
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'silver_stocks') THEN
        TRUNCATE TABLE silver_stocks CASCADE;
        RAISE NOTICE '   ✓ Truncated silver_stocks table';
    ELSE
        RAISE NOTICE '   ⚠ silver_stocks table does not exist';
    END IF;
END $$;

-- Truncate dlq_logs table
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'dlq_logs') THEN
        TRUNCATE TABLE dlq_logs CASCADE;
        RAISE NOTICE '   ✓ Truncated dlq_logs table';
    ELSE
        RAISE NOTICE '   ⚠ dlq_logs table does not exist';
    END IF;
END $$;

-- ==========================================
-- Refresh Continuous Aggregates
-- ==========================================

\echo '   Refreshing continuous aggregates...'

-- Refresh gold_5min continuous aggregate
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM timescaledb_information.continuous_aggregates 
        WHERE view_name = 'gold_5min'
    ) THEN
        -- Truncate the materialized view data
        TRUNCATE TABLE gold_5min CASCADE;
        RAISE NOTICE '   ✓ Cleared gold_5min continuous aggregate';
    ELSE
        RAISE NOTICE '   ⚠ gold_5min continuous aggregate does not exist';
    END IF;
END $$;

-- Refresh gold_hourly continuous aggregate
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM timescaledb_information.continuous_aggregates 
        WHERE view_name = 'gold_hourly'
    ) THEN
        -- Truncate the materialized view data
        TRUNCATE TABLE gold_hourly CASCADE;
        RAISE NOTICE '   ✓ Cleared gold_hourly continuous aggregate';
    ELSE
        RAISE NOTICE '   ⚠ gold_hourly continuous aggregate does not exist';
    END IF;
END $$;

-- ==========================================
-- Vacuum and Analyze
-- ==========================================

\echo '   Running VACUUM and ANALYZE...'

-- Vacuum tables to reclaim space
VACUUM ANALYZE gold_stocks;
VACUUM ANALYZE silver_stocks;
VACUUM ANALYZE dlq_logs;

-- ==========================================
-- Verification
-- ==========================================

\echo '   Verifying cleanup...'

-- Check row counts
DO $$
DECLARE
    gold_count INTEGER;
    silver_count INTEGER;
    dlq_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO gold_count FROM gold_stocks;
    SELECT COUNT(*) INTO silver_count FROM silver_stocks;
    SELECT COUNT(*) INTO dlq_count FROM dlq_logs;
    
    IF gold_count = 0 AND silver_count = 0 AND dlq_count = 0 THEN
        RAISE NOTICE '   ✓ All tables are empty';
    ELSE
        RAISE WARNING '   ⚠ Some tables still have data: gold=%, silver=%, dlq=%', 
            gold_count, silver_count, dlq_count;
    END IF;
END $$;

\echo '   ✓ TimescaleDB cleanup completed'
\echo ''

