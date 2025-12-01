-- TimescaleDB Initialization Script
-- Automatically executed when container first starts

-- Create databases
CREATE DATABASE IF NOT EXISTS stockdata;

-- Connect to stockdata database and enable TimescaleDB extension
\c stockdata;
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ==========================================
-- Gold Layer Table (Main Dashboard Data)
-- ==========================================
CREATE TABLE IF NOT EXISTS gold_stocks (
    symbol VARCHAR(10) NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    close DOUBLE PRECISION,
    volume BIGINT,
    
    -- Technical Indicators
    sma_5 DOUBLE PRECISION,
    sma_20 DOUBLE PRECISION,
    sma_50 DOUBLE PRECISION,
    ema_9 DOUBLE PRECISION,
    ema_21 DOUBLE PRECISION,
    rsi_14 DOUBLE PRECISION,
    vwap DOUBLE PRECISION,
    macd DOUBLE PRECISION,
    macd_signal DOUBLE PRECISION,
    macd_histogram DOUBLE PRECISION,
    atr_14 DOUBLE PRECISION,
    
    -- Derived Metrics
    daily_return DOUBLE PRECISION,
    volatility_5m DOUBLE PRECISION,
    market_phase VARCHAR(20),
    price_change_pct DOUBLE PRECISION,
    computed_at TIMESTAMPTZ,
    
    PRIMARY KEY (symbol, ts)
);

-- Convert to TimescaleDB hypertable (auto-partitioning by time!)
SELECT create_hypertable('gold_stocks', 'ts', 
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- Create indexes for common queries
CREATE INDEX IF NOT EXISTS idx_gold_symbol_ts ON gold_stocks(symbol, ts DESC);
CREATE INDEX IF NOT EXISTS idx_gold_ts ON gold_stocks(ts DESC);
CREATE INDEX IF NOT EXISTS idx_gold_rsi ON gold_stocks(rsi_14) WHERE rsi_14 IS NOT NULL;

-- ==========================================
-- Silver Layer Table (Cleaned Data)
-- ==========================================
CREATE TABLE IF NOT EXISTS silver_stocks (
    symbol VARCHAR(10) NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume BIGINT,
    is_valid BOOLEAN,
    ingestion_date DATE,
    processed_at TIMESTAMPTZ,
    
    PRIMARY KEY (symbol, ts)
);

-- Convert to hypertable
SELECT create_hypertable('silver_stocks', 'ts',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_silver_symbol_ts ON silver_stocks(symbol, ts DESC);
CREATE INDEX IF NOT EXISTS idx_silver_date ON silver_stocks(ingestion_date);

-- ==========================================
-- DLQ Logs Table
-- ==========================================
CREATE TABLE IF NOT EXISTS dlq_logs (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(10),
    error TEXT,
    timestamp TIMESTAMPTZ DEFAULT NOW(),
    original_data JSONB
);

CREATE INDEX IF NOT EXISTS idx_dlq_timestamp ON dlq_logs(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_dlq_ticker ON dlq_logs(ticker);

-- ==========================================
-- Continuous Aggregates (Pre-computed Views)
-- ==========================================

-- 5-minute aggregated view (for faster dashboard queries)
CREATE MATERIALIZED VIEW IF NOT EXISTS gold_5min
WITH (timescaledb.continuous) AS
SELECT 
    symbol,
    time_bucket('5 minutes', ts) AS bucket,
    first(close, ts) as open,
    max(close) as high,
    min(close) as low,
    last(close, ts) as close,
    sum(volume) as volume,
    avg(rsi_14) as avg_rsi,
    avg(macd) as avg_macd,
    avg(daily_return) as avg_return,
    avg(volatility_5m) as avg_volatility
FROM gold_stocks
GROUP BY symbol, bucket
WITH NO DATA;

-- Refresh policy: Update every 5 minutes
SELECT add_continuous_aggregate_policy('gold_5min',
    start_offset => INTERVAL '1 hour',
    end_offset => INTERVAL '1 minute',
    schedule_interval => INTERVAL '5 minutes',
    if_not_exists => TRUE
);

-- Hourly aggregated view (for historical analysis)
CREATE MATERIALIZED VIEW IF NOT EXISTS gold_hourly
WITH (timescaledb.continuous) AS
SELECT 
    symbol,
    time_bucket('1 hour', ts) AS hour,
    first(close, ts) as open,
    max(close) as high,
    min(close) as low,
    last(close, ts) as close,
    sum(volume) as volume,
    avg(rsi_14) as avg_rsi,
    stddev(daily_return) as return_volatility,
    count(*) as data_points
FROM gold_stocks
GROUP BY symbol, hour
WITH NO DATA;

-- Refresh policy: Update every hour
SELECT add_continuous_aggregate_policy('gold_hourly',
    start_offset => INTERVAL '3 hours',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists => TRUE
);

-- ==========================================
-- Compression Policy (Save 90% Storage!)
-- ==========================================

-- Compress data older than 7 days
SELECT add_compression_policy('gold_stocks', 
    INTERVAL '7 days',
    if_not_exists => TRUE
);

SELECT add_compression_policy('silver_stocks',
    INTERVAL '7 days', 
    if_not_exists => TRUE
);

-- ==========================================
-- Retention Policy (Optional - commented out)
-- ==========================================

-- Uncomment to automatically drop data older than 90 days
-- SELECT add_retention_policy('gold_stocks', INTERVAL '90 days', if_not_exists => TRUE);
-- SELECT add_retention_policy('silver_stocks', INTERVAL '90 days', if_not_exists => TRUE);

-- ==========================================
-- Helper Views for Grafana
-- ==========================================

-- Latest prices view
CREATE OR REPLACE VIEW latest_prices AS
SELECT DISTINCT ON (symbol)
    symbol,
    ts,
    close as price,
    daily_return,
    rsi_14,
    volume,
    market_phase
FROM gold_stocks
ORDER BY symbol, ts DESC;

-- Top gainers/losers view
CREATE OR REPLACE VIEW top_movers AS
SELECT 
    symbol,
    close as current_price,
    daily_return,
    rsi_14,
    volatility_5m,
    CASE 
        WHEN daily_return > 0 THEN 'gainer'
        WHEN daily_return < 0 THEN 'loser'
        ELSE 'flat'
    END as movement_type
FROM latest_prices
ORDER BY ABS(daily_return) DESC;

-- RSI signals view
CREATE OR REPLACE VIEW rsi_signals AS
SELECT 
    symbol,
    ts,
    close,
    rsi_14,
    CASE
        WHEN rsi_14 > 70 THEN 'OVERBOUGHT'
        WHEN rsi_14 < 30 THEN 'OVERSOLD'
        ELSE 'NEUTRAL'
    END as signal
FROM gold_stocks
WHERE rsi_14 IS NOT NULL
    AND ts >= NOW() - INTERVAL '24 hours'
ORDER BY ts DESC;

-- ==========================================
-- Performance Tuning
-- ==========================================

-- Set recommended TimescaleDB settings
ALTER DATABASE stockdata SET timescaledb.max_background_workers = 8;

-- Grant all permissions to grafana user
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO grafana;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO grafana;

-- Grant connect permission to grafana user
GRANT CONNECT ON DATABASE stockdata TO grafana;

-- Success message
DO $$
BEGIN
    RAISE NOTICE '✅ TimescaleDB initialized successfully!';
    RAISE NOTICE '📊 Created tables: gold_stocks, silver_stocks, dlq_logs';
    RAISE NOTICE '⚡ Created continuous aggregates: gold_5min, gold_hourly';
    RAISE NOTICE '💾 Compression enabled for data older than 7 days';
    RAISE NOTICE '🔍 Created helper views: latest_prices, top_movers, rsi_signals';
END $$;

