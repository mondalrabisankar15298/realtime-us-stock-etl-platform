# Data Model

This document describes the schemas and data structures used throughout the pipeline.

## Raw Message Schema (Producer → Kafka)

```json
{
  "symbol": "AAPL",
  "timestamp": 1704369600,
  "open": 185.50,
  "high": 186.00,
  "low": 185.25,
  "close": 185.75,
  "volume": 250000,
  "ingested_at": "2025-01-01T15:00:00Z"
}
```

**Field Descriptions**:
- `symbol` (string): Stock ticker symbol
- `timestamp` (long): Unix timestamp in UTC
- `open` (double): Opening price for the 1-minute bar
- `high` (double): Highest price during the minute
- `low` (double): Lowest price during the minute
- `close` (double): Closing price for the minute
- `volume` (long): Total shares traded during the minute
- `ingested_at` (string): ISO 8601 timestamp when data was fetched

## Bronze Layer Schema

**Table**: `bronze_stocks`  
**Format**: Delta Lake (Parquet)  
**Write Mode**: Append-only  

```python
StructType([
    StructField("symbol", StringType(), nullable=False),
    StructField("timestamp", LongType(), nullable=False),
    StructField("open", DoubleType(), nullable=False),
    StructField("high", DoubleType(), nullable=False),
    StructField("low", DoubleType(), nullable=False),
    StructField("close", DoubleType(), nullable=False),
    StructField("volume", LongType(), nullable=False),
    StructField("ingested_at", StringType(), nullable=False),
    StructField("bronze_timestamp", TimestampType(), nullable=False),
    StructField("kafka_key", StringType(), nullable=True),
    StructField("kafka_timestamp", TimestampType(), nullable=True),
    StructField("kafka_partition", IntegerType(), nullable=True),
    StructField("kafka_offset", LongType(), nullable=True),
])
```

**Additional Fields**:
- `bronze_timestamp`: When record was written to Bronze
- `kafka_key`: Kafka message key (usually symbol)
- `kafka_timestamp`: Kafka message timestamp
- `kafka_partition`: Kafka partition number
- `kafka_offset`: Kafka offset number

**Purpose**: 
- Raw data lake layer
- Complete audit trail
- Enables reprocessing if needed
- No transformations applied

## Silver Layer Schema

**Table**: `silver_stocks`  
**Format**: Delta Lake (Parquet)  
**Write Mode**: MERGE (upsert on symbol + ts)  
**Partition**: By `ingestion_date`  

```python
StructType([
    StructField("symbol", StringType(), nullable=False),
    StructField("ts", TimestampType(), nullable=False),
    StructField("open", DoubleType(), nullable=False),
    StructField("high", DoubleType(), nullable=False),
    StructField("low", DoubleType(), nullable=False),
    StructField("close", DoubleType(), nullable=False),
    StructField("volume", LongType(), nullable=False),
    StructField("is_valid", BooleanType(), nullable=False),
    StructField("ingestion_date", StringType(), nullable=False),
    StructField("processed_at", TimestampType(), nullable=False),
])
```

**Primary Key**: (symbol, ts)

**Transformations Applied**:
1. Unix timestamp → TimestampType (UTC)
2. Validation:
   - `open > 0`
   - `high > 0`
   - `low > 0`
   - `close > 0`
   - `high >= low`
   - `high >= open`
   - `high >= close`
   - `low <= open`
   - `low <= close`
   - `volume >= 0`
3. Deduplication by (symbol, ts)
4. Add `is_valid` flag
5. Add `ingestion_date` for partitioning (YYYY-MM-DD)
6. Add `processed_at` timestamp

**Purpose**:
- Clean, validated data
- Business-ready for analytics
- Deduplicated and type-safe
- Partitioned for efficient queries

**Example Record**:
```
symbol: AAPL
ts: 2025-01-01 15:00:00
open: 185.50
high: 186.00
low: 185.25
close: 185.75
volume: 250000
is_valid: true
ingestion_date: 2025-01-01
processed_at: 2025-01-01 15:00:15
```

## Gold Layer Schema

**Table**: `gold_stocks`  
**Format**: Delta Lake (Parquet)  
**Write Mode**: MERGE (upsert on symbol + ts)  

```python
StructType([
    StructField("symbol", StringType(), nullable=False),
    StructField("ts", TimestampType(), nullable=False),
    StructField("close", DoubleType(), nullable=False),
    StructField("volume", LongType(), nullable=False),
    
    # Technical Indicators
    StructField("sma_5", DoubleType(), nullable=True),
    StructField("sma_20", DoubleType(), nullable=True),
    StructField("sma_50", DoubleType(), nullable=True),
    StructField("ema_9", DoubleType(), nullable=True),
    StructField("ema_21", DoubleType(), nullable=True),
    StructField("rsi_14", DoubleType(), nullable=True),
    StructField("vwap", DoubleType(), nullable=True),
    StructField("macd", DoubleType(), nullable=True),
    StructField("macd_signal", DoubleType(), nullable=True),
    StructField("macd_histogram", DoubleType(), nullable=True),
    StructField("atr_14", DoubleType(), nullable=True),
    
    # Derived Metrics
    StructField("daily_return", DoubleType(), nullable=True),
    StructField("volatility_5m", DoubleType(), nullable=True),
    StructField("market_phase", StringType(), nullable=True),
    StructField("price_change_pct", DoubleType(), nullable=True),
    
    StructField("computed_at", TimestampType(), nullable=False),
])
```

**Primary Key**: (symbol, ts)

**Indicator Definitions**:

### Simple Moving Average (SMA)
```
SMA_N = AVG(close) over last N periods
```
- SMA_5: 5-minute moving average
- SMA_20: 20-minute moving average
- SMA_50: 50-minute moving average

### Exponential Moving Average (EMA)
```
EMA_today = (Close_today × multiplier) + (EMA_yesterday × (1 - multiplier))
multiplier = 2 / (N + 1)
```
- EMA_9: 9-period EMA (multiplier = 0.2)
- EMA_21: 21-period EMA (multiplier = 0.091)

### Relative Strength Index (RSI)
```
RS = Average Gain / Average Loss (over 14 periods)
RSI = 100 - (100 / (1 + RS))
```
- RSI_14: 14-period RSI
- Values: 0-100
- Overbought: RSI > 70
- Oversold: RSI < 30

### MACD (Moving Average Convergence Divergence)
```
MACD = EMA_12 - EMA_26
Signal = EMA_9(MACD)
Histogram = MACD - Signal
```
- Bullish signal: Histogram crosses above 0
- Bearish signal: Histogram crosses below 0

### VWAP (Volume Weighted Average Price)
```
VWAP = SUM(close × volume) / SUM(volume)
```
- Cumulative from market open
- Used to determine fair value

### ATR (Average True Range)
```
True Range = MAX(
    high - low,
    ABS(high - prev_close),
    ABS(low - prev_close)
)
ATR = AVG(True Range) over 14 periods
```
- Measures volatility
- Higher ATR = higher volatility

### Daily Return
```
Daily Return (%) = ((close - prev_close) / prev_close) × 100
```

### Volatility (5-minute)
```
Volatility_5m = STDDEV(daily_return) over last 5 periods
```

### Market Phase
```
Based on hour (EST):
- 04:00 - 09:30 → "pre-market"
- 09:30 - 16:00 → "open"
- 16:00 - 20:00 → "post-market"
- 20:00 - 04:00 → "closed"
```

### Price Change Percentage
```
Price Change (%) = ((close - open) / open) × 100
```

**Example Record**:
```
symbol: AAPL
ts: 2025-01-01 15:00:00
close: 185.75
volume: 250000
sma_5: 185.60
sma_20: 185.45
sma_50: 184.80
ema_9: 185.62
ema_21: 185.40
rsi_14: 58.3
vwap: 185.50
macd: 0.15
macd_signal: 0.12
macd_histogram: 0.03
atr_14: 1.25
daily_return: 0.14
volatility_5m: 0.35
market_phase: open
price_change_pct: 0.13
computed_at: 2025-01-01 15:00:20
```

## DLQ Message Schema

**Topic**: `stock-dlq`  
**Format**: JSON  

```json
{
  "ticker": "AAPL",
  "error": "Kafka connection timeout",
  "timestamp": "2025-01-01T15:00:00Z",
  "original_data": {
    "symbol": "AAPL",
    "timestamp": 1704369600,
    "open": 185.50,
    "high": 186.00,
    "low": 185.25,
    "close": 185.75,
    "volume": 250000
  }
}
```

**Purpose**: 
- Track failed message processing
- Enable manual investigation
- Prevent data loss

## State File Schema

**File**: `producer/state/last_fetch.json`  
**Format**: JSON  

```json
{
  "AAPL": "2025-01-01T15:00:00+00:00",
  "MSFT": "2025-01-01T15:00:00+00:00",
  "GOOGL": "2025-01-01T15:00:00+00:00",
  "AMZN": "2025-01-01T15:00:00+00:00",
  "NVDA": "2025-01-01T15:00:00+00:00",
  "META": "2025-01-01T15:00:00+00:00",
  "TSLA": "2025-01-01T15:00:00+00:00",
  "NFLX": "2025-01-01T15:00:00+00:00",
  "AMD": "2025-01-01T15:00:00+00:00",
  "AVGO": "2025-01-01T15:00:00+00:00"
}
```

**Purpose**:
- Track last successful fetch per ticker
- Enable gap detection
- Support backfill operations
- Idempotent producer design

## Query Examples

### Find Latest Prices
```sql
SELECT symbol, ts, close, daily_return
FROM gold_stocks
WHERE ts >= NOW() - INTERVAL '1 hour'
ORDER BY ts DESC, symbol
```

### Top Gainers (Last Hour)
```sql
SELECT symbol, 
       MAX(close) as current_price,
       MAX(daily_return) as max_return
FROM gold_stocks
WHERE ts >= NOW() - INTERVAL '1 hour'
GROUP BY symbol
ORDER BY max_return DESC
LIMIT 10
```

### RSI Overbought Stocks
```sql
SELECT symbol, ts, close, rsi_14
FROM gold_stocks
WHERE ts = (SELECT MAX(ts) FROM gold_stocks)
  AND rsi_14 > 70
ORDER BY rsi_14 DESC
```

### MACD Bullish Signals
```sql
SELECT symbol, ts, close, macd, macd_signal, macd_histogram
FROM gold_stocks
WHERE macd_histogram > 0
  AND ts >= NOW() - INTERVAL '1 hour'
ORDER BY ts DESC, macd_histogram DESC
```

### High Volatility Stocks
```sql
SELECT symbol, 
       AVG(volatility_5m) as avg_volatility,
       AVG(atr_14) as avg_atr
FROM gold_stocks
WHERE ts >= NOW() - INTERVAL '6 hours'
GROUP BY symbol
ORDER BY avg_volatility DESC
```

