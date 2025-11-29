# Data Flow Visualization

## 🔄 Complete Data Flow (Step-by-Step)

```
┌─────────────────────────────────────────────────────────────────┐
│ STEP 1: PRODUCER FETCHES DATA                                   │
│ Every 60 seconds during market hours                            │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ Producer (Python)                                               │
│ • Fetches 1-min OHLCV from Yahoo Finance                       │
│ • Retries on failure (max 3 attempts)                          │
│ • Updates state file (last_fetch.json)                         │
│ • Publishes JSON to Kafka                                      │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ JSON Message
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ STEP 2: KAFKA (REDPANDA) STREAMS DATA                          │
│ Topic: stock-raw-data                                          │
│ Partitioned by: symbol (10 partitions)                         │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ Stream
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ STEP 3: SPARK BRONZE LAYER                                      │
│ • Reads from Kafka                                              │
│ • Parses JSON                                                   │
│ • Writes to Delta Lake (append-only)                           │
│ • Checkpoint: /checkpoints/bronze                               │
│                                                                 │
│ Output: Raw data as-is                                          │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ Delta Table Read
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ STEP 4: SPARK SILVER LAYER                                      │
│ • Reads from Bronze Delta table                                 │
│ • Type casting (timestamp, doubles)                            │
│ • Validation (price > 0, OHLC consistency)                     │
│ • Deduplication (symbol + timestamp)                            │
│ • Timezone normalization (UTC)                                  │
│ • MERGE operation (idempotent upsert)                          │
│                                                                 │
│ Output: Cleaned, validated, deduplicated data                   │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ Delta Table Read
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ STEP 5: SPARK GOLD LAYER                                        │
│ • Reads from Silver Delta table                                 │
│ • Calculates technical indicators:                              │
│   - SMA (5, 20, 50)                                            │
│   - EMA (9, 21)                                                 │
│   - RSI (14-period)                                             │
│   - MACD (12/26 with signal)                                    │
│   - VWAP                                                        │
│   - ATR (14-period)                                             │
│ • Computes derived metrics:                                     │
│   - Daily returns                                               │
│   - Volatility                                                  │
│   - Market phase                                                │
│ • MERGE operation (idempotent upsert)                          │
│                                                                 │
│ Output: Enriched data with KPIs                                 │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ SQL Queries
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ STEP 6: GRAFANA DASHBOARDS                                      │
│ • Queries Gold table via TimescaleDB                           │
│ • Real-time visualization (10s refresh)                        │
│ • 8 panels: prices, indicators, gainers/losers, health          │
└─────────────────────────────────────────────────────────────────┘
```

## ⏱️ Timeline (End-to-End)

```
T+0s    Producer fetches AAPL data
        ↓
T+2s    Message published to Kafka (stock-raw-data)
        ↓
T+10s   Bronze layer reads from Kafka
        ↓
T+12s   Bronze writes to Delta Lake
        ↓
T+25s   Silver layer reads Bronze
        ↓
T+30s   Silver validates & cleans data
        ↓
T+32s   Silver writes to Delta Lake (MERGE)
        ↓
T+45s   Gold layer reads Silver
        ↓
T+50s   Gold calculates KPIs (RSI, MACD, etc.)
        ↓
T+55s   Gold writes to Delta Lake (MERGE)
        ↓
T+60s   Grafana queries Gold table
        ↓
T+61s   Dashboard updates with new data
        ↓
T+60s   Producer fetches next batch (cycle repeats)
```

**Total Latency: ~45-60 seconds**

## 📊 Data at Each Stage

### Stage 1: Producer Output (Kafka Message)
```json
{
  "symbol": "AAPL",
  "timestamp": 1705501800,
  "open": 185.50,
  "high": 186.00,
  "low": 185.25,
  "close": 185.75,
  "volume": 250000,
  "ingested_at": "2025-01-17T15:30:00Z"
}
```

### Stage 2: Bronze Layer (Raw Data)
```
symbol: AAPL
timestamp: 1705501800
open: 185.50
high: 186.00
low: 185.25
close: 185.75
volume: 250000
bronze_timestamp: 2025-01-17 15:30:10
```

### Stage 3: Silver Layer (Cleaned Data)
```
symbol: AAPL
ts: 2025-01-17 15:30:00 (UTC)
open: 185.50
high: 186.00
low: 185.25
close: 185.75
volume: 250000
is_valid: true
ingestion_date: 2025-01-17
processed_at: 2025-01-17 15:30:32
```

### Stage 4: Gold Layer (KPIs)
```
symbol: AAPL
ts: 2025-01-17 15:30:00
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
computed_at: 2025-01-17 15:30:55
```

## 🔍 Verification Commands

### Check Each Stage

```bash
# 1. Producer → Kafka
docker exec -it redpanda rpk topic consume stock-raw-data --num 1

# 2. Kafka → Bronze
docker exec -it spark-master pyspark \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
# In PySpark: spark.read.format("delta").load("/opt/spark/delta_tables/bronze").count()

# 3. Bronze → Silver
# In PySpark: spark.read.format("delta").load("/opt/spark/delta_tables/silver").count()

# 4. Silver → Gold
# In PySpark: spark.read.format("delta").load("/opt/spark/delta_tables/gold").count()

# 5. Gold → Grafana
# Open http://localhost:7010 and check dashboard
```

## 📈 Monitoring Points

| Stage | What to Monitor | How to Check |
|-------|----------------|--------------|
| **Producer** | Fetch success rate | `docker logs stock-producer` |
| **Kafka** | Message count | `rpk topic describe stock-raw-data` |
| **Bronze** | Record count | Spark shell: `df.count()` |
| **Silver** | Validation rate | Spark shell: `df.filter(is_valid).count()` |
| **Gold** | KPI computation | Spark shell: `df.filter(rsi_14.isNotNull()).count()` |
| **Grafana** | Dashboard updates | Visual check at http://localhost:7010 |

## 🎯 Success Indicators

✅ **Producer**: Logs show "Published to Kafka" every 60 seconds  
✅ **Kafka**: Topic has increasing message count  
✅ **Bronze**: Table has records with recent timestamps  
✅ **Silver**: All records have `is_valid=True`  
✅ **Gold**: Records have calculated KPIs (RSI, MACD, etc.)  
✅ **Grafana**: Dashboard shows data and updates every 10 seconds  

---

**See COMPLETE_RUN_GUIDE.md for detailed step-by-step instructions!**

