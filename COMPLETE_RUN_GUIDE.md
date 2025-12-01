# Complete Run Guide - Real-Time Stock ETL Platform

This guide walks you through running the entire project and seeing data flow from Yahoo Finance → Kafka → Delta Lake → Grafana dashboards.

## 📋 Prerequisites

Before starting, ensure you have:

- ✅ **Docker Desktop** installed and running (with at least 8GB RAM allocated)
- ✅ **20GB free disk space**
- ✅ **Internet connection** (for fetching stock data)
- ✅ **Terminal access** (for running commands)

### Check Docker

```bash
# Verify Docker is running
docker info

# Check available resources
docker system df
```

---

## 🚀 Step 1: Start All Services (5 minutes)

### 1.1 Navigate to Project Directory

```bash
cd /Users/pbn/My_project/realtime-us-stock-etl-platform
```

### 1.2 Create Environment File (First Time Only)

```bash
# Copy environment template
cp .env.example .env

# Review/edit if needed (defaults work out of the box)
cat .env
```

**Note:** The `.env.example` file contains all default configuration values. You can customize:
- `STOCK_TICKERS` - Change which stocks to track
- `PRODUCER_INTERVAL_SECONDS` - Change fetch frequency
- `BACKFILL_DAYS` - Change initial backfill duration (default: 30, max: 365)
- `TZ` - Set your timezone
- Passwords and other settings as needed

### 1.3 Start All Docker Services

```bash
# Start all services
docker compose up -d

# Watch logs to see services starting
docker compose logs -f
```

**Wait 30-60 seconds** for all services to be healthy. You should see:
- ✅ Redpanda: Healthy
- ✅ Spark Master: Running
- ✅ PostgreSQL: Ready
- ✅ TimescaleDB: Ready
- ✅ Airflow: Initialized
- ✅ Grafana: Running
- ✅ Producer: Running

### 1.4 Verify Services Are Running

```bash
# Check all containers
docker compose ps

# Expected output: All services should show "Up" status
```

**Expected Output:**
```
NAME                  STATUS          PORTS
redpanda              Up              0.0.0.0:7000-7002->...
redpanda-console      Up              0.0.0.0:7003->...
spark-master          Up              0.0.0.0:7004-7006->...
spark-worker-1        Up              ...
spark-worker-2        Up              ...
postgres-airflow      Up              0.0.0.0:7007->...
postgres-timescale    Up              0.0.0.0:7008->...
airflow-webserver     Up              0.0.0.0:7009->...
airflow-scheduler     Up              ...
stock-producer        Up              ...
grafana               Up              0.0.0.0:7010->...
```

---

## 🔄 Step 2: Spark Streaming Jobs (AUTOMATED!)

**Good News!** Spark streaming jobs now start **automatically** with `docker compose up -d`!

No need to manually open 3 terminals anymore. The jobs are configured as Docker Compose services that:
- ✅ Start automatically
- ✅ Restart on failure
- ✅ Run in correct order (Bronze → Silver → Gold)

### Verify Spark Jobs Are Running

```bash
# Check Spark job containers
docker compose ps | grep spark-

# Expected output:
# spark-bronze-job    Up
# spark-silver-job    Up  
# spark-gold-job      Up
```

### View Spark Job Logs

```bash
# View Bronze layer logs
docker logs -f spark-bronze-job

# View Silver layer logs
docker logs -f spark-silver-job

# View Gold layer logs
docker logs -f spark-gold-job

# View all Spark job logs together
docker compose logs -f spark-bronze spark-silver spark-gold
```

### Check Spark UI

- Open: http://localhost:7004
- You should see 3 running applications:
  - BronzeIngestion
  - SilverCleaning
  - GoldKPIs

### Manual Override (Optional)

If you prefer to run jobs manually (for debugging), you can stop the automated jobs:

```bash
# Stop automated Spark jobs
docker compose stop spark-bronze spark-silver spark-gold

# Then run manually in separate terminals (see old method below)
```

**Old Manual Method (for reference):**
<details>
<summary>Click to expand old manual method</summary>

If you need to run jobs manually:

**Terminal 1 - Bronze:**
```bash
docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages io.delta:delta-core_2.12:2.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /opt/spark-jobs/jobs/bronze_ingestion.py
```

**Terminal 2 - Silver:**
```bash
docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages io.delta:delta-core_2.12:2.4.0 \
  /opt/spark-jobs/jobs/silver_cleaning.py
```

**Terminal 3 - Gold:**
```bash
docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages io.delta:delta-core_2.12:2.4.0 \
  /opt/spark-jobs/jobs/gold_kpis.py
```

</details>

---

## 📊 Step 3: Verify Data Flow (5 minutes)

Now let's verify data is flowing through each stage of the pipeline.

### 3.1 Check Producer is Fetching Data

```bash
# View producer logs
docker logs --tail 50 stock-producer

# You should see messages like:
# "Fetched data for AAPL"
# "Published to Kafka"
# "State updated for AAPL"
```

**Check State File:**
```bash
# View last fetch timestamps
cat producer/state/last_fetch.json | jq

# Expected output:
# {
#   "AAPL": "2025-01-17T15:30:00+00:00",
#   "MSFT": "2025-01-17T15:30:00+00:00",
#   ...
# }
```

### 3.2 Check Kafka (Redpanda) Has Messages

```bash
# Consume messages from Kafka topic
docker exec -it redpanda rpk topic consume stock-raw-data --num 5

# Expected output: JSON messages with stock data
# {
#   "symbol": "AAPL",
#   "timestamp": 1705501800,
#   "open": 185.50,
#   "high": 186.00,
#   "low": 185.25,
#   "close": 185.75,
#   "volume": 250000,
#   "ingested_at": "2025-01-17T15:30:00Z"
# }
```

**Or use Redpanda Console UI:**
- Open: http://localhost:7003
- Navigate to Topics → `stock-raw-data`
- Click "Consume Messages"

### 3.3 Check Bronze Layer (Raw Data)

```bash
# Access Spark shell
docker exec -it spark-master pyspark \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
```

**In PySpark shell:**
```python
# Read Bronze table
df_bronze = spark.read.format("delta").load("/opt/spark/delta_tables/bronze")

# Check record count
print(f"Bronze records: {df_bronze.count()}")

# View recent data
df_bronze.orderBy("bronze_timestamp", ascending=False).show(10, truncate=False)

# Check data per symbol
df_bronze.groupBy("symbol").count().orderBy("symbol").show()

# Exit
exit()
```

**Expected:** You should see records with recent timestamps (within last few minutes).

### 3.4 Check Silver Layer (Cleaned Data)

```bash
# Access Spark shell again
docker exec -it spark-master pyspark \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
```

**In PySpark shell:**
```python
# Read Silver table
df_silver = spark.read.format("delta").load("/opt/spark/delta_tables/silver")

# Check record count
print(f"Silver records: {df_silver.count()}")

# View recent cleaned data
df_silver.filter(df_silver.is_valid == True) \
  .orderBy("processed_at", ascending=False) \
  .show(10, truncate=False)

# Check validation stats
df_silver.groupBy("is_valid").count().show()

# Exit
exit()
```

**Expected:** Cleaned records with `is_valid=True`, proper timestamps, deduplicated.

### 3.5 Check Gold Layer (KPIs)

```bash
# Access Spark shell again
docker exec -it spark-master pyspark \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
```

**In PySpark shell:**
```python
# Read Gold table
df_gold = spark.read.format("delta").load("/opt/spark/delta_tables/gold")

# Check record count
print(f"Gold records: {df_gold.count()}")

# View recent KPIs
df_gold.select("symbol", "ts", "close", "rsi_14", "macd", "daily_return") \
  .orderBy("computed_at", ascending=False) \
  .show(20, truncate=False)

# Check technical indicators
df_gold.filter(df_gold.rsi_14.isNotNull()) \
  .select("symbol", "ts", "close", "rsi_14", "sma_20", "ema_21") \
  .orderBy("ts", ascending=False) \
  .show(10)

# Exit
exit()
```

**Expected:** Records with calculated KPIs (RSI, MACD, SMA, EMA, etc.).

---

## 🎨 Step 4: Access Dashboards (2 minutes)

### 4.1 Grafana Dashboard

1. **Open Grafana:**
   - URL: http://localhost:7010
   - Username: `admin`
   - Password: `admin`

2. **Navigate to Dashboard:**
   - Click "Dashboards" → "Browse"
   - Select "Real-Time Stock Market Dashboard"

3. **What You'll See:**
   - 📈 Real-time stock prices (time series chart)
   - 📊 Daily returns (gauges)
   - 📦 Volume distribution (pie chart)
   - 📉 RSI oscillator (overbought/oversold)
   - 📊 MACD histogram
   - 📋 Top gainers/losers table
   - ⚠️ DLQ message count
   - 🕐 Last data update timestamp

4. **Dashboard Auto-Refreshes:**
   - Every 10 seconds automatically
   - Data appears within 1-2 minutes after starting

### 4.2 Airflow Dashboard

1. **Open Airflow:**
   - URL: http://localhost:7009
   - Username: `admin`
   - Password: `admin`

2. **View DAGs:**
   - Click "DAGs" in the top menu
   - You should see 5 DAGs:
     - `stock_producer` (every 1 minute)
     - `stock_monitoring` (every 5 minutes)
     - `stock_backfill` (manual)
     - `stock_quality_check` (daily)
     - `gold_rebuild` (daily)

3. **Enable and Trigger DAGs:**
   - Toggle DAGs ON (switch on the left)
   - Click on a DAG name to view details
   - Click "Trigger DAG" to run manually

### 4.3 Spark Master UI

1. **Open Spark UI:**
   - URL: http://localhost:7004

2. **View Running Applications:**
   - Click "Running Applications"
   - You should see 3 streaming applications:
     - BronzeIngestion
     - SilverCleaning
     - GoldKPIs

3. **Monitor Performance:**
   - View streaming query progress
   - Check processing times
   - Monitor input/output rates

### 4.4 Redpanda Console

1. **Open Redpanda Console:**
   - URL: http://localhost:7003

2. **View Topics:**
   - Click "Topics" in sidebar
   - Select `stock-raw-data` topic
   - View message count, partitions, offsets

3. **Consume Messages:**
   - Click "Consume Messages"
   - See real-time stock data flowing in

---

## 🔍 Step 5: Monitor Data Flow (Ongoing)

### 5.1 Watch Producer Logs

```bash
# Follow producer logs in real-time
docker logs -f stock-producer

# You'll see:
# - Fetching data for AAPL
# - Published to Kafka
# - State updated for AAPL
# (repeats every 60 seconds)
```

### 5.2 Monitor Spark Streaming Jobs

**In each Spark terminal**, you'll see periodic updates:

```
Batch: 42
Input rows: 10
Processing time: 150ms
Output rows: 10
```

### 5.3 Check Data Freshness

```bash
# Check latest data in Gold table
docker exec -it spark-master pyspark \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
```

```python
from pyspark.sql import functions as F

df = spark.read.format("delta").load("/opt/spark/delta_tables/gold")
latest = df.orderBy("computed_at", ascending=False).first()

print(f"Latest record timestamp: {latest.computed_at}")
print(f"Symbol: {latest.symbol}")
print(f"Price: ${latest.close}")
print(f"RSI: {latest.rsi_14}")

exit()
```

**Expected:** Latest record should be within last 1-2 minutes.

---

## 🎯 Step 6: Verify Complete Pipeline (End-to-End Test)

### Test 1: Producer → Kafka

```bash
# Check producer published to Kafka
docker exec -it redpanda rpk topic describe stock-raw-data

# Should show:
# - Partition count: 10 (one per ticker)
# - Message count: Increasing
```

### Test 2: Kafka → Bronze

```bash
# Check Bronze table has data
docker exec -it spark-master pyspark \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
```

```python
df = spark.read.format("delta").load("/opt/spark/delta_tables/bronze")
print(f"Bronze records: {df.count()}")
df.select("symbol").distinct().show()
exit()
```

**Expected:** 10 distinct symbols (AAPL, MSFT, GOOGL, etc.)

### Test 3: Bronze → Silver

```python
df = spark.read.format("delta").load("/opt/spark/delta_tables/silver")
print(f"Silver records: {df.count()}")
print(f"Valid records: {df.filter(df.is_valid == True).count()}")
df.select("symbol").distinct().show()
exit()
```

**Expected:** Same 10 symbols, all valid records

### Test 4: Silver → Gold

```python
df = spark.read.format("delta").load("/opt/spark/delta_tables/gold")
print(f"Gold records: {df.count()}")
print(f"Records with RSI: {df.filter(df.rsi_14.isNotNull()).count()}")
df.select("symbol", "close", "rsi_14", "macd").orderBy("ts", ascending=False).show(10)
exit()
```

**Expected:** Records with calculated KPIs

### Test 5: Gold → Grafana

1. Open Grafana: http://localhost:7010
2. Check dashboard shows data
3. Verify charts update every 10 seconds
4. Check "Last Data Update" panel shows recent timestamp

---

## 🐛 Troubleshooting

### Issue: No Data Appearing

**Check Market Hours:**
- NYSE operates: Mon-Fri, 9:30 AM - 4:00 PM EST
- Producer only fetches during market hours
- **Solution:** Wait for market hours OR modify producer to run 24/7 for testing

**Check Producer Logs:**
```bash
docker logs stock-producer | tail -50
```

**Check Kafka Has Messages:**
```bash
docker exec -it redpanda rpk topic consume stock-raw-data --num 1
```

### Issue: Spark Jobs Not Processing

**Check Spark Master:**
```bash
# Verify Spark master is running
docker exec -it spark-master curl http://localhost:7004

# Check Spark logs
docker logs spark-master | tail -50
```

**Check Kafka Connection:**
```bash
# Test from Spark container
docker exec -it spark-master python -c "from kafka import KafkaConsumer; c = KafkaConsumer(bootstrap_servers='redpanda:29092'); print('Connected')"
```

### Issue: Grafana Shows No Data

**Check TimescaleDB Connection:**
1. Open Grafana: http://localhost:7010
2. Go to Configuration → Data Sources
3. Click "StockData-TimescaleDB"
4. Click "Test" - should show "Database Connection OK"

**Sync Data to TimescaleDB:**
```bash
# If Gold table has data but Grafana doesn't, sync it
docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages io.delta:delta-core_2.12:2.4.0,org.postgresql:postgresql:42.6.0 \
  /opt/spark-jobs/sync-delta-to-timescale.py
```

### Issue: Services Won't Start

**Check Docker Resources:**
```bash
# Increase Docker Desktop memory to 8GB
# Docker Desktop → Settings → Resources → Memory
```

**Check Port Conflicts:**
```bash
# Check if ports 7000-7010 are in use
lsof -i :7000-7010

# If conflicts, stop conflicting services
```

---

## 📈 Expected Data Flow Timeline

```
Time    | Stage                    | What's Happening
--------|-------------------------|----------------------------------
T+0s    | Producer starts         | Fetches data from Yahoo Finance
T+5s    | Kafka receives          | Messages published to stock-raw-data
T+10s   | Bronze layer processes  | Raw data written to Delta Lake
T+25s   | Silver layer processes  | Data cleaned and validated
T+45s   | Gold layer processes    | KPIs calculated
T+60s   | Grafana updates         | Dashboard shows new data
T+60s   | Producer fetches again  | Cycle repeats every minute
```

**Total End-to-End Latency:** ~45-60 seconds

---

## ✅ Success Checklist

- [ ] All Docker services running (`docker compose ps`)
- [ ] Producer fetching data (check logs)
- [ ] Kafka has messages (`rpk topic consume`)
- [ ] Bronze table has records (Spark shell)
- [ ] Silver table has cleaned records (Spark shell)
- [ ] Gold table has KPIs (Spark shell)
- [ ] Grafana dashboard shows data (http://localhost:7010)
- [ ] Airflow DAGs visible (http://localhost:7009)
- [ ] Spark UI shows 3 running apps (http://localhost:7004)
- [ ] Data updates every 60 seconds

---

## 🎉 You're Done!

Your Real-Time Stock ETL Platform is now running end-to-end:

1. ✅ **Producer** fetches stock data every minute
2. ✅ **Kafka** streams messages in real-time
3. ✅ **Spark** processes data through Bronze → Silver → Gold
4. ✅ **Delta Lake** stores data with ACID guarantees
5. ✅ **Grafana** visualizes data in real-time dashboards
6. ✅ **Airflow** orchestrates and monitors workflows

**Next Steps:**
- Explore Grafana dashboards
- Monitor Airflow DAGs
- Check Spark UI for performance metrics
- Customize tickers in `.env` file
- Add more technical indicators

---

## 🛑 Stopping the System

When you're done:

```bash
# Stop Spark jobs (Ctrl+C in each terminal)

# Stop all Docker services
docker compose down

# Or stop and remove volumes (deletes all data)
docker compose down -v
```

---

**Enjoy your real-time stock ETL platform! 📈🚀**

