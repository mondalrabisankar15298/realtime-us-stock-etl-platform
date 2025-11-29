# Quick Start Guide

Get the Real-Time Stock ETL Platform running in under 10 minutes!

## Prerequisites

- **Docker Desktop** with at least 8GB RAM
- **20GB** free disk space
- **Internet connection** for downloading images and fetching stock data

## 3-Step Setup

### Step 1: Start Services (2 minutes)

```bash
# Navigate to project directory
cd realtime-us-stock-etl-platform

# Create environment file (uses defaults)
cp .env.example .env

# Start all services
docker-compose up -d

# Wait for services to be healthy
sleep 30

# Verify everything is running
docker-compose ps
```

Expected output: All services should show "Up" status.

### Step 2: Start Spark Streaming (3 minutes)

Open **3 separate terminal windows** and run one command in each:

**Terminal 1 - Bronze Layer:**
```bash
docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages io.delta:delta-core_2.12:2.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /opt/spark-jobs/jobs/bronze_ingestion.py
```

**Terminal 2 - Silver Layer:**
```bash
docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages io.delta:delta-core_2.12:2.4.0 \
  /opt/spark-jobs/jobs/silver_cleaning.py
```

**Terminal 3 - Gold Layer:**
```bash
docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages io.delta:delta-core_2.12:2.4.0 \
  /opt/spark-jobs/jobs/gold_kpis.py
```

You should see "Streaming query started successfully" in each terminal.

### Step 3: Access Dashboards (1 minute)

Open these URLs in your browser:

- **Grafana** (Stock Market Dashboard): http://localhost:7010
  - Username: `admin`
  - Password: `admin`
  - Navigate to Dashboards → Real-Time Stock Market Dashboard

- **Airflow** (Workflow Monitoring): http://localhost:7009
  - Username: `admin`
  - Password: `admin`
  - Check DAGs are running

- **Redpanda Console** (Kafka Topics): http://localhost:7003
  - View messages in `stock-raw-data` topic

- **Spark UI** (Job Monitoring): http://localhost:7004
  - View active streaming applications

## Verify Data Flow

### Check Producer is Running

```bash
# View producer logs
docker logs --tail 50 stock-producer

# Check state file (last fetch times)
cat producer/state/last_fetch.json
```

You should see JSON with timestamps for each ticker.

### Check Kafka Messages

```bash
# Consume messages from raw data topic
docker exec -it redpanda rpk topic consume stock-raw-data --num 5
```

You should see JSON messages with stock data.

### Check Delta Tables

```bash
# Access Spark shell
docker exec -it spark-master pyspark \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
```

```python
# Check Bronze table
df_bronze = spark.read.format("delta").load("/opt/spark/delta_tables/bronze")
print(f"Bronze records: {df_bronze.count()}")
df_bronze.show(5)

# Check Silver table
df_silver = spark.read.format("delta").load("/opt/spark/delta_tables/silver")
print(f"Silver records: {df_silver.count()}")
df_silver.show(5)

# Check Gold table with KPIs
df_gold = spark.read.format("delta").load("/opt/spark/delta_tables/gold")
print(f"Gold records: {df_gold.count()}")
df_gold.select("symbol", "ts", "close", "rsi_14", "macd").show(5)

# Exit
exit()
```

## Troubleshooting

### No Data Appearing?

**Check market hours**: NYSE operates Mon-Fri 9:30 AM - 4:00 PM EST.

If market is closed, modify the producer to run 24/7 for testing:
```python
# Edit producer/producer.py line ~420
# Comment out the market hours check:
# if is_weekday and is_market_hours:
#     ...
```

Then restart the producer:
```bash
docker-compose restart producer
```

### Services Won't Start?

```bash
# Check Docker resources
docker info | grep -i memory

# Increase Docker Desktop memory to 8GB in:
# Docker Desktop → Settings → Resources → Memory
```

### Spark Jobs Fail?

```bash
# Check Spark master is healthy
docker exec -it spark-master curl http://localhost:8080

# View Spark logs
docker exec -it spark-master ls /opt/spark/logs/
```

## Next Steps

1. **Explore Grafana Dashboard**
   - View real-time stock prices
   - Analyze technical indicators (RSI, MACD)
   - Check top gainers/losers

2. **Run Airflow DAGs**
   - Navigate to http://localhost:7009
   - Enable and trigger `stock_monitoring` DAG
   - View quality check results

3. **Test Failure Scenarios**
   ```bash
   # Run chaos test
   bash tests/chaos/kill_redpanda.sh
   
   # Run validation tests
   python tests/test_producer_retry.py
   ```

4. **Customize Configuration**
   - Edit `.env` to change stock tickers
   - Adjust fetch interval
   - Modify Spark resources

## Stopping the System

### Graceful Shutdown

```bash
# Stop Spark jobs (Ctrl+C in each terminal)
# Then stop Docker services:
docker-compose down
```

### Quick Shutdown (No Grace Period)

```bash
docker-compose down --timeout 10
```

Data is preserved in Docker volumes. Next startup will resume from where you left off.

## Get Help

- **Documentation**: See `README.md` for full guide
- **Architecture**: See `docs/architecture.md`
- **Operations**: See `docs/runbook.md`
- **Issues**: Check troubleshooting section in README

---

**Enjoy your real-time stock ETL platform! 📈🚀**

