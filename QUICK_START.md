# Quick Start - 5 Minute Setup

## 🚀 Fast Track to Running

### 1. Start Services (1 command)

```bash
cd /Users/pbn/My_project/realtime-us-stock-etl-platform
docker compose up -d
```

Wait 30 seconds, then verify:
```bash
docker compose ps
```

### 2. Spark Jobs Start Automatically! ✅

**No manual steps needed!** Spark streaming jobs are now automated Docker Compose services.

They start automatically with `docker compose up -d` and run continuously.

**Verify they're running:**
```bash
docker compose ps | grep spark-
# Should show: spark-bronze-job, spark-silver-job, spark-gold-job

# View logs
docker logs spark-bronze-job
docker logs spark-silver-job
docker logs spark-gold-job
```

### 3. Access Dashboards

- **Grafana**: http://localhost:7010 (admin/admin)
- **Airflow**: http://localhost:7009 (admin/admin)
- **Spark UI**: http://localhost:7004
- **Redpanda Console**: http://localhost:7003

### 4. Verify Data Flow

```bash
# Check producer logs
docker logs --tail 20 stock-producer

# Check Kafka messages
docker exec -it redpanda rpk topic consume stock-raw-data --num 3

# Check Bronze table
docker exec -it spark-master pyspark \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
```

In PySpark:
```python
spark.read.format("delta").load("/opt/spark/delta_tables/bronze").count()
exit()
```

## ⚠️ Important Notes

- **Market Hours**: Producer only fetches during NYSE hours (Mon-Fri, 9:30 AM - 4:00 PM EST)
- **Data Latency**: End-to-end takes ~45-60 seconds
- **Grafana**: Data appears within 1-2 minutes after starting

## 📚 Full Guide

For complete step-by-step instructions, see: **COMPLETE_RUN_GUIDE.md**

## 🛑 Stop Everything

```bash
# Stop Spark jobs (Ctrl+C in terminals)
# Stop Docker services
docker compose down
```

