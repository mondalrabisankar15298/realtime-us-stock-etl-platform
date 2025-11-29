# Operations Runbook

This guide provides step-by-step procedures for operating the Real-Time Stock ETL Platform.

## Table of Contents
- [System Startup](#system-startup)
- [System Shutdown](#system-shutdown)
- [Monitoring](#monitoring)
- [Troubleshooting](#troubleshooting)
- [Maintenance](#maintenance)
- [Incident Response](#incident-response)

---

## System Startup

### Full System Start

```bash
# 1. Navigate to project directory
cd realtime-us-stock-etl-platform

# 2. Ensure environment file exists
cp .env.example .env  # If first time

# 3. Start all Docker services
docker-compose up -d

# 4. Verify services are running
docker-compose ps

# Expected output: All services should show "Up" status
```

### Start Spark Streaming Jobs

Open 3 separate terminal windows:

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

### Verify System Health

```bash
# Check all containers
docker-compose ps

# Check producer logs
docker logs --tail 50 stock-producer

# Check if data is flowing
docker exec -it redpanda rpk topic consume stock-raw-data --num 5

# Access dashboards
# - Grafana: http://localhost:7010 (admin/admin)
# - Airflow: http://localhost:7009 (admin/admin)
# - Redpanda Console: http://localhost:7003
# - Spark UI: http://localhost:7004
```

---

## System Shutdown

### Graceful Shutdown

```bash
# 1. Stop Spark streaming jobs
# Press Ctrl+C in each terminal running Spark jobs

# 2. Wait for checkpoints to complete (watch logs)
# Look for "Stream stopped gracefully" messages

# 3. Stop Docker services
docker-compose down

# 4. Verify all containers stopped
docker-compose ps
```

### Emergency Shutdown

```bash
# Force stop all containers
docker-compose down --timeout 10

# If containers don't stop
docker-compose kill
```

### Preserve Data on Shutdown

```bash
# Stop services but keep volumes
docker-compose down

# Data persists in:
# - Docker volumes (redpanda-data, spark-delta-tables, etc.)
# - Local directories (producer/state, producer/logs)
```

---

## Monitoring

### Key Metrics to Monitor

**Producer Health:**
```bash
# Check producer is running
docker ps --filter name=stock-producer

# Check recent logs
docker logs --tail 100 stock-producer

# Check state file (last fetch times)
cat producer/state/last_fetch.json | jq

# Check producer error rate
docker logs stock-producer | grep -i error | wc -l
```

**Kafka/Redpanda Health:**
```bash
# Check cluster health
docker exec -it redpanda rpk cluster health

# Check topic lag
docker exec -it redpanda rpk topic describe stock-raw-data

# Check consumer groups
docker exec -it redpanda rpk group list

# Monitor message rate
docker exec -it redpanda rpk topic consume stock-raw-data --num 10
```

**Spark Streaming Health:**
```bash
# Check Spark master UI
open http://localhost:7004

# Check running applications
docker exec -it spark-master curl -s http://localhost:8080/api/v1/applications

# Monitor microbatch duration (from logs)
docker exec -it spark-master grep "Processing time" /opt/spark/logs/*.out

# Check checkpoint directories
docker exec -it spark-master ls -la /opt/spark/checkpoints/
```

**Data Freshness:**
```bash
# Check latest Bronze record
docker exec -it spark-master pyspark --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension
>>> df = spark.read.format("delta").load("/opt/spark/delta_tables/bronze")
>>> df.orderBy("bronze_timestamp", ascending=False).show(5)

# Check Silver table count
>>> df_silver = spark.read.format("delta").load("/opt/spark/delta_tables/silver")
>>> df_silver.count()

# Check Gold table latest
>>> df_gold = spark.read.format("delta").load("/opt/spark/delta_tables/gold")
>>> df_gold.orderBy("computed_at", ascending=False).show(5)
```

**DLQ Monitoring:**
```bash
# Check DLQ message count
docker exec -it redpanda rpk topic consume stock-dlq --num 100 | wc -l

# View recent DLQ messages
docker exec -it redpanda rpk topic consume stock-dlq --num 10

# Check DLQ consumer logs
docker logs dlq-consumer 2>/dev/null || echo "DLQ consumer not running"
```

### Airflow Monitoring

Access Airflow UI: http://localhost:7009

**Check DAG Status:**
- Go to DAGs page
- Verify all DAGs are enabled
- Check for failed task instances
- Review recent run history

**Key DAGs:**
- `stock_producer`: Should run every 1 minute
- `stock_monitoring`: Should run every 5 minutes
- `stock_quality_check`: Should run daily

---

## Troubleshooting

### Problem: Producer Not Fetching Data

**Symptoms:**
- No new messages in Kafka
- State file not updating
- Producer logs show no activity

**Diagnosis:**
```bash
# Check if producer container is running
docker ps --filter name=stock-producer

# Check producer logs
docker logs --tail 100 stock-producer

# Check market hours (NYSE: Mon-Fri 9:30 AM - 4:00 PM EST)
date
```

**Solutions:**

1. **Market is closed:**
   - Expected behavior - producer only fetches during market hours
   - Wait for market to open or modify producer to run 24/7 for testing

2. **Producer crashed:**
   ```bash
   # Restart producer
   docker-compose restart producer
   
   # Check logs after restart
   docker logs -f stock-producer
   ```

3. **Kafka connection issue:**
   ```bash
   # Check Redpanda is healthy
   docker exec -it redpanda rpk cluster health
   
   # Restart Redpanda if needed
   docker-compose restart redpanda
   
   # Wait 30 seconds for reconnection
   ```

4. **Yahoo Finance API issue:**
   ```bash
   # Test yfinance manually
   docker exec -it stock-producer python -c "import yfinance as yf; print(yf.Ticker('AAPL').history(period='1d', interval='1m').tail())"
   
   # Check for rate limiting or API errors
   ```

### Problem: Spark Streaming Job Not Processing

**Symptoms:**
- Kafka has messages but Delta tables empty
- Spark job logs show no activity
- Checkpoint not updating

**Diagnosis:**
```bash
# Check Spark master
docker exec -it spark-master curl http://localhost:8080

# Check if streaming job is running
docker exec -it spark-master ps aux | grep spark-submit

# Check Spark logs
docker exec -it spark-master ls /opt/spark/logs/
```

**Solutions:**

1. **Job not started:**
   ```bash
   # Start the Bronze job
   docker exec -it spark-master spark-submit \
     --master spark://spark-master:7077 \
     --packages io.delta:delta-core_2.12:2.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
     /opt/spark-jobs/jobs/bronze_ingestion.py
   ```

2. **Checkpoint corruption:**
   ```bash
   # Stop streaming job (Ctrl+C)
   
   # Delete checkpoint
   docker exec -it spark-master rm -rf /opt/spark/checkpoints/bronze/*
   
   # Restart job (will start from latest Kafka offset)
   ```

3. **Kafka connectivity issue:**
   ```bash
   # Test Kafka connection from Spark
   docker exec -it spark-master python -c "from kafka import KafkaConsumer; c = KafkaConsumer(bootstrap_servers='redpanda:9092'); print('Connected')"
   ```

### Problem: Grafana Shows No Data

**Symptoms:**
- Dashboards are blank
- Queries return no results
- Data source shows error

**Diagnosis:**
```bash
# Check if Grafana is running
docker ps --filter name=grafana

# Check Grafana logs
docker logs --tail 50 grafana

# Check PostgreSQL data source
docker exec -it postgres-grafana psql -U grafana -d stockdata -c "\dt"
```

**Solutions:**

1. **No data in Gold table:**
   ```bash
   # Check if Gold table exists
   docker exec -it spark-master pyspark --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension
   >>> spark.read.format("delta").load("/opt/spark/delta_tables/gold").count()
   
   # If count is 0, wait for data to flow through pipeline
   # Bronze → Silver → Gold (may take 1-2 minutes)
   ```

2. **Data source not configured:**
   - Open Grafana: http://localhost:7010
   - Go to Configuration → Data Sources
   - Verify "StockData-Postgres" exists and is connected
   - Test connection

3. **PostgreSQL not accessible:**
   ```bash
   # Check PostgreSQL is running
   docker exec -it postgres-grafana pg_isready
   
   # Test connection
   docker exec -it postgres-grafana psql -U grafana -d stockdata -c "SELECT version()"
   ```

### Problem: High DLQ Message Count

**Symptoms:**
- Many messages in stock-dlq topic
- Grafana alert showing DLQ > 10
- DLQ consumer logs show errors

**Diagnosis:**
```bash
# Check DLQ message count
docker exec -it redpanda rpk topic consume stock-dlq --num 100 | wc -l

# View recent failures
docker exec -it redpanda rpk topic consume stock-dlq --num 10

# Check failure patterns
cat dlq/logs/dlq_failures.log | jq '.error' | sort | uniq -c
```

**Solutions:**

1. **Temporary API issues:**
   - Wait for Yahoo Finance API to recover
   - Messages will be retried automatically

2. **Invalid ticker symbols:**
   - Review .env file
   - Ensure STOCK_TICKERS contains valid symbols
   - Remove invalid symbols

3. **Network issues:**
   ```bash
   # Test network connectivity
   docker exec -it stock-producer ping -c 3 yahoo.com
   
   # Check DNS resolution
   docker exec -it stock-producer nslookup query1.finance.yahoo.com
   ```

---

## Maintenance

### Daily Maintenance

```bash
# 1. Check system health
docker-compose ps

# 2. Review Airflow DAG runs
# Open http://localhost:7009 and check for failures

# 3. Monitor disk usage
df -h
docker system df

# 4. Check DLQ message count
docker exec -it redpanda rpk topic describe stock-dlq

# 5. Review Grafana dashboards for anomalies
```

### Weekly Maintenance

```bash
# 1. Optimize Delta tables
docker exec -it spark-master pyspark --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension
>>> spark.sql("OPTIMIZE delta.`/opt/spark/delta_tables/silver`")
>>> spark.sql("OPTIMIZE delta.`/opt/spark/delta_tables/gold`")

# 2. Vacuum old versions (keep 7 days)
>>> spark.sql("VACUUM delta.`/opt/spark/delta_tables/silver` RETAIN 168 HOURS")

# 3. Clean up Docker
docker system prune -f

# 4. Backup state files
cp producer/state/last_fetch.json backups/last_fetch_$(date +%Y%m%d).json

# 5. Review logs for errors
docker-compose logs --since 7d | grep -i error > error_log_$(date +%Y%m%d).txt
```

### Monthly Maintenance

```bash
# 1. Update Docker images
docker-compose pull

# 2. Recreate containers (preserves volumes)
docker-compose up -d --force-recreate

# 3. Review and clean up old backups
ls -lh backups/

# 4. Archive old Delta table versions
# (Implement based on retention policy)

# 5. Performance tuning review
# - Check Spark UI for slow stages
# - Review Kafka partition distribution
# - Optimize Grafana dashboard queries
```

### Backup Procedures

```bash
# Backup state files
tar -czf backups/state_$(date +%Y%m%d).tar.gz producer/state/

# Backup Delta tables
tar -czf backups/delta_$(date +%Y%m%d).tar.gz \
  spark/delta_tables/bronze/ \
  spark/delta_tables/silver/ \
  spark/delta_tables/gold/

# Backup Airflow DAGs
tar -czf backups/dags_$(date +%Y%m%d).tar.gz airflow/dags/

# Backup Grafana dashboards
docker exec -it grafana grafana-cli admin export > backups/dashboards_$(date +%Y%m%d).json
```

---

## Incident Response

### Incident Severity Levels

**P1 - Critical (Immediate Response)**
- Complete system outage
- Data loss detected
- Security breach

**P2 - High (1-hour response)**
- Producer down > 10 minutes
- Spark streaming job failed
- DLQ messages > 100

**P3 - Medium (4-hour response)**
- Performance degradation
- High error rate
- Data quality issues

**P4 - Low (Next business day)**
- Minor bugs
- Documentation updates
- Feature requests

### P1 Incident: Complete System Outage

1. **Assess Impact:**
   ```bash
   docker-compose ps
   docker stats --no-stream
   ```

2. **Restart All Services:**
   ```bash
   docker-compose down
   docker-compose up -d
   ```

3. **Verify Recovery:**
   ```bash
   # Wait 2 minutes
   docker-compose ps
   docker logs --tail 20 stock-producer
   ```

4. **Check Data Integrity:**
   ```bash
   # Verify last state timestamp
   cat producer/state/last_fetch.json
   
   # Check for data gaps
   # Trigger backfill DAG if needed
   ```

### P2 Incident: Producer Down

1. **Check Logs:**
   ```bash
   docker logs --tail 100 stock-producer
   ```

2. **Restart Producer:**
   ```bash
   docker-compose restart producer
   ```

3. **Monitor Recovery:**
   ```bash
   docker logs -f stock-producer
   ```

4. **Backfill Missing Data:**
   - Open Airflow UI: http://localhost:7009
   - Trigger `stock_backfill` DAG
   - Monitor execution

### P2 Incident: Spark Job Failed

1. **Check Spark UI:**
   - Open http://localhost:7004
   - Review failed stages

2. **Check Logs:**
   ```bash
   docker exec -it spark-master ls /opt/spark/logs/
   docker exec -it spark-master cat /opt/spark/logs/<app-id>.log
   ```

3. **Restart Job:**
   ```bash
   # Restart appropriate layer
   docker exec -it spark-master spark-submit \
     --master spark://spark-master:7077 \
     --packages io.delta:delta-core_2.12:2.4.0 \
     /opt/spark-jobs/jobs/silver_cleaning.py
   ```

4. **Verify Recovery:**
   - Check Spark UI for active application
   - Monitor progress in logs

---

## Contact Information

**Oncall Engineer**: [Your Name]  
**Escalation**: [Manager Name]  
**Slack Channel**: #stock-etl-alerts  
**PagerDuty**: [PD Service Link]  

## Additional Resources

- [Architecture Documentation](./architecture.md)
- [Data Model](./data_model.md)
- [GitHub Repository](https://github.com/your-repo/stock-etl-platform)

