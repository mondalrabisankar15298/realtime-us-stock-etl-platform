# Automation Explained - Why Spark Jobs Are Now Automated

## ❌ Previous Problem

Previously, you had to manually:
1. Open 3 separate terminal windows
2. Run each Spark job manually
3. Keep terminals open (jobs run continuously)
4. Restart manually if jobs crash

**This was not user-friendly!**

## ✅ New Solution - Fully Automated

Spark streaming jobs are now **automated Docker Compose services** that:
- ✅ Start automatically with `docker-compose up -d`
- ✅ Restart automatically if they crash (`restart: unless-stopped`)
- ✅ Start in correct order (Bronze → Silver → Gold)
- ✅ No manual terminal windows needed!

## 🔧 How It Works

### Docker Compose Services

Three new services were added to `docker-compose.yml`:

1. **spark-bronze** - Runs Bronze layer ingestion
2. **spark-silver** - Runs Silver layer cleaning
3. **spark-gold** - Runs Gold layer KPI computation

Each service:
- Uses the same Spark image
- Mounts the same volumes (code, data, checkpoints)
- Runs the Spark job as a continuous process
- Automatically restarts on failure

### Startup Sequence

```
1. docker-compose up -d
   ↓
2. Spark master starts
   ↓
3. spark-bronze starts (waits 30s for master)
   ↓
4. spark-silver starts (waits 40s, depends on bronze)
   ↓
5. spark-gold starts (waits 50s, depends on silver)
   ↓
6. All jobs running automatically!
```

## 🚀 Usage

### Start Everything (One Command!)

```bash
docker-compose up -d
```

That's it! All Spark jobs start automatically.

### Check Status

```bash
# Check all services including Spark jobs
docker-compose ps

# Check Spark jobs specifically
docker-compose ps | grep spark-

# Expected output:
# spark-bronze-job    Up
# spark-silver-job    Up
# spark-gold-job      Up
```

### View Logs

```bash
# Bronze layer logs
docker logs -f spark-bronze-job

# Silver layer logs
docker logs -f spark-silver-job

# Gold layer logs
docker logs -f spark-gold-job

# All Spark job logs
docker-compose logs -f spark-bronze spark-silver spark-gold
```

### Stop Jobs

```bash
# Stop specific job
docker-compose stop spark-bronze

# Stop all Spark jobs
docker-compose stop spark-bronze spark-silver spark-gold

# Stop everything
docker-compose down
```

## 📊 Monitoring

### Spark UI
- URL: http://localhost:7004
- View all 3 running applications
- Monitor processing times
- Check input/output rates

### Docker Logs
```bash
# Real-time logs from all jobs
docker-compose logs -f spark-bronze spark-silver spark-gold
```

### Check Job Health
```bash
# List running Spark applications
docker exec spark-master curl -s http://localhost:7004/api/v1/applications | jq

# Should show 3 applications:
# - BronzeIngestion
# - SilverCleaning
# - GoldKPIs
```

## 🔄 Manual Override (If Needed)

If you prefer to run jobs manually (for debugging), you can:

### Option 1: Stop Automated Jobs, Run Manually

```bash
# Stop automated jobs
docker-compose stop spark-bronze spark-silver spark-gold

# Run manually in terminals (as before)
docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages io.delta:delta-core_2.12:2.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /opt/spark-jobs/jobs/bronze_ingestion.py
```

### Option 2: Use Helper Scripts

```bash
# Start all jobs via script (alternative method)
./scripts/start-spark-jobs.sh

# Stop all jobs
./scripts/stop-spark-jobs.sh
```

## ⚙️ Configuration

### Adjust Startup Delays

If jobs start too fast/slow, edit `docker-compose.yml`:

```yaml
spark-bronze:
  command: >
    bash -c "
    sleep 30 &&  # Change this delay
    spark-submit ...
    "
```

### Change Restart Policy

```yaml
spark-bronze:
  restart: unless-stopped  # Options: no, always, on-failure, unless-stopped
```

## 🎯 Benefits

✅ **One Command** - `docker-compose up -d` starts everything  
✅ **Auto-Restart** - Jobs restart automatically on failure  
✅ **No Manual Steps** - No need for 3 terminal windows  
✅ **Production-Ready** - Proper service management  
✅ **Easy Monitoring** - View logs via Docker commands  
✅ **Consistent** - Same behavior every time  

## 🆚 Comparison

### Before (Manual)
```bash
# Terminal 1
docker exec -it spark-master spark-submit ... bronze_ingestion.py

# Terminal 2
docker exec -it spark-master spark-submit ... silver_cleaning.py

# Terminal 3
docker exec -it spark-master spark-submit ... gold_kpis.py

# Problems:
# - Need 3 terminals open
# - Manual restart on crash
# - Easy to forget a job
# - Not production-ready
```

### After (Automated)
```bash
# One command
docker-compose up -d

# Benefits:
# - Everything starts automatically
# - Auto-restart on failure
# - No manual intervention
# - Production-ready
```

## 🐛 Troubleshooting

### Jobs Not Starting

```bash
# Check if Spark master is ready
docker exec spark-master curl http://localhost:7004

# Check job logs
docker logs spark-bronze-job
docker logs spark-silver-job
docker logs spark-gold-job

# Check service status
docker-compose ps spark-bronze spark-silver spark-gold
```

### Jobs Keep Restarting

```bash
# Check logs for errors
docker logs spark-bronze-job | tail -50

# Common issues:
# - Spark master not ready (wait longer)
# - Missing dependencies (check packages)
# - Checkpoint corruption (delete checkpoints)
```

### View Detailed Logs

```bash
# Follow all Spark job logs
docker-compose logs -f spark-bronze spark-silver spark-gold

# Check specific job
docker logs -f spark-bronze-job
```

---

**Now you can start everything with one command: `docker-compose up -d`! 🚀**

