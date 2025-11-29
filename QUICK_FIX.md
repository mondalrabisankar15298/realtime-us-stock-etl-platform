# Quick Fix Guide

## ✅ Issue 1: docker-compose Command Not Found

**Problem:** `zsh: command not found: docker-compose`

**Solution:** Use `docker compose` (space) instead of `docker-compose` (hyphen)

```bash
# ❌ Wrong
docker-compose up -d

# ✅ Correct
docker compose up -d
```

## ✅ Issue 2: Spark Image Not Found

**Problem:** `bitnami/spark:3.5.0: not found`

**Solution:** The project now builds a custom Spark image automatically.

### First Time Setup

```bash
# Build the custom Spark image (includes Delta Lake)
docker build -f Dockerfile.spark -t stock-etl-spark:latest .

# Then start services
docker compose up -d
```

The docker-compose.yml is configured to build the image automatically, but you can build it manually first if needed.

## 🚀 Complete Startup Commands

```bash
# 1. Navigate to project
cd /Users/pbn/My_project/realtime-us-stock-etl-platform

# 2. Create .env file (if not exists)
cp .env.example .env

# 3. Build Spark image (first time only, or if Dockerfile changed)
docker build -f Dockerfile.spark -t stock-etl-spark:latest .

# 4. Start all services
docker compose up -d

# 5. Wait 60 seconds for all services to start

# 6. Check status
docker compose ps

# 7. View logs
docker compose logs -f
```

## 📋 Verify Everything is Running

```bash
# Check all services
docker compose ps

# Should show all services as "Up":
# - redpanda
# - spark-master
# - spark-worker-1
# - spark-worker-2
# - spark-bronze-job
# - spark-silver-job
# - spark-gold-job
# - postgres-airflow
# - postgres-timescale
# - airflow-webserver
# - airflow-scheduler
# - stock-producer
# - grafana
```

## 🔍 Check Spark Jobs

```bash
# View Spark job logs
docker logs spark-bronze-job
docker logs spark-silver-job
docker logs spark-gold-job

# Check Spark UI
# Open: http://localhost:7004
```

## 🎯 Access Dashboards

- **Grafana**: http://localhost:7010 (admin/admin)
- **Airflow**: http://localhost:7009 (admin/admin)
- **Spark UI**: http://localhost:7004
- **Redpanda Console**: http://localhost:7003

---

**All fixed! Use `docker compose` (space) and build the Spark image first! ✅**

