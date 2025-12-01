# Automatic Data Flow - Complete ETL Pipeline Orchestration

## 🎯 Overview

Your ETL pipeline now supports **automatic data flow** through all layers (Bronze → Silver → Gold) with intelligent orchestration and resource management.

## 🔄 How Automatic Data Flow Works

### Architecture Overview
```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Producer  │───▶│    Kafka    │───▶│   Bronze    │───▶│   Silver    │
│             │    │             │    │  (Raw)      │    │ (Cleaned)   │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
                                                          │
                                                          ▼
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Gold      │    │  Airflow   │    │  Metrics   │
│  (KPIs)     │    │Orchestrator│    │ Dashboard  │
└─────────────┘    └─────────────┘    └─────────────┘
```

### Orchestration Components

1. **ETL Pipeline DAG** (`airflow/dags/etl_pipeline_dag.py`)
   - Runs every 5 minutes
   - Monitors Bronze layer activity
   - Automatically starts Silver when Bronze is active
   - Automatically starts Gold when Silver is active

2. **Dynamic Resource Allocation**
   - Spark workers configured for dynamic scaling
   - Resources allocated on-demand
   - Prevents resource contention

3. **Intelligent Decision Making**
   - Checks data flow activity before starting downstream layers
   - Skips layers if upstream data is not flowing
   - Collects metrics and provides observability

## 🚀 Getting Started

### 1. Start the Pipeline
```bash
# Start all services
docker compose up -d

# Trigger automatic orchestration
./scripts/trigger-etl-pipeline.sh
```

### 2. Monitor the Flow
```bash
# Check pipeline status
docker compose logs spark-bronze --tail 10
docker compose logs spark-silver --tail 10
docker compose logs spark-gold --tail 10

# Airflow UI: http://localhost:6009 (admin/admin)
# Spark UI: http://localhost:6004 (Master), http://localhost:6006 (App UI)
```

## 📊 Pipeline States

### Normal Operation Flow
```
Time: T=0     T=5min   T=10min  T=15min
Bronze: ✅ RUNNING ✅ RUNNING ✅ RUNNING
Silver: ⏳ WAITING 🟢 STARTED ✅ RUNNING
Gold:   ⏳ WAITING ⏳ WAITING 🟢 STARTED
```

### Decision Logic

#### Bronze Layer Monitoring
- ✅ **Active**: Shows recent batch processing logs
- ❌ **Inactive**: No recent processing activity

#### Silver Layer Decision
```python
if bronze_is_active:
    start_silver_layer()
else:
    skip_silver_layer()
```

#### Gold Layer Decision
```python
if silver_is_active:
    start_gold_layer()
else:
    skip_gold_layer()
```

## 🔧 Configuration

### Spark Dynamic Allocation Settings
```yaml
# docker-compose.yml
environment:
  - SPARK_DYNAMIC_ALLOCATION_ENABLED=true
  - SPARK_DYNAMIC_ALLOCATION_MIN_EXECUTORS=1
  - SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS=4
  - SPARK_DYNAMIC_ALLOCATION_INITIAL_EXECUTORS=2
  - SPARK_SHUFFLE_SERVICE_ENABLED=true
```

### Orchestration DAG Settings
```python
# Runs every 5 minutes
schedule_interval='*/5 * * * *'

# Intelligent decisions based on data flow
check_bronze_data_flow() → decide_silver_layer()
check_silver_data_flow() → decide_gold_layer()
```

## 📈 Monitoring & Metrics

### Pipeline Metrics Collected
- Bronze batches processed
- Silver records processed
- Gold KPIs computed
- Pipeline status (full_flow/bronze_only/inactive)

### Health Checks
- Container status monitoring
- Data freshness validation
- Processing activity detection
- Resource utilization tracking

## 🔄 Manual Control Options

### Force Start Specific Layers
```bash
# Start individual layers manually
docker compose up -d spark-silver
docker compose up -d spark-gold

# Stop layers to free resources
docker compose stop spark-silver
docker compose stop spark-gold
```

### DAG Operations
```bash
# Manual DAG trigger
docker compose exec airflow-webserver airflow dags trigger etl_pipeline_orchestration

# Pause/unpause DAG
docker compose exec airflow-webserver airflow dags pause etl_pipeline_orchestration
docker compose exec airflow-webserver airflow dags unpause etl_pipeline_orchestration
```

## 🐛 Troubleshooting

### Common Issues

#### 1. Silver/Gold Won't Start
**Symptom**: Layers remain in "WAITING" state
**Solution**:
- Check Bronze is actively processing data
- Verify Spark cluster has available resources
- Check DAG logs in Airflow UI

#### 2. Resource Contention
**Symptom**: Jobs fail to get executors
**Solution**:
- Increase worker cores/memory in `.env`
- Enable dynamic allocation (already configured)
- Stop competing jobs temporarily

#### 3. Data Not Flowing
**Symptom**: Pipeline metrics show "inactive"
**Solution**:
- Verify producer is sending data to Kafka
- Check Bronze job logs for errors
- Validate Delta table paths exist

### Debug Commands
```bash
# Check all service status
docker compose ps

# View DAG runs
docker compose exec airflow-webserver airflow dags list-runs -d etl_pipeline_orchestration

# Check Spark cluster
curl http://localhost:6004 | grep -A 10 "Workers"

# Monitor resource usage
docker stats
```

## 🎯 Benefits

✅ **Automatic Orchestration**: No manual intervention needed
✅ **Resource Efficiency**: Dynamic allocation prevents waste
✅ **Fault Tolerance**: Automatic recovery and retries
✅ **Observability**: Comprehensive monitoring and metrics
✅ **Scalability**: Adapts to data volume changes

## 🚀 Next Steps

1. **Test the Pipeline**: Run `./scripts/trigger-etl-pipeline.sh`
2. **Monitor in Airflow**: Check DAG runs and logs
3. **Scale Resources**: Adjust worker configs based on load
4. **Add Alerts**: Configure notifications for pipeline issues
5. **Optimize Performance**: Tune batch sizes and processing intervals

---

**The pipeline now automatically flows data from Bronze → Silver → Gold with intelligent resource management and comprehensive monitoring!** 🎉
