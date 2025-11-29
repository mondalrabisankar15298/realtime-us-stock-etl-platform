# Real-Time US Stock ETL Platform

A production-grade, real-time streaming ETL pipeline for US stock market data. Built with industry-standard tools and designed to showcase data engineering best practices.

![Architecture](docs/architecture-diagram.png)

## 🎯 Project Overview

This project implements a complete end-to-end real-time data pipeline that:
- Ingests 1-minute OHLCV stock data from Yahoo Finance
- Processes data through Bronze → Silver → Gold medallion architecture
- Calculates technical indicators (SMA, EMA, RSI, MACD, VWAP, ATR)
- Provides real-time dashboards for market monitoring
- Ensures data quality, reliability, and fault tolerance

**Use Case**: Monitor 10 mega-cap US stocks in real-time with automated alerts, technical analysis, and system health monitoring.

## 🏗️ Architecture

### High-Level Design

```
Yahoo Finance → Producer → Redpanda → Spark Streaming → Delta Lake → Grafana
                   ↓                         ↓
                  DLQ                   Checkpoints
                   ↓
              DLQ Consumer
                   
Orchestration: Apache Airflow
```

### Data Flow

1. **Producer** (Python): Fetches 1-min stock data every minute from Yahoo Finance
2. **Redpanda** (Kafka): Message broker for reliable data streaming
3. **Spark Structured Streaming**: 
   - **Bronze Layer**: Raw data ingestion (append-only)
   - **Silver Layer**: Data cleaning, validation, deduplication (MERGE)
   - **Gold Layer**: KPI computation and technical indicators (MERGE)
4. **Delta Lake**: ACID-compliant storage with time travel
5. **Airflow**: Workflow orchestration and monitoring
6. **Grafana**: Real-time dashboards and alerts

## 🔥 Key Features

### Enterprise-Grade Components

✅ **Idempotent Design**: MERGE operations prevent duplicate data  
✅ **Fault Tolerance**: Checkpointing, retry logic, DLQ for failures  
✅ **State Management**: Persistent state tracking per ticker  
✅ **Data Quality**: Validation, outlier detection, completeness checks  
✅ **Observability**: Structured logging, monitoring DAGs, Grafana dashboards  
✅ **Scalability**: Partitioned Kafka topics, distributed Spark processing  

### Technical Indicators

- **Moving Averages**: SMA (5, 20, 50), EMA (9, 21)
- **Momentum**: RSI (14-period)
- **Trend**: MACD (12/26 with 9-period signal)
- **Volatility**: ATR (14-period), 5-minute volatility
- **Volume**: VWAP (Volume Weighted Average Price)
- **Returns**: Daily returns, price change percentage

## 📦 Tech Stack

| Component | Technology |
|-----------|-----------|
| **Data Source** | Yahoo Finance (yfinance) |
| **Message Broker** | Redpanda (Kafka-compatible) |
| **Stream Processing** | Spark Structured Streaming + Delta Lake |
| **Orchestration** | Apache Airflow |
| **Storage** | Delta Lake (Parquet), TimescaleDB |
| **Visualization** | Grafana |
| **Deployment** | Docker Compose |
| **Language** | Python 3.11, PySpark |

## 🚀 Quick Start

### Prerequisites

- Docker Desktop (with at least 8GB RAM allocated)
- Docker Compose v2.0+
- 20GB free disk space

### Installation

1. **Clone the repository**
   ```bash
   cd realtime-us-stock-etl-platform
   ```

2. **Create environment file**
   ```bash
   cp .env.example .env
   # Edit .env if needed (defaults work out of the box)
   ```

3. **Start all services**
   ```bash
   docker compose up -d
   ```

4. **Verify services are running**
   ```bash
   docker compose ps
   ```

5. **Start Spark streaming jobs**
   ```bash
   # Bronze layer
   docker exec -it spark-master spark-submit \
     --master spark://spark-master:7077 \
     /opt/spark-jobs/jobs/bronze_ingestion.py
   
   # Silver layer (in new terminal)
   docker exec -it spark-master spark-submit \
     --master spark://spark-master:7077 \
     /opt/spark-jobs/jobs/silver_cleaning.py
   
   # Gold layer (in new terminal)
   docker exec -it spark-master spark-submit \
     --master spark://spark-master:7077 \
     /opt/spark-jobs/jobs/gold_kpis.py
   ```

### Access the Dashboards

- **Grafana**: http://localhost:7010 (admin/admin)
- **Airflow**: http://localhost:7009 (admin/admin)
- **Redpanda Console**: http://localhost:7003
- **Spark Master UI**: http://localhost:7004

### 📖 Complete Run Guide

For detailed step-by-step instructions on running the project and verifying data flow end-to-end, see:
- **[COMPLETE_RUN_GUIDE.md](COMPLETE_RUN_GUIDE.md)** - Full walkthrough with troubleshooting
- **[QUICK_START.md](QUICK_START.md)** - 5-minute quick reference

## 📊 Dashboards

### Grafana - Real-Time Stock Market Dashboard

The Grafana dashboard includes:

1. **Market Overview**
   - Live stock prices (last 24 hours)
   - Current daily returns by ticker
   - Volume distribution

2. **Technical Indicators**
   - RSI oscillator (overbought/oversold zones)
   - MACD histogram
   - Moving averages overlay

3. **Analytics**
   - Top gainers/losers table
   - Volatility rankings
   - Market phase indicators

4. **System Health**
   - DLQ message count
   - Last data update timestamp
   - Producer heartbeat

### Airflow DAGs

- **stock_producer**: Monitors producer (every 1 minute)
- **stock_monitoring**: Health checks and alerts (every 5 minutes)
- **stock_backfill**: Historical data backfill (manual trigger)
- **stock_quality_check**: Data quality validation (daily)
- **gold_rebuild**: Rebuild Gold layer (daily)

## 📁 Project Structure

```
realtime-us-stock-etl-platform/
├── docker compose.yml          # Multi-service orchestration
├── .env.example                # Environment variables template
├── Dockerfile.producer         # Producer container
├── Dockerfile.spark            # Spark + Delta Lake container
│
├── producer/                   # Stock data producer
│   ├── producer.py            # Main producer logic
│   ├── requirements.txt       # Python dependencies
│   ├── state/                 # State tracking (gitignored)
│   └── logs/                  # Producer logs (gitignored)
│
├── spark/                      # Spark streaming jobs
│   ├── config.py              # Shared configuration
│   ├── jobs/
│   │   ├── bronze_ingestion.py   # Raw data ingestion
│   │   ├── silver_cleaning.py    # Data cleaning & validation
│   │   └── gold_kpis.py          # KPI computation
│   ├── delta_tables/          # Delta Lake storage (gitignored)
│   └── checkpoints/           # Streaming checkpoints (gitignored)
│
├── airflow/                    # Workflow orchestration
│   └── dags/
│       ├── producer_dag.py         # Producer scheduling
│       ├── monitoring_dag.py       # Health monitoring
│       ├── backfill_dag.py         # Historical backfill
│       ├── qc_dag.py               # Quality checks
│       └── gold_rebuild_dag.py     # Gold layer rebuild
│
├── grafana/                    # Visualization
│   ├── datasources/           # Postgres connection
│   └── dashboards/            # Stock market dashboard
│
├── dlq/                        # Dead Letter Queue
│   ├── dlq_consumer.py        # DLQ message handler
│   └── requirements.txt
│
├── tests/                      # Testing suite
│   ├── test_producer_retry.py
│   ├── test_spark_idempotency.py
│   ├── test_backfill.py
│   └── chaos/
│       └── kill_redpanda.sh   # Chaos engineering test
│
└── docs/                       # Documentation
    ├── architecture.md        # Detailed architecture
    ├── data_model.md          # Schema definitions
    └── runbook.md             # Operations guide
```

## 🔧 Configuration

### Stock Tickers

Edit `.env` to change tracked stocks:
```bash
STOCK_TICKERS=AAPL,MSFT,GOOGL,AMZN,NVDA,META,TSLA,NFLX,AMD,AVGO
```

### Producer Interval

Adjust fetch frequency (in seconds):
```bash
PRODUCER_INTERVAL_SECONDS=60  # Default: 1 minute
```

### Spark Resources

Configure worker resources:
```bash
SPARK_WORKER_CORES=2
SPARK_WORKER_MEMORY=2g
```

## 🧪 Testing

### Run All Tests

```bash
# Producer retry tests
python tests/test_producer_retry.py

# Spark idempotency tests
python tests/test_spark_idempotency.py

# Backfill tests
python tests/test_backfill.py

# Chaos test (Redpanda failure)
bash tests/chaos/kill_redpanda.sh
```

### Manual Testing

```bash
# Check Kafka topics
docker exec -it redpanda rpk topic list
docker exec -it redpanda rpk topic consume stock-raw-data

# Check Delta tables
docker exec -it spark-master pyspark
>>> df = spark.read.format("delta").load("/opt/spark/delta_tables/gold")
>>> df.show()

# Check producer state
cat producer/state/last_fetch.json
```

## 📈 Monitoring & Operations

### Start the System

```bash
docker compose up -d
```

### Stop the System

```bash
docker compose down
```

### View Logs

```bash
# Producer logs
docker logs -f stock-producer

# Airflow scheduler
docker logs -f airflow-scheduler

# Redpanda
docker logs -f redpanda
```

### Troubleshooting

**Producer not fetching data?**
- Check if market is open (NYSE: Mon-Fri 9:30 AM - 4:00 PM EST)
- Verify Redpanda is running: `docker ps | grep redpanda`
- Check producer logs for errors

**Spark jobs not processing?**
- Verify Spark master is running: `docker ps | grep spark-master`
- Check if Bronze table has data: Browse Spark UI at http://localhost:7004
- Review checkpoint directories for errors

**Grafana shows no data?**
- Ensure TimescaleDB data source is configured
- Check if Gold table exists and has data in TimescaleDB
- Run sync script to populate TimescaleDB from Delta Lake
  ```bash
  docker exec -it spark-master spark-submit \
    --packages io.delta:delta-core_2.12:2.4.0,org.postgresql:postgresql:42.6.0 \
    /opt/spark-jobs/sync-delta-to-timescale.py
  ```

## 🎓 Learning Outcomes

This project demonstrates:

- ✅ **Stream Processing**: Kafka + Spark Structured Streaming
- ✅ **Medallion Architecture**: Bronze → Silver → Gold data layers
- ✅ **Data Quality**: Validation, deduplication, outlier detection
- ✅ **Fault Tolerance**: Checkpointing, retry logic, DLQ pattern
- ✅ **ACID Transactions**: Delta Lake MERGE for idempotency
- ✅ **Orchestration**: Airflow DAGs for scheduling and monitoring
- ✅ **Observability**: Logging, metrics, dashboards, alerts
- ✅ **Infrastructure as Code**: Docker Compose for reproducibility

## 🌟 Production Enhancements

To make this production-ready:

1. **Cloud Deployment**: Migrate to AWS/GCP/Azure
   - Use managed Kafka (MSK, Confluent Cloud)
   - Deploy Spark on EMR/Databricks/Dataproc
   - Store Delta tables in S3/GCS/ADLS

2. **Security**
   - Enable Kafka SSL/SASL authentication
   - Implement IAM roles and secrets management
   - Add API authentication for dashboards

3. **Scalability**
   - Auto-scaling Spark workers
   - Kafka partitioning by ticker
   - Horizontal Grafana scaling

4. **Data Governance**
   - Schema registry (Confluent/AWS Glue)
   - Data lineage tracking
   - GDPR compliance measures

5. **Advanced Features**
   - Real-time alerting (PagerDuty/Slack)
   - Machine learning predictions
   - Multi-region replication

## 📝 License

This project is open-source and available under the MIT License.

## 🤝 Contributing

Contributions welcome! Please:
1. Fork the repository
2. Create a feature branch
3. Submit a pull request with tests

## 📧 Contact

For questions or feedback, reach out via GitHub Issues.

---

**Built with ❤️ for data engineering excellence**

