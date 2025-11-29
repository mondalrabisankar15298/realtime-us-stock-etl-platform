# System Architecture

## Overview

The Real-Time Stock ETL Platform is designed as a multi-layered streaming architecture that ingests, processes, and visualizes stock market data in near real-time.

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                        YAHOO FINANCE API                             │
│                     (1-minute OHLCV data)                            │
└────────────────────────────┬────────────────────────────────────────┘
                             │
                             │ HTTP Requests
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      PRODUCER (Python)                               │
│  • Fetches data every 60 seconds                                    │
│  • Retry logic with exponential backoff                             │
│  • State management (last fetch timestamp)                          │
│  • DLQ publishing on failure                                        │
└────────────────────────────┬────────────────────────────────────────┘
                             │
                             │ Publish JSON
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    REDPANDA (Kafka API)                              │
│  Topics:                                                             │
│    • stock-raw-data (partitioned by symbol)                         │
│    • stock-dlq (dead letter queue)                                  │
│  • 48-hour retention                                                │
│  • High-throughput messaging                                        │
└───────────────────┬─────────────────────────────────────────────────┘
                    │
                    │ Stream
                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│              SPARK STRUCTURED STREAMING                              │
│                                                                      │
│  ┌──────────────────────────────────────────────────────────┐      │
│  │  BRONZE LAYER (Raw Ingestion)                            │      │
│  │  • Read from Kafka                                       │      │
│  │  • Parse JSON                                            │      │
│  │  • Append-only writes                                    │      │
│  │  • Checkpoint: /checkpoints/bronze                       │      │
│  └────────────────────┬─────────────────────────────────────┘      │
│                       │                                             │
│                       ▼                                             │
│  ┌──────────────────────────────────────────────────────────┐      │
│  │  SILVER LAYER (Cleaning & Validation)                    │      │
│  │  • Type casting                                          │      │
│  │  • Deduplication (symbol, timestamp)                     │      │
│  │  • Validation (price > 0, volume >= 0)                   │      │
│  │  • Timezone normalization (UTC)                          │      │
│  │  • MERGE for idempotency                                 │      │
│  │  • Checkpoint: /checkpoints/silver                       │      │
│  └────────────────────┬─────────────────────────────────────┘      │
│                       │                                             │
│                       ▼                                             │
│  ┌──────────────────────────────────────────────────────────┐      │
│  │  GOLD LAYER (KPI Computation)                            │      │
│  │  • Technical indicators: SMA, EMA, RSI, MACD, VWAP, ATR  │      │
│  │  • Derived metrics: daily returns, volatility            │      │
│  │  • Market phase detection                                │      │
│  │  • MERGE for idempotency                                 │      │
│  │  • Checkpoint: /checkpoints/gold                         │      │
│  └────────────────────┬─────────────────────────────────────┘      │
│                       │                                             │
└───────────────────────┼─────────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       DELTA LAKE                                     │
│  • ACID transactions                                                │
│  • Time travel (versioning)                                         │
│  • Parquet file format                                              │
│  • Schema evolution                                                 │
│  Paths:                                                             │
│    /delta_tables/bronze/                                            │
│    /delta_tables/silver/ (partitioned by ingestion_date)            │
│    /delta_tables/gold/                                              │
└────────────────────────────┬────────────────────────────────────────┘
                             │
                             │ SQL Queries
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      GRAFANA DASHBOARDS                              │
│  • Real-time stock prices                                           │
│  • Technical indicators (charts)                                    │
│  • Top gainers/losers                                               │
│  • System health metrics                                            │
└─────────────────────────────────────────────────────────────────────┘


┌─────────────────────────────────────────────────────────────────────┐
│                    APACHE AIRFLOW (Orchestration)                    │
│  DAGs:                                                               │
│    • producer_dag: Monitor producer (1 min)                         │
│    • monitoring_dag: Health checks (5 min)                          │
│    • backfill_dag: Historical data gaps (manual)                    │
│    • qc_dag: Data quality checks (daily)                            │
│    • gold_rebuild_dag: Rebuild Gold layer (daily)                   │
└─────────────────────────────────────────────────────────────────────┘


┌─────────────────────────────────────────────────────────────────────┐
│                      DLQ CONSUMER                                    │
│  • Monitors stock-dlq topic                                         │
│  • Logs failures to file                                            │
│  • Sends alerts (Slack/Email placeholder)                           │
│  • Enables manual intervention                                      │
└─────────────────────────────────────────────────────────────────────┘
```

## Component Details

### 1. Producer

**Technology**: Python 3.11, yfinance, kafka-python, tenacity

**Responsibilities**:
- Fetch 1-minute OHLCV data from Yahoo Finance
- Track last successful fetch per ticker in state file
- Retry with exponential backoff (max 3 attempts)
- Publish to Kafka topic `stock-raw-data`
- Send failed messages to DLQ

**Design Patterns**:
- Circuit Breaker: Stop retrying after max attempts
- State Management: Persist last timestamp to enable gap detection
- Idempotent Design: Same timestamp won't create duplicates downstream

### 2. Redpanda (Kafka)

**Technology**: Redpanda v23.3.3 (Kafka API compatible)

**Configuration**:
- Topics: `stock-raw-data`, `stock-dlq`
- Partitioning: By symbol (ensures ordering per ticker)
- Retention: 48 hours
- Replication: Single node (production would use 3+)

**Advantages over Kafka**:
- Simpler deployment (no ZooKeeper)
- Lower resource usage
- Faster performance

### 3. Spark Structured Streaming

**Technology**: Spark 3.5.0, Delta Lake 2.4.0, PySpark

**Bronze Layer**:
- **Input**: Kafka topic `stock-raw-data`
- **Processing**: Parse JSON, add ingestion timestamp
- **Output**: Delta table (append-only)
- **Trigger**: 10-second microbatches
- **Checkpoint**: Ensures exactly-once processing

**Silver Layer**:
- **Input**: Bronze Delta table
- **Processing**:
  - Type casting (timestamp, doubles)
  - Validation (price > 0, OHLC consistency)
  - Deduplication by (symbol, timestamp)
  - Timezone normalization
- **Output**: Delta table (MERGE for idempotency)
- **Trigger**: 15-second microbatches
- **Partition**: By ingestion_date

**Gold Layer**:
- **Input**: Silver Delta table
- **Processing**:
  - Calculate SMA (5, 20, 50 periods)
  - Calculate EMA (9, 21 periods)
  - Calculate RSI (14-period)
  - Calculate MACD (12/26 with 9-period signal)
  - Calculate ATR (14-period)
  - Calculate VWAP
  - Compute daily returns, volatility
  - Detect market phase (pre-market/open/post-market/closed)
- **Output**: Delta table (MERGE for idempotency)
- **Trigger**: 20-second microbatches

### 4. Delta Lake

**Technology**: Delta Lake 2.4.0 (Parquet-based)

**Features Used**:
- **ACID Transactions**: Ensures data consistency
- **MERGE Operation**: Upserts for idempotency
- **Time Travel**: Query historical versions
- **Schema Evolution**: Add columns without breaking
- **Partitioning**: Optimize queries by date

**Storage Format**:
```
delta_tables/
├── bronze/
│   ├── _delta_log/
│   └── *.parquet
├── silver/
│   ├── _delta_log/
│   ├── ingestion_date=2025-01-01/
│   └── ingestion_date=2025-01-02/
└── gold/
    ├── _delta_log/
    └── *.parquet
```

### 5. Apache Airflow

**Technology**: Airflow 2.8.0, LocalExecutor, PostgreSQL metadata DB

**DAGs**:
1. **stock_producer** (*/1 * * * *): Monitor producer health
2. **stock_monitoring** (*/5 * * * *): System health checks
3. **stock_backfill** (manual): Fill data gaps
4. **stock_quality_check** (daily): Data validation
5. **gold_rebuild** (daily): Recompute Gold layer

**Monitoring Checks**:
- Producer heartbeat (state file freshness)
- DLQ message count
- Data freshness (last Bronze record)
- Spark streaming job health

### 6. Grafana

**Technology**: Grafana 10.2.3, PostgreSQL data source

**Dashboards**:
- Real-time stock prices (time series)
- Daily returns (gauges)
- Volume distribution (pie chart)
- RSI oscillator (time series with thresholds)
- MACD histogram (bar chart)
- Top gainers/losers (table)
- System health (stat panels)

**Refresh Rate**: 10 seconds

### 7. DLQ Consumer

**Technology**: Python 3.11, kafka-python

**Responsibilities**:
- Consume from `stock-dlq` topic
- Log failures to `/app/logs/dlq_failures.log`
- Analyze failure patterns
- Send notifications (Slack/Email placeholders)

## Data Flow

### Normal Flow

1. Producer fetches AAPL data at 10:00:00 AM
2. Publishes to Kafka: `{"symbol": "AAPL", "timestamp": 1704369600, ...}`
3. Bronze job reads from Kafka, writes to Bronze Delta table
4. Silver job reads Bronze, validates, deduplicates, writes to Silver
5. Gold job reads Silver, calculates RSI/MACD, writes to Gold
6. Grafana queries Gold table, displays on dashboard

### Failure Flow

1. Producer fails to fetch AAPL (Yahoo Finance timeout)
2. Retry 3 times with exponential backoff
3. After max retries, publish to DLQ topic
4. DLQ consumer logs failure
5. Alert sent to operations team
6. Manual investigation and potential backfill

## Scalability Considerations

### Current Limits
- 10 tickers, 1-minute granularity
- ~600 records/hour/ticker = 6,000 records/hour total
- Storage: ~1GB/month (uncompressed), ~100MB/month (Parquet)

### Scaling to 1000 Tickers
1. **Producer**: Multi-threaded or multiple producer instances
2. **Kafka**: Increase partitions (10-100), add brokers (3-5 nodes)
3. **Spark**: Add workers (5-10), increase memory per worker
4. **Delta Lake**: Partition by symbol + date, optimize file sizes
5. **Grafana**: Pre-aggregate data, use materialized views

## High Availability

### Single Points of Failure (Current)
- ❌ Redpanda: Single node
- ❌ Spark Master: Single node
- ❌ PostgreSQL: Single instance
- ❌ Grafana: Single instance

### Production HA Design
- ✅ Redpanda: 3-node cluster with replication factor 3
- ✅ Spark: Standalone HA or managed service (EMR/Databricks)
- ✅ PostgreSQL: Primary-replica with failover
- ✅ Grafana: Load balanced with session persistence
- ✅ Delta Lake: Store in S3/GCS (99.999999999% durability)

## Security Considerations

### Current (Development)
- No authentication on Kafka
- Hardcoded passwords in .env
- No encryption in transit
- No access controls

### Production Hardening
1. **Kafka**: Enable SASL/SSL, ACLs per topic
2. **Secrets**: Use AWS Secrets Manager / HashiCorp Vault
3. **Network**: VPC isolation, security groups
4. **Encryption**: TLS for all inter-service communication
5. **Authentication**: OAuth2 for Grafana/Airflow
6. **Audit Logs**: Track all data access

## Cost Optimization

### AWS Cost Estimate (1000 tickers)
- Kafka (MSK): $500/month
- Spark (EMR): $800/month
- S3 Storage: $50/month
- RDS PostgreSQL: $100/month
- Grafana (managed): $200/month
- **Total**: ~$1,650/month

### Optimization Strategies
1. Use Spot instances for Spark workers (70% savings)
2. Compress Parquet files (80% storage reduction)
3. Lifecycle policies: Move old data to Glacier
4. Right-size instances based on actual usage
5. Use S3 Intelligent Tiering

## Disaster Recovery

### Backup Strategy
- **State Files**: Backup to S3 every hour
- **Delta Tables**: Incremental backups to S3
- **Airflow Metadata**: PostgreSQL daily snapshots
- **Grafana Dashboards**: Export JSON to Git

### Recovery Objectives
- **RTO** (Recovery Time Objective): 1 hour
- **RPO** (Recovery Point Objective): 5 minutes

### Recovery Procedures
1. Restore state files from S3
2. Restore Delta tables from S3 backup
3. Restore Airflow metadata from snapshot
4. Restart all services
5. Backfill any missing data (5-minute window)

## Monitoring & Alerting

### Metrics to Track
- Kafka lag per partition
- Spark microbatch duration
- DLQ message rate
- Data freshness (age of latest record)
- Producer error rate
- Disk usage on Delta tables

### Alerts
- 🚨 CRITICAL: Producer down > 5 minutes
- 🚨 CRITICAL: DLQ messages > 100
- ⚠️ WARNING: Data stale > 15 minutes
- ⚠️ WARNING: Kafka lag > 10,000 messages
- ℹ️ INFO: Quality check failed (outliers detected)

## Future Enhancements

1. **Real-Time ML**: Predict price movements using streaming models
2. **Multi-Exchange**: Add NASDAQ, NYSE, crypto exchanges
3. **Alert Engine**: Price threshold alerts, volume spikes
4. **Portfolio Tracking**: Real-time P&L calculation
5. **Historical Analysis**: Backtesting strategies
6. **API Layer**: REST API for querying data

