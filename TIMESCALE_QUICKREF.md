# TimescaleDB Quick Reference

## 🚀 One-Command Upgrade

```bash
# Stop services, start with TimescaleDB (auto-initializes)
docker-compose down && docker-compose up -d
```

That's it! TimescaleDB is now running with all tables, indexes, and continuous aggregates configured.

## 🔍 Quick Verification

```bash
# Check TimescaleDB is running
docker exec -it postgres-timescale psql -U grafana -d stockdata -c "SELECT * FROM timescaledb_information.hypertables;"

# Should show: gold_stocks and silver_stocks as hypertables
```

## 📊 Useful SQL Queries

### Check Latest Data
```sql
SELECT * FROM latest_prices;
```

### Top Gainers/Losers
```sql
SELECT * FROM top_movers ORDER BY daily_return DESC LIMIT 5;
```

### RSI Signals
```sql
SELECT * FROM rsi_signals WHERE signal != 'NEUTRAL';
```

### 5-Minute Aggregates (Fast!)
```sql
SELECT * FROM gold_5min 
WHERE bucket >= NOW() - INTERVAL '6 hours'
ORDER BY bucket DESC;
```

### Time-Bucket Custom Aggregation
```sql
SELECT 
    time_bucket('15 minutes', ts) as period,
    symbol,
    avg(close) as avg_price,
    max(rsi_14) as max_rsi
FROM gold_stocks
WHERE ts >= NOW() - INTERVAL '24 hours'
GROUP BY period, symbol
ORDER BY period DESC;
```

### Compression Stats
```sql
SELECT 
    pg_size_pretty(before_compression_total_bytes) as before,
    pg_size_pretty(after_compression_total_bytes) as after,
    100 - (after_compression_total_bytes::float / before_compression_total_bytes::float * 100) as savings_pct
FROM timescaledb_information.compression_stats;
```

## 🔧 Common Commands

### Connect to TimescaleDB
```bash
docker exec -it postgres-timescale psql -U grafana -d stockdata
```

### View All Tables
```sql
\dt
```

### View Hypertables
```sql
SELECT * FROM timescaledb_information.hypertables;
```

### View Continuous Aggregates
```sql
SELECT * FROM timescaledb_information.continuous_aggregates;
```

### Check Table Sizes
```sql
SELECT 
    hypertable_name,
    pg_size_pretty(total_bytes) as total_size
FROM timescaledb_information.hypertables
ORDER BY total_bytes DESC;
```

## 🎯 Grafana Query Examples

### Real-Time Prices (Use Continuous Aggregate)
```sql
SELECT 
    bucket as time,
    symbol as metric,
    close as value
FROM gold_5min
WHERE $__timeFilter(bucket)
ORDER BY bucket;
```

### RSI with Threshold Lines
```sql
SELECT 
    ts as time,
    symbol as metric,
    rsi_14 as value
FROM gold_stocks
WHERE $__timeFilter(ts)
  AND rsi_14 IS NOT NULL
ORDER BY ts;
```

### Top Movers Table
```sql
SELECT 
    symbol,
    current_price as "Price",
    daily_return as "Return %",
    rsi_14 as "RSI",
    movement_type as "Type"
FROM top_movers
ORDER BY ABS(daily_return) DESC
LIMIT 10;
```

## 📈 Performance Tips

1. **Use `gold_5min` for dashboards** - 50x faster than raw data
2. **Use `time_bucket()` for aggregations** - Optimized for time-series
3. **Add indexes on filtered columns** - Speeds up WHERE clauses
4. **Query recent data first** - TimescaleDB optimizes recent chunks
5. **Use helper views** - Pre-built queries for common use cases

## 🐛 Troubleshooting

### Issue: Container won't start
```bash
docker logs postgres-timescale
# Check for initialization errors
```

### Issue: Extension not loaded
```bash
docker exec -it postgres-timescale psql -U grafana -d stockdata -c "CREATE EXTENSION timescaledb;"
```

### Issue: No data visible
```bash
# Sync from Delta Lake
docker exec -it spark-master spark-submit \
  --packages io.delta:delta-core_2.12:2.4.0,org.postgresql:postgresql:42.6.0 \
  /opt/spark-jobs/sync-delta-to-timescale.py
```

### Issue: Grafana connection failed
```bash
# Restart Grafana
docker-compose restart grafana

# Test connection manually
docker exec -it postgres-timescale pg_isready -U grafana
```

## 📚 Full Documentation

- **UPGRADE_SUMMARY.md** - What changed and why
- **TIMESCALE_MIGRATION.md** - Complete step-by-step guide
- **docs/database_upgrade_guide.md** - Technical deep dive

## 🎉 Key Benefits

✅ **50-100x faster** queries  
✅ **90% less storage** (compression)  
✅ **Auto-partitioning** (hypertables)  
✅ **Pre-computed aggregates** (continuous aggregates)  
✅ **100% PostgreSQL compatible**  

---

**TimescaleDB = PostgreSQL + Time-Series Superpowers! ⚡📈**

