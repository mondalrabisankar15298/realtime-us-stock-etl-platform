# Internal Ports Update Summary

## ✅ Updated Internal Ports

The following internal container ports have been updated to match the 7000-7999 range:

| Service | Old Internal Port | New Internal Port | Configuration Method |
|---------|-------------------|-------------------|---------------------|
| **Grafana** | 3000 | **7010** | `GF_SERVER_HTTP_PORT=7010` |
| **Airflow Webserver** | 8080 | **7009** | `AIRFLOW__WEBSERVER__WEB_SERVER_PORT=7009` |
| **Spark Master UI** | 8080 | **7004** | `SPARK_MASTER_WEBUI_PORT=7004` |
| **Spark App UI** | 4040 | **7006** | `SPARK_UI_PORT=7006` |
| **Redpanda Console** | 8080 | **7003** | `PORT=7003` |

## ⚠️ Ports Kept as Standard (Protocol-Specific)

These ports remain unchanged as they are standard protocol ports:

| Service | Internal Port | Reason |
|---------|---------------|--------|
| **PostgreSQL (Airflow)** | 5432 | Standard PostgreSQL port - changing would require connection string updates |
| **TimescaleDB** | 5432 | Standard PostgreSQL port - changing would require connection string updates |
| **Spark Master** | 7077 | Standard Spark master port - changing would break Spark cluster communication |
| **Redpanda Kafka** | 9092 | Standard Kafka port - changing would break Kafka clients |
| **Redpanda Pandaproxy** | 28082 | Redpanda-specific port |
| **Redpanda Admin** | 9644 | Redpanda-specific port |
| **Redpanda RPC** | 33145 | Redpanda-specific port |

## Why Keep Some Ports Standard?

### Database Ports (5432)
- PostgreSQL/TimescaleDB clients expect port 5432 by default
- Changing would require updating all connection strings in:
  - Airflow configuration
  - Grafana datasource configuration
  - Spark JDBC connections
  - Application code
- Standard port makes it easier for developers familiar with PostgreSQL

### Spark Master Port (7077)
- Spark workers connect to master on port 7077
- Changing would require updating:
  - `SPARK_MASTER_URL` in all worker configurations
  - Spark job submission commands
  - Internal Spark cluster communication
- Standard port ensures Spark compatibility

### Kafka Ports (9092, etc.)
- Kafka clients expect port 9092 by default
- Changing would require updating:
  - Producer configuration
  - Consumer configuration
  - All Kafka client libraries
- Standard ports ensure Kafka compatibility

## Benefits of Updated Internal Ports

✅ **Consistency** - All web UI ports now in 7000-7999 range  
✅ **No Conflicts** - Even internal ports avoid common ranges  
✅ **Easier Debugging** - Consistent port numbering  
✅ **Future-Proof** - Less likely to conflict with new services  

## Port Mapping (Complete)

| Service | Host Port | Container Port | Purpose |
|---------|-----------|----------------|---------|
| Redpanda Kafka | 7000 | 9092 | Kafka API |
| Redpanda Pandaproxy | 7001 | 28082 | Pandaproxy API |
| Redpanda Admin | 7002 | 9644 | Admin API |
| Redpanda Console | 7003 | **7003** ✅ | Web UI |
| Spark Master UI | 7004 | **7004** ✅ | Web UI |
| Spark Master | 7005 | 7077 | Master port |
| Spark App UI | 7006 | **7006** ✅ | App UI |
| PostgreSQL (Airflow) | 7007 | 5432 | Database |
| TimescaleDB | 7008 | 5432 | Database |
| Airflow Webserver | 7009 | **7009** ✅ | Web UI |
| Grafana | 7010 | **7010** ✅ | Web UI |

## Health Checks Updated

All health checks have been updated to use new internal ports:

- ✅ Grafana: `http://localhost:7010/api/health`
- ✅ Airflow: `http://localhost:7009/health`
- ✅ Spark Master: `http://localhost:7004`

## Testing

After restarting services, verify internal ports:

```bash
# Check Grafana
docker exec -it grafana curl http://localhost:7010/api/health

# Check Airflow
docker exec -it airflow-webserver curl http://localhost:7009/health

# Check Spark Master
docker exec -it spark-master curl http://localhost:7004

# Check Redpanda Console
docker exec -it redpanda-console curl http://localhost:7003
```

## Notes

- **Internal ports are isolated** - Each container has its own network namespace
- **No conflicts** - Even if multiple containers use the same internal port, Docker isolates them
- **Standard ports kept** - Database and protocol ports remain standard for compatibility
- **Web UI ports changed** - All web interfaces now use 7000-7999 range

---

**All configurable internal ports are now in the 7000-7999 range! ✅**

