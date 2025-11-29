# Port Mapping Reference - 7000-7999 Range

All service ports have been remapped to the **7000-7999** range to avoid conflicts with other projects.

## Port Mapping Table

| Service | Host Port | Container Port | Purpose | Access URL |
|---------|-----------|----------------|---------|------------|
| **Redpanda Kafka** | 7000 | 9092 | Kafka API endpoint | `localhost:7000` |
| **Redpanda Pandaproxy** | 7001 | 28082 | Pandaproxy API | `localhost:7001` |
| **Redpanda Admin** | 7002 | 9644 | Admin/metrics API | `localhost:7002` |
| **Redpanda Console** | 7003 | **7003** ✅ | Web UI for Kafka topics | http://localhost:7003 |
| **Spark Master UI** | 7004 | **7004** ✅ | Spark cluster web UI | http://localhost:7004 |
| **Spark Master** | 7005 | 7077 | Spark master port | `localhost:7005` |
| **Spark App UI** | 7006 | **7006** ✅ | Spark application UI | http://localhost:7006 |
| **PostgreSQL (Airflow)** | 7007 | 5432 | Airflow metadata DB | `localhost:7007` |
| **TimescaleDB** | 7008 | 5432 | Grafana data source | `localhost:7008` |
| **Airflow Webserver** | 7009 | **7009** ✅ | Airflow web UI | http://localhost:7009 |
| **Grafana** | 7010 | **7010** ✅ | Grafana dashboards | http://localhost:7010 |

## Quick Access URLs

### Dashboards & UIs
- **Grafana**: http://localhost:7010 (admin/admin)
- **Airflow**: http://localhost:7009 (admin/admin)
- **Redpanda Console**: http://localhost:7003
- **Spark Master UI**: http://localhost:7004
- **Spark App UI**: http://localhost:7006

### API Endpoints
- **Kafka Broker**: `localhost:7000`
- **Pandaproxy**: `localhost:7001`
- **Redpanda Admin**: `localhost:7002`

### Database Connections
- **PostgreSQL (Airflow)**: `localhost:7007`
- **TimescaleDB**: `localhost:7008`

## Internal Ports

**Note**: Most internal container ports have been updated to match the 7000-7999 range for consistency.

### Updated Internal Ports ✅
- Grafana internal: `3000` → **7010**
- Airflow internal: `8080` → **7009**
- Spark Master UI internal: `8080` → **7004**
- Spark App UI internal: `4040` → **7006**
- Redpanda Console internal: `8080` → **7003**

### Standard Protocol Ports (Unchanged) ⚠️
- Spark master internal: `7077` (standard Spark port)
- PostgreSQL internal: `5432` (standard PostgreSQL port)
- Kafka internal: `9092` (standard Kafka port)

These remain unchanged as they are protocol-specific and changing them would require extensive configuration updates.

## Connection Strings

### Kafka Producer/Consumer
```python
# Use host port 7000
bootstrap_servers = "localhost:7000"
```

### PostgreSQL/TimescaleDB
```python
# Use host port 7008 for TimescaleDB
connection_string = "postgresql://grafana:grafana@localhost:7008/stockdata"
```

### Spark Master URL
```bash
# Internal port 7077 (unchanged)
spark://spark-master:7077
```

## Verification

After starting services, verify ports are accessible:

```bash
# Check Grafana
curl http://localhost:7010/api/health

# Check Airflow
curl http://localhost:7009/health

# Check Spark Master
curl http://localhost:7004

# Check Redpanda Console
curl http://localhost:7003
```

## Port Conflict Resolution

If you encounter port conflicts:

1. Check if port is in use:
   ```bash
   lsof -i :7000-7010
   ```

2. Stop conflicting services or change port mapping in `docker-compose.yml`

3. Restart services:
   ```bash
   docker-compose down
   docker-compose up -d
   ```

## Updated Files

The following files have been updated with new port mappings:

- ✅ `docker-compose.yml` - All port mappings
- ✅ `README.md` - Access URLs
- ✅ `QUICKSTART.md` - Access URLs
- ✅ `scripts/start.sh` - Access URLs
- ✅ `TIMESCALE_MIGRATION.md` - Port references
- ✅ `UPGRADE_SUMMARY.md` - Port references
- ✅ `FILE_INVENTORY.md` - Port references
- ✅ `docs/runbook.md` - Port references

---

**All ports are now in the 7000-7999 range! ✅**

