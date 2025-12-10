"""
Monitoring DAG - System Health Checks and Alerts
Monitors producer health, DLQ, data freshness, and Kafka lag
Runs every 5 minutes
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import json
import os
from pathlib import Path

# Default arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['alerts@example.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# DAG definition
dag = DAG(
    'stock_monitoring',
    default_args=default_args,
    description='Monitor system health and alert on issues',
    schedule_interval='*/5 * * * *',  # Every 5 minutes
    catchup=False,
    is_paused_upon_creation=True,  # Start paused - manually enable when needed
    tags=['monitoring', 'alerts', 'health-check'],
)


def check_producer_heartbeat(**context):
    """
    Check if producer has updated state recently
    Alert if no updates in last 10 minutes
    """
    state_file = Path('/opt/airflow/producer/state/last_fetch.json')
    
    if not state_file.exists():
        print("⚠️  State file doesn't exist yet - producer may not have run")
        return True
    
    try:
        # Read state file
        with open(state_file, 'r') as f:
            state = json.load(f)
        
        if not state:
            print("⚠️  State file is empty")
            return True
        
        # Check timestamps
        from datetime import datetime
        latest_timestamp = None
        
        for ticker, ts_str in state.items():
            ts = datetime.fromisoformat(ts_str)
            if latest_timestamp is None or ts > latest_timestamp:
                latest_timestamp = ts
        
        if latest_timestamp:
            age_minutes = (datetime.now(latest_timestamp.tzinfo) - latest_timestamp).total_seconds() / 60
            print(f"Latest data timestamp: {latest_timestamp}")
            print(f"Age: {age_minutes:.1f} minutes")
            
            if age_minutes > 10:
                print(f"⚠️  WARNING: No new data in {age_minutes:.1f} minutes!")
                # In production, send alert here
                return False
            else:
                print(f"✓ Producer heartbeat OK (data age: {age_minutes:.1f}m)")
                return True
        
    except Exception as e:
        print(f"Error checking producer heartbeat: {e}")
        return False


def monitor_dlq(**context):
    """
    Check DLQ topic for failed messages
    Alert if too many failures
    """
    try:
        from kafka import KafkaConsumer
        import os
        
        broker = os.getenv('REDPANDA_BROKER', 'redpanda:9092')
        dlq_topic = os.getenv('KAFKA_TOPIC_DLQ', 'stock-dlq')
        
        # Create consumer to check DLQ
        consumer = KafkaConsumer(
            dlq_topic,
            bootstrap_servers=broker,
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            consumer_timeout_ms=5000,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        
        # Count messages in last 5 minutes
        from datetime import datetime, timezone
        recent_failures = 0
        cutoff_time = datetime.now(timezone.utc) - timedelta(minutes=5)
        
        for message in consumer:
            try:
                data = message.value
                msg_time = datetime.fromisoformat(data.get('timestamp', ''))
                
                if msg_time >= cutoff_time:
                    recent_failures += 1
                    print(f"DLQ message: {data.get('ticker')} - {data.get('error')}")
                    
            except Exception as e:
                print(f"Error parsing DLQ message: {e}")
        
        consumer.close()
        
        print(f"DLQ messages in last 5 minutes: {recent_failures}")
        
        if recent_failures > 10:
            print(f"⚠️  WARNING: High DLQ message count: {recent_failures}")
            return False
        elif recent_failures > 0:
            print(f"⚠️  {recent_failures} messages in DLQ")
            return True
        else:
            print("✓ DLQ check OK (no recent failures)")
            return True
            
    except Exception as e:
        print(f"Error monitoring DLQ: {e}")
        return True  # Don't fail the DAG if monitoring has issues


def check_data_freshness(**context):
    """
    Verify that we have recent data in Delta tables
    Check Bronze table for recent records
    """
    try:
        from pyspark.sql import SparkSession
        from delta import configure_spark_with_delta_pip
        from datetime import datetime, timedelta
        
        # Create Spark session
        builder = (
            SparkSession.builder
            .appName("DataFreshnessCheck")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )
        
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        
        bronze_path = "/opt/spark/delta_tables/bronze"
        
        # Check if table exists
        if not Path(bronze_path).exists():
            print("⚠️  Bronze table doesn't exist yet")
            spark.stop()
            return True
        
        # Read Bronze table
        df = spark.read.format("delta").load(bronze_path)
        
        if df.count() == 0:
            print("⚠️  Bronze table is empty")
            spark.stop()
            return True
        
        # Get latest timestamp
        latest_row = df.orderBy(df.bronze_timestamp.desc()).first()
        latest_time = latest_row.bronze_timestamp
        
        age_minutes = (datetime.now() - latest_time.replace(tzinfo=None)).total_seconds() / 60
        
        print(f"Latest Bronze record: {latest_time}")
        print(f"Age: {age_minutes:.1f} minutes")
        
        spark.stop()
        
        if age_minutes > 15:
            print(f"⚠️  WARNING: Data is stale ({age_minutes:.1f}m old)")
            return False
        else:
            print(f"✓ Data freshness OK")
            return True
            
    except Exception as e:
        print(f"Error checking data freshness: {e}")
        import traceback
        traceback.print_exc()
        return True


def check_spark_streams(**context):
    """
    Check if Spark streaming jobs are running
    (This is a placeholder - in production you'd check Spark master UI or logs)
    """
    print("Checking Spark streaming jobs...")
    
    # In production, you would:
    # 1. Query Spark master API for active streaming queries
    # 2. Check checkpoint directories for recent updates
    # 3. Monitor Spark logs for errors
    
    print("✓ Spark streams check (placeholder)")
    return True


# Define tasks
task_producer_heartbeat = PythonOperator(
    task_id='check_producer_heartbeat',
    python_callable=check_producer_heartbeat,
    dag=dag,
)

task_monitor_dlq = PythonOperator(
    task_id='monitor_dlq',
    python_callable=monitor_dlq,
    dag=dag,
)

task_data_freshness = PythonOperator(
    task_id='check_data_freshness',
    python_callable=check_data_freshness,
    dag=dag,
)

task_spark_streams = PythonOperator(
    task_id='check_spark_streams',
    python_callable=check_spark_streams,
    dag=dag,
)

# All checks run in parallel
[task_producer_heartbeat, task_monitor_dlq, task_data_freshness, task_spark_streams]

