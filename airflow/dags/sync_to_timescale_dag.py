"""
Sync Delta Lake to TimescaleDB DAG
Automatically syncs all Gold Delta table data to TimescaleDB
Runs hourly to ensure TimescaleDB stays in sync with Delta Lake
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# Default arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['alerts@example.com'],
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition - DISABLED (replaced by gold_timescale_sync DAG that runs every 2 minutes)
dag = DAG(
    'sync_delta_to_timescale',
    default_args=default_args,
    description='DISABLED - Sync Gold Delta table to TimescaleDB automatically (replaced by gold_timescale_sync)',
    schedule_interval=None,  # Disabled - replaced by more frequent gold_timescale_sync DAG
    catchup=False,
    tags=['sync', 'timescaledb', 'delta-lake', 'disabled'],
)


def sync_delta_to_timescale(**context):
    """
    Sync all Gold Delta table data to TimescaleDB
    Uses batch mode to sync ALL historical data, not just 24 hours
    """
    print("=" * 80)
    print("STARTING AUTOMATIC DELTA → TIMESCALEDB SYNC")
    print("=" * 80)

    try:
        # Run the sync script via docker exec from the host
        # This requires the docker socket to be available
        cmd = [
            'docker', 'exec', 'spark-master',
            'spark-submit',
            '--master', 'spark://spark-master:7077',
            '--packages', 'io.delta:delta-core_2.12:2.4.0,org.postgresql:postgresql:42.6.0',
            '--conf', 'spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension',
            '--conf', 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog',
            '/opt/spark-jobs/sync-delta-to-timescale.py',
            '--batch'  # This tells the script to sync ALL data, not just 24 hours
        ]

        print(f"Running sync command: {' '.join(cmd)}")

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True
        )

        print("✓ Sync completed successfully!")
        print("STDOUT:", result.stdout)

        # Log success in Airflow
        context['task_instance'].xcom_push(key='sync_result', value='success')

    except subprocess.CalledProcessError as e:
        print(f"✗ Sync failed with exit code {e.returncode}")
        print("STDERR:", e.stderr)
        print("STDOUT:", e.stdout)
        raise e


def verify_sync(**context):
    """
    Verify that the sync was successful by checking TimescaleDB
    """
    print("Verifying sync results...")

    try:
        # Check TimescaleDB record count
        check_cmd = [
            'docker', 'exec', 'postgres-timescale',
            'psql', '-h', 'localhost', '-U', 'grafana', '-d', 'stockdata',
            '-c', 'SELECT COUNT(*) as total_records FROM gold_stocks;'
        ]

        result = subprocess.run(
            check_cmd,
            capture_output=True,
            text=True,
            check=True
        )

        print("TimescaleDB verification:")
        print(result.stdout)

        # Also check Delta table count for comparison
        delta_check_cmd = [
            'docker', 'exec', 'spark-master',
            'python3', '-c', '''
import sys
sys.path.insert(0, "/opt/spark-jobs")
from config import create_spark_session, DELTA_PATH_GOLD
spark = create_spark_session("DeltaCheck")
df = spark.read.format("delta").load(DELTA_PATH_GOLD)
print(f"Delta Gold table records: {df.count()}")
spark.stop()
'''
        ]

        delta_result = subprocess.run(
            delta_check_cmd,
            capture_output=True,
            text=True,
            check=True
        )

        print("Delta Lake verification:")
        print(delta_result.stdout)

        print("✓ Sync verification completed")

    except subprocess.CalledProcessError as e:
        print(f"⚠️ Verification failed: {e}")
        print("STDERR:", e.stderr)
        # Don't fail the DAG for verification issues


# Define tasks
sync_task = BashOperator(
    task_id='sync_delta_to_timescale',
    bash_command='cd /opt/airflow/producer && ./run_sync.sh',
    dag=dag,
)

verify_task = PythonOperator(
    task_id='verify_sync',
    python_callable=verify_sync,
    dag=dag,
)

# Task dependencies
sync_task >> verify_task

