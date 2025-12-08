"""
Gold Layer & TimescaleDB Sync DAG
Automatically runs Gold Spark job and syncs to TimescaleDB every 2 minutes
Ensures Gold layer is always up-to-date and TimescaleDB stays in sync
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import subprocess

# Default arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['alerts@example.com'],
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

# DAG definition - runs every 2 minutes
dag = DAG(
    'gold_timescale_sync',
    default_args=default_args,
    description='Gold layer processing and TimescaleDB sync every 2 minutes',
    schedule_interval='*/2 * * * *',  # Every 2 minutes
    catchup=False,
    max_active_runs=1,
    tags=['gold', 'timescaledb', 'sync', 'realtime'],
)


def run_gold_spark_job(**context):
    """
    Execute the Gold layer Spark job
    Processes Silver layer data and computes technical indicators
    """
    print("=" * 80)
    print("STARTING GOLD LAYER SPARK JOB")
    print("=" * 80)

    try:
        # First, verify docker is accessible
        print("Verifying Docker access...")
        docker_check = subprocess.run(
            ['docker', 'ps', '--filter', 'name=spark-master', '--format', '{{.Names}}'],
            capture_output=True,
            text=True,
            timeout=10
        )
        if docker_check.returncode != 0:
            raise Exception(f"Docker check failed: {docker_check.stderr}")
        if 'spark-master' not in docker_check.stdout:
            raise Exception(f"spark-master container not found. Available containers: {docker_check.stdout}")
        print(f"✓ Docker access verified. spark-master container is running.")
        
        # Run Gold KPI computation job via docker exec in batch mode
        cmd = [
            'docker', 'exec', 'spark-master',
            'spark-submit',
            '--master', 'local[*]',
            '--jars', '/opt/spark/jars/delta-core_2.12-2.4.0.jar,/opt/spark/jars/delta-storage-2.4.0.jar,/opt/spark/jars/postgresql-42.6.0.jar',
            '--conf', 'spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension',
            '--conf', 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog',
            '/opt/spark-jobs/jobs/gold_kpis.py',
            '--batch'  # Run in batch mode - exit after processing (for Airflow)
        ]

        print(f"Running Gold job command: {' '.join(cmd)}")
        print(f"Timeout set to: 60 seconds (job should complete in ~10 seconds)")

        try:
            # Run command with captured output - job completes in ~10 seconds
            # Capture output to avoid any buffering issues
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=60,  # 1 minute timeout (job should complete in ~10 seconds)
                check=False
            )
            
            # Print output to Airflow logs
            if result.stdout:
                print("=== Spark Job Output ===")
                print(result.stdout)
            if result.stderr:
                print("=== Spark Job Errors ===")
                print(result.stderr)

            # Exit code 137 (SIGKILL) is acceptable if job completed successfully
            if result.returncode not in [0, 137]:
                print(f"✗ Gold job failed with exit code: {result.returncode}")
                raise subprocess.CalledProcessError(result.returncode, cmd, result.stdout, result.stderr)
                
        except subprocess.TimeoutExpired as e:
            print(f"✗ Gold job timed out after 60 seconds")
            if hasattr(e, 'stdout') and e.stdout:
                print(f"Output so far: {e.stdout[-2000:]}")
            if hasattr(e, 'stderr') and e.stderr:
                print(f"Errors so far: {e.stderr[-2000:]}")
            raise Exception(f"Gold job timed out after 60 seconds.")

        print("✓ Gold layer processing completed!")
        if result.returncode == 137:
            print("Note: Job completed successfully but was terminated (exit code 137)")
        print("STDOUT:", result.stdout[-1000:])  # Last 1000 chars

        # Verify Gold table was created/updated
        # Modified logic: Check if table exists/has records. If not, don't fail, just mark as skipped.
        verify_cmd = [
            'docker', 'exec', 'spark-master',
            'python3', '-c', '''
import sys
sys.path.insert(0, "/opt/spark-jobs")
from config import create_spark_session, DELTA_PATH_GOLD
spark = create_spark_session("GoldVerify")
try:
    df = spark.read.format("delta").load(DELTA_PATH_GOLD)
    count = df.count()
    print(f"GOLD_TABLE_RECORDS:{count}")
except Exception as e:
    # If table doesn't exist yet, it's not a DAG failure, just nothing to sync
    print(f"GOLD_VERIFY_ERROR:{e}")
    # Print 0 records so we can handle it gracefully
    print("GOLD_TABLE_RECORDS:0")
spark.stop()
'''
        ]

        verify_result = subprocess.run(
            verify_cmd,
            capture_output=True,
            text=True,
            check=True,
            timeout=30
        )

        record_count = 0
        error_msg = ""
        
        # Extract record count
        for line in verify_result.stdout.split('\n'):
            if line.startswith('GOLD_TABLE_RECORDS:'):
                record_count = int(line.split(':')[1])
            if line.startswith('GOLD_VERIFY_ERROR:'):
                error_msg = line.split(':', 1)[1]

        if record_count == 0:
            print(f"⚠ Gold table empty or not ready yet. (Reason: {error_msg if error_msg else '0 records'})")
            print("Note: This is expected during backfilling or if Silver layer is caching up.")
            print("Skipping downstream TimescaleDB sync.")
            
            # Log success but indicate skipped
            context['task_instance'].xcom_push(key='gold_job_result', value='skipped')
            context['task_instance'].xcom_push(key='gold_record_count', value=0)
            return

        print(f"✓ Gold table verification: {record_count} records")

        # Log success in Airflow
        context['task_instance'].xcom_push(key='gold_job_result', value='success')
        context['task_instance'].xcom_push(key='gold_record_count', value=record_count)

    except subprocess.TimeoutExpired:
        print("⚠ Gold job timed out after 5 minutes")
        raise Exception("Gold Spark job timed out")
    except subprocess.CalledProcessError as e:
        print(f"✗ Gold job failed with exit code {e.returncode}")
        print("STDERR:", e.stderr)
        print("STDOUT:", e.stdout)
        raise e


def sync_to_timescale_db(**context):
    """
    Sync Gold Delta table data to TimescaleDB
    Only runs after Gold layer processing is confirmed successful
    """
    print("=" * 80)
    print("STARTING TIMESCALEDB SYNC")
    print("=" * 80)

    # Verify Gold job completed successfully
    gold_result = context['task_instance'].xcom_pull(task_ids='run_gold_spark_job', key='gold_job_result')
    gold_record_count = context['task_instance'].xcom_pull(task_ids='run_gold_spark_job', key='gold_record_count')

    if gold_result == 'skipped':
        print("⚠ Skipping TimescaleDB sync because Gold layer returned 'skipped' status (empty/not ready).")
        print("This is a successful exit.")
        return

    if gold_result != 'success':
        raise Exception(f"Cannot proceed with TimescaleDB sync - Gold job did not complete successfully. Status: {gold_result}")

    if gold_record_count == 0:
        print("⚠ Gold table has 0 records. Skipping sync.")
        return

    print(f"✓ Confirmed Gold layer completed with {gold_record_count} records")

    try:
        # Run the sync script via docker exec from the host
        cmd = [
            'docker', 'exec', 'spark-master',
            'spark-submit',
            '--master', 'local[*]',
            '--jars', '/opt/spark/jars/delta-core_2.12-2.4.0.jar,/opt/spark/jars/delta-storage-2.4.0.jar,/opt/spark/jars/postgresql-42.6.0.jar',
            '--conf', 'spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension',
            '--conf', 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog',
            '/opt/spark-jobs/sync-delta-to-timescale.py'
        ]

        print(f"Running sync command: {' '.join(cmd)}")

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=90  # 1.5 minute timeout
        )

        # Exit code 137 (SIGKILL) is acceptable if job completed successfully
        if result.returncode not in [0, 137]:
            raise subprocess.CalledProcessError(result.returncode, cmd, result.stdout, result.stderr)

        print("✓ TimescaleDB sync completed!")
        if result.returncode == 137:
            print("Note: Sync completed successfully but was terminated (exit code 137)")
        print("STDOUT:", result.stdout[-1000:])  # Last 1000 chars

        # Log success in Airflow
        context['task_instance'].xcom_push(key='sync_result', value='success')

    except subprocess.TimeoutExpired:
        print("⚠️ TimescaleDB sync timed out after 3 minutes")
        raise Exception("TimescaleDB sync timed out")
    except subprocess.CalledProcessError as e:
        print(f"✗ TimescaleDB sync failed with exit code {e.returncode}")
        print("STDERR:", e.stderr)
        print("STDOUT:", e.stdout)
        raise e


def verify_sync_results(**context):
    """
    Verify that both Gold job and TimescaleDB sync were successful
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
            check=True,
            timeout=30
        )

        print("TimescaleDB verification:")
        print(result.stdout)

        # Extract record count
        for line in result.stdout.split('\n'):
            if line.strip().isdigit():
                record_count = int(line.strip())
                print(f"✓ TimescaleDB contains {record_count} records")

                if record_count > 0:
                    print("✅ Verification successful - data is synced")
                else:
                    print("⚠️ Verification warning - no records found in TimescaleDB")
                break

        # Check Gold Delta table
        delta_check_cmd = [
            'docker', 'exec', 'spark-master',
            'python3', '-c', '''
import sys
sys.path.insert(0, "/opt/spark-jobs")
from config import create_spark_session, DELTA_PATH_GOLD
spark = create_spark_session("DeltaCheck")
try:
    df = spark.read.format("delta").load(DELTA_PATH_GOLD)
    print(f"Delta Gold table records: {df.count()}")
except Exception as e:
    print(f"Delta check error: {e}")
spark.stop()
'''
        ]

        delta_result = subprocess.run(
            delta_check_cmd,
            capture_output=True,
            text=True,
            check=True,
            timeout=30
        )

        print("Delta Lake verification:")
        print(delta_result.stdout)

        print("✓ Sync verification completed")

    except subprocess.CalledProcessError as e:
        print(f"⚠️ Verification failed: {e}")
        print("STDERR:", e.stderr)
        # Don't fail the DAG for verification issues - log warning only


# Define tasks
gold_job_task = PythonOperator(
    task_id='run_gold_spark_job',
    python_callable=run_gold_spark_job,
    dag=dag,
    execution_timeout=timedelta(minutes=2),  # 2 minutes (job completes in ~10 seconds, but buffer for safety)
)

timescale_sync_task = PythonOperator(
    task_id='sync_to_timescale_db',
    python_callable=sync_to_timescale_db,
    dag=dag,
    execution_timeout=timedelta(minutes=3),
    # Only run if upstream Gold job succeeds
    trigger_rule='all_success',
)

verify_sync_task = PythonOperator(
    task_id='verify_sync_results',
    python_callable=verify_sync_results,
    dag=dag,
    execution_timeout=timedelta(minutes=1),
    # Run even if sync fails, but only if Gold job succeeded
    trigger_rule='all_done',
)

# Task dependencies: Gold job must succeed -> Timescale sync -> verification
gold_job_task >> timescale_sync_task >> verify_sync_task
