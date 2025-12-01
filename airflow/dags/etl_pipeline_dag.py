"""
ETL Pipeline Orchestration DAG - Complete Data Flow Automation
Coordinates Bronze → Silver → Gold layer processing
Runs continuously to ensure data flows through all layers automatically
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
import json
import os
from pathlib import Path
import subprocess
import time

# Default arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['alerts@example.com'],
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

# DAG definition - Runs every 5 minutes
dag = DAG(
    'etl_pipeline_orchestration',
    default_args=default_args,
    description='Complete ETL pipeline orchestration: Bronze → Silver → Gold',
    schedule_interval='*/5 * * * *',  # Every 5 minutes
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'pipeline', 'orchestration', 'bronze', 'silver', 'gold'],
)


def check_bronze_data_flow(**context):
    """
    Check if Bronze layer is actively processing data
    Returns True if data is flowing, False if stagnant
    """
    try:
        # Check if bronze job is running and processing
        result = subprocess.run(
            ['docker', 'compose', 'ps', 'spark-bronze'],
            capture_output=True,
            text=True,
            timeout=30,
            cwd='/Users/pbn/My_project/realtime-us-stock-etl-platform'
        )

        if result.returncode != 0 or 'Up' not in result.stdout:
            print("❌ Bronze job is not running")
            return False

        # Check recent logs for processing activity
        log_result = subprocess.run(
            ['docker', 'compose', 'logs', '--tail', '10', 'spark-bronze'],
            capture_output=True,
            text=True,
            timeout=30,
            cwd='/Users/pbn/My_project/realtime-us-stock-etl-platform'
        )

        logs = log_result.stdout
        if 'Batch:' in logs and 'Processing time:' in logs:
            print("✅ Bronze job is actively processing data")
            return True
        else:
            print("⚠️ Bronze job is running but not showing recent processing")
            return False

    except Exception as e:
        print(f"Error checking bronze data flow: {e}")
        return False


def should_start_silver(**context):
    """
    Decide whether to start Silver layer based on Bronze activity
    """
    bronze_active = check_bronze_data_flow(**context)

    if bronze_active:
        print("🟢 Starting Silver layer - Bronze is active")
        return 'start_silver_layer'
    else:
        print("🟡 Skipping Silver layer - Bronze is not active")
        return 'skip_silver_layer'


def check_silver_data_flow(**context):
    """
    Check if Silver layer is processing data
    """
    try:
        # Check if silver job is running
        result = subprocess.run(
            ['docker', 'compose', 'ps', 'spark-silver'],
            capture_output=True,
            text=True,
            timeout=30,
            cwd='/Users/pbn/My_project/realtime-us-stock-etl-platform'
        )

        if result.returncode != 0 or 'Up' not in result.stdout:
            print("❌ Silver job is not running")
            return False

        # Check if silver table exists and has data
        silver_path = Path('/opt/spark/delta_tables/silver')
        if not silver_path.exists():
            print("⚠️ Silver table doesn't exist yet")
            return False

        print("✅ Silver job is running and table exists")
        return True

    except Exception as e:
        print(f"Error checking silver data flow: {e}")
        return False


def should_start_gold(**context):
    """
    Decide whether to start Gold layer based on Bronze activity (parallel execution)
    """
    bronze_active = check_bronze_data_flow(**context)

    if bronze_active:
        print("🟢 Starting Gold layer - Bronze is active")
        return 'start_gold_layer'
    else:
        print("🟡 Skipping Gold layer - Bronze is not active")
        return 'skip_gold_layer'


def start_silver_layer(**context):
    """
    Start the Silver layer processing
    """
    try:
        print("🚀 Starting Silver layer processing...")

        # Brief wait to ensure Bronze is stable
        time.sleep(10)  # Give Bronze time to accumulate some data

        # Start Silver job
        result = subprocess.run(
            ['docker', 'compose', 'up', '-d', 'spark-silver'],
            capture_output=True,
            text=True,
            timeout=120,
            cwd='/Users/pbn/My_project/realtime-us-stock-etl-platform'
        )

        if result.returncode == 0:
            print("✅ Silver layer started successfully")
            return True
        else:
            print(f"❌ Failed to start Silver layer: {result.stderr}")
            return False

    except Exception as e:
        print(f"Error starting Silver layer: {e}")
        return False


def start_gold_layer(**context):
    """
    Start the Gold layer processing
    """
    try:
        print("🚀 Starting Gold layer processing...")

        # Brief wait to ensure system stability
        time.sleep(10)  # Give system time to stabilize

        # Start Gold job
        result = subprocess.run(
            ['docker', 'compose', 'up', '-d', 'spark-gold'],
            capture_output=True,
            text=True,
            timeout=120,
            cwd='/Users/pbn/My_project/realtime-us-stock-etl-platform'
        )

        if result.returncode == 0:
            print("✅ Gold layer started successfully")
            return True
        else:
            print(f"❌ Failed to start Gold layer: {result.stderr}")
            return False

    except Exception as e:
        print(f"Error starting Gold layer: {e}")
        return False


def collect_pipeline_metrics(**context):
    """
    Collect and log pipeline metrics
    """
    try:
        metrics = {
            'timestamp': datetime.now().isoformat(),
            'bronze_batches_processed': 0,
            'silver_records_processed': 0,
            'gold_kpis_computed': 0,
            'pipeline_status': 'unknown'
        }

        # Check bronze logs for batch count
        try:
            log_result = subprocess.run(
                ['docker', 'compose', 'logs', 'spark-bronze'],
                capture_output=True,
                text=True,
                timeout=30,
                cwd='/Users/pbn/My_project/realtime-us-stock-etl-platform'
            )

            batch_count = log_result.stdout.count('Batch:')
            metrics['bronze_batches_processed'] = batch_count
        except:
            pass

        # Check silver table size
        try:
            silver_path = Path('/opt/spark/delta_tables/silver')
            if silver_path.exists():
                # Count parquet files as rough estimate
                parquet_files = list(silver_path.glob('**/*.parquet'))
                metrics['silver_records_processed'] = len(parquet_files)
        except:
            pass

        # Check gold table size
        try:
            gold_path = Path('/opt/spark/delta_tables/gold')
            if gold_path.exists():
                parquet_files = list(gold_path.glob('**/*.parquet'))
                metrics['gold_kpis_computed'] = len(parquet_files)
        except:
            pass

        # Determine pipeline status
        bronze_active = check_bronze_data_flow(**context)
        silver_active = check_silver_data_flow(**context)

        if bronze_active and silver_active:
            metrics['pipeline_status'] = 'full_flow'
        elif bronze_active:
            metrics['pipeline_status'] = 'bronze_only'
        else:
            metrics['pipeline_status'] = 'inactive'

        print(f"📊 Pipeline Metrics: {json.dumps(metrics, indent=2)}")

        # Push metrics to XCom for potential downstream use
        context['task_instance'].xcom_push(key='pipeline_metrics', value=metrics)

        return metrics

    except Exception as e:
        print(f"Error collecting metrics: {e}")
        return {}


# Define tasks

# Bronze layer monitoring (always runs)
check_bronze = PythonOperator(
    task_id='check_bronze_layer',
    python_callable=check_bronze_data_flow,
    dag=dag,
)

# Decision point for Silver layer
silver_decision = BranchPythonOperator(
    task_id='decide_silver_layer',
    python_callable=should_start_silver,
    dag=dag,
)

# Silver layer tasks
start_silver = PythonOperator(
    task_id='start_silver_layer',
    python_callable=start_silver_layer,
    dag=dag,
)

skip_silver = DummyOperator(
    task_id='skip_silver_layer',
    dag=dag,
)

# Decision point for Gold layer
gold_decision = BranchPythonOperator(
    task_id='decide_gold_layer',
    python_callable=should_start_gold,
    dag=dag,
)

# Gold layer tasks
start_gold = PythonOperator(
    task_id='start_gold_layer',
    python_callable=start_gold_layer,
    dag=dag,
)

skip_gold = DummyOperator(
    task_id='skip_gold_layer',
    dag=dag,
)

# Metrics collection (always runs)
collect_metrics = PythonOperator(
    task_id='collect_pipeline_metrics',
    python_callable=collect_pipeline_metrics,
    dag=dag,
    trigger_rule=TriggerRule.ALL_DONE,  # Run even if upstream tasks fail
)

# Task dependencies - Parallel execution
check_bronze >> [silver_decision, gold_decision]
silver_decision >> [start_silver, skip_silver]
gold_decision >> [start_gold, skip_gold]
[start_silver, skip_silver, start_gold, skip_gold] >> collect_metrics
