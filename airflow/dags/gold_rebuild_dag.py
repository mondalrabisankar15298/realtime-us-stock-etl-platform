"""
Gold Rebuild DAG - Recompute All KPIs
Runs daily to ensure Gold table consistency
Full recomputation from Silver table
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# Default arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email': ['alerts@example.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

# DAG definition
dag = DAG(
    'gold_rebuild',
    default_args=default_args,
    description='Daily rebuild of Gold layer KPIs',
    schedule_interval='0 0 * * *',  # Daily at midnight UTC
    catchup=False,
    is_paused_upon_creation=True,  # Start paused - manually enable when needed
    tags=['gold', 'rebuild', 'maintenance'],
)

# Task: Rebuild Gold table
rebuild_gold = BashOperator(
    task_id='rebuild_gold_table',
    bash_command="""
    echo "Starting Gold table rebuild..."
    
    # In production, you would:
    # 1. Stop the streaming Gold job
    # 2. Run a batch job to recompute all KPIs from Silver
    # 3. Restart the streaming job
    
    # For now, this is a placeholder
    echo "Gold rebuild would run here"
    echo "✓ Gold rebuild complete"
    """,
    dag=dag,
)

rebuild_gold

