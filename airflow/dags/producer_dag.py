"""
Producer DAG - Schedule Stock Data Producer
Runs every minute during market hours to fetch real-time stock data
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import pytz

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

# DAG definition
dag = DAG(
    'stock_producer',
    default_args=default_args,
    description='Fetch and publish stock data to Kafka every minute',
    schedule_interval='*/1 * * * *',  # Every minute
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,  # Start paused - manually enable when needed
    tags=['stock', 'producer', 'real-time'],
)


def check_market_hours(**context):
    """
    Check if market is currently open
    NYSE hours: 9:30 AM - 4:00 PM EST, Monday-Friday
    """
    ny_tz = pytz.timezone('America/New_York')
    now = datetime.now(ny_tz)
    
    # Check if weekday (Monday=0, Friday=4)
    is_weekday = now.weekday() < 5
    
    # Check market hours (9:30 AM - 4:00 PM)
    market_open_time = now.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close_time = now.replace(hour=16, minute=0, second=0, microsecond=0)
    is_market_hours = market_open_time <= now <= market_close_time
    
    print(f"Current time (EST): {now}")
    print(f"Is weekday: {is_weekday}")
    print(f"Is market hours: {is_market_hours}")
    
    # For testing purposes, you can comment out the check below
    # to allow producer to run 24/7
    if not (is_weekday and is_market_hours):
        print("Market is closed. Skipping producer run.")
        return False
    
    print("Market is open. Proceeding with producer.")
    return True


def check_producer_health(**context):
    """
    Check producer container health and logs
    """
    import subprocess
    
    try:
        # Check if producer container is running
        result = subprocess.run(
            ['docker', 'ps', '--filter', 'name=stock-producer', '--format', '{{.Status}}'],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        if result.returncode == 0 and 'Up' in result.stdout:
            print(f"✓ Producer container is healthy: {result.stdout.strip()}")
            return True
        else:
            print(f"✗ Producer container is not running")
            return False
            
    except Exception as e:
        print(f"Error checking producer health: {e}")
        return False


# Task: Check market hours
check_market = PythonOperator(
    task_id='check_market_hours',
    python_callable=check_market_hours,
    dag=dag,
)

# Task: Check producer health
health_check = PythonOperator(
    task_id='check_producer_health',
    python_callable=check_producer_health,
    dag=dag,
)

# Task: Verify producer is running (it runs as standalone container)
verify_producer = BashOperator(
    task_id='verify_producer_running',
    bash_command="""
    # Check producer container status
    if docker ps --filter name=stock-producer --format '{{.Names}}' | grep -q stock-producer; then
        echo "✓ Producer container is running"
        
        # Check recent logs for errors
        echo "Recent producer logs:"
        docker logs --tail 20 stock-producer
        
        exit 0
    else
        echo "✗ Producer container is not running"
        exit 1
    fi
    """,
    dag=dag,
)

# Task dependencies
check_market >> health_check >> verify_producer

