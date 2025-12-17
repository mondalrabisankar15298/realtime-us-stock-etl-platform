"""
Service Sentinel DAG - Self-Healing Watchdog
Monitors core platform services and triggers auto-remediation (restarts) if they are unhealthy.
Runs every 5 minutes.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import subprocess
import json
from airflow.utils.dates import days_ago

# Default arguments
default_args = {
    'owner': 'ops',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
}

# DAG definition
dag = DAG(
    'service_sentinel',
    default_args=default_args,
    description='Watchdog for platform health and auto-remediation',
    schedule_interval='*/5 * * * *',  # Every 5 minutes
    catchup=False,
    max_active_runs=1,
    tags=['health', 'monitoring', 'self-healing', 'ops'],
    is_paused_upon_creation=False,
)

def check_and_remediate_service(service_name, container_name, **context):
    """
    Check the health of a specific docker service and restart it if unhealthy.
    """
    print(f"checking health of {service_name} ({container_name})...")
    
    try:
        # Inspect container
        result = subprocess.run(
            ['docker', 'inspect', container_name],
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            print(f"⚠️  Container {container_name} not found or docker error.")
            # If it doesn't exist, we might need to 'up' it, but for now we assume it should exist
            return 'missing'

        data = json.loads(result.stdout)[0]
        state = data.get('State', {})
        status = state.get('Status', 'unknown')
        health = state.get('Health', {}).get('Status', 'unknown')
        
        print(f"Status: {status}, Health: {health}")
        
        needs_restart = False
        reason = ""
        
        # Criteria for restart
        if status == 'exited':
            if state.get('ExitCode') != 0:
                needs_restart = True
                reason = f"Container exited with code {state.get('ExitCode')}"
        elif health == 'unhealthy':
            needs_restart = True
            reason = "Container is reporting UNHEALTHY status"
        elif status == 'dead':
            needs_restart = True
            reason = "Container is DEAD"
            
        if needs_restart:
            print(f"❌ REMEDIATION TRIGGERED: {reason}")
            print(f"Restarting {container_name}...")
            
            restart_cmd = subprocess.run(
                ['docker', 'restart', container_name],
                capture_output=True,
                text=True,
                timeout=60
            )
            
            if restart_cmd.returncode == 0:
                 print(f"✅ Successfully restarted {container_name}")
                 return 'restarted'
            else:
                 print(f"⛔ Failed to restart {container_name}: {restart_cmd.stderr}")
                 raise Exception(f"Failed to remediate {container_name}")
        else:
            print(f"✓ {service_name} is healthy")
            return 'healthy'

    except Exception as e:
        print(f"Error checking {service_name}: {e}")
        # Don't fail the DAG, just log error
        return 'error'

def run_sentinel(**context):
    """
    Main sentinel function to check all critical services
    """
    services = [
        {'name': 'Redpanda', 'container': 'redpanda'},
        {'name': 'TimescaleDB', 'container': 'postgres-timescale'},
        {'name': 'Polars ETL', 'container': 'polars-etl'},
        {'name': 'Stock Producer', 'container': 'stock-producer'},
        {'name': 'Airflow Scheduler', 'container': 'airflow-scheduler'}
    ]
    
    results = {}
    
    for service in services:
        status = check_and_remediate_service(service['name'], service['container'], **context)
        results[service['name']] = status
        
    print("\nSentinel Run Summary:")
    print(json.dumps(results, indent=2))
    
    # Fail task if remediation failed? 
    # For now, we succeed so the DAG continues running next schedule
    return results

# Define task
sentinel_task = PythonOperator(
    task_id='check_and_remediate',
    python_callable=run_sentinel,
    dag=dag,
)
