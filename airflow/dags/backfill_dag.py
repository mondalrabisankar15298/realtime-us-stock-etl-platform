"""
Backfill DAG - Historical Data Backfill
Manually triggered to fill gaps in stock data
Detects timestamp gaps and fetches historical data from Yahoo Finance
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import json
from pathlib import Path

# Default arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email': ['alerts@example.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition - Manual trigger only
dag = DAG(
    'stock_backfill',
    default_args=default_args,
    description='Backfill historical stock data for detected gaps',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['stock', 'backfill', 'maintenance'],
)


def detect_gaps(**context):
    """
    Detect timestamp gaps in state files
    Returns list of (ticker, start_time, end_time) tuples for gaps
    """
    state_file = Path('/opt/airflow/producer/state/last_fetch.json')
    
    if not state_file.exists():
        print("No state file found - nothing to backfill")
        return []
    
    try:
        with open(state_file, 'r') as f:
            state = json.load(f)
        
        gaps = []
        now = datetime.now()
        
        for ticker, ts_str in state.items():
            last_time = datetime.fromisoformat(ts_str)
            gap_hours = (now - last_time).total_seconds() / 3600
            
            # If gap > 2 hours, mark for backfill
            if gap_hours > 2:
                print(f"Gap detected for {ticker}: {gap_hours:.1f} hours")
                gaps.append({
                    'ticker': ticker,
                    'start': last_time.isoformat(),
                    'end': now.isoformat(),
                    'gap_hours': gap_hours
                })
        
        # Push to XCom for next task
        context['task_instance'].xcom_push(key='gaps', value=gaps)
        
        print(f"\nFound {len(gaps)} gaps to backfill")
        return gaps
        
    except Exception as e:
        print(f"Error detecting gaps: {e}")
        return []


def backfill_data(**context):
    """
    Fetch historical data for detected gaps
    Publish to Kafka with backfill flag
    """
    import yfinance as yf
    from kafka import KafkaProducer
    import os
    import pytz
    
    # Get gaps from previous task
    ti = context['task_instance']
    gaps = ti.xcom_pull(key='gaps', task_ids='detect_gaps')
    
    if not gaps:
        print("No gaps to backfill")
        return
    
    # Setup Kafka producer
    broker = os.getenv('REDPANDA_BROKER', 'redpanda:9092')
    topic = os.getenv('KAFKA_TOPIC_RAW', 'stock-raw-data')
    
    producer = KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None,
    )
    
    utc_tz = pytz.UTC
    total_records = 0
    
    for gap_info in gaps:
        ticker = gap_info['ticker']
        start_time = datetime.fromisoformat(gap_info['start'])
        end_time = datetime.fromisoformat(gap_info['end'])
        
        print(f"\nBackfilling {ticker} from {start_time} to {end_time}")
        
        try:
            # Fetch historical data
            stock = yf.Ticker(ticker)
            
            # Calculate period in days
            days = max(1, int((end_time - start_time).total_seconds() / 86400))
            
            # Fetch 1-minute data (max 7 days for yfinance)
            df = stock.history(period=f"{min(days, 7)}d", interval="1m")
            
            if df.empty:
                print(f"No data available for {ticker}")
                continue
            
            # Filter to gap period
            df_filtered = df[(df.index >= start_time) & (df.index <= end_time)]
            
            print(f"Found {len(df_filtered)} records for {ticker}")
            
            # Publish each record
            for timestamp, row in df_filtered.iterrows():
                # Convert to UTC
                if timestamp.tzinfo is None:
                    timestamp = pytz.timezone('America/New_York').localize(timestamp)
                timestamp_utc = timestamp.astimezone(utc_tz)
                
                message = {
                    "symbol": ticker,
                    "timestamp": int(timestamp_utc.timestamp()),
                    "open": float(row['Open']),
                    "high": float(row['High']),
                    "low": float(row['Low']),
                    "close": float(row['Close']),
                    "volume": int(row['Volume']),
                    "ingested_at": datetime.now(utc_tz).isoformat(),
                    "backfill": True,  # Flag for backfilled data
                }
                
                producer.send(topic, key=ticker, value=message)
                total_records += 1
            
            print(f"✓ Backfilled {len(df_filtered)} records for {ticker}")
            
        except Exception as e:
            print(f"Error backfilling {ticker}: {e}")
    
    producer.flush()
    producer.close()
    
    print(f"\n✓ Backfill complete: {total_records} total records published")


def update_state(**context):
    """
    Update state file after successful backfill
    """
    state_file = Path('/opt/airflow/producer/state/last_fetch.json')
    
    if not state_file.exists():
        print("No state file to update")
        return
    
    # In a real implementation, you would update the state file
    # with the latest backfilled timestamps
    print("✓ State file would be updated here")


# Define tasks
task_detect_gaps = PythonOperator(
    task_id='detect_gaps',
    python_callable=detect_gaps,
    dag=dag,
)

task_backfill = PythonOperator(
    task_id='backfill_data',
    python_callable=backfill_data,
    dag=dag,
)

task_update_state = PythonOperator(
    task_id='update_state',
    python_callable=update_state,
    dag=dag,
)

# Task dependencies
task_detect_gaps >> task_backfill >> task_update_state

