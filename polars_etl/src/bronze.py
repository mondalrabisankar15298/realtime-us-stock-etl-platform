import asyncio
import json
import os
import time
from kafka import KafkaConsumer
from deltalake import write_deltalake
import pandas as pd
import logging

logger = logging.getLogger("BronzeLayer")

# Configuration
KAFKA_BROKER = os.getenv("REDPANDA_BROKER", "redpanda:9092")
TOPIC = os.getenv("KAFKA_TOPIC_RAW", "stock_market_data")
DELTA_PATH_BRONZE = "/opt/spark/delta_tables/bronze"

def get_consumer():
    # Check for backfill flag
    force_backfill = os.getenv("FORCE_BACKFILL", "false").lower() == "true"
    
    if force_backfill:
        logger.info("FORCE_BACKFILL=true detected. Using earliest offset with unique consumer group for full backfill.")
        # Use earliest offset and a unique group ID to force re-reading all data
        auto_offset_reset = 'earliest'
        # Append timestamp to ensure we don't pick up old backfill commits
        group_id = f'polars_bronze_backfill_{int(time.time())}'
    else:
        # Normal operation: use 'earliest' as fallback
        # This means:
        # - If consumer group has committed offsets, resume from there (no data loss)
        # - If no committed offsets exist (first run), start from earliest
        auto_offset_reset = 'earliest'
        group_id = 'polars_bronze_group'
        logger.info(f"Using consumer group '{group_id}' with auto_offset_reset='earliest' (will resume from last commit if exists)")

    return KafkaConsumer(
        TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset=auto_offset_reset,
        enable_auto_commit=True,  # Auto-commit offsets to prevent data loss on restart
        auto_commit_interval_ms=5000,  # Commit every 5 seconds
        group_id=group_id,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

async def run_bronze():
    logger.info("Starting Bronze Layer ingestion...")
    
    # Wait for Kafka to be ready
    await asyncio.sleep(10) 
    
    try:
        consumer = get_consumer()
        buffer = []
        batch_size = 1000
        last_flush = time.time()
        flush_interval = 2.0 # Seconds
        
        logger.info(f"Connected to Kafka topic: {TOPIC}")
        
        # We run this in a loop, but non-blocking manner? 
        # KafkaConsumer is blocking. In a real async app we'd use aiokafka.
        # For simplicity here, we use short polling in a loop with asyncio.sleep(0) to yield.
        
        while True:
            # Poll for messages (non-blockingish)
            msg_batch = consumer.poll(timeout_ms=100)
            
            for tp, messages in msg_batch.items():
                for message in messages:
                    data = message.value
                    # Add simple ingestion timestamp
                    data['ingestion_timestamp'] = time.time()
                    buffer.append(data)
            
            # Check flush conditions
            current_time = time.time()
            if len(buffer) >= batch_size or (len(buffer) > 0 and current_time - last_flush > flush_interval):
                
                # Create DataFrame
                df = pd.DataFrame(buffer)
                
                # Write to Delta
                # Lock handling is automatic by deltalake-rs
                try:
                    write_deltalake(
                        DELTA_PATH_BRONZE,
                        df,
                        mode="append",
                        schema_mode="merge" # Allow schema evolution
                    )
                    logger.info(f"Flushed {len(buffer)} records to Bronze Delta")
                    buffer = []
                    last_flush = current_time
                    
                except Exception as e:
                    logger.error(f"Error writing to Bronze Delta: {e}")
                    # In production, we would retry or DLQ
            
            await asyncio.sleep(0.01) # Yield to event loop
            
    except Exception as e:
        logger.error(f"Bronze Layer crashed: {e}")
        raise
