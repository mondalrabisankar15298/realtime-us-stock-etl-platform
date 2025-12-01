#!/usr/bin/env python3
"""
Bronze Layer Ingestion Job
Reads raw stock data from Redpanda/Kafka and writes to Delta Lake Bronze table
Append-only, no transformations - raw data lake layer
"""

import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from delta.tables import DeltaTable

from config import (
    create_spark_session,
    get_kafka_read_options,
    KAFKA_TOPIC_RAW,
    DELTA_PATH_BRONZE,
    CHECKPOINT_PATH_BRONZE,
    RAW_MESSAGE_SCHEMA,
)


def process_bronze_stream():
    """
    Main Bronze layer streaming job
    - Reads from Kafka
    - Parses JSON
    - Writes to Delta Lake (append-only)
    """
    # Create Spark session
    spark = create_spark_session("BronzeIngestion")
    
    print("=" * 80)
    print("BRONZE LAYER INGESTION - Starting")
    print("=" * 80)
    print(f"Reading from Kafka topic: {KAFKA_TOPIC_RAW}")
    print(f"Writing to Delta table: {DELTA_PATH_BRONZE}")
    print(f"Checkpoint location: {CHECKPOINT_PATH_BRONZE}")
    print("=" * 80)
    
    try:
        # Read from Kafka
        kafka_df = (
            spark.readStream
            .format("kafka")
            .options(**get_kafka_read_options(KAFKA_TOPIC_RAW))
            .load()
        )
        
        print("Kafka stream connected successfully")
        
        # Parse JSON from Kafka value
        parsed_df = (
            kafka_df
            .select(
                F.col("key").cast(StringType()).alias("kafka_key"),
                F.from_json(
                    F.col("value").cast(StringType()),
                    RAW_MESSAGE_SCHEMA
                ).alias("data"),
                F.col("timestamp").alias("kafka_timestamp"),
                F.col("partition").alias("kafka_partition"),
                F.col("offset").alias("kafka_offset"),
            )
            .select(
                "data.*",
                F.current_timestamp().alias("bronze_timestamp"),
                "kafka_key",
                "kafka_timestamp",
                "kafka_partition",
                "kafka_offset",
            )
        )
        
        # Write to Delta Lake Bronze table
        query = (
            parsed_df
            .writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", CHECKPOINT_PATH_BRONZE)
            .option("mergeSchema", "true")  # Allow schema evolution
            .trigger(processingTime="10 seconds")  # Microbatch every 10 seconds
            .start(DELTA_PATH_BRONZE)
        )
        
        print("✓ Bronze streaming query started successfully")
        print(f"Query ID: {query.id}")
        print(f"Status: {query.status}")
        print("\nStreaming to Bronze layer (append-only mode)")
        print("Press Ctrl+C to stop...\n")
        
        # Monitor the stream
        while query.isActive:
            # Print progress every 30 seconds
            query.awaitTermination(30)
            
            progress = query.lastProgress
            if progress:
                print("-" * 80)
                print(f"Batch: {progress.get('batchId', 'N/A')}")
                print(f"Input rows: {progress.get('numInputRows', 0)}")
                print(f"Processing time: {progress.get('durationMs', {}).get('triggerExecution', 0)}ms")
                
                # Show source stats
                sources = progress.get('sources', [])
                for source in sources:
                    print(f"  Kafka offset: start={source.get('startOffset', 'N/A')}, "
                          f"end={source.get('endOffset', 'N/A')}")
                
                # Show sink stats
                sink = progress.get('sink', {})
                print(f"Output rows: {sink.get('numOutputRows', 0)}")
                print("-" * 80)
        
    except KeyboardInterrupt:
        print("\n\nStopping Bronze ingestion stream...")
        query.stop()
        print("✓ Stream stopped gracefully")
        
    except Exception as e:
        print(f"\n✗ Error in Bronze ingestion: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        spark.stop()


if __name__ == "__main__":
    process_bronze_stream()

