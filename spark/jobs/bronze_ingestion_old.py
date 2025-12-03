#!/usr/bin/env python3
"""
Bronze Layer Ingestion Job
Reads raw stock data from Redpanda/Kafka and writes to Delta Lake Bronze table
Append-only, no transformations - raw data lake layer
"""

import sys
import os
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
    - Performs initial batch load if Bronze table is empty
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
    
    query = None
    
    try:
        # Check if Bronze table exists and is empty
        # If empty, do initial batch load from Kafka with "earliest" offset
        bronze_table_exists = os.path.exists(DELTA_PATH_BRONZE) and os.path.isdir(DELTA_PATH_BRONZE)
        
        if bronze_table_exists:
            try:
                bronze_df = spark.read.format("delta").load(DELTA_PATH_BRONZE)
                bronze_count = bronze_df.count()
                if bronze_count == 0:
                    bronze_table_exists = False  # Treat empty table as non-existent
                    print(f"⚠ Bronze table exists but is empty - will load historical data")
            except Exception:
                bronze_table_exists = False
        
        if not bronze_table_exists:
            print("\n⚠ Bronze table does not exist or is empty - performing initial batch load from Kafka...")
            print("Reading all historical data from Kafka (startingOffsets: earliest)")
            
            try:
                # Read all historical data from Kafka in batch mode
                kafka_batch_df = (
                    spark.read
                    .format("kafka")
                    .options(**get_kafka_read_options(KAFKA_TOPIC_RAW, starting_offsets="earliest"))
                    .load()
                )
                
                # Parse JSON from Kafka value (same as streaming)
                parsed_batch_df = (
                    kafka_batch_df
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
                    .filter(F.col("data.symbol").isNotNull())  # Filter out null records
                )
                
                batch_count = parsed_batch_df.count()
                
                if batch_count > 0:
                    print(f"✓ Found {batch_count} records in Kafka - writing to Bronze table...")
                    
                    # Write initial batch to Bronze Delta table
                    (
                        parsed_batch_df
                        .write
                        .format("delta")
                        .mode("append")
                        .option("mergeSchema", "true")
                        .save(DELTA_PATH_BRONZE)
                    )
                    
                    print(f"✓ Initial batch load completed - wrote {batch_count} records to Bronze table")
                else:
                    print("⚠ No data found in Kafka - will wait for new messages...")
                    
            except Exception as e:
                print(f"⚠ Error during initial batch load: {e}")
                print("Will proceed with streaming for new data...")
                import traceback
                traceback.print_exc()
        
        # Now start streaming for new data (with "latest" offset)
        print("\nStarting streaming query for new data (startingOffsets: latest)...")
        
        # Read from Kafka stream (new data only)
        kafka_df = (
            spark.readStream
            .format("kafka")
            .options(**get_kafka_read_options(KAFKA_TOPIC_RAW, starting_offsets="latest"))
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
        if query:
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

