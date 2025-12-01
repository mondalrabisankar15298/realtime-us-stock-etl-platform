#!/usr/bin/env python3
"""
Silver Layer Cleaning Job
Reads from Bronze Delta table, applies transformations, and writes to Silver Delta table
Transformations: type casting, deduplication, validation, timezone normalization
Uses MERGE for idempotent upserts
"""

import sys
import os
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

from config import (
    create_spark_session,
    DELTA_PATH_BRONZE,
    DELTA_PATH_SILVER,
    CHECKPOINT_PATH_SILVER,
)


def clean_and_validate(df):
    """
    Apply data quality transformations
    - Convert timestamp to proper TimestampType
    - Validate price and volume
    - Add validation flag
    - Add partition column
    Note: Deduplication moved to foreachBatch function for streaming compatibility
    """

    # Convert Unix timestamp to TimestampType (UTC)
    df_cleaned = df.withColumn(
        "ts",
        F.from_unixtime(F.col("timestamp")).cast("timestamp")
    )

    # Data validation rules
    df_cleaned = df_cleaned.withColumn(
        "is_valid",
        (
            (F.col("open") > 0) &
            (F.col("high") > 0) &
            (F.col("low") > 0) &
            (F.col("close") > 0) &
            (F.col("high") >= F.col("low")) &
            (F.col("high") >= F.col("open")) &
            (F.col("high") >= F.col("close")) &
            (F.col("low") <= F.col("open")) &
            (F.col("low") <= F.col("close")) &
            (F.col("volume") >= 0)
        )
    )

    # Add ingestion date for partitioning (YYYY-MM-DD)
    df_cleaned = df_cleaned.withColumn(
        "ingestion_date",
        F.date_format(F.col("ts"), "yyyy-MM-dd")
    )

    # Add processing timestamp
    df_cleaned = df_cleaned.withColumn(
        "processed_at",
        F.current_timestamp()
    )

    # Select final columns for Silver layer
    # Note: Deduplication will be done in foreachBatch to avoid streaming window restrictions
    df_silver = df_cleaned.select(
        "symbol",
        "ts",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "is_valid",
        "ingestion_date",
        "processed_at",
        "bronze_timestamp"  # Keep for deduplication in batch
    )

    return df_silver


def upsert_to_silver(microBatchDF, batchId):
    """
    Upsert data to Silver Delta table using MERGE
    This ensures idempotency - same data won't create duplicates
    Includes deduplication logic moved from streaming transformation
    """

    if microBatchDF.count() == 0:
        print(f"Batch {batchId}: No records to process")
        return

    # Deduplicate by (symbol, ts) - keep latest bronze_timestamp
    # This is done in batch processing since streaming windows are restricted
    window_spec = Window.partitionBy("symbol", "ts").orderBy(F.col("bronze_timestamp").desc())

    deduped_df = (
        microBatchDF
        .withColumn("row_num", F.row_number().over(window_spec))
        .filter(F.col("row_num") == 1)
        .drop("row_num", "bronze_timestamp")  # Remove bronze_timestamp after deduplication
    )

    # Filter only valid records (optional: you can keep invalid for audit)
    valid_df = deduped_df.filter(F.col("is_valid") == True)

    if valid_df.count() == 0:
        print(f"Batch {batchId}: No valid records to process after deduplication")
        return

    print(f"Batch {batchId}: Processing {valid_df.count()} valid records (after deduplication)")

    # Check if Silver table exists
    spark = microBatchDF.sparkSession

    try:
        silver_table = DeltaTable.forPath(spark, DELTA_PATH_SILVER)

        # MERGE operation (upsert)
        (
            silver_table.alias("target")
            .merge(
                valid_df.alias("source"),
                "target.symbol = source.symbol AND target.ts = source.ts"
            )
            .whenMatchedUpdate(
                set={
                    "open": "source.open",
                    "high": "source.high",
                    "low": "source.low",
                    "close": "source.close",
                    "volume": "source.volume",
                    "is_valid": "source.is_valid",
                    "processed_at": "source.processed_at"
                }
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

        print(f"Batch {batchId}: MERGE completed successfully")

    except Exception as e:
        # Table doesn't exist - create it with initial write
        print(f"Batch {batchId}: Creating Silver table (first write)")

        (
            valid_df
            .write
            .format("delta")
            .mode("append")
            .partitionBy("ingestion_date")
            .save(DELTA_PATH_SILVER)
        )

        print(f"Batch {batchId}: Silver table created successfully")


def process_silver_stream():
    """
    Main Silver layer streaming job
    - Reads from Bronze Delta table
    - Applies cleaning and validation
    - Writes to Silver Delta table with MERGE (idempotent)
    - Performs initial batch load if checkpoint doesn't exist
    """
    # Create Spark session
    spark = create_spark_session("SilverCleaning")
    
    print("=" * 80)
    print("SILVER LAYER CLEANING - Starting")
    print("=" * 80)
    print(f"Reading from Bronze table: {DELTA_PATH_BRONZE}")
    print(f"Writing to Silver table: {DELTA_PATH_SILVER}")
    print(f"Checkpoint location: {CHECKPOINT_PATH_SILVER}")
    print("=" * 80)
    
    query = None
    
    try:
        # Always try to do initial batch load if Silver table doesn't exist
        # This ensures we process existing Bronze data on first run
        silver_table_exists = os.path.exists(DELTA_PATH_SILVER) and os.path.isdir(DELTA_PATH_SILVER)
        
        if not silver_table_exists:
            print("\n⚠ Silver table does not exist - performing initial batch load of existing Bronze data...")
            sys.stdout.flush()
            try:
                # Check if Bronze table exists and has data
                bronze_batch = spark.read.format("delta").load(DELTA_PATH_BRONZE)
                bronze_count = bronze_batch.count()
                
                if bronze_count > 0:
                    print(f"✓ Found {bronze_count} existing records in Bronze table")
                    print("Processing initial batch...")
                    
                    # Apply transformations
                    silver_batch = clean_and_validate(bronze_batch)
                    
                    # Process initial batch using the same upsert logic
                    upsert_to_silver(silver_batch, batchId=0)
                    
                    print(f"✓ Initial batch load completed - processed {bronze_count} records")
                else:
                    print("⚠ Bronze table exists but is empty - waiting for new data...")
            except Exception as e:
                print(f"⚠ Bronze table may not exist yet or is empty: {e}")
                print("Will process data once it arrives...")
        else:
            print(f"✓ Silver table already exists at {DELTA_PATH_SILVER}")
        
        # Now start streaming for new data
        print("\nStarting streaming query for new data...")
        bronze_df = (
            spark.readStream
            .format("delta")
            .load(DELTA_PATH_BRONZE)
        )
        
        print("Bronze stream connected successfully")
        
        # Apply transformations
        silver_df = clean_and_validate(bronze_df)
        
        # Write to Silver Delta table using foreachBatch for MERGE
        query = (
            silver_df
            .writeStream
            .foreachBatch(upsert_to_silver)
            .outputMode("update")
            .option("checkpointLocation", CHECKPOINT_PATH_SILVER)
            .trigger(processingTime="15 seconds")  # Process every 15 seconds
            .start()
        )
        
        print("✓ Silver streaming query started successfully")
        print(f"Query ID: {query.id}")
        print(f"Status: {query.status}")
        print("\nStreaming to Silver layer (MERGE mode for idempotency)")
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
                print("-" * 80)
        
    except KeyboardInterrupt:
        print("\n\nStopping Silver cleaning stream...")
        if query:
            query.stop()
        print("✓ Stream stopped gracefully")
        
    except Exception as e:
        print(f"\n✗ Error in Silver cleaning: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        spark.stop()


if __name__ == "__main__":
    process_silver_stream()

