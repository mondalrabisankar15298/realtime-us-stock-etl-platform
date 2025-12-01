#!/usr/bin/env python3
"""
Gold Layer KPI Computation Job
Reads from Silver Delta table and computes financial KPIs
Technical Indicators: SMA, EMA, RSI, VWAP, MACD, ATR
Derived Metrics: daily return, volatility, market phase
Uses MERGE for idempotent upserts
"""

import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType
from delta.tables import DeltaTable

from config import (
    create_spark_session,
    DELTA_PATH_SILVER,
    DELTA_PATH_GOLD,
    CHECKPOINT_PATH_GOLD,
    get_market_phase,
)


# Define Gold table schema explicitly
GOLD_SCHEMA = StructType([
    StructField("symbol", StringType(), True),
    StructField("ts", TimestampType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", IntegerType(), True),
    StructField("sma_5", DoubleType(), True),
    StructField("sma_20", DoubleType(), True),
    StructField("sma_50", DoubleType(), True),
    StructField("ema_9", DoubleType(), True),
    StructField("ema_21", DoubleType(), True),
    StructField("rsi_14", DoubleType(), True),
    StructField("vwap", DoubleType(), True),
    StructField("macd", DoubleType(), True),
    StructField("macd_signal", DoubleType(), True),
    StructField("macd_histogram", DoubleType(), True),
    StructField("atr_14", DoubleType(), True),
    StructField("daily_return", DoubleType(), True),
    StructField("volatility_5m", DoubleType(), True),
    StructField("market_phase", StringType(), True),
    StructField("price_change_pct", DoubleType(), True),
    StructField("computed_at", TimestampType(), True),
])


def calculate_technical_indicators(df):
    """
    Calculate technical indicators for each symbol
    Note: Window calculations are done in batch processing (foreachBatch)
    This function only prepares the data for streaming
    """
    # For streaming, we only do basic transformations
    # All window-based calculations are moved to foreachBatch
    
    # Add columns needed for calculations (open, high, low are needed for indicators)
    # These should already be in the Silver layer, but we ensure they're available
    df_prepared = df.select(
        "symbol",
        "ts",
        "open",
        "high",
        "low",
        "close",
        "volume"
    )
    
    return df_prepared


def upsert_to_gold(microBatchDF, batchId):
    """
    Upsert data to Gold Delta table using MERGE
    For now, just pass through the data without complex calculations
    Ensures idempotency
    """

    if microBatchDF.count() == 0:
        print(f"Batch {batchId}: No records to process")
        return

    print(f"Batch {batchId}: Processing {microBatchDF.count()} records")

    spark = microBatchDF.sparkSession

    # For now, just add a computed timestamp and pass through the data
    # TODO: Add technical indicators later
    df_gold = microBatchDF.withColumn(
        "computed_at",
        F.current_timestamp()
    ).withColumn(
        "sma_5", F.lit(None).cast("double")  # Placeholder columns
    ).withColumn(
        "sma_20", F.lit(None).cast("double")
    ).withColumn(
        "sma_50", F.lit(None).cast("double")
    ).withColumn(
        "ema_9", F.lit(None).cast("double")
    ).withColumn(
        "ema_21", F.lit(None).cast("double")
    ).withColumn(
        "rsi_14", F.lit(None).cast("double")
    ).withColumn(
        "vwap", F.lit(None).cast("double")
    ).withColumn(
        "macd", F.lit(None).cast("double")
    ).withColumn(
        "macd_signal", F.lit(None).cast("double")
    ).withColumn(
        "macd_histogram", F.lit(None).cast("double")
    ).withColumn(
        "atr_14", F.lit(None).cast("double")
    ).withColumn(
        "daily_return", F.lit(None).cast("double")
    ).withColumn(
        "volatility_5m", F.lit(None).cast("double")
    ).withColumn(
        "market_phase", F.lit("unknown")
    ).withColumn(
        "price_change_pct", F.lit(None).cast("double")
    )

    # Check if Gold table exists
    try:
        gold_table = DeltaTable.forPath(spark, DELTA_PATH_GOLD)

        # MERGE operation (upsert)
        (
            gold_table.alias("target")
            .merge(
                df_gold.alias("source"),
                "target.symbol = source.symbol AND target.ts = source.ts"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

        print(f"Batch {batchId}: MERGE completed successfully")

    except Exception as e:
        # Table doesn't exist - create it with initial write
        print(f"Batch {batchId}: Creating Gold table (first write)")

        # Ensure we have data to write
        if df_gold.count() > 0:
            # Create table with explicit schema
            (
                df_gold
                .write
                .format("delta")
                .mode("append")
                .save(DELTA_PATH_GOLD)
            )

            print(f"Batch {batchId}: Gold table created successfully")
        else:
            print(f"Batch {batchId}: Skipping table creation - no data available")


def process_gold_stream():
    """
    Main Gold layer streaming job
    - Reads from Silver Delta table
    - Calculates technical indicators and KPIs
    - Writes to Gold Delta table with MERGE
    """
    # Create Spark session
    spark = create_spark_session("GoldKPIs")
    
    print("=" * 80)
    print("GOLD LAYER KPI COMPUTATION - Starting")
    print("=" * 80)
    print(f"Reading from Silver table: {DELTA_PATH_SILVER}")
    print(f"Writing to Gold table: {DELTA_PATH_GOLD}")
    print(f"Checkpoint location: {CHECKPOINT_PATH_GOLD}")
    print("=" * 80)
    
    try:
        # Read from Silver Delta table as a stream
        silver_df = (
            spark.readStream
            .format("delta")
            .option("skipChangeCommits", "true")  # Skip updates to handle backfill data
            .load(DELTA_PATH_SILVER)
            .filter(F.col("is_valid") == True)  # Only valid records
            .select("symbol", "ts", "open", "high", "low", "close", "volume")  # Select required columns
        )
        
        print("Silver stream connected successfully")
        
        # Prepare data for batch processing (window calculations done in foreachBatch)
        gold_df = calculate_technical_indicators(silver_df)
        
        # Write to Gold Delta table using foreachBatch for MERGE
        query = (
            gold_df
            .writeStream
            .foreachBatch(upsert_to_gold)
            .outputMode("update")
            .option("checkpointLocation", CHECKPOINT_PATH_GOLD)
            .trigger(processingTime="20 seconds")  # Process every 20 seconds
            .start()
        )
        
        print("✓ Gold streaming query started successfully")
        print(f"Query ID: {query.id}")
        print(f"Status: {query.status}")
        print("\nComputing KPIs and streaming to Gold layer")
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
        print("\n\nStopping Gold KPI stream...")
        query.stop()
        print("✓ Stream stopped gracefully")
        
    except Exception as e:
        print(f"\n✗ Error in Gold KPI computation: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        spark.stop()


if __name__ == "__main__":
    process_gold_stream()

