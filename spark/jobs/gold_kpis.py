#!/usr/bin/env python3
"""
Gold Layer KPI Computation Job
Reads from Silver Delta table and computes financial KPIs
Technical Indicators: SMA, EMA, RSI, VWAP, MACD, ATR
Derived Metrics: daily return, volatility, market phase
Uses MERGE for idempotent upserts
"""

import sys
import time
import os
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


def calculate_technical_indicators_batch(df):
    """
    Calculate technical indicators in batch mode - PRESERVE TIME-SERIES STRUCTURE
    Processes each (symbol, ts) record and adds technical indicators
    """
    from pyspark.sql.window import Window

    # Define window specifications for time-series calculations
    # Partition by symbol, order by timestamp
    symbol_window = Window.partitionBy("symbol").orderBy("ts").rowsBetween(-4, 0)  # 5 rows (current + 4 previous)
    symbol_window_20 = Window.partitionBy("symbol").orderBy("ts").rowsBetween(-19, 0)  # 20 rows
    symbol_window_50 = Window.partitionBy("symbol").orderBy("ts").rowsBetween(-49, 0)  # 50 rows
    
    # Start with the original time-series data from silver
    gold_df = df.select(
        "symbol",
        "ts",
        "open",
        "high",
        "low",
        "close",
        "volume"
    )
    
    # Calculate Simple Moving Averages (SMA)
    gold_df = gold_df.withColumn(
        "sma_5", F.avg("close").over(symbol_window)
    ).withColumn(
        "sma_20", F.avg("close").over(symbol_window_20)
    ).withColumn(
        "sma_50", F.avg("close").over(symbol_window_50)
    )
    
    # Calculate price change and daily return
    # Get previous close for return calculation
    prev_close_window = Window.partitionBy("symbol").orderBy("ts").rowsBetween(Window.unboundedPreceding, Window.currentRow - 1)
    gold_df = gold_df.withColumn(
        "prev_close", F.lag("close", 1).over(Window.partitionBy("symbol").orderBy("ts"))
    ).withColumn(
        "price_change_pct", 
        F.when(F.col("prev_close").isNotNull(), 
               ((F.col("close") - F.col("prev_close")) / F.col("prev_close")) * 100)
        .otherwise(None)
    )
    
    # Calculate daily return (simplified - using price change)
    gold_df = gold_df.withColumn(
        "daily_return", F.col("price_change_pct")
    )
    
    # Calculate volatility (rolling standard deviation of returns)
    return_window = Window.partitionBy("symbol").orderBy("ts").rowsBetween(-4, 0)  # 5-minute window
    gold_df = gold_df.withColumn(
        "volatility_5m", F.stddev("price_change_pct").over(return_window)
    )
    
    # Placeholder columns for future technical indicators (to be implemented)
    gold_df = gold_df.withColumn(
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
        "market_phase", F.lit("unknown")
    ).withColumn(
        "computed_at", F.current_timestamp()
    )
    
    # Drop helper column
    gold_df = gold_df.drop("prev_close")
    
    return gold_df


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


def process_gold_stream(batch_mode=False):
    """
    Main Gold layer batch processing job
    - Reads from Silver Delta table
    - Calculates technical indicators and KPIs
    - Writes to Gold Delta table
    - Waits for Silver table to exist if needed
    
    Args:
        batch_mode: If True, exit after initial processing (for Airflow/scheduled runs)
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
        # Wait for Silver table to exist (with retry logic)
        max_retries = 3
        retry_delay = 10  # seconds
        
        for attempt in range(max_retries):
            try:
                # Check if Silver table exists
                if os.path.exists(DELTA_PATH_SILVER):
                    # Try to read from Silver table
                    silver_df = (
                        spark.read
                        .format("delta")
                        .load(DELTA_PATH_SILVER)
                        .filter(F.col("is_valid") == True)  # Only valid records
                        .select("symbol", "ts", "open", "high", "low", "close", "volume")  # Select required columns
                    )
                    
                    silver_count = silver_df.count()
                    
                    if silver_count > 0:
                        print(f"✓ Silver table found with {silver_count} valid rows")
                        break
                    else:
                        print(f"⚠ Silver table exists but is empty (attempt {attempt + 1}/{max_retries})")
                else:
                    print(f"⚠ Silver table not found yet (attempt {attempt + 1}/{max_retries})")
                
                if attempt < max_retries - 1:
                    print(f"Waiting {retry_delay} seconds before retry...")
                    time.sleep(retry_delay)
                else:
                    print("✗ Silver table not available after maximum retries")
                    print("Gold job will exit gracefully (no data to process).")
                    return
                    
            except Exception as e:
                if attempt < max_retries - 1:
                    print(f"⚠ Error reading Silver table (attempt {attempt + 1}/{max_retries}): {e}")
                    print(f"Waiting {retry_delay} seconds before retry...")
                    time.sleep(retry_delay)
                else:
                    raise

        # Process Silver data
        print(f"\nProcessing {silver_count} rows from Silver table...")

        # Calculate technical indicators in batch mode
        gold_df = calculate_technical_indicators_batch(silver_df)

        # Write to Gold Delta table in batch mode
        (
            gold_df
            .write
            .format("delta")
            .mode("overwrite")  # Overwrite for batch processing
            .save(DELTA_PATH_GOLD)
        )

        print("✓ Gold table created/updated successfully")
        print(f"Gold table location: {DELTA_PATH_GOLD}")
        print(f"Gold table contains {gold_df.count()} aggregated records")

        # If batch mode, exit after processing
        if batch_mode:
            print("\n✓ Gold layer batch processing completed (batch mode - exiting).")
            return

        # Keep the process alive and periodically reprocess (continuous mode)
        print("\nGold layer batch processing completed.")
        print("Job will run periodically to update Gold table with new Silver data...")
        print("Press Ctrl+C to stop...\n")
        
        while True:
            time.sleep(300)  # Wait 5 minutes before reprocessing
            
            try:
                # Check if Silver table has new data
                silver_df_new = (
                    spark.read
                    .format("delta")
                    .load(DELTA_PATH_SILVER)
                    .filter(F.col("is_valid") == True)
                    .select("symbol", "ts", "open", "high", "low", "close", "volume")
                )
                
                new_count = silver_df_new.count()
                
                if new_count > silver_count:
                    print(f"\n📊 Detected new data in Silver ({new_count} rows, was {silver_count})")
                    print("Reprocessing Gold table...")
                    
                    gold_df_new = calculate_technical_indicators_batch(silver_df_new)
                    
                    (
                        gold_df_new
                        .write
                        .format("delta")
                        .mode("overwrite")
                        .save(DELTA_PATH_GOLD)
                    )
                    
                    silver_count = new_count
                    print(f"✓ Gold table updated successfully ({gold_df_new.count()} records)")
                else:
                    print(f"✓ No new data detected (still {silver_count} rows)")
                    
            except Exception as e:
                print(f"⚠ Error during periodic update: {e}")
        
    except KeyboardInterrupt:
        print("\n\nStopping Gold KPI job...")
        print("✓ Job stopped gracefully")
        
    except Exception as e:
        print(f"\n✗ Error in Gold KPI computation: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        spark.stop()


if __name__ == "__main__":
    # Check for batch mode flag (for Airflow/scheduled runs)
    batch_mode = "--batch" in sys.argv
    process_gold_stream(batch_mode=batch_mode)

