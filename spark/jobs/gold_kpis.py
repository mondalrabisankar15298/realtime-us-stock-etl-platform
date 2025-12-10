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
    Calculate technical indicators in batch mode using PySpark Native UDFs on Arrays.
    This avoids Pandas UDFs while handling recursive calculations (EMA, MACD) efficiently.
    Strategy: GroupBy Symbol -> Collect List -> UDF(Array -> Array) -> Explode
    """
    from pyspark.sql.types import ArrayType, DoubleType, StructType, StructField
    
    # --- Python Helper Functions (Pure Python, No Pandas) ---
    def calculate_ema_series_py(values, span):
        if not values: return []
        alpha = 2.0 / (span + 1)
        ema = []
        # Initial EMA is usually SMA of first N or just first price
        # Using first price for simplicity and convergence
        curr = values[0] if values[0] is not None else 0.0
        ema.append(curr)
        for i in range(1, len(values)):
             val = values[i]
             if val is None: val = curr # Carry forward
             curr = (val * alpha) + (curr * (1 - alpha))
             ema.append(curr)
        return ema
        
    def calculate_rsi_series_py(closes):
        # ROI logic on list
        period = 14
        if len(closes) < period + 1: return [None] * len(closes)
        
        rsi = [None] * len(closes)
        gains = []
        losses = []
        
        # Calculate changes
        for i in range(1, len(closes)):
            change = closes[i] - closes[i-1]
            if change > 0:
                gains.append(change)
                losses.append(0.0)
            else:
                gains.append(0.0)
                losses.append(abs(change))
        
        # Initial Avg Gain/Loss (SMA)
        avg_gain = sum(gains[:period]) / period
        avg_loss = sum(losses[:period]) / period
        
        # First RSI
        if avg_loss == 0:
            rsi[period] = 100.0
        else:
            rs = avg_gain / avg_loss
            rsi[period] = 100.0 - (100.0 / (1.0 + rs))
            
        # Wilder's Smoothing for subsequent
        for i in range(period + 1, len(closes)):
             # Index in gains/losses is i-1
             current_gain = gains[i-1]
             current_loss = losses[i-1]
             
             avg_gain = ((avg_gain * (period - 1)) + current_gain) / period
             avg_loss = ((avg_loss * (period - 1)) + current_loss) / period
             
             if avg_loss == 0:
                 rsi[i] = 100.0
             else:
                 rs = avg_gain / avg_loss
                 rsi[i] = 100.0 - (100.0 / (1.0 + rs))
        
        return rsi

    # UDF Definition
    # Input: sorted lists of numeric data
    # Output: Struct of lists
    output_schema = StructType([
        StructField("ema_9", ArrayType(DoubleType())),
        StructField("ema_21", ArrayType(DoubleType())),
        StructField("rsi_14", ArrayType(DoubleType())),
        StructField("macd", ArrayType(DoubleType())),
        StructField("macd_signal", ArrayType(DoubleType())),
        StructField("macd_histogram", ArrayType(DoubleType())),
        StructField("atr_14", ArrayType(DoubleType())),
        StructField("market_phase", ArrayType(StringType())),
        StructField("sorted_indices", ArrayType(IntegerType())) # To verify order
    ])

    @F.udf(returnType=output_schema)
    def compute_indicators_udf(ts_list, close_list, high_list, low_list, ema9_pd_placeholder):
        # 1. Sort by timestamp strictly inside UDF
        data = sorted(zip(ts_list, close_list, high_list, low_list, range(len(ts_list))), key=lambda x: x[0])
        ts_sorted, closes, highs, lows, indices = zip(*data)
        
        n = len(closes)
        
        # EMA
        ema9 = calculate_ema_series_py(closes, 9)
        ema21 = calculate_ema_series_py(closes, 21)
        
        # MACD
        ema12 = calculate_ema_series_py(closes, 12)
        ema26 = calculate_ema_series_py(closes, 26)
        macd_line = [(e12 - e26) if e12 is not None and e26 is not None else None for e12, e26 in zip(ema12, ema26)]
        
        # Signal Line (EMA 9 of MACD)
        # Handle None in macd_line for initial values
        # We can fill None with 0.0 or skip
        valid_macd = [m if m is not None else 0.0 for m in macd_line]
        macd_signal = calculate_ema_series_py(valid_macd, 9)
        macd_hist = [(m - s) for m, s in zip(valid_macd, macd_signal)]
        
        # RSI
        rsi14 = calculate_rsi_series_py(closes)
        
        # ATR (14)
        # TR = Max(H-L, |H-Cp|, |L-Cp|)
        tr_list = []
        tr_list.append(highs[0] - lows[0]) # First TR is H-L
        for i in range(1, n):
            hl = highs[i] - lows[i]
            h_cp = abs(highs[i] - closes[i-1])
            l_cp = abs(lows[i] - closes[i-1])
            tr_list.append(max(hl, h_cp, l_cp))
            
        # ATR is SMA of TR (or RMA) - using SMA for simplicity here matching Pandas Code
        atr14 = [None] * n
        if n > 14:
            # Simple rolling mean
            window_sum = sum(tr_list[:14])
            atr14[13] = window_sum / 14
            for i in range(14, n):
                 window_sum = window_sum - tr_list[i-14] + tr_list[i]
                 atr14[i] = window_sum / 14
        
        # Market Phase
        phases = []
        for i in range(n):
            c = closes[i]
            e9 = ema9[i]
            e21 = ema21[i]
            if c is None or e9 is None or e21 is None:
                phases.append("unknown")
            elif c > e9 and e9 > e21:
                phases.append("bullish")
            elif c < e9 and e9 < e21:
                phases.append("bearish")
            else:
                phases.append("consolidation")

        return {
            "ema_9": ema9,
            "ema_21": ema21,
            "rsi_14": rsi14,
            "macd": macd_line,
            "macd_signal": macd_signal,
            "macd_histogram": macd_hist,
            "atr_14": atr14,
            "market_phase": phases,
            "sorted_indices": indices 
        }

    # --- Spark Logic ---
    
    # 1. Calculate Non-Recursive Indicators (SMA, Returns) using Standard Window functions
    # (Existing logic is fine, we keep it)
    symbol_window = Window.partitionBy("symbol").orderBy("ts").rowsBetween(-4, 0)
    symbol_window_20 = Window.partitionBy("symbol").orderBy("ts").rowsBetween(-19, 0)
    symbol_window_50 = Window.partitionBy("symbol").orderBy("ts").rowsBetween(-49, 0)
    
    gold_df = df.select("symbol", "ts", "open", "high", "low", "close", "volume")
    
    gold_df = gold_df.withColumn("sma_5", F.avg("close").over(symbol_window)) \
                     .withColumn("sma_20", F.avg("close").over(symbol_window_20)) \
                     .withColumn("sma_50", F.avg("close").over(symbol_window_50))
    
    # Daily Return logic...
    prev_close_win = Window.partitionBy("symbol").orderBy("ts").rowsBetween(Window.unboundedPreceding, Window.currentRow - 1)
    gold_df = gold_df.withColumn("prev_close", F.lag("close", 1).over(Window.partitionBy("symbol").orderBy("ts")))
    gold_df = gold_df.withColumn("price_change_pct", 
        F.when(F.col("prev_close").isNotNull(), ((F.col("close") - F.col("prev_close")) / F.col("prev_close")) * 100).otherwise(0.0)
    )
    gold_df = gold_df.withColumn("daily_return", F.col("price_change_pct"))
    gold_df = gold_df.withColumn("volatility_5m", F.stddev("price_change_pct").over(symbol_window)) # Reuse window

    # 2. VWAP (Cumulative Session) - Can be done with Window unboundedPreceding
    # Partition by Day + Symbol. For now, assuming Global Cumulative for simplicity or single day batch.
    # To do it properly native:
    gold_df = gold_df.withColumn("day", F.to_date("ts"))
    daily_window = Window.partitionBy("symbol", "day").orderBy("ts").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    
    gold_df = gold_df.withColumn("cum_pv", F.sum(F.col("close") * F.col("volume")).over(daily_window))
    gold_df = gold_df.withColumn("cum_vol", F.sum("volume").over(daily_window))
    gold_df = gold_df.withColumn("vwap", F.when(F.col("cum_vol") == 0, F.col("close")).otherwise(F.col("cum_pv") / F.col("cum_vol")))
    
    # 3. Recursive Indicators via UDF (EMA, MACD, RSI, ATR, Phase)
    # Collect lists
    grouped = gold_df.groupBy("symbol").agg(
        F.collect_list("ts").alias("ts_list"),
        F.collect_list("close").alias("close_list"),
        F.collect_list("high").alias("high_list"),
        F.collect_list("low").alias("low_list")
    )
    
    # Apply UDF
    grouped_computed = grouped.withColumn("indicators", compute_indicators_udf(
        "ts_list", "close_list", "high_list", "low_list", F.lit(None)
    ))
    
    # Explode back
    # indicators is a Struct of Arrays. We need to zip them all back.
    # F.arrays_zip(ts_list, indicators.ema_9, ...)
    
    exploded = grouped_computed.select(
        "symbol",
        F.explode(F.arrays_zip(
            F.col("ts_list"),
            F.col("indicators.ema_9"),
            F.col("indicators.ema_21"),
            F.col("indicators.rsi_14"),
            F.col("indicators.macd"),
            F.col("indicators.macd_signal"),
            F.col("indicators.macd_histogram"),
            F.col("indicators.atr_14"),
            F.col("indicators.market_phase")
        )).alias("zipped")
    ).select(
        "symbol",
        F.col("zipped.0").alias("ts"),
        F.col("zipped.1").alias("ema_9"),
        F.col("zipped.2").alias("ema_21"),
        F.col("zipped.3").alias("rsi_14"),
        F.col("zipped.4").alias("macd"),
        F.col("zipped.5").alias("macd_signal"),
        F.col("zipped.6").alias("macd_histogram"),
        F.col("zipped.7").alias("atr_14"),
        F.col("zipped.8").alias("market_phase")
    )
    
    # Join back with original Windowed DF (SMA, VWAP)
    # Join key: symbol, ts
    # Warning: timestamps must match exactly. The UDF sorted them, so arrays_zip respects that order.
    
    final_df = gold_df.join(exploded, ["symbol", "ts"], "inner")
    
    # Add computed_at and formatting
    final_df = final_df.withColumn("computed_at", F.current_timestamp())
    
    # Select Final Schema Columns
    cols = [
            "symbol", "ts", "close", "volume", 
            "sma_5", "sma_20", "sma_50", 
            "ema_9", "ema_21", "rsi_14", "vwap", 
            "macd", "macd_signal", "macd_histogram", "atr_14", 
            "daily_return", "volatility_5m", "market_phase", 
            "price_change_pct", "computed_at"
    ]
    
    return final_df.select(cols)


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
                    raise

        import gc
        
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
        
        # Cleanup
        spark.catalog.clearCache()
        gc.collect()

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
                # Cleanup before new iteration
                spark.catalog.clearCache()
                gc.collect()
                
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
                    
                    # Cleanup after write
                    gold_df_new.unpersist()
                    spark.catalog.clearCache()
                    gc.collect()
                else:
                    print(f"✓ No new data detected (still {silver_count} rows)")
                    
            except Exception as e:
                print(f"⚠ Error during periodic update: {e}")
                import traceback
                traceback.print_exc()
        
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

