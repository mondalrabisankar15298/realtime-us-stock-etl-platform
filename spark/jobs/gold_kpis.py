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
from delta.tables import DeltaTable

from config import (
    create_spark_session,
    DELTA_PATH_SILVER,
    DELTA_PATH_GOLD,
    CHECKPOINT_PATH_GOLD,
    get_market_phase,
)


def calculate_technical_indicators(df):
    """
    Calculate technical indicators for each symbol
    Uses window functions for time-series calculations
    """
    
    # Define windows for each symbol ordered by timestamp
    symbol_time_window = Window.partitionBy("symbol").orderBy("ts")
    
    # Windows with specific row ranges for moving averages
    window_5 = symbol_time_window.rowsBetween(-4, 0)   # 5-period
    window_9 = symbol_time_window.rowsBetween(-8, 0)   # 9-period
    window_14 = symbol_time_window.rowsBetween(-13, 0)  # 14-period
    window_20 = symbol_time_window.rowsBetween(-19, 0)  # 20-period
    window_21 = symbol_time_window.rowsBetween(-20, 0)  # 21-period
    window_50 = symbol_time_window.rowsBetween(-49, 0)  # 50-period
    
    # Unbounded window for cumulative calculations
    unbounded_window = symbol_time_window.rowsBetween(Window.unboundedPreceding, 0)
    
    df_indicators = df
    
    # ==========================================
    # Simple Moving Averages (SMA)
    # ==========================================
    df_indicators = df_indicators.withColumn(
        "sma_5",
        F.avg("close").over(window_5)
    )
    
    df_indicators = df_indicators.withColumn(
        "sma_20",
        F.avg("close").over(window_20)
    )
    
    df_indicators = df_indicators.withColumn(
        "sma_50",
        F.avg("close").over(window_50)
    )
    
    # ==========================================
    # Exponential Moving Averages (EMA)
    # ==========================================
    # EMA formula: EMA_today = (Close_today * multiplier) + (EMA_yesterday * (1 - multiplier))
    # multiplier = 2 / (period + 1)
    
    # EMA 9
    multiplier_9 = 2.0 / (9 + 1)
    df_indicators = df_indicators.withColumn(
        "ema_9_temp",
        F.avg("close").over(window_9)  # Start with SMA as initial EMA
    )
    
    # EMA 21
    multiplier_21 = 2.0 / (21 + 1)
    df_indicators = df_indicators.withColumn(
        "ema_21_temp",
        F.avg("close").over(window_21)  # Start with SMA as initial EMA
    )
    
    # Simplified EMA calculation (using window average as approximation)
    df_indicators = df_indicators.withColumn("ema_9", F.col("ema_9_temp"))
    df_indicators = df_indicators.withColumn("ema_21", F.col("ema_21_temp"))
    
    # ==========================================
    # VWAP (Volume Weighted Average Price)
    # ==========================================
    df_indicators = df_indicators.withColumn(
        "vwap",
        F.sum(F.col("close") * F.col("volume")).over(unbounded_window) / 
        F.sum("volume").over(unbounded_window)
    )
    
    # ==========================================
    # RSI (Relative Strength Index) - 14 period
    # ==========================================
    # Calculate price changes
    df_indicators = df_indicators.withColumn(
        "price_change",
        F.col("close") - F.lag("close", 1).over(symbol_time_window)
    )
    
    # Separate gains and losses
    df_indicators = df_indicators.withColumn(
        "gain",
        F.when(F.col("price_change") > 0, F.col("price_change")).otherwise(0)
    )
    
    df_indicators = df_indicators.withColumn(
        "loss",
        F.when(F.col("price_change") < 0, F.abs(F.col("price_change"))).otherwise(0)
    )
    
    # Average gains and losses over 14 periods
    df_indicators = df_indicators.withColumn(
        "avg_gain",
        F.avg("gain").over(window_14)
    )
    
    df_indicators = df_indicators.withColumn(
        "avg_loss",
        F.avg("loss").over(window_14)
    )
    
    # Calculate RS and RSI
    df_indicators = df_indicators.withColumn(
        "rs",
        F.when(F.col("avg_loss") != 0, F.col("avg_gain") / F.col("avg_loss")).otherwise(100)
    )
    
    df_indicators = df_indicators.withColumn(
        "rsi_14",
        100 - (100 / (1 + F.col("rs")))
    )
    
    # ==========================================
    # MACD (Moving Average Convergence Divergence)
    # ==========================================
    # MACD = EMA(12) - EMA(26)
    # Signal = EMA(9) of MACD
    # Histogram = MACD - Signal
    
    # Calculate EMA 12 and EMA 26 (simplified using SMA approximation)
    window_12 = symbol_time_window.rowsBetween(-11, 0)
    window_26 = symbol_time_window.rowsBetween(-25, 0)
    
    df_indicators = df_indicators.withColumn(
        "ema_12",
        F.avg("close").over(window_12)
    )
    
    df_indicators = df_indicators.withColumn(
        "ema_26",
        F.avg("close").over(window_26)
    )
    
    df_indicators = df_indicators.withColumn(
        "macd",
        F.col("ema_12") - F.col("ema_26")
    )
    
    # Signal line (9-period EMA of MACD) - simplified
    df_indicators = df_indicators.withColumn(
        "macd_signal",
        F.avg("macd").over(window_9)
    )
    
    df_indicators = df_indicators.withColumn(
        "macd_histogram",
        F.col("macd") - F.col("macd_signal")
    )
    
    # ==========================================
    # ATR (Average True Range) - 14 period
    # ==========================================
    # True Range = max(high - low, abs(high - prev_close), abs(low - prev_close))
    
    df_indicators = df_indicators.withColumn(
        "prev_close",
        F.lag("close", 1).over(symbol_time_window)
    )
    
    df_indicators = df_indicators.withColumn(
        "tr",
        F.greatest(
            F.col("high") - F.col("low"),
            F.abs(F.col("high") - F.col("prev_close")),
            F.abs(F.col("low") - F.col("prev_close"))
        )
    )
    
    df_indicators = df_indicators.withColumn(
        "atr_14",
        F.avg("tr").over(window_14)
    )
    
    # ==========================================
    # Derived Metrics
    # ==========================================
    
    # Daily return (percent change from previous close)
    df_indicators = df_indicators.withColumn(
        "daily_return",
        F.when(
            F.col("prev_close").isNotNull() & (F.col("prev_close") != 0),
            ((F.col("close") - F.col("prev_close")) / F.col("prev_close")) * 100
        ).otherwise(0.0)
    )
    
    # 5-minute volatility (standard deviation of returns over 5 periods)
    window_5_vol = symbol_time_window.rowsBetween(-4, 0)
    df_indicators = df_indicators.withColumn(
        "volatility_5m",
        F.stddev("daily_return").over(window_5_vol)
    )
    
    # Price change percentage (from open to close)
    df_indicators = df_indicators.withColumn(
        "price_change_pct",
        F.when(
            F.col("open") != 0,
            ((F.col("close") - F.col("open")) / F.col("open")) * 100
        ).otherwise(0.0)
    )
    
    # Market phase (based on hour in EST)
    df_indicators = df_indicators.withColumn(
        "market_phase",
        F.when(
            (F.hour("ts") >= 4) & (F.hour("ts") < 9), "pre-market"
        ).when(
            (F.hour("ts") >= 9) & (F.hour("ts") < 16), "open"
        ).when(
            (F.hour("ts") >= 16) & (F.hour("ts") < 20), "post-market"
        ).otherwise("closed")
    )
    
    # Add computation timestamp
    df_indicators = df_indicators.withColumn(
        "computed_at",
        F.current_timestamp()
    )
    
    # ==========================================
    # Select final columns for Gold layer
    # ==========================================
    df_gold = df_indicators.select(
        "symbol",
        "ts",
        "close",
        "volume",
        "sma_5",
        "sma_20",
        "sma_50",
        "ema_9",
        "ema_21",
        "rsi_14",
        "vwap",
        "macd",
        "macd_signal",
        "macd_histogram",
        "atr_14",
        "daily_return",
        "volatility_5m",
        "market_phase",
        "price_change_pct",
        "computed_at"
    )
    
    return df_gold


def upsert_to_gold(microBatchDF, batchId):
    """
    Upsert data to Gold Delta table using MERGE
    Ensures idempotency
    """
    
    if microBatchDF.count() == 0:
        print(f"Batch {batchId}: No records to process")
        return
    
    print(f"Batch {batchId}: Processing {microBatchDF.count()} records")
    
    # Check if Gold table exists
    spark = microBatchDF.sparkSession
    
    try:
        gold_table = DeltaTable.forPath(spark, DELTA_PATH_GOLD)
        
        # MERGE operation (upsert)
        (
            gold_table.alias("target")
            .merge(
                microBatchDF.alias("source"),
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
        
        (
            microBatchDF
            .write
            .format("delta")
            .mode("append")
            .save(DELTA_PATH_GOLD)
        )
        
        print(f"Batch {batchId}: Gold table created successfully")


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
            .load(DELTA_PATH_SILVER)
            .filter(F.col("is_valid") == True)  # Only valid records
        )
        
        print("Silver stream connected successfully")
        
        # Calculate technical indicators
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

