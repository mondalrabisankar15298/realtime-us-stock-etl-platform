#!/usr/bin/env python3
"""
Simple test script to verify Spark can read Silver and write Gold
"""

import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent / "spark"))

from pyspark.sql import functions as F
from delta.tables import DeltaTable

from config import (
    create_spark_session,
    DELTA_PATH_SILVER,
    DELTA_PATH_GOLD,
)

def test_spark_operations():
    """Test basic Spark operations"""
    spark = create_spark_session("TestSpark")

    print("Testing Spark operations...")

    try:
        # Read from Silver
        print(f"Reading from Silver: {DELTA_PATH_SILVER}")
        df_silver = spark.read.format("delta").load(DELTA_PATH_SILVER)
        silver_count = df_silver.count()
        print(f"Silver table has {silver_count} records")

        if silver_count == 0:
            print("No data in Silver table")
            return

        # Simple transformation - just select key columns and add timestamp
        df_gold = df_silver.select(
            "symbol", "ts", "close", "volume"
        ).withColumn(
            "computed_at", F.current_timestamp()
        ).withColumn(
            "sma_5", F.lit(None).cast("double")  # Placeholder
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

        # Write to Gold (overwrite mode for testing)
        print(f"Writing to Gold: {DELTA_PATH_GOLD}")
        df_gold.write \
            .format("delta") \
            .mode("overwrite") \
            .save(DELTA_PATH_GOLD)

        gold_count = df_gold.count()
        print(f"Successfully wrote {gold_count} records to Gold table")

        print("✅ Spark operations test completed successfully!")

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

    finally:
        spark.stop()

if __name__ == "__main__":
    test_spark_operations()
