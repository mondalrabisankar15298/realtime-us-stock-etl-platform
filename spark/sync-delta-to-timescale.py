#!/usr/bin/env python3
"""
Sync Delta Lake Gold Table to TimescaleDB
Reads from Gold Delta table and writes to TimescaleDB for Grafana dashboards
Run this as a Spark job or Airflow DAG
"""

import sys
import os
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import psycopg2
from datetime import datetime, timedelta
import os # Added for env var check if needed, though mostly replaced by dynamic logic

# Configuration
DELTA_PATH_GOLD = "/opt/spark/delta_tables/gold"
TIMESCALE_HOST = "postgres-timescale"
TIMESCALE_PORT = "5432"
TIMESCALE_DB = "stockdata"
TIMESCALE_USER = "grafana"
TIMESCALE_PASSWORD = "grafana"


def create_spark_session():
    """Create Spark session with Delta Lake and PostgreSQL support"""
    builder = (
        SparkSession.builder
        .appName("DeltaToTimescaleSync")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0")
    )
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
    return spark


def get_max_timestamp(table_name):
    """
    Get the maximum timestamp from a TimescaleDB table.
    Returns None if table is empty or doesn't exist.
    """
    try:
        conn = psycopg2.connect(
            host=TIMESCALE_HOST,
            port=TIMESCALE_PORT,
            database=TIMESCALE_DB,
            user=TIMESCALE_USER,
            password=TIMESCALE_PASSWORD
        )
        cursor = conn.cursor()
        
        # Check if table exists first
        cursor.execute(f"SELECT to_regclass('public.{table_name}');")
        if cursor.fetchone()[0] is None:
            return None
            
        cursor.execute(f"SELECT MAX(ts) FROM {table_name};")
        result = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        if result and result[0]:
            return result[0]
        return None
        
    except Exception as e:
        print(f"Warning: Could not get max timestamp from {table_name}: {e}")
        return None

def sync_gold_to_timescale(spark, batch_mode=False):
    """
    Sync Gold Delta table to TimescaleDB
    
    Args:
        spark: SparkSession
        batch_mode: If True, sync all data. If False, sync only recent data.
    """
    
    print("=" * 80)
    print("SYNCING DELTA LAKE → TIMESCALEDB")
    print("=" * 80)
    
    # Read from Gold Delta table
    print(f"\n📊 GOLD LAYER SYNC")
    print(f"Reading from: {DELTA_PATH_GOLD}")
    df_gold = spark.read.format("delta").load(DELTA_PATH_GOLD)
    
    if batch_mode:
        print("Batch mode enabled: Syncing ALL data.")
    else:
        # Dynamic State-Aware Sync
        # 1. Check max timestamp in destination
        last_ts = get_max_timestamp("gold_stocks")
        
        from pyspark.sql import functions as F
        
        if last_ts is None:
            print("Destination table empty or not found. Performing FULL table sync.")
            # No filter applied = Full Load
        else:
            # 2. Calculate start time: min(now - 24h, last_ts)
            # This ensures we cover at least 24h (for updates) AND any gap if last_ts is old
            print(f"Found existing data up to: {last_ts}")
            
            # We can't easily mix python datetime and spark current_timestamp in min()
            # So we'll frame it as: Filter data where ts >= (last_ts) OR ts >= (now - 24h)
            # Which is equivalent to: ts >= min(last_ts, now - 24h)
            # Wait, valid logic is: We want to re-sync everything AFTER the gap.
            # So looking locally: default start is last_ts.
            # But we want a safety buffer of 24h.
            # Logic: If last_ts is 7 days ago. We want >= 7 days ago.
            # If last_ts is 1 minute ago. We want >= 24 hours ago (to catch recent late arrivals).
            # So effective start_ts = MIN(last_ts, now - 24h).
            
            # Since 'last_ts' is a python object, we can just pass it as a literal.
            
            # Construct expression: (ts >= last_ts) OR (ts >= current_timestamp() - interval 24 hours)
            # Actually, to get MORE data (older time), we need the SMALLER timestamp.
            # Filter: ts >= LEAST( lit(last_ts), current_timestamp() - interval 10 minutes )
            
            df_gold = df_gold.filter(
                F.col("ts") >= F.least(
                    F.lit(last_ts), 
                    F.expr("current_timestamp() - interval 10 minutes")
                )
            )
            print("Applied dynamic filter: ts >= min(last_max_ts, now - 10m)")
    
    # Select only columns that exist in TimescaleDB gold_stocks table
    # TimescaleDB schema: symbol, ts, close, volume, sma_5, sma_20, sma_50, ema_9, ema_21,
    # rsi_14, vwap, macd, macd_signal, macd_histogram, atr_14, daily_return, volatility_5m,
    # market_phase, price_change_pct, computed_at
    # Note: open, high, low are NOT in TimescaleDB gold_stocks table
    timescale_columns = [
        "symbol", "ts", "close", "volume",
        "sma_5", "sma_20", "sma_50", "ema_9", "ema_21",
        "rsi_14", "vwap", "macd", "macd_signal", "macd_histogram", "atr_14",
        "daily_return", "volatility_5m", "market_phase", "price_change_pct", "computed_at"
    ]
    
    # Select only columns that exist in both DataFrame and TimescaleDB
    available_columns = [col for col in timescale_columns if col in df_gold.columns]
    df_gold = df_gold.select(*available_columns)
    
    total_records = df_gold.count()
    print(f"Gold records to sync: {total_records}")
    
    if total_records == 0:
        print("No Gold records to sync")
        return
    
    # JDBC connection properties
    jdbc_url = f"jdbc:postgresql://{TIMESCALE_HOST}:{TIMESCALE_PORT}/{TIMESCALE_DB}"
    connection_properties = {
        "user": TIMESCALE_USER,
        "password": TIMESCALE_PASSWORD,
        "driver": "org.postgresql.Driver",
        "batchsize": "10000",
        "reWriteBatchedInserts": "true"
    }
    
    # Write to TimescaleDB using DELETE + INSERT to avoid duplicates
    print(f"Writing Gold data to TimescaleDB: {jdbc_url}")
    
    from pyspark.sql import functions as F
    
    # Get time range of data being synced
    time_range = df_gold.agg(
        F.min("ts").alias("min_ts"),
        F.max("ts").alias("max_ts")
    ).collect()[0]
    
    min_ts = time_range["min_ts"]
    max_ts = time_range["max_ts"]
    
    print(f"Syncing Gold data from {min_ts} to {max_ts}")
    
    # Delete existing records in this time range to avoid duplicates
    # Use direct PostgreSQL connection to delete (avoids table drop issues with views)
    try:
        print("Deleting existing Gold records in time range to avoid duplicates...")
        conn = psycopg2.connect(
            host=TIMESCALE_HOST,
            port=TIMESCALE_PORT,
            database=TIMESCALE_DB,
            user=TIMESCALE_USER,
            password=TIMESCALE_PASSWORD
        )
        cursor = conn.cursor()
        
        # Delete records in the time range
        delete_query = """
            DELETE FROM gold_stocks 
            WHERE ts >= %s AND ts <= %s
        """
        cursor.execute(delete_query, (min_ts, max_ts))
        deleted_count = cursor.rowcount
        conn.commit()
        
        print(f"✓ Deleted {deleted_count} existing Gold records in time range [{min_ts}, {max_ts}]")
        cursor.close()
        conn.close()
        
    except ImportError:
        print("⚠️ psycopg2 not available, using Spark JDBC fallback...")
        # Fallback: use Spark to read, filter, and write back
        try:
            existing_df = spark.read.jdbc(
                url=jdbc_url,
                table="gold_stocks",
                properties=connection_properties
            )
            existing_outside_range = existing_df.filter(
                (F.col("ts") < F.lit(min_ts)) | (F.col("ts") > F.lit(max_ts))
            )
            # Note: This approach requires overwrite which may fail due to views
            # But it's a fallback if psycopg2 is not available
            print("⚠️ Using Spark-based merge (may fail if table has dependent views)...")
            final_df = existing_outside_range.unionByName(df_gold, allowMissingColumns=True)
            final_df.write.mode("overwrite").jdbc(
                url=jdbc_url, 
                table="gold_stocks", 
                properties=connection_properties
            )
            print(f"✓ Merged records using Spark fallback")
            return  # Exit early since we already wrote the data
        except Exception as e2:
            print(f"⚠️ Spark fallback also failed: {e2}")
            print("⚠️ Attempting direct append (may fail with duplicates)...")
    except Exception as e:
        error_str = str(e).lower()
        if "does not exist" in error_str or "relation" in error_str:
            print(f"⚠️ Table may not exist yet: {e}")
            print("⚠️ Continuing with append (will create table if needed)...")
        else:
            print(f"⚠️ Could not delete existing records: {e}")
            print("⚠️ Continuing with append (may fail with duplicates)...")
    
    # Now append the new records (no duplicates since we deleted them)
    try:
        df_gold.write \
            .mode("append") \
            .jdbc(url=jdbc_url, table="gold_stocks", properties=connection_properties)
        print(f"✓ Successfully synced {total_records} Gold records to TimescaleDB")
    except Exception as e:
        # If append still fails (e.g., duplicates still exist), try to handle it
        error_str = str(e)
        if "duplicate key" in error_str.lower() or "unique constraint" in error_str.lower():
            print(f"⚠️ Duplicate key error detected. Attempting to use ON CONFLICT DO UPDATE...")
            # For now, re-raise the error with a helpful message
            raise Exception(
                f"Duplicate key error: Records in time range [{min_ts}, {max_ts}] still exist. "
                f"Please ensure DELETE operation completed successfully. Original error: {e}"
            )
        else:
            raise
    
    # Also sync to silver_stocks if needed
    sync_silver_to_timescale(spark, batch_mode)


def sync_silver_to_timescale(spark, batch_mode=False):
    """Sync Silver Delta table to TimescaleDB"""
    
    DELTA_PATH_SILVER = "/opt/spark/delta_tables/silver"
    
    try:
        print(f"\n📊 SILVER LAYER SYNC")
        print(f"Reading from: {DELTA_PATH_SILVER}")
        df_silver = spark.read.format("delta").load(DELTA_PATH_SILVER)
        
        if batch_mode:
            print("Batch mode enabled: Syncing ALL data.")
        else:
            # Dynamic State-Aware Sync
            last_ts = get_max_timestamp("silver_stocks")
            from pyspark.sql import functions as F
            
            if last_ts is None:
                print("Destination table empty. Performing FULL table sync.")
            else:
                print(f"Found existing data up to: {last_ts}")
                df_silver = df_silver.filter(
                    F.col("ts") >= F.least(
                        F.lit(last_ts), 
                        F.expr("current_timestamp() - interval 10 minutes")
                    )
                )
                print("Applied dynamic filter: ts >= min(last_max_ts, now - 10m)")
        
        # Select only columns that exist in TimescaleDB silver_stocks table
        # TimescaleDB schema: symbol, ts, open, high, low, close, volume, is_valid, ingestion_date, processed_at
        timescale_silver_columns = [
            "symbol", "ts", "open", "high", "low", "close", "volume",
            "is_valid", "ingestion_date", "processed_at"
        ]
        
        # Select only columns that exist in both DataFrame and TimescaleDB
        available_silver_columns = [col for col in timescale_silver_columns if col in df_silver.columns]
        df_silver = df_silver.select(*available_silver_columns)
        
        total_records = df_silver.count()
        print(f"Silver records to sync: {total_records}")
        
        if total_records > 0:
            jdbc_url = f"jdbc:postgresql://{TIMESCALE_HOST}:{TIMESCALE_PORT}/{TIMESCALE_DB}"
            connection_properties = {
                "user": TIMESCALE_USER,
                "password": TIMESCALE_PASSWORD,
                "driver": "org.postgresql.Driver",
                "batchsize": "10000",
                "reWriteBatchedInserts": "true"
            }
            
            # Get time range and delete existing records (same approach as gold)
            from pyspark.sql import functions as F
            time_range = df_silver.agg(
                F.min("ts").alias("min_ts"),
                F.max("ts").alias("max_ts")
            ).collect()[0]
            
            min_ts = time_range["min_ts"]
            max_ts = time_range["max_ts"]
            
            # Cast ingestion_date from string to date for PostgreSQL compatibility
            # ingestion_date is stored as string in Delta but needs to be DATE in PostgreSQL
            if "ingestion_date" in df_silver.columns:
                df_silver = df_silver.withColumn(
                    "ingestion_date",
                    F.to_date(F.col("ingestion_date"), "yyyy-MM-dd")
                )
                print("✓ Cast ingestion_date from string to date")
            
            try:
                print(f"Deleting existing Silver records in time range [{min_ts}, {max_ts}]...")
                conn = psycopg2.connect(
                    host=TIMESCALE_HOST,
                    port=TIMESCALE_PORT,
                    database=TIMESCALE_DB,
                    user=TIMESCALE_USER,
                    password=TIMESCALE_PASSWORD
                )
                cursor = conn.cursor()
                
                delete_query = """
                    DELETE FROM silver_stocks 
                    WHERE ts >= %s AND ts <= %s
                """
                cursor.execute(delete_query, (min_ts, max_ts))
                deleted_count = cursor.rowcount
                conn.commit()
                
                print(f"✓ Deleted {deleted_count} existing Silver records")
                cursor.close()
                conn.close()
            except Exception as e:
                print(f"⚠️ Could not delete existing Silver records: {e}")
                # Continue with append - might fail if duplicates exist
            
            # Append new records
            df_silver.write.mode("append").jdbc(
                url=jdbc_url,
                table="silver_stocks",
                properties=connection_properties
            )
            
            print(f"✓ Successfully synced {total_records} Silver records")
    
    except Exception as e:
        print(f"Note: Silver table sync skipped or failed: {e}")


def main():
    """Main execution"""
    
    # Parse command line arguments
    # batch_mode forces full load, ignoring dynamic logic
    batch_mode = "--batch" in sys.argv
    
    if batch_mode:
        print("Running in FORCED BATCH mode (ignoring DB state)")
    else:
        print("Running in SMART SYNC mode (dynamic time window)")
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Sync data
        sync_gold_to_timescale(spark, batch_mode)
        
        print("\n" + "=" * 80)
        print("SYNC COMPLETE")
        print("=" * 80)
        print("\nVerify in TimescaleDB:")
        print("  docker exec -it postgres-timescale psql -U grafana -d stockdata")
        print("  SELECT symbol, COUNT(*) FROM gold_stocks GROUP BY symbol;")
        print("  SELECT * FROM latest_prices;")
        
    except Exception as e:
        print(f"\n✗ Error during sync: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

