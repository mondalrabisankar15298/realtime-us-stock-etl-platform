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
    Sync Gold Delta table to TimescaleDB using Staging Table + UPSERT
    """
    
    print("=" * 80)
    print("SYNCING DELTA LAKE → TIMESCALEDB (Staging Upsert Mode)")
    print("=" * 80)
    
    # Read from Gold Delta table
    print(f"\n📊 GOLD LAYER SYNC")
    print(f"Reading from: {DELTA_PATH_GOLD}")
    df_gold = spark.read.format("delta").load(DELTA_PATH_GOLD)
    
    if batch_mode:
        print("Batch mode enabled: Syncing ALL data.")
    else:
        # Dynamic State-Aware Sync
        last_ts = get_max_timestamp("gold_stocks")
        from pyspark.sql import functions as F
        
        if last_ts is None:
            print("Destination table empty or not found. Performing FULL table sync.")
        else:
            print(f"Found existing data up to: {last_ts}")
            # Filter for new or updated data (with 10-minute overlap)
            df_gold = df_gold.filter(
                F.col("ts") >= F.least(
                    F.lit(last_ts), 
                    F.expr("current_timestamp() - interval 10 minutes")
                )
            )
            print("Applied dynamic filter: ts >= min(last_max_ts, now - 10m)")
    
    # Define columns
    timescale_columns = [
        "symbol", "ts", "close", "volume",
        "sma_5", "sma_20", "sma_50", "ema_9", "ema_21",
        "rsi_14", "vwap", "macd", "macd_signal", "macd_histogram", "atr_14",
        "daily_return", "volatility_5m", "market_phase", "price_change_pct", "computed_at"
    ]
    
    # Filter columns
    available_columns = [col for col in timescale_columns if col in df_gold.columns]
    df_gold = df_gold.select(*available_columns)
    
    total_records = df_gold.count()
    print(f"Gold records to sync: {total_records}")
    
    if total_records == 0:
        print("No Gold records to sync")
        return
    
    # JDBC Config
    jdbc_url = f"jdbc:postgresql://{TIMESCALE_HOST}:{TIMESCALE_PORT}/{TIMESCALE_DB}"
    connection_properties = {
        "user": TIMESCALE_USER,
        "password": TIMESCALE_PASSWORD,
        "driver": "org.postgresql.Driver",
        "batchsize": "10000",
        "reWriteBatchedInserts": "true"
    }
    
    staging_table = "gold_stocks_staging"
    
    # Deduplicate data based on unique keys (symbol, ts) to prevent cardinality violations during UPSERT
    # This keeps the first occurrence and drops duplicates within the batch
    print("Deduplicating records by (symbol, ts)...")
    df_gold = df_gold.dropDuplicates(['symbol', 'ts'])
    
    # Recount after deduplication
    dedup_count = df_gold.count()
    if dedup_count < total_records:
        print(f"⚠ Found and dropped {total_records - dedup_count} duplicate records. New count: {dedup_count}")
    
    print(f"Writing {dedup_count} records to staging table: {staging_table}")
    
    # 1. Write to Staging Table (Overwrite)
    try:
        df_gold.write \
            .mode("overwrite") \
            .jdbc(url=jdbc_url, table=staging_table, properties=connection_properties)
        print(f"✓ Staging table loaded")
    except Exception as e:
        print(f"✗ Failed to write to staging table: {e}")
        raise e

    # 2. Perform UPSERT from Staging to Target
    print("Performing UPSERT from Staging to Target...")
    try:
        conn = psycopg2.connect(
            host=TIMESCALE_HOST,
            port=TIMESCALE_PORT,
            database=TIMESCALE_DB,
            user=TIMESCALE_USER,
            password=TIMESCALE_PASSWORD
        )
        cursor = conn.cursor()
        
        # Build UPSERT Query dynamically based on available columns
        # DO UPDATE SET col = EXCLUDED.col
        update_cols = [col for col in available_columns if col not in ['symbol', 'ts']]
        update_clause = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_cols])
        columns_str = ", ".join(available_columns)
        
        upsert_query = f"""
            INSERT INTO gold_stocks ({columns_str})
            SELECT {columns_str} FROM {staging_table}
            ON CONFLICT (symbol, ts) 
            DO UPDATE SET {update_clause};
        """
        
        cursor.execute(upsert_query)
        upsert_count = cursor.rowcount
        conn.commit()
        
        print(f"✓ Successfully UPSERTED {upsert_count} records into gold_stocks")
        
        # Cleanup
        cursor.execute(f"DROP TABLE IF EXISTS {staging_table}")
        conn.commit()
        print("✓ Staging table cleaned up")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"✗ Error performing UPSERT: {e}")
        raise e
    
    # Sync Silver (Optional: could implement similar logic if needed, keeping current for now)
    # User requested removal as silver_cleaning.py handles this streaming sync
    # sync_silver_to_timescale(spark, batch_mode)


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

