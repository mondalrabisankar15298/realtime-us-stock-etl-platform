#!/usr/bin/env python3
"""
Sync Delta Lake Gold Table to TimescaleDB
Reads from Gold Delta table and writes to TimescaleDB for Grafana dashboards
Run this as a Spark job or Airflow DAG
"""

import sys
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import psycopg2

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
    print(f"Reading from: {DELTA_PATH_GOLD}")
    df_gold = spark.read.format("delta").load(DELTA_PATH_GOLD)
    
    if not batch_mode:
        # Only sync data from last 24 hours for incremental updates
        from pyspark.sql import functions as F
        df_gold = df_gold.filter(
            F.col("ts") >= F.expr("current_timestamp() - interval 24 hours")
        )
    
    total_records = df_gold.count()
    print(f"Records to sync: {total_records}")
    
    if total_records == 0:
        print("No records to sync")
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
    
    # Write to TimescaleDB with UPSERT logic for batch mode
    print(f"Writing to TimescaleDB: {jdbc_url}")

    if batch_mode:
        print("⚠️  BATCH MODE: This will UPSERT all historical data")
        print("   Note: TimescaleDB doesn't support direct UPSERT via JDBC")
        print("   Using APPEND mode - you may need to TRUNCATE table first for full sync")

        # For batch mode, truncate and reload (since JDBC doesn't support UPSERT easily)
        try:
            # Create a temporary connection to truncate
            import psycopg2
            conn = psycopg2.connect(
                host=TIMESCALE_HOST,
                port=TIMESCALE_PORT,
                database=TIMESCALE_DB,
                user=TIMESCALE_USER,
                password=TIMESCALE_PASSWORD
            )
            cursor = conn.cursor()

            # Truncate table for full reload
            cursor.execute("TRUNCATE TABLE gold_stocks;")
            conn.commit()

            print("✓ Truncated gold_stocks table for full reload")
            cursor.close()
            conn.close()

        except Exception as e:
            print(f"✗ Could not truncate table: {e}")
            print("   Batch mode requires truncation. Aborting sync.")
            return

    # Calculate time range for the current batch
    from pyspark.sql import functions as F
    time_stats = df_gold.agg(F.min("ts").alias("min_ts"), F.max("ts").alias("max_ts")).collect()[0]
    min_ts = time_stats["min_ts"]
    max_ts = time_stats["max_ts"]
    
    print(f"Syncing Gold data from {min_ts} to {max_ts}")
    
    # "Smart Sync": Delete existing records in this time range to avoid duplicates
    try:
        import psycopg2
        conn = psycopg2.connect(
            host=TIMESCALE_HOST,
            port=TIMESCALE_PORT,
            database=TIMESCALE_DB,
            user=TIMESCALE_USER,
            password=TIMESCALE_PASSWORD
        )
        cursor = conn.cursor()
        
        print(f"Deleting existing Gold records in time range to avoid duplicates...")
        delete_query = "DELETE FROM gold_stocks WHERE ts >= %s AND ts <= %s"
        cursor.execute(delete_query, (min_ts, max_ts))
        deleted_count = cursor.rowcount
        conn.commit()
        
        print(f"✓ Deleted {deleted_count} existing Gold records in time range [{min_ts}, {max_ts}]")
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"⚠ Warning: Could not delete existing records: {e}")
        print("Proceeding with append... (might fail with duplicates)")

    # Write data
    df_gold.write \
        .mode("append") \
        .jdbc(url=jdbc_url, table="gold_stocks", properties=connection_properties)
    
    print(f"✓ Successfully synced {total_records} records to TimescaleDB")
    
    # Also sync to silver_stocks if needed
    # sync_silver_to_timescale(spark, batch_mode) # DISABLED: Handled by dedicated streaming job spark-timescale-sync


def sync_silver_to_timescale(spark, batch_mode=False):
    """Sync Silver Delta table to TimescaleDB"""
    
    DELTA_PATH_SILVER = "/opt/spark/delta_tables/silver"
    
    try:
        print(f"\nReading from: {DELTA_PATH_SILVER}")
        df_silver = spark.read.format("delta").load(DELTA_PATH_SILVER)
        
        if not batch_mode:
            from pyspark.sql import functions as F
            df_silver = df_silver.filter(
                F.col("ts") >= F.expr("current_timestamp() - interval 24 hours")
            )
        
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
            
            df_silver.write \
                .mode("append") \
                .jdbc(url=jdbc_url, table="silver_stocks", properties=connection_properties)
            
            print(f"✓ Successfully synced {total_records} Silver records")
    
    except Exception as e:
        print(f"Note: Silver table sync skipped or failed: {e}")


def main():
    """Main execution"""
    
    # Parse command line arguments
    batch_mode = "--batch" in sys.argv
    
    if batch_mode:
        print("Running in BATCH mode (syncing all historical data)")
    else:
        print("Running in INCREMENTAL mode (syncing last 24 hours)")
    
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

