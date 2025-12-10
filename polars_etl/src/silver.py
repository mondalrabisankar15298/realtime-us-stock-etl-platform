import asyncio
import os
import time
import polars as pl
from deltalake import DeltaTable, write_deltalake
import psycopg2
import logging

logger = logging.getLogger("SilverLayer")

DELTA_PATH_BRONZE = "/opt/spark/delta_tables/bronze"
DELTA_PATH_SILVER = "/opt/spark/delta_tables/silver"

# TimescaleDB Config (shared with existing setup)
PG_HOST = "postgres-timescale"
PG_DB = os.getenv("POSTGRES_GRAFANA_DB", "stockdata")
PG_USER = os.getenv("POSTGRES_GRAFANA_USER", "grafana")
PG_PASS = os.getenv("POSTGRES_GRAFANA_PASSWORD", "grafana")

def get_pg_connection():
    return psycopg2.connect(
        host=PG_HOST,
        database=PG_DB,
        user=PG_USER,
        password=PG_PASS
    )

async def run_silver():
    logger.info("Starting Silver Layer processing...")
    await asyncio.sleep(15) # Wait for Bronze to start
    
    from .checkpoint import read_checkpoint, write_checkpoint
    
    last_processed_version = -1
    
    while True:
        try:
            # Check if Bronze table exists
            if not os.path.exists(DELTA_PATH_BRONZE):
                logger.warning("Bronze table not found yet, waiting...")
                await asyncio.sleep(5)
                continue
                
            dt_bronze = DeltaTable(DELTA_PATH_BRONZE)
            current_version = dt_bronze.version()
            
            if current_version > last_processed_version:
                logger.info(f"New Bronze version detected: {current_version} (last processed: {last_processed_version})")
                
                # Read checkpoint to get last processed timestamp
                last_checkpoint_ts = read_checkpoint('silver')
                
                # INCREMENTAL PROCESSING: Filter by ingestion_timestamp
                # Read Bronze and filter for NEW data only
                df_bronze = pl.scan_delta(DELTA_PATH_BRONZE)
                
                # Filter for records newer than checkpoint
                if last_checkpoint_ts > 0:
                    df_bronze = df_bronze.filter(pl.col("ingestion_timestamp") > last_checkpoint_ts)
                    logger.info(f"Filtering Bronze data where ingestion_timestamp > {last_checkpoint_ts}")
                else:
                    logger.info("No checkpoint found - processing all Bronze data")
                
                # Basic Cleaning
                # Bronze has: symbol, timestamp (Unix epoch in seconds), open, high, low, close, volume, ingestion_timestamp
                # We need to convert timestamp from Unix epoch to datetime and select OHLC columns
                df_silver = (
                    df_bronze
                    .with_columns([
                        # Convert Unix epoch (seconds) to datetime
                        pl.from_epoch(pl.col("timestamp"), time_unit="s").alias("ts"),
                    ])
                    .unique(subset=["symbol", "ts"]) # Dedupe
                    .with_columns([
                        pl.col("open").cast(pl.Float64),
                        pl.col("high").cast(pl.Float64),
                        pl.col("low").cast(pl.Float64),
                        pl.col("close").cast(pl.Float64),
                        pl.col("volume").cast(pl.Int64),
                        pl.lit(True).alias("is_valid"),
                        pl.col("ingestion_timestamp")  # Keep for checkpoint tracking
                    ])
                    # Select final columns for silver (matching TimescaleDB schema)
                    .select(["symbol", "ts", "open", "high", "low", "close", "volume", "is_valid", "ingestion_timestamp"])
                )
                
                # Execute
                df_clean = df_silver.collect()
                
                if df_clean.height > 0:
                    # Get max ingestion_timestamp for checkpoint
                    max_ingestion_ts = df_clean["ingestion_timestamp"].max()
                    
                    # Remove ingestion_timestamp before writing to Delta (not part of schema)
                    df_to_write = df_clean.select(["symbol", "ts", "open", "high", "low", "close", "volume", "is_valid"])
                    
                    # Append to Silver Delta (incremental)
                    # TimescaleDB upserts handle duplicates, so we can safely append
                    try:
                        write_deltalake(
                            DELTA_PATH_SILVER,
                            df_to_write.to_arrow(),
                            mode="append",  # Append only NEW data
                            schema_mode="merge"
                        )
                        logger.info(f"Appended {df_to_write.height} NEW rows to Silver Delta (checkpoint-based incremental)")
                        
                        # Write to TimescaleDB (Sync)
                        # Upserts handle duplicates automatically
                        _write_to_timescale(df_to_write)
                        
                        # Update checkpoint with max ingestion_timestamp
                        write_checkpoint('silver', max_ingestion_ts)
                        logger.info(f"Updated Silver checkpoint: max_ingestion_timestamp={max_ingestion_ts}")
                        
                    except Exception as e:
                        logger.error(f"Error writing Silver: {e}")
                else:
                    logger.info("No new data to process in this cycle")
                
                last_processed_version = current_version
            
            else:
                # No new version, sleep
                pass
                
            await asyncio.sleep(1) # Check every second
            
        except Exception as e:
            logger.error(f"Silver loop error: {e}")
            await asyncio.sleep(5)

def _write_to_timescale(df: pl.DataFrame):
    # Prepare data for insertion to silver_stocks table
    # Ensure columns match DB schema
    try:
        conn = get_pg_connection()
        cursor = conn.cursor()
        
        # Map to silver_stocks schema: (ts, symbol, open, high, low, close, volume)
        data = df.select([
            pl.col("ts"),
            pl.col("symbol"),
            pl.col("open"),
            pl.col("high"),
            pl.col("low"),
            pl.col("close"),
            pl.col("volume")
        ]).rows()
        
        # Timescale Upsert to silver_stocks
        insert_query = """
        INSERT INTO silver_stocks (ts, symbol, open, high, low, close, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (ts, symbol) 
        DO UPDATE SET 
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume;
        """
        
        cursor.executemany(insert_query, data)
        conn.commit()
        logger.info(f"Synced {len(data)} rows to TimescaleDB silver_stocks table")
        
        cursor.close()
        conn.close()
    except Exception as e:
        logger.error(f"TimescaleDB sync failed: {e}")
