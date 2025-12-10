import asyncio
import os
import polars as pl
from deltalake import DeltaTable, write_deltalake
import psycopg2
import logging

logger = logging.getLogger("GoldLayer")

DELTA_PATH_SILVER = "/opt/spark/delta_tables/silver"
DELTA_PATH_GOLD = "/opt/spark/delta_tables/gold"

# TimescaleDB Config
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

def _write_gold_to_timescale(df: pl.DataFrame):
    """Write Gold KPIs to TimescaleDB gold_stocks table"""
    try:
        conn = get_pg_connection()
        cursor = conn.cursor()
        
        # Prepare data matching gold_stocks schema
        data = df.select([
            pl.col("ts"),
            pl.col("symbol"),
            pl.col("close"),
            pl.col("volume"),
            pl.col("sma_5"),
            pl.col("sma_20"),
            pl.col("sma_50"),
            pl.col("ema_9"),
            pl.col("ema_21"),
            pl.col("rsi_14"),
            pl.col("market_phase"),
            pl.col("daily_return"),
            pl.col("volatility_5m")
        ]).rows()
        
        # Upsert to gold_stocks
        insert_query = """
        INSERT INTO gold_stocks (
            ts, symbol, close, volume,
            sma_5, sma_20, sma_50,
            ema_9, ema_21, rsi_14,
            market_phase, daily_return, volatility_5m
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (ts, symbol)
        DO UPDATE SET
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            sma_5 = EXCLUDED.sma_5,
            sma_20 = EXCLUDED.sma_20,
            sma_50 = EXCLUDED.sma_50,
            ema_9 = EXCLUDED.ema_9,
            ema_21 = EXCLUDED.ema_21,
            rsi_14 = EXCLUDED.rsi_14,
            market_phase = EXCLUDED.market_phase,
            daily_return = EXCLUDED.daily_return,
            volatility_5m = EXCLUDED.volatility_5m;
        """
        
        cursor.executemany(insert_query, data)
        conn.commit()
        logger.info(f"Synced {len(data)} rows to TimescaleDB gold_stocks table")
        
        cursor.close()
        conn.close()
    except Exception as e:
        logger.error(f"Gold TimescaleDB sync failed: {e}")

async def run_gold():
    logger.info("Starting Gold Layer processing...")
    await asyncio.sleep(20) # Wait for Silver
    
    from .checkpoint import read_checkpoint, write_checkpoint
    
    last_processed_version = -1
    
    while True:
        try:
            if not os.path.exists(DELTA_PATH_SILVER):
                await asyncio.sleep(5)
                continue
            
            # Check Silver version
            dt_silver = DeltaTable(DELTA_PATH_SILVER)
            current_version = dt_silver.version()
            
            if current_version > last_processed_version:
                logger.info(f"New Silver version detected: {current_version} (last processed: {last_processed_version})")
                
                # Read checkpoint to get last synced timestamp
                last_gold_checkpoint_ts = read_checkpoint('gold')
                
                # Read FULL Silver table for indicator calculations
                # (We need historical data for rolling windows/EMAs)
                df_silver = pl.from_arrow(dt_silver.to_pyarrow_table())
                
                if df_silver.height == 0:
                    await asyncio.sleep(1)
                    continue
                
                # Log Silver Delta table size
                logger.info(f"Silver Delta table contains {df_silver.height} total records")
                    
                # Sort for window functions
                df_silver = df_silver.sort(["symbol", "ts"])
                
                # Calculate Indicators on FULL dataset
                df_gold = df_silver.with_columns([
                    # SMAs
                    pl.col("close").rolling_mean(5).over("symbol").alias("sma_5"),
                    pl.col("close").rolling_mean(20).over("symbol").alias("sma_20"),
                    pl.col("close").rolling_mean(50).over("symbol").alias("sma_50"),
                    
                    # EMAs
                    pl.col("close").ewm_mean(span=9, adjust=False).over("symbol").alias("ema_9"),
                    pl.col("close").ewm_mean(span=21, adjust=False).over("symbol").alias("ema_21"),
                    
                    # Daily Return
                    pl.col("close").pct_change().over("symbol").alias("daily_return"),
                    
                    # Volatility (Rolling StdDev)
                    pl.col("close").pct_change().rolling_std(5).over("symbol").alias("volatility_5m")
                ])
                
                # RSI Calculation
                def calculate_rsi(series, period=14):
                    delta = series.diff()
                    up = delta.clip(lower_bound=0)
                    down = -1 * delta.clip(upper_bound=0)
                    ema_up = up.ewm_mean(span=period, adjust=False)
                    ema_down = down.ewm_mean(span=period, adjust=False)
                    rs = ema_up / ema_down
                    return 100 - (100 / (1 + rs))

                df_gold = df_gold.with_columns(
                     calculate_rsi(pl.col("close")).over("symbol").alias("rsi_14")
                )
                
                # Market Phase
                df_gold = df_gold.with_columns(
                    pl.when((pl.col("close") > pl.col("ema_9")) & (pl.col("ema_9") > pl.col("ema_21")))
                    .then(pl.lit("bullish"))
                    .when((pl.col("close") < pl.col("ema_9")) & (pl.col("ema_9") < pl.col("ema_21")))
                    .then(pl.lit("bearish"))
                    .otherwise(pl.lit("consolidation"))
                    .alias("market_phase")
                )
                
                # Write to Gold Delta (overwrite because we recalculate all indicators)
                # Note: We need full history for rolling calculations, so overwrite is appropriate here
                write_deltalake(
                    DELTA_PATH_GOLD,
                    df_gold.to_arrow(),
                    mode="overwrite",
                    schema_mode="overwrite"
                )
                
                logger.info(f"Updated Gold layer: {df_gold.height} records (full recalc for indicators)")
                
                # OPTIMIZED: Only sync NEW records to TimescaleDB based on checkpoint
                if last_gold_checkpoint_ts > 0:
                    # Filter for NEW records where ts (as epoch) > checkpoint
                    # Convert datetime column to epoch seconds for comparison
                    df_new_gold = df_gold.filter(
                        (pl.col("ts").dt.epoch(time_unit="s")) > last_gold_checkpoint_ts
                    )
                    
                    if df_new_gold.height > 0:
                        logger.info(f"Syncing {df_new_gold.height} NEW rows to TimescaleDB gold_stocks (checkpoint-based)")
                        _write_gold_to_timescale(df_new_gold)
                        
                        # Update checkpoint with max ts
                        max_ts_epoch = df_new_gold.select(pl.col("ts").dt.epoch(time_unit="s").max()).item()
                        write_checkpoint('gold', max_ts_epoch)
                        logger.info(f"Updated Gold checkpoint: max_ts={max_ts_epoch}")
                    else:
                        logger.info("No new Gold records to sync to TimescaleDB")
                else:
                    # First run - sync all data
                    logger.info(f"Syncing {df_gold.height} rows to TimescaleDB gold_stocks (initial load)")
                    _write_gold_to_timescale(df_gold)
                    
                    # Create checkpoint with max ts as epoch
                    max_ts_epoch = df_gold.select(pl.col("ts").dt.epoch(time_unit="s").max()).item()
                    write_checkpoint('gold', max_ts_epoch)
                    logger.info(f"Created Gold checkpoint: max_ts={max_ts_epoch}")
                
                last_processed_version = current_version
            
            # Sleep between checks
            await asyncio.sleep(5)
            
        except Exception as e:
            logger.error(f"Gold loop error: {e}")
            await asyncio.sleep(5)
