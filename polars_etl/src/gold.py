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
        # Schema: ts, symbol, close, volume, sma_5, sma_20, sma_50, ema_9, ema_21, 
        #         rsi_14, upper_band, lower_band, vwap, market_phase, trading_signal, 
        #         daily_return, volatility_5m, macd, macd_signal, macd_histogram
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
            pl.col("upper_band"),
            pl.col("lower_band"),
            pl.col("vwap"),
            pl.col("market_phase"),
            pl.col("trading_signal"),
            pl.col("daily_return"),
            pl.col("volatility_5m"),
            pl.col("macd"),
            pl.col("macd_signal"),
            pl.col("macd"),
            pl.col("macd_signal"),
            pl.col("macd_histogram"),
            pl.col("atr_14")
        ]).rows()
        
        # Upsert to gold_stocks
        insert_query = """
        INSERT INTO gold_stocks (
            ts, symbol, close, volume,
            sma_5, sma_20, sma_50,
            ema_9, ema_21, rsi_14,
            upper_band, lower_band, vwap,
            market_phase, trading_signal,
            daily_return, volatility_5m,
            macd, macd_signal, macd_histogram,
            atr_14
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
            upper_band = EXCLUDED.upper_band,
            lower_band = EXCLUDED.lower_band,
            vwap = EXCLUDED.vwap,
            market_phase = EXCLUDED.market_phase,
            trading_signal = EXCLUDED.trading_signal,
            daily_return = EXCLUDED.daily_return,
            volatility_5m = EXCLUDED.volatility_5m,
            macd = EXCLUDED.macd,
            macd_signal = EXCLUDED.macd_signal,
            macd_histogram = EXCLUDED.macd_histogram,
            atr_14 = EXCLUDED.atr_14;
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
                    pl.col("close").pct_change().rolling_std(5).over("symbol").alias("volatility_5m"),

                    # MACD Calculation
                    pl.col("close").ewm_mean(span=12, adjust=False).over("symbol").alias("ema_12"),
                    pl.col("close").ewm_mean(span=26, adjust=False).over("symbol").alias("ema_26")
                ])
                
                # VWAP Calculation (Rolling 20 periods for streaming approx or CumSum if possible)
                # Using CumSum relative to the loaded batch (as we load full history for now)
                # Or a simple rolling VWAP
                df_gold = df_gold.with_columns(
                     (pl.col("close") * pl.col("volume")).alias("pv")
                )
                df_gold = df_gold.with_columns(
                     (pl.col("pv").rolling_sum(20).over("symbol") / pl.col("volume").rolling_sum(20).over("symbol")).alias("vwap")
                )

                # Bollinger Bands
                df_gold = df_gold.with_columns(
                    pl.col("close").rolling_std(20).over("symbol").alias("std_20")
                )
                
                df_gold = df_gold.with_columns([
                     (pl.col("sma_20") + (2 * pl.col("std_20"))).alias("upper_band"),
                     (pl.col("sma_20") - (2 * pl.col("std_20"))).alias("lower_band")
                ])
                
                # MACD Line, Signal Line, and Histogram
                df_gold = df_gold.with_columns(
                    (pl.col("ema_12") - pl.col("ema_26")).alias("macd")
                )

                df_gold = df_gold.with_columns(
                    pl.col("macd").ewm_mean(span=9, adjust=False).over("symbol").alias("macd_signal")
                )

                df_gold = df_gold.with_columns(
                    (pl.col("macd") - pl.col("macd_signal")).alias("macd_histogram")
                )
                
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
                
                # ATR Calculation
                # TR = Max(High-Low, Abs(High-Close_prev), Abs(Low-Close_prev))
                df_gold = df_gold.with_columns(
                     pl.col("close").shift(1).over("symbol").alias("prev_close")
                )
                df_gold = df_gold.with_columns(
                     pl.max_horizontal([
                         (pl.col("high") - pl.col("low")),
                         (pl.col("high") - pl.col("prev_close")).abs(),
                         (pl.col("low") - pl.col("prev_close")).abs()
                     ]).alias("tr")
                )
                df_gold = df_gold.with_columns(
                     pl.col("tr").rolling_mean(14).over("symbol").alias("atr_14")
                )
                # Fill null ATR with 0 or fill_null strategy
                df_gold = df_gold.with_columns(
                     pl.col("atr_14").fill_null(0)
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
                
                # Trading Signals
                # BUY: RSI < 30 AND MACD crossing UP (historgram becoming positive or just > signal)
                # SELL: RSI > 70 AND MACD crossing DOWN
                # NEUTRAL: Otherwise
                
                # Simple logic for now: State based
                df_gold = df_gold.with_columns(
                    pl.when((pl.col("rsi_14") < 35) & (pl.col("macd_histogram") > 0))
                    .then(pl.lit("BUY"))
                    .when((pl.col("rsi_14") > 65) & (pl.col("macd_histogram") < 0))
                    .then(pl.lit("SELL"))
                    .otherwise(pl.lit("NEUTRAL"))
                    .alias("trading_signal")
                )
                
                # Fill nulls for new columns to avoid DB errors if any
                # Use fill_nan THEN fill_null to catch both cases
                df_gold = df_gold.with_columns([
                    pl.col("vwap").fill_nan(pl.col("close")).fill_null(pl.col("close")),
                    pl.col("upper_band").fill_nan(pl.col("close")).fill_null(pl.col("close")),
                    pl.col("lower_band").fill_nan(pl.col("close")).fill_null(pl.col("close"))
                ])

                # Write to Gold Delta (overwrite because we recalculate all indicators)
                write_deltalake(
                    DELTA_PATH_GOLD,
                    df_gold.to_arrow(),
                    mode="overwrite",
                    schema_mode="overwrite"
                )
                
                logger.info(f"Updated Gold layer: {df_gold.height} records (full recalc for indicators)")
                
                # FORCE FULL SYNC: Bypass checkpoint for now to backfill everything since we dropped table
                if True:
                    df_new_gold = df_gold
                    if df_new_gold.height > 0:
                        logger.info(f"Syncing {df_new_gold.height} NEW rows to TimescaleDB gold_stocks")
                        _write_gold_to_timescale(df_new_gold)
                        
                        # Update checkpoint
                        max_ts_epoch = df_new_gold.select(pl.col("ts").dt.epoch(time_unit="s").max()).item()
                        write_checkpoint('gold', max_ts_epoch)
                        logger.info(f"Updated Gold checkpoint: max_ts={max_ts_epoch}")
                    else:
                        logger.info("No new Gold records to sync to TimescaleDB")
                
                last_processed_version = current_version
            
            # Sleep between checks
            await asyncio.sleep(5)
            
        except Exception as e:
            logger.error(f"Gold loop error: {e}")
            await asyncio.sleep(5)
