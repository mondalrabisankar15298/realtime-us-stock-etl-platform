#!/usr/bin/env python3
"""
Silver Layer Cleaning Job (minimal logs)

- Plain print() logging (no external deps)
- Suppresses verbose Spark INFO logs (sets Spark log level to WARN)
- Uses foreachBatch to perform dedupe + MERGE (idempotent upserts)
- Prints compact per-batch summary only when input rows exist
"""

import sys
import os
import traceback
import time
from pathlib import Path

# add parent dir for config import
sys.path.insert(0, str(Path(__file__).parent.parent))

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

from config import (
    create_spark_session,
    DELTA_PATH_BRONZE,
    DELTA_PATH_SILVER,
    CHECKPOINT_PATH_SILVER,
)

# Configuration for TimescaleDB
TIMESCALE_HOST = "postgres-timescale"
TIMESCALE_PORT = "5432"
TIMESCALE_DB = "stockdata"
TIMESCALE_USER = "grafana"
TIMESCALE_PASSWORD = "grafana"

# Optional: change these if you want periodic idle prints
IDLE_PRINT_EVERY = 0  # 0 = never print idle batches; set >0 to print heartbeat every N idle batches

def _ensure_paths():
    Path(DELTA_PATH_SILVER).parent.mkdir(parents=True, exist_ok=True)
    Path(CHECKPOINT_PATH_SILVER).parent.mkdir(parents=True, exist_ok=True)

def clean_and_validate(df):
    """
    Apply data quality transformations
    - Normalize available timestamp columns into 'ts' (TimestampType)
    - Validate price & volume
    - Add ingestion_date and processed_at
    """
    # lazily import Spark SQL types for lightweight checks
    from pyspark.sql.types import LongType, IntegerType, TimestampType, StringType

    cols = set(df.columns)

    # 1) If there's a 'ts_utc' column from new bronze layer -> use it directly
    if "ts_utc" in cols:
        # Bronze layer already created proper UTC timestamp
        df2 = df.withColumn("ts", F.col("ts_utc"))
    
    # 2) Else if there is an epoch-seconds 'timestamp' column -> convert it
    elif "timestamp" in cols:
        # Convert epoch seconds directly to timestamp
        # Using expression to ensure UTC interpretation regardless of server timezone
        df2 = df.withColumn("ts", (F.col("timestamp").cast("long").cast("timestamp")))

    # 3) Else if there's a 'ts' column already -> try to normalize it
    elif "ts" in cols:
        # is it numeric epoch or already timestamp?
        ts_field = next((f for f in df.schema.fields if f.name == "ts"), None)
        if ts_field is not None and isinstance(ts_field.dataType, (LongType, IntegerType)):
            # numeric epoch seconds -> convert using direct cast to ensure UTC
            df2 = df.withColumn("ts", (F.col("ts").cast("long").cast("timestamp")))
        else:
            # assume it's already a TimestampType or string; cast to timestamp to be safe
            if ts_field is not None and isinstance(ts_field.dataType, StringType):
                df2 = df.withColumn("ts", F.to_timestamp(F.col("ts")))
            else:
                df2 = df.withColumn("ts", F.col("ts"))

    # 4) Else try ingested_at (many pipelines include an ingestion timestamp there)
    elif "ingested_at" in cols:
        # ingested_at might be ISO string or epoch; try a best-effort conversion:
        # - if numeric: treat as epoch seconds
        bt_field = next((f for f in df.schema.fields if f.name == "ingested_at"), None)
        if bt_field is not None and isinstance(bt_field.dataType, (LongType, IntegerType)):
            df2 = df.withColumn("ts", (F.col("ingested_at").cast("long").cast("timestamp")))
        else:
            # try parsing ISO string to timestamp
            df2 = df.withColumn("ts", F.to_timestamp(F.col("ingested_at")))

    else:
        # helpful error: list available columns so you can see what the microBatch has
        raise ValueError(
            "No timestamp column found in micro-batch. Expected one of: "
            "'ts_utc', 'timestamp' (epoch sec), 'ts', or 'ingested_at'. "
            f"Available columns: {sorted(list(cols))}"
        )

    # ---------- rest of your original validations ----------
    df2 = df2.withColumn(
        "is_valid",
        (
            (F.col("open") > 0) &
            (F.col("high") > 0) &
            (F.col("low") > 0) &
            (F.col("close") > 0) &
            (F.col("high") >= F.col("low")) &
            (F.col("volume") >= 0)
        )
    )

    df2 = df2.withColumn("ingestion_date", F.date_format(F.col("ts"), "yyyy-MM-dd"))
    df2 = df2.withColumn("processed_at", F.current_timestamp())

    # Keep ingested_at (for dedup) in the micro-batch
    out = df2.select(
        "symbol",
        "ts",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "is_valid",
        "ingestion_date",
        "processed_at",
        "ingested_at"
    )
    return out


def _delta_table_exists(spark, path):
    try:
        return DeltaTable.isDeltaTable(spark, path)
    except Exception:
        # fallback to simple path check (useful on local/dev)
        return os.path.exists(path) and os.path.isdir(path)

def _merge_to_silver(spark, valid_df, batchId, valid_count):
    """
    Merge valid_df into existing Silver delta table. Assumes valid_df has required columns.
    """
    try:
        silver_table = DeltaTable.forPath(spark, DELTA_PATH_SILVER)
        # Use expr() to map source values explicitly
        update_map = {
            "open": "source.open",
            "high": "source.high",
            "low": "source.low",
            "close": "source.close",
            "volume": "source.volume",
            "is_valid": "source.is_valid",
            "processed_at": "source.processed_at",
            "ingestion_date": "source.ingestion_date"
        }
        (silver_table.alias("target")
            .merge(
                valid_df.alias("source"),
                "target.symbol = source.symbol AND target.ts = source.ts"
            )
            .whenMatchedUpdate(set={k: F.expr(v) for k, v in update_map.items()})
            .whenNotMatchedInsertAll()
            .execute()
        )
        print(f"[batch {batchId}] MERGE updated/inserted {valid_count} rows into Silver")
    except Exception as e:
        print(f"[batch {batchId}][error] MERGE failed: {e}")
        traceback.print_exc()
        traceback.print_exc()
        raise

def write_to_timescale(batch_df, batch_id):
    """
    Write micro-batch to TimescaleDB
    """
    try:
        # Optimistic count check
        count = batch_df.count()
        if count == 0:
            return

        jdbc_url = f"jdbc:postgresql://{TIMESCALE_HOST}:{TIMESCALE_PORT}/{TIMESCALE_DB}"

        connection_properties = {
            "user": TIMESCALE_USER,
            "password": TIMESCALE_PASSWORD,
            "driver": "org.postgresql.Driver",
            "batchsize": "5000",
            "reWriteBatchedInserts": "true"
        }

        # Select only necessary columns for DB
        # Ensure columns match what's expected in the DB schema
        db_df = batch_df.select(
            "ts",
            "symbol",
            "open",
            "high",
            "low",
            "close",
            "volume"
        )

        # Append to TimescaleDB
        db_df.write \
            .mode("append") \
            .jdbc(url=jdbc_url, table="silver_stocks", properties=connection_properties)
        
        print(f"[batch {batch_id}] Synced {count} rows to TimescaleDB")

    except Exception as e:
        print(f"[batch {batch_id}][warning] TimescaleDB sync failed: {e}")
        # trace but do not fail the main stream
        # traceback.print_exc()

def upsert_to_silver(microBatchDF, batchId):
    """
    foreachBatch function: dedupe, validate, and upsert to Silver Delta table.
    Prints compact summary only when there are input rows.
    """
    t0 = time.time()
    try:
        if microBatchDF is None:
            return

        # cheap emptiness check
        first = microBatchDF.head(1)
        if not first:
            # nothing to do
            return

        # transform
        df = clean_and_validate(microBatchDF)

        # deduplicate by (symbol, ts), keep latest ingested_at
        win = Window.partitionBy("symbol", "ts").orderBy(F.col("ingested_at").desc())
        deduped = (
            df.withColumn("row_num", F.row_number().over(win))
              .filter(F.col("row_num") == 1)
              .drop("row_num")
        )

        # filter valid rows
        valid_df = deduped.filter(F.col("is_valid") == True)

        # count valid rows once
        try:
            valid_count = valid_df.count()
        except Exception:
            # If count fails, assume zero to avoid crashing
            valid_count = 0

        # If nothing valid, optionally write invalids to audit (not doing here) and return
        if valid_count == 0:
            # print a minimal message for debugging if needed
            # Do not print for every idle batch to keep logs minimal
            return

        # Upsert: if Silver exists -> MERGE, else create table by write
        spark = microBatchDF.sparkSession
        if _delta_table_exists(spark, DELTA_PATH_SILVER):
            _merge_to_silver(spark, valid_df, batchId, valid_count)
        else:
            try:
                valid_df.write.format("delta").mode("append").partitionBy("ingestion_date").save(DELTA_PATH_SILVER)
                valid_df.write.format("delta").mode("append").partitionBy("ingestion_date").save(DELTA_PATH_SILVER)
                print(f"[batch {batchId}] Silver table created and wrote {valid_count} rows (initial write)")
            except Exception as e:
                print(f"[batch {batchId}][error] initial Silver write failed: {e}")
                traceback.print_exc()
                raise

        # --- DUAL WRITE: Sync to TimescaleDB for Real-Time Dashboard ---
        write_to_timescale(valid_df, batchId)

        dur_ms = int((time.time() - t0) * 1000)
        # print compact per-batch summary
        print("--------------------------------------------------------------------------------")
        print(f"Batch: {batchId}")
        print(f"Input rows (approx): {first and 1 or 0}+ (dedup/valid output below)")
        print(f"Processed valid rows: {valid_count}")
        print(f"Processing time (triggerExecution): {dur_ms} ms")
        print("Silver write: done")
        print("--------------------------------------------------------------------------------")

    except Exception as e:
        print(f"[foreach_batch][exception] batch {batchId}: {e}")
        traceback.print_exc()

def wait_for_bronze_table(spark, timeout_seconds=600):
    """
    Blocks until the Bronze Delta table exists or timeout is reached.
    """
    import time
    start_time = time.time()
    print(f"Waiting for Bronze table at {DELTA_PATH_BRONZE}...")
    
    while True:
        if _delta_table_exists(spark, DELTA_PATH_BRONZE):
            print("✓ Bronze table found!")
            return True
            
        if time.time() - start_time > timeout_seconds:
            raise TimeoutError(f"Bronze table not found after {timeout_seconds} seconds")
            
        time.sleep(10)
        print("... still waiting for Bronze table ...")

def perform_initial_load_if_needed(spark):
    """
    If Silver missing, try to read Bronze and do an initial upsert.
    Keeps logs minimal.
    """
    try:
        silver_exists = _delta_table_exists(spark, DELTA_PATH_SILVER)
        if silver_exists:
            # nothing to do
            return

        # Ensure Bronze exists before trying to read
        wait_for_bronze_table(spark)

        # if Silver missing, try to read Bronze for any existing data
        bronze_exists = os.path.exists(DELTA_PATH_BRONZE) and os.path.isdir(DELTA_PATH_BRONZE)
        if not bronze_exists:
            print("⚠ Silver missing and Bronze not available yet - skipping initial load")
            return

        bronze_df = spark.read.format("delta").load(DELTA_PATH_BRONZE)
        try:
            bronze_count = bronze_df.count()
        except Exception:
            bronze_count = 0

        if bronze_count == 0:
            print("⚠ Bronze exists but empty - skipping initial load")
            return

        print(f"✓ Performing initial Silver load from Bronze ({bronze_count} rows)")
        silver_batch = clean_and_validate(bronze_df)

        # dedupe and upsert using same logic as foreachBatch (call upsert_to_silver with batchId=0)
        upsert_to_silver(silver_batch, batchId=0)
        print("✓ Initial Silver load completed")
    except Exception as e:
        print(f"[initial_load][exception]: {e}")
        traceback.print_exc()

def main():
    _ensure_paths()
    spark = create_spark_session("SilverCleaningMinimal")

    # quiet spark logs
    try:
        spark.sparkContext.setLogLevel("WARN")
    except Exception:
        pass

    print("Starting Silver Layer Cleaning (minimal logs)")
    print(f"Reading from Bronze: {DELTA_PATH_BRONZE}")
    print(f"Writing to Silver: {DELTA_PATH_SILVER}")
    print(f"Checkpoint: {CHECKPOINT_PATH_SILVER}")

    print(f"Checkpoint: {CHECKPOINT_PATH_SILVER}")

    # Wait for Bronze table to be ready (handles race condition on fresh start)
    try:
        wait_for_bronze_table(spark)
    except Exception as e:
        print(f"FATAL: {e}")
        sys.exit(1)

    perform_initial_load_if_needed(spark)

    # connect stream
    bronze_stream = spark.readStream.format("delta").load(DELTA_PATH_BRONZE)
    silver_stream = clean_and_validate(bronze_stream)

    query = (
        silver_stream.writeStream
        .foreachBatch(lambda df, bid: upsert_to_silver(df, bid))
        .outputMode("update")
        .option("checkpointLocation", CHECKPOINT_PATH_SILVER)
        .trigger(processingTime="15 seconds")
        .start()
    )

    print(f"Streaming query started (id={query.id}) - micro-batch every 15s")

    idle_counter = 0
    try:
        while query.isActive:
            prog = query.lastProgress
            if prog:
                num_in = prog.get("numInputRows", 0)
                if num_in > 0:
                    idle_counter = 0
                    # rely on foreachBatch prints for per-batch details
                else:
                    idle_counter += 1
                    if IDLE_PRINT_EVERY > 0 and idle_counter >= IDLE_PRINT_EVERY:
                        b = prog.get("batchId", "N/A")
                        dur = prog.get("durationMs", {}).get("triggerExecution", 0)
                        print("--------------------------------------------------------------------------------")
                        print(f"Batch: {b} (idle)")
                        print(f"Input rows: {num_in}")
                        print(f"Processing time (triggerExecution): {dur} ms")
                        print("Silver: idle")
                        print("--------------------------------------------------------------------------------")
                        idle_counter = 0
            # wake up every 30s
            try:
                query.awaitTermination(30)
            except Exception:
                # depending on Spark version awaitTermination can raise on timeout - ignore
                pass
    except KeyboardInterrupt:
        print("Stopping Silver cleaning stream (user requested)")
    finally:
        if query and query.isActive:
            query.stop()
        try:
            spark.stop()
        except Exception:
            pass
        print("Silver cleaning stopped")

if __name__ == "__main__":
    main()
