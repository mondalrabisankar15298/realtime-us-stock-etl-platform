#!/usr/bin/env python3
"""
Bronze ingestion (minimal logs)

- Suppresses verbose Spark INFO logs (sets Spark log level to WARN).
- Prints compact per-batch summary + "Bronze written" only when there are input rows,
  or once every IDLE_PRINT_EVERY idle batches (configurable).
- Uses foreachBatch to write to Delta (append, mergeSchema).
"""

import sys
import os
import time
import traceback
from pathlib import Path
from typing import Dict

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType
)

# Configurable behaviour
MICROBATCH_TRIGGER = "10 seconds"
IDLE_PRINT_EVERY = 0  # 0 = never print for idle batches; set to e.g. 30 to log once every 30 idle iterations

# Try to import config from repo; otherwise use sensible defaults for local dev
try:
    from config import (
        create_spark_session,
        get_kafka_read_options,
        KAFKA_TOPIC_RAW,
        DELTA_PATH_BRONZE,
        CHECKPOINT_PATH_BRONZE,
        RAW_MESSAGE_SCHEMA,
    )
except Exception:
    def create_spark_session(app_name: str):
        return SparkSession.builder \
            .appName(app_name) \
            .master("local[*]") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()

    def get_kafka_read_options(topic: str, starting_offsets: str = "latest") -> Dict:
        return {
            "kafka.bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", "redpanda:9092"),
            "subscribe": topic,
            "startingOffsets": starting_offsets,
            "failOnDataLoss": "false"
        }

    KAFKA_TOPIC_RAW = os.getenv("KAFKA_TOPIC_RAW", "stock-raw-data")
    DELTA_PATH_BRONZE = os.getenv("DELTA_PATH_BRONZE", "/opt/spark/delta_tables/bronze")
    CHECKPOINT_PATH_BRONZE = os.getenv("CHECKPOINT_PATH_BRONZE", "/opt/spark/checkpoints/bronze")
    RAW_MESSAGE_SCHEMA = StructType([
        StructField("symbol", StringType(), True),
        StructField("timestamp", LongType(), True),
        StructField("open", DoubleType(), True),
        StructField("high", DoubleType(), True),
        StructField("low", DoubleType(), True),
        StructField("close", DoubleType(), True),
        StructField("volume", LongType(), True),
        StructField("source", StringType(), True),
        StructField("source_interval", StringType(), True),
        StructField("fetched_from", StringType(), True),
        StructField("ingested_at", StringType(), True),
        StructField("backfill_window_start", StringType(), True),
        StructField("backfill_window_end", StringType(), True),
    ])

DLQ_PATH = os.getenv("BRONZE_DLQ_PATH", "/opt/spark/delta_tables/bronze_dlq")

def _ensure_paths():
    Path(DELTA_PATH_BRONZE).parent.mkdir(parents=True, exist_ok=True)
    Path(CHECKPOINT_PATH_BRONZE).parent.mkdir(parents=True, exist_ok=True)
    Path(DLQ_PATH).parent.mkdir(parents=True, exist_ok=True)

def _safe_rename_metadata(df):
    rename_map = {
        "kafka_key": "kafka_key_meta",
        "kafka_timestamp": "kafka_timestamp_meta",
        "kafka_partition": "kafka_partition_meta",
        "kafka_offset": "kafka_offset_meta",
    }
    cols = set(df.columns)
    for old, new in rename_map.items():
        if old in cols and new not in cols:
            df = df.withColumnRenamed(old, new)
        elif old in cols and new in cols:
            df = df.drop(old)
    return df

def _get_offset_summary(batch_df):
    start_offsets = {}
    end_offsets = {}
    try:
        if "kafka_partition" in batch_df.columns and "kafka_offset" in batch_df.columns:
            part_col = "kafka_partition"
            off_col = "kafka_offset"
        elif "kafka_partition_meta" in batch_df.columns and "kafka_offset_meta" in batch_df.columns:
            part_col = "kafka_partition_meta"
            off_col = "kafka_offset_meta"
        else:
            return {}, {}

        agg = batch_df.groupBy(part_col).agg(
            F.min(off_col).alias("start_off"),
            F.max(off_col).alias("end_off"),
        ).collect()

        for row in agg:
            p = str(row[part_col])
            start_offsets[p] = int(row["start_off"])
            end_offsets[p] = int(row["end_off"])
    except Exception:
        return {}, {}

    return start_offsets, end_offsets

def _format_offset_map(offset_map):
    if not offset_map:
        return "{}"
    return "{" + ", ".join([f"'{k}': {v}" for k, v in offset_map.items()]) + "}"

def foreach_batch_writer(batch_df, batch_id):
    start = time.time()
    try:
        if batch_df is None:
            return

        df = _safe_rename_metadata(batch_df)
        try:
            input_rows = df.count()
        except Exception:
            input_rows = 0

        start_offsets, end_offsets = _get_offset_summary(df)

        if "symbol" in df.columns:
            good = df.filter(F.col("symbol").isNotNull())
            bad = df.filter(F.col("symbol").isNull())
        else:
            good = df.limit(0)
            bad = df

        bad_written = 0
        try:
            bad_count = bad.count()
            if bad_count > 0:
                bad_with_meta = bad.select(F.current_timestamp().alias("failed_at"), F.lit(batch_id).alias("batch_id"), *bad.columns)
                bad_with_meta.write.format("delta").mode("append").option("mergeSchema", "true").save(DLQ_PATH)
                bad_written = bad_count
        except Exception:
            bad_written = 0

        bronze_written = 0
        try:
            good_count = good.count()
            if good_count > 0:
                good_safe = _safe_rename_metadata(good)
                good_safe.write.format("delta").mode("append").option("mergeSchema", "true").save(DELTA_PATH_BRONZE)
                bronze_written = good_count
        except Exception as e:
            print(f"[error] Bronze write failed batch {batch_id}: {e}")
            traceback.print_exc()
            try:
                df_with_meta = df.select(F.current_timestamp().alias("failed_at"), F.lit(batch_id).alias("batch_id"), *df.columns)
                df_with_meta.write.format("delta").mode("append").option("mergeSchema", "true").save(DLQ_PATH)
            except Exception:
                pass
            bronze_written = 0

        dur_ms = int((time.time() - start) * 1000)

        # Print only when actual input rows exist — keeps logs minimal
        if input_rows > 0 or IDLE_PRINT_EVERY > 0:
            # If idle printing is enabled, we still want to print only every IDLE_PRINT_EVERY idle iterations.
            # For simplicity we let the main loop decide idle printing. Here print when input_rows>0.
            print("--------------------------------------------------------------------------------")
            print(f"Batch: {batch_id}")
            print(f"Input rows: {input_rows}")
            print(f"Processing time (triggerExecution): {dur_ms} ms")
            start_fmt = _format_offset_map(start_offsets)
            end_fmt = _format_offset_map(end_offsets)
            print(f"  Kafka offset: start={{{KAFKA_TOPIC_RAW}: {start_fmt}}}, end={{{KAFKA_TOPIC_RAW}: {end_fmt}}}")
            print(f"Output rows: -1")
            print(f"Bronze written: {bronze_written} rows; DLQ written: {bad_written} rows")
            print("--------------------------------------------------------------------------------")

    except Exception as e:
        print(f"[foreach_batch][exception] batch {batch_id}: {e}")
        traceback.print_exc()

def perform_initial_load_if_needed(spark):
    try:
        bronze_exists = os.path.exists(DELTA_PATH_BRONZE) and os.path.isdir(DELTA_PATH_BRONZE)
        if bronze_exists:
            try:
                existing = spark.read.format("delta").load(DELTA_PATH_BRONZE)
                cnt = existing.count()
                if cnt > 0:
                    print(f"✓ Bronze table exists with {cnt} rows")
                    return
            except Exception:
                pass

        print("⚠ Bronze missing/empty - doing initial batch read from Kafka (earliest)")
        opts = get_kafka_read_options(KAFKA_TOPIC_RAW, starting_offsets="earliest")
        kafka_batch = spark.read.format("kafka").options(**opts).load()
        parsed = kafka_batch.select(
            F.col("key").cast(StringType()).alias("kafka_key"),
            F.from_json(F.col("value").cast(StringType()), RAW_MESSAGE_SCHEMA).alias("data"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset")
        ).select("data.*", F.current_timestamp().alias("bronze_timestamp"), "kafka_key", "kafka_timestamp", "kafka_partition", "kafka_offset")

        parsed_safe = _safe_rename_metadata(parsed)
        non_null = parsed_safe.filter(F.col("symbol").isNotNull())
        c = non_null.count()
        if c > 0:
            non_null.write.format("delta").mode("append").option("mergeSchema", "true").save(DELTA_PATH_BRONZE)
            print(f"[initial] wrote {c} rows to Bronze")
        else:
            print("[initial] no parsed rows found during earliest read")
    except Exception as e:
        print(f"[initial][error] {e}")
        traceback.print_exc()

def main():
    _ensure_paths()
    spark = create_spark_session("BronzeIngestMinimal")
    # Quiet Spark internal logs
    try:
        spark.sparkContext.setLogLevel("WARN")
    except Exception:
        pass

    print("BRONZE LAYER INGESTION - Starting (minimal logs)")
    print(f"Reading from Kafka topic: {KAFKA_TOPIC_RAW}")
    print(f"Writing to Delta table: {DELTA_PATH_BRONZE}")
    print(f"Checkpoint location: {CHECKPOINT_PATH_BRONZE}")

    perform_initial_load_if_needed(spark)

    kafka_opts = get_kafka_read_options(KAFKA_TOPIC_RAW, starting_offsets="latest")
    kafka_df = spark.readStream.format("kafka").options(**kafka_opts).load()

    parsed_stream = kafka_df.select(
        F.col("key").cast(StringType()).alias("kafka_key"),
        F.col("value").cast(StringType()).alias("raw_value"),
        F.from_json(F.col("value").cast(StringType()), RAW_MESSAGE_SCHEMA).alias("data"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
    ).select("data.*", F.current_timestamp().alias("bronze_timestamp"), "kafka_key", "kafka_timestamp", "kafka_partition", "kafka_offset")

    query = (
        parsed_stream.writeStream
        .foreachBatch(lambda df, bid: foreach_batch_writer(df, bid))
        .option("checkpointLocation", CHECKPOINT_PATH_BRONZE)
        .trigger(processingTime=MICROBATCH_TRIGGER)
        .start()
    )

    print(f"Streaming query started (id={query.id}) - micro-batch every {MICROBATCH_TRIGGER}")

    idle_counter = 0
    try:
        while query.isActive:
            prog = query.lastProgress
            if prog:
                num_in = prog.get("numInputRows", 0)
                # print only when there are input rows or if idle printing configured
                if num_in > 0:
                    # we rely on foreachBatch to print the full per-batch summary and bronze written line
                    idle_counter = 0
                else:
                    idle_counter += 1
                    if IDLE_PRINT_EVERY > 0 and idle_counter >= IDLE_PRINT_EVERY:
                        # print a tiny heartbeat so you know it's alive but quiet
                        b = prog.get("batchId", "N/A")
                        dur = prog.get("durationMs", {}).get("triggerExecution", 0)
                        start_off = prog.get("sources", [{}])[0].get("startOffset", "N/A")
                        end_off = prog.get("sources", [{}])[0].get("endOffset", "N/A")
                        print("--------------------------------------------------------------------------------")
                        print(f"Batch: {b} (idle)")
                        print(f"Input rows: {num_in}")
                        print(f"Processing time (triggerExecution): {dur} ms")
                        print(f"  Kafka offset: start={start_off}, end={end_off}")
                        print("Output rows: -1")
                        print("--------------------------------------------------------------------------------")
                        idle_counter = 0
            query.awaitTermination(30)
    except KeyboardInterrupt:
        print("Stopping Bronze ingestion (user requested)")
    finally:
        if query and query.isActive:
            query.stop()
        spark.stop()

if __name__ == "__main__":
    main()
