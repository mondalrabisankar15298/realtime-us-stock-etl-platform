#!/usr/bin/env python3
"""
Bronze ingestion job: read raw JSON messages from Kafka, deduplicate & upsert into a Delta bronze table.

How it prevents duplicates:
  - Each micro-batch we:
    1) parse JSON value, normalize/convert types
    2) compute event timestamp (ts_utc) from epoch seconds in message["timestamp"]
    3) apply event-time watermark (configurable)
    4) within the micro-batch, deduplicate by (symbol, ts_utc, source_interval) keeping the row with newest ingested_at
    5) MERGE into the Delta bronze table on the same key (symbol, ts_utc, source_interval).
       When matched -> update (to overwrite older duplicate). When not matched -> insert.
       
**TIMEZONE FIX**: Uses direct epoch-to-timestamp cast to ensure UTC consistency regardless of server timezone.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import os
import json

# === Configuration (from env) ===
KAFKA_BOOTSTRAP = os.getenv("REDPANDA_BROKER", "redpanda:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_RAW", "stock-raw-data")
CHECKPOINT_LOCATION = os.getenv("BRONZE_CHECKPOINT", "/opt/spark/checkpoints/bronze")
BRONZE_TABLE_PATH = os.getenv("BRONZE_TABLE_PATH", "/opt/spark/delta_tables/bronze")
WATERMARK_DELAY = os.getenv("BRONZE_WATERMARK_DELAY", "1 day")  # Acceptable lateness
MAX_OFFSETS_PER_TRIGGER = os.getenv("MAX_OFFSETS_PER_TRIGGER", None)  # optional
STARTING_OFFSETS = os.getenv("KAFKA_STARTING_OFFSETS", "latest")  # "latest" in production normally
APP_NAME = "bronze_ingestion"

# === Spark session with Delta config ===
spark = (
    SparkSession.builder.appName(APP_NAME)
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    # Critical: Set timezone to UTC for consistent timestamp handling
    .config("spark.sql.session.timeZone", "UTC")
    # tune if needed:
    .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))
    .getOrCreate()
)

# Helpful for debugging/logging
spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))


# === Helper: parse JSON string value to columns ===
def parse_kafka_value(df: DataFrame) -> DataFrame:
    """
    Expects Kafka raw stream with value column as bytes/string (JSON).
    Produces columns:
      symbol, timestamp (long), open, high, low, close, volume,
      source, source_interval, fetched_from, ingested_at (ISO string),
      backfill_window_start, backfill_window_end
    plus computed:
      ts_utc (timestamp), date (string yyyy-MM-dd)
    """
    # value is bytes -> cast to string
    df2 = df.selectExpr("CAST(value AS STRING) as json_str", "topic", "partition", "offset")
    
    # parse common fields; do robust JSON parsing to avoid job crash on malformed messages
    df3 = df2.withColumn("_parsed", F.from_json("json_str", F.schema_of_json(F.lit(json.dumps({
        "symbol":"", "timestamp":0, "open":0.0, "high":0.0, "low":0.0, "close":0.0, "volume":0,
        "source":"", "source_interval":"", "fetched_from":"", "ingested_at":"", 
        "backfill_window_start":"", "backfill_window_end":""
    })))))
    
    # explode parsed into columns (null-safe)
    parsed = df3.select(
        F.col("_parsed.symbol").alias("symbol"),
        F.col("_parsed.timestamp").alias("timestamp_secs"),
        F.col("_parsed.open").alias("open"),
        F.col("_parsed.high").alias("high"),
        F.col("_parsed.low").alias("low"),
        F.col("_parsed.close").alias("close"),
        F.col("_parsed.volume").alias("volume"),
        F.col("_parsed.source").alias("source"),
        F.col("_parsed.source_interval").alias("source_interval"),
        F.col("_parsed.fetched_from").alias("fetched_from"),
        F.col("_parsed.ingested_at").alias("ingested_at_str"),
        F.col("_parsed.backfill_window_start").alias("backfill_window_start_str"),
        F.col("_parsed.backfill_window_end").alias("backfill_window_end_str"),
        "topic", "partition", "offset"
    )
    
    # Normalize and type-cast
    parsed = parsed.withColumn("symbol", F.trim(F.upper(F.col("symbol"))))
    parsed = parsed.withColumn("timestamp_secs", F.col("timestamp_secs").cast("long"))
    
    # **CRITICAL FIX**: Convert epoch secs -> timestamp using direct cast (ensures UTC)
    # OLD (BROKEN): F.to_timestamp(F.from_unixtime(F.col("timestamp_secs")), "yyyy-MM-dd HH:mm:ss")
    # NEW (FIXED): Direct cast to timestamp - Spark interprets epoch as UTC
    parsed = parsed.withColumn("ts_utc", F.col("timestamp_secs").cast("long").cast("timestamp"))
    
    parsed = parsed.withColumn("open", F.col("open").cast("double"))
    parsed = parsed.withColumn("high", F.col("high").cast("double"))
    parsed = parsed.withColumn("low", F.col("low").cast("double"))
    parsed = parsed.withColumn("close", F.col("close").cast("double"))
    parsed = parsed.withColumn("volume", F.col("volume").cast("long"))
    
    # ingested_at: try parse string iso -> timestamp; fallback to current_timestamp if missing
    parsed = parsed.withColumn(
        "ingested_at",
        F.coalesce(
            F.to_timestamp("ingested_at_str"),
            F.current_timestamp()
        )
    )
    
    parsed = parsed.withColumn(
        "backfill_window_start",
        F.to_timestamp("backfill_window_start_str")
    ).withColumn(
        "backfill_window_end",
        F.to_timestamp("backfill_window_end_str")
    )
    
    # Add date partition (UTC date of event)
    parsed = parsed.withColumn("date", F.date_format(F.col("ts_utc"), "yyyy-MM-dd"))
    
    # Drop rows without symbol or timestamp or non-sensible prices
    parsed = parsed.filter(F.col("symbol").isNotNull() & F.col("timestamp_secs").isNotNull())
    
    return parsed


# === Upsert (MERGE) helper ===
def merge_to_bronze(batch_df: DataFrame, batch_id: int):
    """
    Called per micro-batch. Performs:
      - inside-batch dedup by (symbol, ts_utc, source_interval) keeping row with latest ingested_at
      - MERGE into delta table
    """
    if batch_df.rdd.isEmpty():
        print(f"[bronze] Batch {batch_id} empty; skipping")
        return
    
    # Normalize columns and dedupe within batch:
    # Keep newest row per (symbol, ts_utc, source_interval) determined by ingested_at.
    window = Window.partitionBy("symbol", "ts_utc", "source_interval").orderBy(F.col("ingested_at").desc())
    deduped = (
        batch_df
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    
    # Select final set of columns to persist
    bronze_cols = [
        "symbol",
        "ts_utc",
        "timestamp_secs",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "source",
        "source_interval",
        "fetched_from",
        "ingested_at",
        "backfill_window_start",
        "backfill_window_end",
        "date"
    ]
    deduped = deduped.select(*[c for c in bronze_cols if c in deduped.columns])
    
    # Ensure directory/table exists
    if not DeltaTable.isDeltaTable(spark, BRONZE_TABLE_PATH):
        # initial write: partition by date and symbol
        deduped.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("date", "symbol").save(BRONZE_TABLE_PATH)
        print(f"[bronze] Batch {batch_id}: created bronze table at {BRONZE_TABLE_PATH} with {deduped.count()} rows")
        return
    
    # MERGE: match on (symbol, ts_utc, source_interval)
    bronze_dt = DeltaTable.forPath(spark, BRONZE_TABLE_PATH)
    
    # Build merge condition
    merge_condition = "target.symbol = source.symbol AND target.ts_utc = source.ts_utc AND target.source_interval = source.source_interval"
    
    # When matched, update all fields (we prefer incoming row since it is the latest ingested_at within micro-batch)
    update_map = {
        "timestamp_secs": "source.timestamp_secs",
        "open": "source.open",
        "high": "source.high",
        "low": "source.low",
        "close": "source.close",
        "volume": "source.volume",
        "source": "source.source",
        "fetched_from": "source.fetched_from",
        "ingested_at": "source.ingested_at",
        "backfill_window_start": "source.backfill_window_start",
        "backfill_window_end": "source.backfill_window_end",
        "date": "source.date"
    }
    
    # Do the merge using DataFrame as source (create temp view)
    source_temp = f"tmp_batch_{batch_id}"
    deduped.createOrReplaceTempView(source_temp)
    src_df = spark.table(source_temp)
    
    # Perform delta merge
    bronze_dt.alias("target").merge(
        src_df.alias("source"),
        merge_condition
    ).whenMatchedUpdate(
        set={k: F.expr(v) for k, v in update_map.items()}
    ).whenNotMatchedInsert(
        values={k: F.expr(f"source.{k}") for k in update_map.keys()} | {
            # we also need to insert symbol, ts_utc and source_interval (keys)
            "symbol": F.expr("source.symbol"),
            "ts_utc": F.expr("source.ts_utc"),
            "source_interval": F.expr("source.source_interval")
        }
    ).execute()
    
    print(f"[bronze] Batch {batch_id}: merged {deduped.count()} rows into bronze table")


# === Main streaming read ===
def main():
    read_options = {
        "kafka.bootstrap.servers": KAFKA_BOOTSTRAP,
        "subscribe": KAFKA_TOPIC,
        "startingOffsets": STARTING_OFFSETS,
        "failOnDataLoss": "false",
    }
    
    if MAX_OFFSETS_PER_TRIGGER:
        read_options["maxOffsetsPerTrigger"] = int(MAX_OFFSETS_PER_TRIGGER)
    
    raw_stream = (
        spark.readStream.format("kafka")
        .options(**read_options)
        .load()
    )
    
    parsed = parse_kafka_value(raw_stream)
    
    # Apply watermark on event time (ts_utc)
    # Note: watermark applies to aggregation state and will also help Spark drop very late events based on watermark.
    # We still do dedupe per micro-batch and MERGE, so slight lateness is tolerated as long as event hasn't been garbage-collected.
    stream_with_watermark = parsed.withWatermark("ts_utc", WATERMARK_DELAY)
    
    # Because we use foreachBatch + MERGE, we can pass micro-batches directly
    query = (
        stream_with_watermark.writeStream
        .foreachBatch(merge_to_bronze)
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .trigger(processingTime=os.getenv("BRONZE_TRIGGER", "30 seconds"))
        .start()
    )
    
    query.awaitTermination()


if __name__ == "__main__":
    main()
