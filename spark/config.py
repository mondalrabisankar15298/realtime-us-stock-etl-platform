"""
Spark Configuration for Stock ETL Pipeline
Shared configurations, schemas, and utilities
"""

import os
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    IntegerType,
    TimestampType,
    BooleanType,
)

# ==========================================
# Environment Configuration
# ==========================================
REDPANDA_BROKER = os.getenv("REDPANDA_BROKER", "redpanda:9092")
KAFKA_TOPIC_RAW = os.getenv("KAFKA_TOPIC_RAW", "stock-raw-data")
KAFKA_TOPIC_DLQ = os.getenv("KAFKA_TOPIC_DLQ", "stock-dlq")

# Delta Lake Paths
DELTA_PATH_BRONZE = os.getenv("DELTA_PATH_BRONZE", "/opt/spark/delta_tables/bronze")
DELTA_PATH_SILVER = os.getenv("DELTA_PATH_SILVER", "/opt/spark/delta_tables/silver")
DELTA_PATH_GOLD = os.getenv("DELTA_PATH_GOLD", "/opt/spark/delta_tables/gold")

# Checkpoint Paths
CHECKPOINT_PATH_BRONZE = os.getenv("CHECKPOINT_PATH_BRONZE", "/opt/spark/checkpoints/bronze")
CHECKPOINT_PATH_SILVER = os.getenv("CHECKPOINT_PATH_SILVER", "/opt/spark/checkpoints/silver")
CHECKPOINT_PATH_GOLD = os.getenv("CHECKPOINT_PATH_GOLD", "/opt/spark/checkpoints/gold")

# Stock Tickers
STOCK_TICKERS = os.getenv("STOCK_TICKERS", "AAPL,MSFT,GOOGL,AMZN,NVDA,META,TSLA,NFLX,AMD,AVGO").split(",")


# ==========================================
# Spark Session Configuration
# ==========================================
def get_spark_config():
    """Returns Spark configuration for Delta Lake and Kafka"""
    return {
        "spark.app.name": "StockETLPipeline",
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.sql.streaming.checkpointLocation.checkpointSchema": "true",
        "spark.sql.streaming.schemaInference": "true",
        "spark.databricks.delta.retentionDurationCheck.enabled": "false",
        "spark.databricks.delta.schema.autoMerge.enabled": "true",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
    }


# ==========================================
# Kafka Configuration
# ==========================================
def get_kafka_read_options(topic: str, starting_offsets: str = "latest"):
    """Returns Kafka read options for Structured Streaming"""
    return {
        "kafka.bootstrap.servers": REDPANDA_BROKER,
        "subscribe": topic,
        "startingOffsets": starting_offsets,
        "failOnDataLoss": "false",
        "maxOffsetsPerTrigger": "1000",  # Rate limiting
    }


# ==========================================
# Schema Definitions
# ==========================================

# Raw message schema from Kafka (from producer)
RAW_MESSAGE_SCHEMA = StructType([
    StructField("symbol", StringType(), False),
    StructField("timestamp", LongType(), False),
    StructField("open", DoubleType(), False),
    StructField("high", DoubleType(), False),
    StructField("low", DoubleType(), False),
    StructField("close", DoubleType(), False),
    StructField("volume", LongType(), False),
    StructField("ingested_at", StringType(), False),
])


# Bronze schema (raw data as-is)
BRONZE_SCHEMA = StructType([
    StructField("symbol", StringType(), False),
    StructField("timestamp", LongType(), False),
    StructField("open", DoubleType(), False),
    StructField("high", DoubleType(), False),
    StructField("low", DoubleType(), False),
    StructField("close", DoubleType(), False),
    StructField("volume", LongType(), False),
    StructField("ingested_at", StringType(), False),
    StructField("bronze_timestamp", TimestampType(), False),  # When written to bronze
])


# Silver schema (cleaned and validated)
SILVER_SCHEMA = StructType([
    StructField("symbol", StringType(), False),
    StructField("ts", TimestampType(), False),  # Normalized timestamp
    StructField("open", DoubleType(), False),
    StructField("high", DoubleType(), False),
    StructField("low", DoubleType(), False),
    StructField("close", DoubleType(), False),
    StructField("volume", LongType(), False),
    StructField("is_valid", BooleanType(), False),
    StructField("ingestion_date", StringType(), False),  # Partition column
    StructField("processed_at", TimestampType(), False),
])


# Gold schema (KPIs and features)
GOLD_SCHEMA = StructType([
    StructField("symbol", StringType(), False),
    StructField("ts", TimestampType(), False),
    StructField("close", DoubleType(), False),
    StructField("volume", LongType(), False),
    # Technical Indicators
    StructField("sma_5", DoubleType(), True),
    StructField("sma_20", DoubleType(), True),
    StructField("sma_50", DoubleType(), True),
    StructField("ema_9", DoubleType(), True),
    StructField("ema_21", DoubleType(), True),
    StructField("rsi_14", DoubleType(), True),
    StructField("vwap", DoubleType(), True),
    StructField("macd", DoubleType(), True),
    StructField("macd_signal", DoubleType(), True),
    StructField("macd_histogram", DoubleType(), True),
    StructField("atr_14", DoubleType(), True),
    # Derived metrics
    StructField("daily_return", DoubleType(), True),
    StructField("volatility_5m", DoubleType(), True),
    StructField("market_phase", StringType(), True),  # pre-market/open/post-market
    StructField("price_change_pct", DoubleType(), True),
    StructField("computed_at", TimestampType(), False),
])


# ==========================================
# Utility Functions
# ==========================================

def create_spark_session(app_name: str = "StockETL"):
    """Create and configure Spark session with Delta Lake support"""
    from pyspark.sql import SparkSession
    from delta import configure_spark_with_delta_pip
    
    builder = SparkSession.builder.appName(app_name)
    
    # Apply configurations
    for key, value in get_spark_config().items():
        builder = builder.config(key, value)
    
    # Create session with Delta Lake
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    
    # Set log level
    spark.sparkContext.setLogLevel("WARN")
    
    return spark


def get_market_phase(hour: int) -> str:
    """
    Determine market phase based on hour (EST/EDT)
    Pre-market: 4:00 AM - 9:30 AM
    Market hours: 9:30 AM - 4:00 PM
    After-hours: 4:00 PM - 8:00 PM
    Closed: 8:00 PM - 4:00 AM
    """
    if 4 <= hour < 9:
        return "pre-market"
    elif 9 <= hour < 16:
        return "open"
    elif 16 <= hour < 20:
        return "post-market"
    else:
        return "closed"

