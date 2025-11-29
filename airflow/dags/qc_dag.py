"""
Quality Check DAG - Data Quality Validation
Validates schema, completeness, and data quality in Silver tables
Runs daily
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from pathlib import Path

# Default arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email': ['alerts@example.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'stock_quality_check',
    default_args=default_args,
    description='Data quality checks on Silver layer',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    catchup=False,
    tags=['quality', 'validation', 'monitoring'],
)


def validate_schema(**context):
    """
    Validate schema of Silver table matches expected schema
    """
    try:
        from pyspark.sql import SparkSession
        from delta import configure_spark_with_delta_pip
        
        builder = (
            SparkSession.builder
            .appName("SchemaValidation")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )
        
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        
        silver_path = "/opt/spark/delta_tables/silver"
        
        if not Path(silver_path).exists():
            print("⚠️  Silver table doesn't exist yet")
            spark.stop()
            return True
        
        df = spark.read.format("delta").load(silver_path)
        
        expected_columns = [
            'symbol', 'ts', 'open', 'high', 'low', 'close',
            'volume', 'is_valid', 'ingestion_date', 'processed_at'
        ]
        
        actual_columns = df.columns
        
        missing_columns = set(expected_columns) - set(actual_columns)
        extra_columns = set(actual_columns) - set(expected_columns)
        
        if missing_columns:
            print(f"✗ Missing columns: {missing_columns}")
            spark.stop()
            return False
        
        if extra_columns:
            print(f"⚠️  Extra columns (may be ok): {extra_columns}")
        
        print(f"✓ Schema validation passed")
        print(f"Columns: {actual_columns}")
        
        spark.stop()
        return True
        
    except Exception as e:
        print(f"Error validating schema: {e}")
        import traceback
        traceback.print_exc()
        return False


def check_completeness(**context):
    """
    Check if all expected tickers have recent data
    """
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
        from delta import configure_spark_with_delta_pip
        import os
        
        builder = (
            SparkSession.builder
            .appName("CompletenessCheck")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )
        
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        
        silver_path = "/opt/spark/delta_tables/silver"
        
        if not Path(silver_path).exists():
            print("⚠️  Silver table doesn't exist yet")
            spark.stop()
            return True
        
        df = spark.read.format("delta").load(silver_path)
        
        # Get expected tickers
        expected_tickers = set(
            os.getenv("STOCK_TICKERS", "AAPL,MSFT,GOOGL,AMZN,NVDA,META,TSLA,NFLX,AMD,AVGO").split(",")
        )
        
        # Get yesterday's data
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        recent_df = df.filter(F.col("ingestion_date") >= yesterday)
        actual_tickers = set([row.symbol for row in recent_df.select("symbol").distinct().collect()])
        
        missing_tickers = expected_tickers - actual_tickers
        
        if missing_tickers:
            print(f"⚠️  Missing tickers in recent data: {missing_tickers}")
        else:
            print(f"✓ All {len(expected_tickers)} tickers present")
        
        # Check record counts per ticker
        print("\nRecord counts (last 24h):")
        counts = (
            recent_df
            .groupBy("symbol")
            .count()
            .orderBy("symbol")
            .collect()
        )
        
        for row in counts:
            print(f"  {row.symbol}: {row['count']} records")
        
        spark.stop()
        return len(missing_tickers) == 0
        
    except Exception as e:
        print(f"Error checking completeness: {e}")
        import traceback
        traceback.print_exc()
        return False


def detect_outliers(**context):
    """
    Detect price outliers (spikes > 10%)
    """
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window
        from delta import configure_spark_with_delta_pip
        
        builder = (
            SparkSession.builder
            .appName("OutlierDetection")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )
        
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        
        silver_path = "/opt/spark/delta_tables/silver"
        
        if not Path(silver_path).exists():
            print("⚠️  Silver table doesn't exist yet")
            spark.stop()
            return True
        
        df = spark.read.format("delta").load(silver_path)
        
        # Get yesterday's data
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        recent_df = df.filter(F.col("ingestion_date") >= yesterday)
        
        # Calculate price changes
        window_spec = Window.partitionBy("symbol").orderBy("ts")
        
        df_with_change = recent_df.withColumn(
            "prev_close",
            F.lag("close", 1).over(window_spec)
        ).withColumn(
            "price_change_pct",
            F.when(
                F.col("prev_close").isNotNull() & (F.col("prev_close") != 0),
                ((F.col("close") - F.col("prev_close")) / F.col("prev_close")) * 100
            )
        )
        
        # Find outliers (> 10% change)
        outliers = df_with_change.filter(F.abs(F.col("price_change_pct")) > 10)
        
        outlier_count = outliers.count()
        
        if outlier_count > 0:
            print(f"⚠️  Found {outlier_count} price outliers (>10% change)")
            outliers.select("symbol", "ts", "close", "prev_close", "price_change_pct").show(20, False)
        else:
            print("✓ No significant price outliers detected")
        
        spark.stop()
        return True
        
    except Exception as e:
        print(f"Error detecting outliers: {e}")
        import traceback
        traceback.print_exc()
        return False


def check_volume_anomalies(**context):
    """
    Detect volume anomalies (unusually high/low volume)
    """
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
        from delta import configure_spark_with_delta_pip
        
        builder = (
            SparkSession.builder
            .appName("VolumeAnomalyCheck")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )
        
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        
        silver_path = "/opt/spark/delta_tables/silver"
        
        if not Path(silver_path).exists():
            print("⚠️  Silver table doesn't exist yet")
            spark.stop()
            return True
        
        df = spark.read.format("delta").load(silver_path)
        
        # Calculate volume statistics per ticker
        stats = (
            df
            .groupBy("symbol")
            .agg(
                F.avg("volume").alias("avg_volume"),
                F.stddev("volume").alias("stddev_volume"),
                F.max("volume").alias("max_volume"),
                F.min("volume").alias("min_volume")
            )
        )
        
        print("\nVolume statistics by ticker:")
        stats.show(20, False)
        
        # Check for zero volume records
        zero_volume = df.filter(F.col("volume") == 0).count()
        
        if zero_volume > 0:
            print(f"⚠️  Found {zero_volume} records with zero volume")
        else:
            print("✓ No zero-volume records")
        
        spark.stop()
        return True
        
    except Exception as e:
        print(f"Error checking volume anomalies: {e}")
        import traceback
        traceback.print_exc()
        return False


# Define tasks
task_schema = PythonOperator(
    task_id='validate_schema',
    python_callable=validate_schema,
    dag=dag,
)

task_completeness = PythonOperator(
    task_id='check_completeness',
    python_callable=check_completeness,
    dag=dag,
)

task_outliers = PythonOperator(
    task_id='detect_outliers',
    python_callable=detect_outliers,
    dag=dag,
)

task_volume = PythonOperator(
    task_id='check_volume_anomalies',
    python_callable=check_volume_anomalies,
    dag=dag,
)

# All checks run in parallel
[task_schema, task_completeness, task_outliers, task_volume]

