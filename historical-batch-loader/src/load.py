"""
Load Module for Titan Historical ETL Pipeline.
Reads raw CSV from MinIO, transforms data, and loads into Iceberg table.
Uses MERGE INTO for atomic upsert idempotency.
"""

import argparse
import sys
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, DoubleType, StringType
from pyspark.sql.functions import col

from config import (
    MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_REGION,
    RAW_BUCKET, ICEBERG_CATALOG_URI, ICEBERG_WAREHOUSE,
    SPARK_MASTER, SPARK_DRIVER_HOST, TABLE_NAME, NAMESPACE
)


# Schema for Binance 1-minute kline CSV
BINANCE_SCHEMA = StructType([
    StructField("open_time_ms", LongType(), True),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", DoubleType(), True),
    StructField("close_time_ms", LongType(), True),
    StructField("quote_asset_volume", DoubleType(), True),
    StructField("trades", LongType(), True),
    StructField("taker_buy_base", DoubleType(), True),
    StructField("taker_buy_quote", DoubleType(), True),
    StructField("ignore", StringType(), True)
])


def get_spark_session() -> SparkSession:
    """Create Spark session with Iceberg and S3A support."""
    packages = [
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2",
        "org.apache.iceberg:iceberg-aws-bundle:1.4.2",
        "org.apache.hadoop:hadoop-aws:3.3.4"
    ]
    
    return SparkSession.builder \
        .appName("Titan-Historical-Load") \
        .master(SPARK_MASTER) \
        .config("spark.driver.host", SPARK_DRIVER_HOST) \
        .config("spark.driver.bindAddress", "0.0.0.0") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.jars.packages", ",".join(packages)) \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.titan", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.titan.type", "rest") \
        .config("spark.sql.catalog.titan.uri", ICEBERG_CATALOG_URI) \
        .config("spark.sql.catalog.titan.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.sql.catalog.titan.warehouse", ICEBERG_WAREHOUSE) \
        .config("spark.sql.catalog.titan.s3.endpoint", MINIO_ENDPOINT) \
        .config("spark.sql.catalog.titan.s3.access-key-id", MINIO_ACCESS_KEY) \
        .config("spark.sql.catalog.titan.s3.secret-access-key", MINIO_SECRET_KEY) \
        .config("spark.sql.catalog.titan.s3.path-style-access", "true") \
        .config("spark.sql.catalog.titan.client.region", MINIO_REGION) \
        .config("spark.sql.catalog.titan.s3.ssl.enabled", "false") \
        .config("spark.network.timeout", "600s") \
        .config("spark.executor.heartbeatInterval", "120s") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()


def get_s3_client():
    """Create MinIO S3 client."""
    return boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version='s3v4'),
        region_name=MINIO_REGION
    )


def check_raw_file_exists(symbol: str, year: str, month: str) -> bool:
    """Check if the raw file exists in MinIO."""
    s3_key = f"btc_trades/year={year}/month={month}/{symbol}-1m-{year}-{month}.csv"
    s3 = get_s3_client()
    try:
        s3.head_object(Bucket=RAW_BUCKET, Key=s3_key)
        return True
    except ClientError:
        return False


def ensure_table_exists(spark: SparkSession):
    """Create namespace and table if they don't exist."""
    print(f"[LOAD] Ensuring namespace exists: {NAMESPACE}")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {NAMESPACE}")
    
    print(f"[LOAD] Ensuring table exists: {TABLE_NAME}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume DOUBLE,
            quote_asset_volume DOUBLE,
            trades LONG,
            taker_buy_base DOUBLE,
            taker_buy_quote DOUBLE,
            event_time TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (months(event_time))
    """)


def merge_into_iceberg(spark: SparkSession, source_df, year: str, month: str) -> int:
    """
    Merge source data into Iceberg table using MERGE INTO for atomic upsert.
    Uses event_time as the key for matching.
    
    Returns:
        Number of rows in source dataframe.
    """
    # Create temporary view for the source data
    source_df.createOrReplaceTempView("source_data")
    
    row_count = source_df.count()
    print(f"[LOAD] Merging {row_count} rows into {TABLE_NAME}...")
    
    # MERGE INTO for atomic upsert
    # - If event_time matches: UPDATE existing row
    # - If event_time doesn't match: INSERT new row
    spark.sql(f"""
        MERGE INTO {TABLE_NAME} target
        USING source_data source
        ON target.event_time = source.event_time
        WHEN MATCHED THEN UPDATE SET
            open = source.open,
            high = source.high,
            low = source.low,
            close = source.close,
            volume = source.volume,
            quote_asset_volume = source.quote_asset_volume,
            trades = source.trades,
            taker_buy_base = source.taker_buy_base,
            taker_buy_quote = source.taker_buy_quote
        WHEN NOT MATCHED THEN INSERT (
            open, high, low, close, volume,
            quote_asset_volume, trades, taker_buy_base, taker_buy_quote, event_time
        ) VALUES (
            source.open, source.high, source.low, source.close, source.volume,
            source.quote_asset_volume, source.trades, source.taker_buy_base,
            source.taker_buy_quote, source.event_time
        )
    """)
    
    print(f"[LOAD] MERGE complete.")
    return row_count


def run_load(symbol: str, year: str, month: str, skip_if_exists: bool = False) -> bool:
    """
    Main load function.
    
    Args:
        symbol: Trading pair (e.g., BTCUSDT)
        year: Year as string (e.g., "2023")
        month: Month as zero-padded string (e.g., "01")
        skip_if_exists: If True, skip loading if data already exists in Iceberg
        
    Returns:
        True if load successful, False otherwise.
    """
    print(f"\n{'='*60}")
    print(f"[LOAD] Starting load for {symbol} {year}-{month}")
    print(f"{'='*60}\n")
    
    # 1. Check if raw file exists
    if not check_raw_file_exists(symbol, year, month):
        print(f"[LOAD] ERROR: Raw file not found. Run extract first.")
        return False
    
    spark = None
    try:
        # 2. Initialize Spark
        print("[LOAD] Initializing Spark session...")
        spark = get_spark_session()
        
        # 3. Ensure table exists
        ensure_table_exists(spark)
        
        # 4. Read raw CSV from MinIO
        s3_path = f"s3a://{RAW_BUCKET}/btc_trades/year={year}/month={month}/{symbol}-1m-{year}-{month}.csv"
        print(f"[LOAD] Reading from: {s3_path}")
        
        raw_df = spark.read.csv(s3_path, schema=BINANCE_SCHEMA)
        
        # 5. Transform
        print("[LOAD] Transforming data...")
        final_df = raw_df \
            .withColumn("event_time", (col("open_time_ms") / 1000).cast("timestamp")) \
            .drop("ignore", "open_time_ms", "close_time_ms") \
            .repartition(1) \
            .sortWithinPartitions("event_time")
        
        # 6. MERGE into Iceberg (atomic upsert for idempotency)
        row_count = merge_into_iceberg(spark, final_df, year, month)
        
        print(f"\n[LOAD] SUCCESS: Merged {row_count} rows for {year}-{month}!")
        return True
        
    except Exception as e:
        print(f"[LOAD] ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        if spark:
            spark.stop()
            print("[LOAD] Spark session stopped.")


def check_data_exists(year: str, month: str) -> bool:
    """Check if data for the given month exists in Iceberg table."""
    spark = None
    try:
        spark = get_spark_session()
        ensure_table_exists(spark)
        
        y = int(year)
        m = int(month)
        start_date = f"{y}-{m:02d}-01"
        if m == 12:
            end_date = f"{y+1}-01-01"
        else:
            end_date = f"{y}-{m+1:02d}-01"
        
        count = spark.sql(f"""
            SELECT 1 FROM {TABLE_NAME}
            WHERE event_time >= '{start_date}' AND event_time < '{end_date}'
            LIMIT 1
        """).count()
        
        return count > 0
        
    finally:
        if spark:
            spark.stop()


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Load transformed data into Iceberg table")
    subparsers = parser.add_subparsers(dest='command', required=True)
    
    # Load command
    load_parser = subparsers.add_parser('load', help='Load data into Iceberg')
    load_parser.add_argument("--symbol", type=str, default="BTCUSDT", help="Trading pair symbol")
    load_parser.add_argument("--year", type=str, required=True, help="Year (e.g., 2023)")
    load_parser.add_argument("--month", type=str, required=True, help="Month (e.g., 01)")
    
    # Check command
    check_parser = subparsers.add_parser('check', help='Check if data exists in Iceberg')
    check_parser.add_argument("--year", type=str, required=True, help="Year (e.g., 2023)")
    check_parser.add_argument("--month", type=str, required=True, help="Month (e.g., 01)")
    
    args = parser.parse_args()
    
    if args.command == 'load':
        success = run_load(
            symbol=args.symbol,
            year=args.year,
            month=args.month.zfill(2)
        )
        sys.exit(0 if success else 1)
        
    elif args.command == 'check':
        exists = check_data_exists(args.year, args.month.zfill(2))
        if exists:
            print("STATUS: EXISTS")
        else:
            print("STATUS: MISSING")
        sys.exit(0)


if __name__ == "__main__":
    main()
