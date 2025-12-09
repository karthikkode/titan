import argparse
import sys
import os
import requests
import zipfile
import shutil
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, DoubleType, StringType
from pyspark.sql.functions import col, from_unixtime, to_timestamp

# --- SCHEMA DEFINITION ---
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


def init_minio_bucket(bucket_name):
    """Ensures the MinIO bucket exists (creates if missing)."""
    print(f"--- Checking MinIO Bucket: {bucket_name} ---")
    try:
        s3 = boto3.client(
            's3',
            endpoint_url="http://minio:9000",
            aws_access_key_id="admin",
            aws_secret_access_key="password",
            config=Config(signature_version='s3v4'),
            region_name="us-east-1"
        )
        try:
            s3.head_bucket(Bucket=bucket_name)
            print(f"Bucket '{bucket_name}' already exists.")
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                print(f"Bucket '{bucket_name}' does not exist. Creating...")
                s3.create_bucket(Bucket=bucket_name)
                print(f"Bucket '{bucket_name}' created.")
            else:
                raise e
    except Exception as e:
        print(f"Error initializing MinIO bucket: {e}")


def get_spark_session():
    """Creates a Spark Session configured for Titan Lakehouse with Iceberg and raw S3A support."""
    # Added hadoop-aws for 's3a://' support (reading raw CSVs from MinIO)
    packages = [
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2",
        "org.apache.iceberg:iceberg-aws-bundle:1.4.2",
        "org.apache.hadoop:hadoop-aws:3.3.4"
    ]
    
    return SparkSession.builder \
        .appName("Titan-Historical-Loader") \
        .master("spark://spark-master:7077") \
        .config("spark.driver.host", "titan-airflow-scheduler") \
        .config("spark.driver.bindAddress", "0.0.0.0") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.jars.packages", ",".join(packages)) \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.titan", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.titan.type", "rest") \
        .config("spark.sql.catalog.titan.uri", "http://iceberg-catalog:8181") \
        .config("spark.sql.catalog.titan.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.sql.catalog.titan.warehouse", "s3://warehouse") \
        .config("spark.sql.catalog.titan.s3.endpoint", "http://minio:9000") \
        .config("spark.sql.catalog.titan.s3.access-key-id", "admin") \
        .config("spark.sql.catalog.titan.s3.secret-access-key", "password") \
        .config("spark.sql.catalog.titan.s3.path-style-access", "true") \
        .config("spark.sql.catalog.titan.client.region", "us-east-1") \
        .config("spark.sql.catalog.titan.s3.ssl.enabled", "false") \
        .config("spark.network.timeout", "600s") \
        .config("spark.executor.heartbeatInterval", "120s") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()


def download_and_extract(symbol, year, month):
    file_name = f"{symbol}-1m-{year}-{month}.zip"
    download_url = f"https://data.binance.vision/data/spot/monthly/klines/{symbol}/1m/{file_name}"
    base_dir = "/opt/shared" if os.path.exists("/opt/shared") else "/tmp"
    local_zip_path = f"{base_dir}/{file_name}"
    extract_dir = f"{base_dir}/extracted"
    
    print(f"--- 1. Downloading from: {download_url} ---")
    try:
        response = requests.get(download_url, stream=True)
        if response.status_code == 404:
            print(f"Error: Data not found for {year}-{month}. Skipping.")
            return None
        with open(local_zip_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        print(f"--- 2. Extracting to {extract_dir} ---")
        with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)
            
        for f in os.listdir(extract_dir):
            if f.endswith('.csv'):
                return os.path.join(extract_dir, f)
        return None
    except Exception as e:
        print(f"Error downloading/extracting: {e}")
        return None


def upload_to_raw(local_csv_path, symbol, year, month):
    """Uploads the CSV to MinIO raw bucket."""
    s3_key = f"btc_trades/year={year}/month={month}/{symbol}-1m-{year}-{month}.csv"
    s3_path = f"s3a://raw/{s3_key}"
    print(f"--- 3a. Uploading to Raw S3: {s3_path} ---")
    
    s3 = boto3.client(
        's3',
        endpoint_url="http://minio:9000",
        aws_access_key_id="admin",
        aws_secret_access_key="password",
        config=Config(signature_version='s3v4'),
        region_name="us-east-1"
    )
    try:
        s3.upload_file(local_csv_path, "raw", s3_key)
        print("Upload successful.")
        return s3_path
    except Exception as e:
        print(f"Failed to upload to S3: {e}")
        raise e


def check_data_exists(spark, year, month):
    """Checks if data for the given month already exists in the Iceberg table."""
    try:
        y = int(year)
        m = int(month)
        # Define start/end for the month
        start_date = f"{y}-{m:02d}-01"
        if m == 12:
            end_date = f"{y+1}-01-01"
        else:
            end_date = f"{y}-{m+1:02d}-01"
            
        print(f"--- QUERY: SELECT 1 FROM titan.crypto.btc_trades WHERE event_time >= '{start_date}' AND event_time < '{end_date}' ---")
        
        # Ensure namespace exists to avoid error on first run
        spark.sql("CREATE NAMESPACE IF NOT EXISTS titan.crypto")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS titan.crypto.btc_trades (
                open double, high double, low double, close double, volume double,
                quote_asset_volume double, trades long, taker_buy_base double,
                taker_buy_quote double, event_time timestamp
            ) USING iceberg PARTITIONED BY (months(event_time))
        """)
        
        # Query
        count = spark.sql(f"""
            SELECT 1 FROM titan.crypto.btc_trades 
            WHERE event_time >= '{start_date}' AND event_time < '{end_date}' 
            LIMIT 1
        """).count()
        
        print(f"--- QUERY RESULT: {count} rows found. ---")
        return count > 0
    except Exception as e:
        print(f"Error checking data existence (might be first run): {e}")
        return False


def process_with_spark(s3_csv_path, year, month):
    spark = get_spark_session()
    
    # 0. Check Idempotency
    if check_data_exists(spark, year, month):
        print(f"SKIPPING: Data for {year}-{month} already exists in Iceberg.")
        spark.stop()
        return

    print(f"--- 3b. Reading Raw CSV from S3: {s3_csv_path} ---")
    raw_df = spark.read.csv(s3_csv_path, schema=BINANCE_SCHEMA)
    
    final_df = raw_df \
        .withColumn("event_time", (col("open_time_ms") / 1000).cast("timestamp")) \
        .drop("ignore", "open_time_ms", "close_time_ms") \
        .repartition(1) \
        .sortWithinPartitions("event_time")
        
    print("--- 4. Writing to Iceberg Table (titan.crypto.btc_trades) ---")
    final_df.writeTo("titan.crypto.btc_trades").append()
    print(f"SUCCESS: Data for {year}-{month} loaded into Iceberg!")
    spark.stop()


def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest='command', required=True)

    # Common args
    parent_parser = argparse.ArgumentParser(add_help=False)
    parent_parser.add_argument("--symbol", type=str, default="BTCUSDT")
    parent_parser.add_argument("--year", type=str, required=True)
    parent_parser.add_argument("--month", type=str, required=True)

    # 1. CHECK Command
    check_parser = subparsers.add_parser('check', parents=[parent_parser], help='Check if data exists')

    # 2. INGEST Command
    ingest_parser = subparsers.add_parser('ingest', parents=[parent_parser], help='Download and Upload to Raw')

    # 3. PROCESS Command
    process_parser = subparsers.add_parser('process', parents=[parent_parser], help='Process Raw Data to Iceberg')

    args = parser.parse_args()
    print(f"--- EXECUTING COMMAND: {args.command.upper()} [{args.year}-{args.month}] ---")

    if args.command == 'check':
        spark = get_spark_session()
        exists = check_data_exists(spark, args.year, args.month)
        spark.stop()
        if exists:
            print("STATUS: EXISTS")
            # Return code 0 (Success) but output "EXISTS" for ShortCircuit to catch?
            # Or use exit code. Let's use Output "STATUS: EXISTS".
        else:
            print("STATUS: MISSING")

    elif args.command == 'ingest':
        init_minio_bucket("raw")
        
        # Download
        local_csv_path = download_and_extract(args.symbol, args.year, args.month)
        if not local_csv_path:
            print(f"No data found for {args.year}-{args.month}")
            sys.exit(1) # Fail the task if no data (optional, or skip?)
        
        # Upload
        try:
            upload_to_raw(local_csv_path, args.symbol, args.year, args.month)
        finally:
            # Cleanup
            extract_dir = os.path.dirname(local_csv_path)
            shutil.rmtree(extract_dir, ignore_errors=True)
            base_dir = os.path.dirname(extract_dir)
            try:
                zip_name = f"{args.symbol}-1m-{args.year}-{args.month}.zip"
                os.remove(os.path.join(base_dir, zip_name))
            except Exception:
                pass

    elif args.command == 'process':
        # Reconstruct the expected S3 path
        s3_key = f"btc_trades/year={args.year}/month={args.month}/{args.symbol}-1m-{args.year}-{args.month}.csv"
        s3_path = f"s3a://raw/{s3_key}"
        
        # Process
        process_with_spark(s3_path, args.year, args.month)

if __name__ == "__main__":
    main()