"""
Config module for Titan Historical ETL Pipeline.
Shared constants and configurations across Extract and Load modules.
"""

# MinIO Configuration
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password"
MINIO_REGION = "us-east-1"

# Bucket Names
RAW_BUCKET = "raw"
WAREHOUSE_BUCKET = "warehouse"

# Iceberg Configuration
ICEBERG_CATALOG_URI = "http://iceberg-catalog:8181"
ICEBERG_WAREHOUSE = "s3://warehouse"

# Spark Configuration
SPARK_MASTER = "spark://spark-master:7077"
SPARK_DRIVER_HOST = "titan-airflow-scheduler"

# Binance Data URL Template
BINANCE_BASE_URL = "https://data.binance.vision/data/spot/monthly/klines"

# Table Configuration
TABLE_NAME = "titan.crypto.btc_trades"
NAMESPACE = "titan.crypto"

# Shared directory for temp files
SHARED_DIR = "/opt/shared"
