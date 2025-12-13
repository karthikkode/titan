"""
Extract Module for Titan Historical ETL Pipeline.
Downloads historical kline data from Binance and uploads to MinIO raw bucket.
"""

import os
import sys
import argparse
import requests
import zipfile
import shutil
import boto3
from typing import Optional
from botocore.client import Config
from botocore.exceptions import ClientError

from config import (
    MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_REGION,
    RAW_BUCKET, BINANCE_BASE_URL, SHARED_DIR
)


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


def ensure_bucket_exists(bucket_name: str):
    """Ensures the MinIO bucket exists (creates if missing)."""
    print(f"[EXTRACT] Checking bucket: {bucket_name}")
    s3 = get_s3_client()
    try:
        s3.head_bucket(Bucket=bucket_name)
        print(f"[EXTRACT] Bucket '{bucket_name}' exists.")
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            print(f"[EXTRACT] Creating bucket '{bucket_name}'...")
            s3.create_bucket(Bucket=bucket_name)
            print(f"[EXTRACT] Bucket '{bucket_name}' created.")
        else:
            raise e


def download_from_binance(symbol: str, year: str, month: str) -> Optional[str]:
    """
    Downloads kline data ZIP from Binance and extracts CSV.
    
    Returns:
        Path to extracted CSV file, or None if data not found.
    """
    file_name = f"{symbol}-1m-{year}-{month}.zip"
    download_url = f"{BINANCE_BASE_URL}/{symbol}/1m/{file_name}"
    
    # Use shared directory if available, otherwise /tmp
    base_dir = SHARED_DIR if os.path.exists(SHARED_DIR) else "/tmp"
    local_zip_path = os.path.join(base_dir, file_name)
    extract_dir = os.path.join(base_dir, "extracted")
    
    print(f"[EXTRACT] Downloading: {download_url}")
    
    try:
        response = requests.get(download_url, stream=True, timeout=60)
        
        if response.status_code == 404:
            print(f"[EXTRACT] Data not found for {year}-{month}. Binance may not have this data yet.")
            return None
        
        response.raise_for_status()
        
        # Save ZIP file
        with open(local_zip_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        print(f"[EXTRACT] Downloaded to: {local_zip_path}")
        
        # Extract ZIP
        print(f"[EXTRACT] Extracting to: {extract_dir}")
        os.makedirs(extract_dir, exist_ok=True)
        
        with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)
        
        # Find the CSV file
        for f in os.listdir(extract_dir):
            if f.endswith('.csv'):
                csv_path = os.path.join(extract_dir, f)
                print(f"[EXTRACT] Extracted CSV: {csv_path}")
                return csv_path
        
        print("[EXTRACT] ERROR: No CSV found in ZIP!")
        return None
        
    except requests.RequestException as e:
        print(f"[EXTRACT] Download error: {e}")
        return None


def upload_to_raw_bucket(local_csv_path: str, symbol: str, year: str, month: str) -> str:
    """
    Uploads CSV to MinIO raw bucket with partitioned path.
    
    Returns:
        S3 path of uploaded file (s3a:// format for Spark).
    """
    s3_key = f"btc_trades/year={year}/month={month}/{symbol}-1m-{year}-{month}.csv"
    
    print(f"[EXTRACT] Uploading to: s3://{RAW_BUCKET}/{s3_key}")
    
    s3 = get_s3_client()
    s3.upload_file(local_csv_path, RAW_BUCKET, s3_key)
    
    s3_path = f"s3a://{RAW_BUCKET}/{s3_key}"
    print(f"[EXTRACT] Upload complete: {s3_path}")
    
    return s3_path


def cleanup_temp_files(symbol: str, year: str, month: str):
    """Cleanup temporary download and extraction files."""
    base_dir = SHARED_DIR if os.path.exists(SHARED_DIR) else "/tmp"
    
    # Remove extracted directory
    extract_dir = os.path.join(base_dir, "extracted")
    if os.path.exists(extract_dir):
        shutil.rmtree(extract_dir, ignore_errors=True)
        print(f"[EXTRACT] Cleaned up: {extract_dir}")
    
    # Remove ZIP file
    zip_file = os.path.join(base_dir, f"{symbol}-1m-{year}-{month}.zip")
    if os.path.exists(zip_file):
        os.remove(zip_file)
        print(f"[EXTRACT] Cleaned up: {zip_file}")


def check_raw_file_exists(symbol: str, year: str, month: str) -> bool:
    """Check if the raw file already exists in MinIO."""
    s3_key = f"btc_trades/year={year}/month={month}/{symbol}-1m-{year}-{month}.csv"
    
    s3 = get_s3_client()
    try:
        s3.head_object(Bucket=RAW_BUCKET, Key=s3_key)
        return True
    except ClientError:
        return False


def run_extract(symbol: str, year: str, month: str, force: bool = False) -> bool:
    """
    Main extract function.
    
    Args:
        symbol: Trading pair (e.g., BTCUSDT)
        year: Year as string (e.g., "2023")
        month: Month as zero-padded string (e.g., "01")
        force: If True, re-download even if file exists
        
    Returns:
        True if extraction successful, False otherwise.
    """
    print(f"\n{'='*60}")
    print(f"[EXTRACT] Starting extraction for {symbol} {year}-{month}")
    print(f"{'='*60}\n")
    
    # 1. Ensure bucket exists
    ensure_bucket_exists(RAW_BUCKET)
    
    # 2. Check if already extracted (skip unless forced)
    if not force and check_raw_file_exists(symbol, year, month):
        print(f"[EXTRACT] File already exists in raw bucket. Use --force to re-download.")
        return True
    
    # 3. Download from Binance
    local_csv_path = download_from_binance(symbol, year, month)
    if not local_csv_path:
        print(f"[EXTRACT] FAILED: Could not download data for {year}-{month}")
        return False
    
    try:
        # 4. Upload to raw bucket
        upload_to_raw_bucket(local_csv_path, symbol, year, month)
        
        print(f"\n[EXTRACT] SUCCESS: {symbol} {year}-{month} extracted to raw bucket!")
        return True
        
    finally:
        # 5. Cleanup temp files
        cleanup_temp_files(symbol, year, month)


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Extract historical kline data from Binance")
    parser.add_argument("--symbol", type=str, default="BTCUSDT", help="Trading pair symbol")
    parser.add_argument("--year", type=str, required=True, help="Year (e.g., 2023)")
    parser.add_argument("--month", type=str, required=True, help="Month (e.g., 01)")
    parser.add_argument("--force", action="store_true", help="Force re-download even if exists")
    
    args = parser.parse_args()
    
    success = run_extract(
        symbol=args.symbol,
        year=args.year,
        month=args.month.zfill(2),  # Ensure zero-padded
        force=args.force
    )
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
