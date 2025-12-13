"""
Titan Historical Extract DAG
Downloads historical kline data from Binance and uploads to MinIO raw bucket.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

# Add the loader module to path
sys.path.insert(0, '/opt/airflow/dags/repo/historical-batch-loader/src')

from extract import run_extract


def load_coins_config():
    """Load active coins from coins.txt config file."""
    config_path = '/opt/airflow/dags/repo/historical-batch-loader/coins.txt'
    coins = []
    
    try:
        with open(config_path, 'r') as f:
            for line in f:
                line = line.strip()
                # Skip empty lines and comments
                if line and not line.startswith('#'):
                    coins.append(line)
    except FileNotFoundError:
        # Default to BTCUSDT if config not found
        coins = ['BTCUSDT']
    
    return coins


def extract_task_callable(symbol: str, year: str, month: str, **context):
    """
    Airflow task callable for extraction.
    Uses execution_date to determine which month to process.
    """
    # Zero-pad month
    month_padded = str(month).zfill(2)
    
    print(f"[DAG] Extracting {symbol} for {year}-{month_padded}")
    
    success = run_extract(
        symbol=symbol,
        year=str(year),
        month=month_padded,
        force=False  # Don't re-download if exists
    )
    
    if not success:
        raise Exception(f"Extraction failed for {symbol} {year}-{month_padded}")
    
    return f"Extracted {symbol} {year}-{month_padded}"


# Default DAG arguments
default_args = {
    'owner': 'titan',
    'start_date': datetime(2023, 1, 1),
    'retries': 2,
    'retry_delay': 60,  # 1 minute
}


# Create DAG
with DAG(
    dag_id='titan_historical_extract',
    default_args=default_args,
    description='Extract historical kline data from Binance to raw bucket',
    schedule_interval='@monthly',
    catchup=True,
    max_active_runs=1,  # Process one month at a time
    tags=['titan', 'extract', 'historical'],
) as dag:
    
    # Load coins from config
    coins = load_coins_config()
    
    # Create extract task for each coin
    for symbol in coins:
        extract_task = PythonOperator(
            task_id=f'extract_{symbol.lower()}',
            python_callable=extract_task_callable,
            op_kwargs={
                'symbol': symbol,
                'year': '{{ data_interval_start.year }}',
                'month': '{{ data_interval_start.month }}',
            },
        )
