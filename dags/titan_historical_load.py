"""
Titan Historical Load DAG
Loads data from raw bucket into Iceberg tables using MERGE for idempotency.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

# Add the loader module to path
sys.path.insert(0, '/opt/airflow/dags/repo/historical-batch-loader/src')

from load import run_load


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


def load_task_callable(symbol: str, year: str, month: str, **context):
    """
    Airflow task callable for loading data into Iceberg.
    Uses MERGE INTO for atomic upsert.
    """
    # Zero-pad month
    month_padded = str(month).zfill(2)
    
    print(f"[DAG] Loading {symbol} for {year}-{month_padded} into Iceberg")
    
    success = run_load(
        symbol=symbol,
        year=str(year),
        month=month_padded
    )
    
    if not success:
        raise Exception(f"Load failed for {symbol} {year}-{month_padded}")
    
    return f"Loaded {symbol} {year}-{month_padded}"


# Default DAG arguments
default_args = {
    'owner': 'titan',
    'start_date': datetime(2023, 1, 1),
    'retries': 2,
    'retry_delay': 120,  # 2 minutes (Spark jobs may need warmup)
}


# Create DAG
with DAG(
    dag_id='titan_historical_load',
    default_args=default_args,
    description='Load raw data from MinIO into Iceberg tables',
    schedule_interval=None,  # Triggered by backfill or manually
    catchup=False,
    max_active_runs=1,  # One Spark job at a time
    tags=['titan', 'load', 'historical', 'iceberg'],
) as dag:
    
    # Load coins from config
    coins = load_coins_config()
    
    # Create load task for each coin
    for symbol in coins:
        load_task = PythonOperator(
            task_id=f'load_{symbol.lower()}',
            python_callable=load_task_callable,
            op_kwargs={
                'symbol': symbol,
                'year': '{{ dag_run.conf.get("year", "2023") }}',
                'month': '{{ dag_run.conf.get("month", "01") }}',
            },
        )
