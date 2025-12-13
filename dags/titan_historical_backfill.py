"""
Titan Historical Backfill DAG
Orchestrates Extract and Load DAGs for missing months.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
import sys

# Add the loader module to path
sys.path.insert(0, '/opt/airflow/dags/repo/historical-batch-loader/src')


def load_coins_config():
    """Load active coins from coins.txt config file."""
    config_path = '/opt/airflow/dags/repo/historical-batch-loader/coins.txt'
    coins = []
    
    try:
        with open(config_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    coins.append(line)
    except FileNotFoundError:
        coins = ['BTCUSDT']
    
    return coins


# Default DAG arguments
default_args = {
    'owner': 'titan',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}


with DAG(
    dag_id='titan_historical_backfill',
    default_args=default_args,
    description='Orchestrates Extract and Load for historical data backfill',
    schedule_interval='@monthly',
    catchup=True,
    max_active_runs=1,  # Process one month at a time (sequential)
    tags=['titan', 'backfill', 'orchestrator'],
) as dag:
    
    # Trigger Extract DAG
    trigger_extract = TriggerDagRunOperator(
        task_id='trigger_extract',
        trigger_dag_id='titan_historical_extract',
        execution_date='{{ data_interval_start }}',
        wait_for_completion=True,
        poke_interval=30,  # Check every 30 seconds
        reset_dag_run=True,  # Reset if already exists
        allowed_states=['success'],
        failed_states=['failed'],
    )
    
    # Trigger Load DAG with year/month params
    trigger_load = TriggerDagRunOperator(
        task_id='trigger_load',
        trigger_dag_id='titan_historical_load',
        conf={
            'year': '{{ data_interval_start.year }}',
            'month': '{{ data_interval_start.month }}',
        },
        wait_for_completion=True,
        poke_interval=60,  # Check every minute (Spark jobs are slower)
        allowed_states=['success'],
        failed_states=['failed'],
    )
    
    trigger_extract >> trigger_load
