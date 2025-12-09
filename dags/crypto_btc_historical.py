from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import ShortCircuitOperator
from datetime import datetime
import subprocess

def check_if_data_exists_callable(year, month):
    try:
        # Run main.py check command
        result = subprocess.run(
            [
                "python", 
                "/opt/airflow/dags/repo/historical-batch-loader/src/main.py", 
                "check", 
                "--year", str(year), 
                "--month", str(month)
            ],
            capture_output=True,
            text=True,
            check=True
        )
        print("Check Output:", result.stdout)
        
        if "STATUS: EXISTS" in result.stdout:
            print(f"Data for {year}-{month} already exists. Skipping.")
            return False # Skip downstream
        else:
            print(f"Data for {year}-{month} is missing. Proceeding.")
            return True # Proceed
            
    except subprocess.CalledProcessError as e:
        print(f"Error checking data: {e}")
        # If check fails (e.g. spark error), assuming missing or retry?
        # Let's assume proceed so we can retry ingestion? 
        # Or fail?
        # If Spark fails to check, maybe safer to proceed and let ingestion try/fail?
        # But for now let's return True to be safe (idempotent writes overwrite anyway?)
        # Wait, appends duplicate? No `check_data_exists` in `process_with_spark` handles it too.
        # But this is for skipping download.
        return True

default_args = {
    'owner': 'titan',
    'start_date': datetime(2023, 1, 1), # Start form Jan 2023
    'retries': 1,
}

with DAG(
    'titan_historical_backfill',
    default_args=default_args,
    schedule_interval='@monthly', 
    catchup=True, # <--- The Magic Switch
    max_active_runs=1, # SAFETY: Run 1 month at a time (Sequential) to save RAM
) as dag:

    # Task 1: Check if Data Exists (Short Circuit)
    check_task = ShortCircuitOperator(
        task_id='check_if_exists',
        python_callable=check_if_data_exists_callable,
        op_kwargs={
            'year': "{{ execution_date.year }}",
            'month': "{{ execution_date.month }}",
        },
        ignore_downstream_trigger_rules=False, # If False, skipped tasks skip downstream
    )

    # Task 2: Ingest Raw Data (Bash)
    ingest_task = BashOperator(
        task_id='ingest_raw',
        bash_command="""
        python /opt/airflow/dags/repo/historical-batch-loader/src/main.py ingest \
        --year {{ data_interval_start.format('YYYY') }} \
        --month {{ data_interval_start.format('MM') }} \
        --symbol BTCUSDT
        """
    )

    # Task 3: Process to Iceberg (Bash)
    process_task = BashOperator(
        task_id='process_to_iceberg',
        bash_command="""
        python /opt/airflow/dags/repo/historical-batch-loader/src/main.py process \
        --year {{ data_interval_start.format('YYYY') }} \
        --month {{ data_interval_start.format('MM') }} \
        --symbol BTCUSDT
        """
    )

    check_task >> ingest_task >> process_task