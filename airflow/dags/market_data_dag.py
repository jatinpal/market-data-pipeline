from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 30),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'market_data_pipeline',
    default_args=default_args,
    description='Daily market data ETL pipeline',
    schedule_interval='0 8 * * 1-5',  # 8 AM weekdays
    catchup=False,
    tags=['market_data', 'etl'],
)

# Task 1: Extract market data and upload to GCS
extract_task = BashOperator(
    task_id='extract_market_data',
    bash_command="""
    python3 /opt/airflow/project/airflow/scripts/extract.py \
        --path_tickers /opt/airflow/project/airflow/scripts/tickers.json \
        --path_extract /opt/airflow/project/data/raw/ \
        --business_date {{ ds }} \
        --upload_gcs \
        --gcs_bucket market-data-lake-alpha
    """,
    dag=dag,
)

# Task 2: Load GCS data to BigQuery
load_to_bq_task = BashOperator(
    task_id='load_to_bigquery',
    bash_command="""
    bq load \
        --source_format=NEWLINE_DELIMITED_JSON \
        --autodetect \
        --noreplace \
        raw_data.market_data_raw \
        'gs://market-data-lake-alpha/raw/price/{{ ds }}.json'
    """,
    dag=dag,
)

# Task 3: Run dbt transformations
dbt_run_task = BashOperator(
    task_id='dbt_transformations',
    bash_command="""
    cd /opt/airflow/project/dbt/market_data_dbt && \
    dbt run
    """,
    dag=dag,
)

# Set dependencies
extract_task >> load_to_bq_task >> dbt_run_task