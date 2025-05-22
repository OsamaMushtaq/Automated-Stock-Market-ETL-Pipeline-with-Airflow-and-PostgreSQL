
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 22),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'stock_data_etl',
    default_args=default_args,
    description='ETL DAG to fetch stock data hourly',
    schedule_interval='@hourly',
    catchup=False,
) as dag:

    run_etl = BashOperator(
        task_id='run_stock_etl_script',
        bash_command='python /opt/airflow/etl/fetch_stock_data.py'
    )