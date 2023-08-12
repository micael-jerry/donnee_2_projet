from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator

from donnee_2_rates_etl import extract_transform_data
from donnee_2_rates_etl import load_data

default_args = {
    "owner": "admin",
    "start_date": datetime.today() - timedelta(days=1)
}

with DAG(
        dag_id="rates_changes_data",
        default_args=default_args,
        schedule_interval="45 12 * * *"
) as dag:
    @task
    def start_extract():
        print("Start extract rates data")

    extract__and_transform_data_to_local = PythonOperator(
        task_id="extract_data_to_local",
        python_callable=extract_transform_data
    )

    load_data_to_s3_bucket = PythonOperator(
        task_id="load_data_to_s3_bucket",
        python_callable=load_data
    )

    start_extract() >> extract__and_transform_data_to_local >> load_data_to_s3_bucket

with DAG()