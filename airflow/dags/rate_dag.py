from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from rate_etl import extract_transform_load_data

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


    extract_transform_load_data_to_s3 = PythonOperator(
        task_id="extract_transform_load_data_to_s3",
        python_callable=extract_transform_load_data
    )

    trigger_get_data_s3 = TriggerDagRunOperator(
        task_id="trigger_get_s3_data",
        trigger_dag_id="get_s3_data"
    )

    start_extract() >> extract_transform_load_data_to_s3
