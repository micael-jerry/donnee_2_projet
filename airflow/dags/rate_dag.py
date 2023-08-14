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

    update_s3_data_csv = TriggerDagRunOperator(
        task_id="trigger_update_s3_data_csv",
        trigger_dag_id="update_s3_data_csv"
    )

    start_extract() >> extract_transform_load_data_to_s3 >> update_s3_data_csv
