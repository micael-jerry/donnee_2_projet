from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator

from data_update_service import update_data

default_args = {
    "owner": "admin",
    "start_date": datetime.today() - timedelta(days=1)
}

with DAG(
        dag_id="update_s3_data_csv",
        default_args=default_args,
        schedule_interval=None
) as dag:
    @task
    def start_update():
        print("UPDATE DATA")

    update_data = PythonOperator(
        task_id="update_data",
        python_callable=update_data
    )

    start_update() >> update_data
