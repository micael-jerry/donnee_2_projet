from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator

from get_data_for_analyzed import get_data_to_be_analyzed

default_args = {
    "owner": "admin",
    "start_date": datetime.today() - timedelta(days=1)
}

with DAG(
        dag_id="get_data_and_transform_for_analyzed",
        default_args=default_args,
        schedule_interval=None
) as dag:
    @task
    def start_get_data_for_analyzed():
        print("Retrieve the data to be analyzed")

    get_data_and_transform = PythonOperator(
        task_id="get_data_and_transform",
        python_callable=get_data_to_be_analyzed
    )

    start_get_data_for_analyzed() >> get_data_and_transform