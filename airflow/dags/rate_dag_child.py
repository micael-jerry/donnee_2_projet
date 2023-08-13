from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task

default_args = {
    "owner": "admin",
    "start_date": datetime.today() - timedelta(days=1)
}

with DAG(
        dag_id="get_s3_data",
        default_args=default_args,
        schedule_interval=None
) as dag:
    @task
    def start_extract():
        print("GET S3 DATA")

    start_extract()
