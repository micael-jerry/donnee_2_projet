from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

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
    
    trigger_get_data_and_transform_for_analyzed_in_data_update_dag = TriggerDagRunOperator(
        task_id="trigger_get_data_and_transform_for_analyzed",
        trigger_dag_id="get_data_and_transform_for_analyzed"
    )

    start_update() >> update_data >> trigger_get_data_and_transform_for_analyzed_in_data_update_dag
