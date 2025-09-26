from airflow import DAG
from airflow.decorators import task
from datetime import datetime

with DAG(
    dag_id="source_dag",
    start_date=datetime(2025, 1, 1),
    schedule='@daily',
    catchup=False,
) as dag:

    @task
    def finish_task():
        print("Source DAG finished!")
        

    finish_task() 