import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta

default_args = {
    'owner': 'olivier',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 9, 26),
}

DBT_DIR = "/opt/airflow/dbt"

with DAG(
    dag_id='dbt_artists_union',
    start_date=datetime(2025, 1, 1),
    schedule="0 2 * * *",   # daily at 2 AM
    catchup=False,
    default_args=default_args,
    tags=['dbt', 'music'],
) as dag:


    run_dbt_artists_union = BashOperator(
        task_id='run_dbt_artists_union',
        bash_command=f"""
        cd {DBT_DIR}
        DBT_PROFILES_DIR=$(pwd) dbt run --models artists_union --target prod
        """,
    )

    run_dbt_artists_union
