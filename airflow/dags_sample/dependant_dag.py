from airflow import DAG
from airflow.decorators import task
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime


with DAG(
    dag_id="dependent_dag",
    start_date=datetime(2025, 1, 1),
    schedule='@daily',
    catchup=False,
) as dag:

    # Sensor qui attend la fin de source_dag.finish_task
    wait_for_source = ExternalTaskSensor(
        task_id="wait_for_source",
        external_dag_id="source_dag",
        external_task_id="finish_task",
        mode="reschedule",
        poke_interval=30,
        timeout=300,
    )

    @task
    def run_after_source():
        print("Dependent DAG running after source DAG!")

    run_after = run_after_source()

    wait_for_source >> run_after