from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.models.param import Param

with DAG(
    dag_id="branch_and_mapping_dag",
    start_date=datetime(2025, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=["example"],
    # Add the params definition here
    params={
        "sizes": Param(500, type="integer", description="Size parameter for branching")
    }
) as dag:
    # --- Branching ---
    @task.branch
    def route(**context):
        # Access params from context instead of dag.params
        size = context["params"]["sizes"]
        print(f"Size parameter: {size}")
        return "heavy" if size > 1000 else "light"
    heavy_task = BashOperator(task_id="heavy", bash_command="echo heavy")
    light_task = BashOperator(task_id="light", bash_command="echo light")
    # Call the branch task without parameters - it will get them from context
    route() >> [heavy_task, light_task]