
# --- Dynamic mapping ---
from airflow import DAG
from airflow.decorators import task
from datetime import datetime

with DAG(
    dag_id="expand_dag",
    start_date=datetime(2025, 1, 1),
    schedule='@daily',  # remplace schedule_interval
    catchup=False,
    tags=["example"],
) as dag:
    
    @task
    def ids():
        return [1, 2, 3]
    #on recupere une liste (exemple des id)

    @task
    def process(i):
        print(f"Processing id {i}")
    
    #on execute process pour chaque valeur de la fonction ids    
    process.expand(i=ids())