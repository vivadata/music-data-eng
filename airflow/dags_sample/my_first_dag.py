from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Définition du DAG
with DAG(
    dag_id="my_first_dag",
    start_date=datetime(2025, 1, 1),  # date de départ
    schedule="*/2 * * * *",                # exécution quotidienne
    catchup=False,                     # ne pas rattraper le passé
    tags=["demo"],
) as dag:

    # Tâche 1 : Python avec TaskFlow
    @task()
    def create_user():
        return 'Anto'  # peut renvoyer une valeur pour XCom
    
    create_user_ = create_user() 

    @task()
    def say_hello_user(user):
        print(f'Hello {user}')
        
    say_hello = say_hello_user(create_user_)
    
        # Tâche 2 : BashOperator
    task3 = BashOperator(
        task_id="B",
        bash_command="echo 'B'"
    )
    
        # Tâche 2 : BashOperator
    task4 = BashOperator(
        task_id="say_goodbye",
        bash_command="echo 'Fin du DAG ✅'"
    )

    # Chaînage correct TaskFlow + BashOperator
    # TaskFlow API renvoie un _XComArg
    create_user_  >> [say_hello, task3] >> task4