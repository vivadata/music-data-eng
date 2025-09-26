from airflow import DAG, Dataset
from airflow.decorators import task
from datetime import datetime
import pandas as pd
import os

# Dataset partagé
orders_dataset = Dataset("/home/airflow/data/orders_generated.csv")

with DAG(
    dag_id="producer_dag",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["example"],
) as dag:

    @task(outlets=[orders_dataset])  # 👈 signale qu'on produit ce dataset
    def generate_orders():
        os.makedirs("/opt/airflow/data", exist_ok=True)
        df = pd.DataFrame({
            "order_id": [1, 2, 3],
            "product_id": [101, 102, 103],
            "quantity": [2, 1, 5]
        })
        df.to_csv("/opt/airflow/data/orders_generated.csv", index=False)
        print("Orders CSV written!")
        
    @task
    def process_orders():
        df = pd.read_csv("/opt/airflow/data/orders_generated.csv")
        top_product = df.groupby("product_id")["quantity"].sum().idxmax()
        print(f"✅ Top product is {top_product}")

    generate_orders() >> process_orders() 