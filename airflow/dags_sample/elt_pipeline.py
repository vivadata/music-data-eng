from datetime import datetime
import pandas as pd
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from loguru import logger
from airflow.decorators import task
from google.cloud import bigquery



# Définition du DAG
with DAG(
    dag_id="elt_pipeline",
    start_date=datetime(2025, 1, 1),  # date de départ
    schedule="*/2 * * * *",                # exécution quotidienne
    catchup=False,                     # ne pas rattraper le passé
    tags=["demo"],
) as dag:

    @task()
    def ingest_csv():
        url = "https://storage.googleapis.com/schoolofdata-datasets/Data-Analysis.Data-Visualization/CO2_per_capita.csv"
    
        df = pd.read_csv(url, sep=';')
        df.columns = [
            col.strip()                 # enlever espaces en début/fin
           .lower()                 # mettre en minuscule
           .replace(" ", "_")       # remplacer espace par _
           .replace("(", "")        # enlever (
           .replace(")", "")        # enlever )
           .replace("-", "_")       # remplacer - par _
        for col in df.columns
        ]
        client = bigquery.Client()
        table_id = "advance-vector-468708-t0.batch67_sql.co2_table"
        job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )
    
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        return "Loaded to BQ"
    
    ingest_csv_task = ingest_csv()