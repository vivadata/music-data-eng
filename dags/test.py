from airflow import DAG
from airflow.decorators import task
from datetime import datetime
from google.cloud import bigquery
import pandas as pd
from datetime import timedelta

CSV_FILE_PATH = "/opt/airflow/data/products.csv"
PROJECT_ID = "music-data-eng"
DATASET_ID = "music_dataset"
TABLE_ID = "product"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="upload_product_csv_to_bq_taskflow",
    start_date=datetime(2025, 1, 1),  # date de départ
    schedule="*/2 * * * *",                # exécution quotidienne
    catchup=False,                     # ne pas rattraper le passé
    tags=["demo"],
) as dag:

    @task
    def upload_csv_to_bq():
        # Lire le CSV
        df = pd.read_csv(CSV_FILE_PATH)
        
        # Créer le client BigQuery
        client = bigquery.Client(project=PROJECT_ID)
        
        # Référence de la table
        table_ref = client.dataset(DATASET_ID).table(TABLE_ID)
        
        # Configurer l'import
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        
        # Charger le DataFrame dans BigQuery
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()  # Attendre que le job se termine
        print(f"{len(df)} lignes chargées dans {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}")

    upload_csv_to_bq()
