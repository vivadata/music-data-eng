from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from google.cloud import bigquery
import pandas as pd
from datetime import timedelta

CSV_FILE_PATH = "/opt/airflow/data/product.csv"
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
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["example", "bigquery"],
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
