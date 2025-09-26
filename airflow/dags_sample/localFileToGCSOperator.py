from airflow import DAG
from airflow.decorators import task
from datetime import datetime
import os
from google.cloud import storage

BUCKET_NAME = "monbucketoliviergoz"
LOCAL_DATA_PATH = "/opt/airflow/data"   # monté via docker-compose

@task
def list_csv_files():
    """Retourne la liste des chemins CSV dans le dossier local."""
    files = [
        os.path.join(LOCAL_DATA_PATH, f)
        for f in os.listdir(LOCAL_DATA_PATH)
        if f.endswith(".csv")
    ]
    print(f"Fichiers trouvés : {files}")
    return files

@task
def upload_to_gcs(file_path: str):
    """Upload un fichier unique dans GCS."""
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    filename = os.path.basename(file_path)
    blob = bucket.blob(f"data/{filename}")  # sous-dossier "data/" dans ton bucket
    blob.upload_from_filename(file_path)

    print(f"✅ Uploadé : {file_path} -> gs://{BUCKET_NAME}/data/{filename}")

with DAG(
    dag_id="upload_csvs_taskmap",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    files = list_csv_files()
    upload_to_gcs.expand(file_path=files)