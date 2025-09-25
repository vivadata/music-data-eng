from airflow.decorators import dag, task
from datetime import datetime
from google.cloud import bigquery
import pandas as pd
from datetime import timedelta
import requests
from loguru import logger
import time


PROJECT_ID = "music-data-eng"
DATASET_ID = "music_dataset"
TABLE_ID_WIKIDATA = "wikidata_artists"
OUTPUT_TABLE = "deezer_results"

CHUNK_SIZE = 500  

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def chunk_list(values):
    """Découpe une liste en morceaux de taille CHUNK_SIZE"""
    #values = [v for v in values if v]  # enlève les NULL / None
    #return [values[i:i+CHUNK_SIZE] for i in range(0, len(values), CHUNK_SIZE)]
    return [values[0:50]]

@dag(
    dag_id="wikidata_to_deezer_processing",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["wikidata", "artists"],
)
def process_wikidata_artists_dag():

    @task
    def extract_deezer_ids():
        client = bigquery.Client(project=PROJECT_ID)
        df = client.query(
            f"SELECT deezer_artist_id FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID_WIKIDATA}`"
        ).to_dataframe()
        ids = df["deezer_artist_id"].dropna().unique().tolist()
        return chunk_list(ids)

  
   
    @task
    def process_deezer(chunk: list):
        logger.info(f"Traitement Deezer : {len(chunk)} IDs")
        results = []
        for i, artist_id in enumerate(chunk, start=1):
            url = f"https://api.deezer.com/artist/{artist_id}"
            resp = requests.get(url)
            if resp.status_code != 200:
                continue
            data = resp.json()
            if "error" in data:
                continue

            results.append({
                "id": str(artist_id),
                "name": data.get("name"),
                "link": data.get("link"),
                "nb_fan": data.get("nb_fan"),
            })

            # respect du quota Deezer (50 req / 5s)
            if i % 50 == 0:
                import time
                time.sleep(5)

        return results

    @task
    def load_results_to_bq(results: list):
        """Charge les résultats dans BigQuery (partitionné par ingestion_date)"""
        if not results:
            return

        df = pd.DataFrame(results)
        df["ingestion_date"] = datetime.now().date()

        client = bigquery.Client(project=PROJECT_ID)
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{OUTPUT_TABLE}"

        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="ingestion_date"
            ),
        )

        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        logger.info(f"{len(df)} lignes chargées dans {table_id}")
  
    # Pipeline
    chunks=extract_deezer_ids()
    load_results_to_bq.expand(results=process_deezer.expand(chunk=chunks))
   
dag_instance = process_wikidata_artists_dag()
