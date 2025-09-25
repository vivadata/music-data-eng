from airflow.decorators import dag, task
from datetime import datetime, timedelta
from google.cloud import bigquery
import pandas as pd
from loguru import logger

PROJECT_ID = "music-data-eng"
DATASET_ID = "music_dataset"
TABLE_ID = "wikidata_artists"

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
    values = [v for v in values if v]  # enlève les NULL / None
    return [values[i:i+CHUNK_SIZE] for i in range(0, len(values), CHUNK_SIZE)]

@dag(
    dag_id="process_wikidata_artists_v2",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["wikidata", "artists"],
)
def process_wikidata_artists_dag():

    @task
    def extract_spotify_ids():
        client = bigquery.Client(project=PROJECT_ID)
        df = client.query(
            f"SELECT spotify_artist_id FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`"
        ).to_dataframe()
        ids = df["spotify_artist_id"].dropna().unique().tolist()
        return chunk_list(ids)

    @task
    def extract_deezer_ids():
        client = bigquery.Client(project=PROJECT_ID)
        df = client.query(
            f"SELECT deezer_artist_id FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`"
        ).to_dataframe()
        ids = df["deezer_artist_id"].dropna().unique().tolist()
        return chunk_list(ids)

    @task
    def extract_youtube_ids():
        client = bigquery.Client(project=PROJECT_ID)
        df = client.query(
            f"SELECT youtube_artist_id FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`"
        ).to_dataframe()
        ids = df["youtube_artist_id"].dropna().unique().tolist()
        return chunk_list(ids)

    @task
    def process_spotify(chunk: list):
        logger.info(f"Traitement Spotify : {len(chunk)} IDs")
        # traitement Spotify ici
        return len(chunk)

    @task
    def process_deezer(chunk: list):
        logger.info(f"Traitement Deezer : {len(chunk)} IDs")
        # traitement Deezer ici
        return len(chunk)

    @task
    def process_youtube(chunk: list):
        logger.info(f"Traitement YouTube : {len(chunk)} IDs")
        # traitement YouTube ici
        return len(chunk)

    # Pipeline
    process_spotify.expand(chunk=extract_spotify_ids())
    process_deezer.expand(chunk=extract_deezer_ids())
    process_youtube.expand(chunk=extract_youtube_ids())

dag_instance = process_wikidata_artists_dag()
