from airflow.decorators import dag, task
from datetime import datetime, timedelta
from google.cloud import bigquery
import pandas as pd
import requests
from loguru import logger
import io


PROJECT_ID = "music-data-eng"
DATASET_ID = "music_dataset"
OUTPUT_TABLE = "wikidata_artists"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="wikidata_pipeline",
    start_date=datetime(2025, 1, 1),  
    schedule="@daily",                
    catchup=False,  
    default_args=default_args,                   
    tags=["wikidata", "artists"],
)
def process_wikidata_artists_dag():

    @task
    def process_wikidata():
        """Fetch data from Wikidata and return a DataFrame."""
        logger.info(f"Démarrage du traitement Wikidata")
        query = """
        SELECT 
            ?item 
            ?itemLabel 
            ?deezer_artist_id 
            ?spotify_artist_id 
            ?youtube_artist_id 
            ?artist_official_url 
        WHERE {
            ?item wdt:P2722 ?deezer_artist_id.
            OPTIONAL { ?item wdt:P1902 ?spotify_artist_id. }
            OPTIONAL { ?item wdt:P2397 ?youtube_artist_id. }
            OPTIONAL { ?item wdt:P856 ?artist_official_url. }
            SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
        }
        """
        url = "https://query.wikidata.org/sparql"
        headers = {"Accept": "text/csv"}
        response = requests.get(url, params={"query": query}, headers=headers)
        response.raise_for_status()

        df = pd.read_csv(io.StringIO(response.text))
        logger.info(f"Wikidata query returned {df.shape[0]} rows with {df.shape[1]} columns")
        return df
        
    @task
    def load_results_to_bq(df: pd.DataFrame):
        """Charge les résultats dans BigQuery"""
        if df.empty:
            logger.warning("Aucun résultat à charger")
            return

        client = bigquery.Client(project=PROJECT_ID)
        table_id = client.dataset(DATASET_ID).table(OUTPUT_TABLE)

        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("item", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("itemLabel", "STRING"),
                bigquery.SchemaField("deezer_artist_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("spotify_artist_id", "STRING"), 
                bigquery.SchemaField("youtube_artist_id", "STRING"),
                bigquery.SchemaField("artist_official_url", "STRING"),
            ],
            write_disposition="WRITE_TRUNCATE",
        )

        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        logger.info(f"{len(df)} lignes chargées dans {table_id}")
  
    # Pipeline
    df=process_wikidata()
    load_results_to_bq(df)

dag_instance = process_wikidata_artists_dag()