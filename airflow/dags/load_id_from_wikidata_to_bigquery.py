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
CHUNK_SIZE = 5000  # nombre de lignes par batch pour BigQuery

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
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
def fetch_wikidata_artists_data_dag():

    @task
    def fetch_wikidata_data() -> pd.DataFrame:
        """
        Récupère les données depuis Wikidata et retourne un DataFrame Pandas.
        """
        logger.info("Démarrage de la récupération Wikidata")
        
        # Requête SPARQL
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

        # Exécution de la requête
        response = requests.get(url, params={"query": query}, headers=headers)
        response.raise_for_status()

        # Lecture des résultats dans un DataFrame
        df = pd.read_csv(io.StringIO(response.text))
        logger.info(f"La requête Wikidata a retourné {df.shape[0]} lignes et {df.shape[1]} colonnes")
        return df

    @task
    def write_to_bigquery(df: pd.DataFrame):
        """
        Charge le DataFrame dans BigQuery par lots pour gérer les gros volumes.
        """
        if df.empty:
            logger.warning("Aucun résultat à charger")
            return

        client = bigquery.Client(project=PROJECT_ID)
        table_id = client.dataset(DATASET_ID).table(OUTPUT_TABLE)

        # Configuration du job BigQuery
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

        # Découpage en chunks
        total_rows = len(df)
        logger.info(f"Chargement par chunks de {CHUNK_SIZE} lignes (total {total_rows})")

        for start in range(0, total_rows, CHUNK_SIZE):
            end = min(start + CHUNK_SIZE, total_rows)
            chunk_df = df.iloc[start:end]
            logger.info(f"Chargement du chunk lignes {start} à {end}")
            job = client.load_table_from_dataframe(chunk_df, table_id, job_config=job_config if start == 0 else None)
            job.result()

        logger.info(f"Chargement terminé : {total_rows} lignes insérées dans {table_id}")
  
    # Définition de la pipeline
    df = fetch_wikidata_data()
    write_to_bigquery(df)

dag_instance = fetch_wikidata_artists_data_dag()