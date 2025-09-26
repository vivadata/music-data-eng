from airflow.decorators import dag, task
from datetime import datetime, timedelta
from google.cloud import bigquery
import pandas as pd
import requests
from loguru import logger

PROJECT_ID = "music-data-eng"
DATASET_ID = "music_dataset"
TABLE_ID_WIKIDATA = "wikidata_artists"
OUTPUT_TABLE = "deezer_artists"
CHUNK_SIZE = 500  

# ---- Arguments par défaut pour le DAG ----
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

def chunk_list(values):
    """Découpe une liste en sous-listes de taille CHUNK_SIZE"""
    values = [v for v in values if v]  # suppression des NULL / None
    return [values[i:i+CHUNK_SIZE] for i in range(0, len(values), CHUNK_SIZE)]

@dag(
    dag_id="deezer_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["deezer", "artists"],
)
def fetch_deezer_artists_data_dag():

    @task
    def extract_deezer_ids():
        """Extrait les identifiants Deezer depuis la table Wikidata dans BigQuery"""
        client = bigquery.Client(project=PROJECT_ID)
        df = client.query(
            f"SELECT deezer_artist_id FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID_WIKIDATA}`"
        ).to_dataframe()
        ids = df["deezer_artist_id"].dropna().unique().tolist()
        logger.info(f"{len(ids)} identifiants Deezer extraits depuis Wikidata")
        return chunk_list(ids)
   
    @task(max_active_tis_per_dag=1)
    def fetch_deezer_data(chunk: list):
        """Appelle l'API Deezer pour enrichir les informations artistes d'un chunk d'identifiants"""
        logger.info(f"Traitement d'un chunk Deezer : {len(chunk)} identifiants à interroger")
        results = []
        for i, artist_id in enumerate(chunk, start=1):
            url = f"https://api.deezer.com/artist/{artist_id}"
            resp = requests.get(url)
            if resp.status_code != 200:
                logger.warning(f"Échec de la requête pour l'identifiant Deezer {artist_id} (code {resp.status_code})")
                continue
            data = resp.json()
            if "error" in data:
                logger.warning(f"Erreur Deezer renvoyée pour l'ID {artist_id}")
                continue

            results.append({
                "deezer_artist_id": str(artist_id),
                "deezer_artist_name": data.get("name"),
                "deezer_artist_url": data.get("link"),
                "deezer_artist_total_followers": data.get("nb_fan"),
            })

            # Respect du quota Deezer (50 requêtes par 5 secondes)
            if i % 50 == 0:
                import time
                logger.info("Quota atteint : pause de 5 secondes pour respecter les limites Deezer")
                time.sleep(5)

        logger.info(f"Chunk Deezer traité : {len(results)} résultats valides")
        return results

    @task
    def write_to_bigquery(results: list):
        """Charge les résultats Deezer dans BigQuery (partitionnés par date d'ingestion)"""
        if not results:
            logger.warning("Aucun résultat à charger dans BigQuery (chunk vide ou erreurs).")
            return

        df = pd.DataFrame(results)
        df["ingestion_date"] = datetime.now().date()

        client = bigquery.Client(project=PROJECT_ID)
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{OUTPUT_TABLE}"

        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("deezer_artist_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("deezer_artist_name", "STRING"),
                bigquery.SchemaField("deezer_artist_url", "STRING"),
                bigquery.SchemaField("deezer_artist_total_followers", "INTEGER"),
                bigquery.SchemaField("ingestion_date", "DATE"),
            ],
            write_disposition="WRITE_APPEND",
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="ingestion_date"
            ),
        )

        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        logger.info(f"{len(df)} lignes chargées avec succès dans {table_id}")
  
    # ---- Orchestration du DAG ----
    chunks = extract_deezer_ids()
    write_to_bigquery.expand(results=fetch_deezer_data.expand(chunk=chunks))
   
dag_instance = fetch_deezer_artists_data_dag()
