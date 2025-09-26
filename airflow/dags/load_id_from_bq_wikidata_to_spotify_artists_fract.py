from airflow.decorators import dag, task
from datetime import datetime, timedelta
from google.cloud import bigquery
import pandas as pd
import requests
from loguru import logger
import time
import os
import threading
import base64

PROJECT_ID = "music-data-eng"
DATASET_ID = "music_dataset"
TABLE_ID_WIKIDATA = "wikidata_artists"
OUTPUT_TABLE = "spotify_artists"
CHUNK_SIZE = 50
SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")

# ---- Limiteur de débit global pour l’API Spotify ----
class RateLimiter:
    """Classe utilitaire pour limiter la fréquence des appels API."""

    def __init__(self, requests_per_second):
        self.min_interval = 1.0 / requests_per_second
        self._last_call = 0.0
        self._lock = threading.Lock()
    
    def wait_if_needed(self):
        """Attend si nécessaire afin de respecter le débit maximal autorisé."""
        with self._lock:
            now = time.time()
            elapsed = now - self._last_call
            if elapsed < self.min_interval:
                time.sleep(self.min_interval - elapsed)
            self._last_call = time.time()

artists_limiter = RateLimiter(15)  # 15 requêtes/s pour l’endpoint Spotify /artists

def rate_limited_request(method, url, headers=None, params=None, limiter=None):
    """Exécute une requête HTTP en respectant un éventuel limiteur de débit."""
    if limiter:
        limiter.wait_if_needed()
    return requests.request(method, url, headers=headers, params=params)

# ---- Récupération du token Spotify ----
def get_spotify_token():
    """Récupère un token d'authentification Spotify en utilisant Client Credentials Flow."""
    logger.info("Récupération du token Spotify")
    url = "https://accounts.spotify.com/api/token"
    auth_str = f"{SPOTIFY_CLIENT_ID}:{SPOTIFY_CLIENT_SECRET}"
    b64_auth = base64.b64encode(auth_str.encode()).decode()        
    headers = {"Authorization": f"Basic {b64_auth}"}
    data = {"grant_type": "client_credentials"}
    resp = requests.post(url, headers=headers, data=data)
    resp.raise_for_status()
    logger.info(f"Token Spotify récupéré avec succès")
    return resp.json()["access_token"]

# ---- Paramètres par défaut du DAG ----
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

def chunk_list(values):
    """Découpe une liste en morceaux de taille fixe (CHUNK_SIZE)."""
    values = [v for v in values if v]  # enlève les valeurs NULL / None
    return [values[i:i+CHUNK_SIZE] for i in range(0, len(values), CHUNK_SIZE)]

@dag(
    dag_id="spotify_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["spotify", "artists"],
)
def fetch_spotify_artists_data_dag():

    @task
    def extract_spotify_ids():
        """Extrait les identifiants Spotify depuis la table Wikidata dans BigQuery."""
        client = bigquery.Client(project=PROJECT_ID)
        df = client.query(
            f"SELECT spotify_artist_id FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID_WIKIDATA}`"
        ).to_dataframe()
        ids = df["spotify_artist_id"].dropna().unique().tolist()
        logger.info(f"{len(ids)} identifiants Spotify trouvés dans Wikidata")
        return chunk_list(ids)

    @task(max_active_tis_per_dag=5, retries=3, retry_delay=timedelta(minutes=2))
    def fetch_spotify_data(chunk: list):
        """Récupère les métadonnées Spotify pour un chunk d'artistes."""
        try:
            logger.info(f"Traitement d'un chunk Spotify : {len(chunk)} identifiants")
            token = get_spotify_token()
            url = "https://api.spotify.com/v1/artists"
            headers = {"Authorization": f"Bearer {token}"}
            params = {"ids": ",".join(chunk)}

            resp = rate_limited_request("GET", url, headers=headers, params=params, limiter=artists_limiter)
            while resp.status_code == 429:
                wait_time = int(resp.headers.get("Retry-After", "1"))
                logger.warning(f"Limite de débit Spotify atteinte. Attente de {wait_time} secondes...")
                time.sleep(wait_time)
                resp = rate_limited_request("GET", url, headers=headers, params=params, limiter=artists_limiter)

            resp.raise_for_status()
            artists = resp.json().get("artists", [])  

            results = []
            for artist in artists:
                if not artist:
                    continue    
                results.append({
                    "spotify_artist_id": artist["id"],
                    "spotify_artist_name": artist.get("name"),
                    "spotify_artist_genres": artist.get("genres", []),
                    "spotify_artist_url": artist.get("external_urls", {}).get("spotify"),
                    "spotify_artist_popularity": artist.get("popularity"),
                    "spotify_artist_total_followers": artist.get("followers", {}).get("total") if artist.get("followers") else None,
                })

            return results
        
        except Exception as e:
            logger.error(f"Échec du traitement du chunk : {e}")
            return None 
    
    @task
    def flatten_results(results_list: list[list[dict]]) -> list[dict]:
        """Aplati la liste de listes de résultats en une seule liste."""
        valid_results = [item for sublist in results_list for item in sublist]
        logger.info(f"Résultats aplatis en {len(valid_results)} enregistrements")
        return valid_results
    
    @task
    def write_to_bigquery(results: list[dict]):
        """Charge les résultats finaux dans BigQuery"""
        if not results:
            logger.warning("Aucun résultat valide à charger (tous les chunks ont échoué).")
            return
        
        df = pd.DataFrame(results)
        df["ingestion_date"] = datetime.now().date()

        # Vérification : genres doit toujours être une liste
        df["spotify_artist_genres"] = df["spotify_artist_genres"].apply(lambda x: x if isinstance(x, list) else [])

        client = bigquery.Client(project=PROJECT_ID)
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{OUTPUT_TABLE}"

        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("spotify_artist_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("spotify_artist_name", "STRING"),
                bigquery.SchemaField("spotify_artist_genres", "STRING", mode="REPEATED"),
                bigquery.SchemaField("spotify_artist_url", "STRING"),
                bigquery.SchemaField("spotify_artist_popularity", "INTEGER"),
                bigquery.SchemaField("spotify_artist_total_followers", "INTEGER"),
                bigquery.SchemaField("ingestion_date", "DATE"),
            ],
            write_disposition="WRITE_APPEND",
            time_partitioning=bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.DAY, field="ingestion_date")
        )

        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        logger.info(f"{len(df)} lignes chargées dans la table {table_id}")

    # ---- DAG orchestration ----
    chunks = extract_spotify_ids()
    results_list = fetch_spotify_data.expand(chunk=chunks)
    flat_results = flatten_results(results_list)
    write_to_bigquery(flat_results)

dag_instance = fetch_spotify_artists_data_dag()
