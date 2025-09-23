from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
from google.cloud import bigquery
import pandas as pd
import requests
import base64
from loguru import logger
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import time
import io

PROJECT_ID = "music-data-eng"
DATASET_ID = "music_dataset"
TABLE_ID_WIKIDATA = "wikidata_artists"
TABLE_ID_SPOTIFY = "spotify_artists"

SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="wikidata_to_spotify_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="0 2 * * *",   # tous les jours à 2h du matin
    catchup=False,
    tags=["wikidata", "spotify"],
    default_args=default_args,
) as dag:

    # ----------------
    # Étape 1 : Upload Wikidata -> BQ (CSV)
    # ----------------
    @task
    def upload_wikidata_to_bq():
        query = """
        SELECT ?item ?itemLabel ?deezer ?spotify ?youtube ?website WHERE {
            ?item wdt:P2722 ?deezer.                  # mandatory Deezer ID
            OPTIONAL { ?item wdt:P1902 ?spotify. }    # Spotify ID
            OPTIONAL { ?item wdt:P2397 ?youtube. }    # YouTube channel ID
            OPTIONAL { ?item wdt:P856 ?website. }     # Official website
            SERVICE wikibase:label { 
                bd:serviceParam wikibase:language "en". 
            }
        }
        """
        url = "https://query.wikidata.org/sparql"
        headers = {"Accept": "text/csv"}
        response = requests.get(url, params={"query": query}, headers=headers)
        response.raise_for_status()

        df = pd.read_csv(io.StringIO(response.text))
        logger.info(f"Dataframe shape: {df.shape}")
        logger.info(f"Columns: {df.columns.tolist()}")

        # ---- Upload vers BigQuery ----
        client = bigquery.Client(project=PROJECT_ID)
        table_ref = client.dataset(DATASET_ID).table(TABLE_ID_WIKIDATA)
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        logger.info(f"Loaded {len(df)} rows into {PROJECT_ID}.{DATASET_ID}.{TABLE_ID_WIKIDATA}")

        # ⚠️ retourner seulement les Spotify IDs pour la tâche suivante
        return df["spotify"].dropna().unique().tolist() if "spotify" in df.columns else []

    # ----------------
    # Étape 2 : Récupération du token Spotify
    # ----------------
    def get_spotify_token():
        url = "https://accounts.spotify.com/api/token"
        auth_str = f"{SPOTIFY_CLIENT_ID}:{SPOTIFY_CLIENT_SECRET}"
        b64_auth = base64.b64encode(auth_str.encode()).decode()
        headers = {"Authorization": f"Basic {b64_auth}"}
        data = {"grant_type": "client_credentials"}

        resp = requests.post(url, headers=headers, data=data)
        resp.raise_for_status()
        return resp.json()["access_token"]

    # ----------------
    # Étape 3 : Appels Spotify (parallélisés)
    # ----------------
    def fetch_batch(batch, token):
        url = "https://api.spotify.com/v1/artists"
        headers = {"Authorization": f"Bearer {token}"}
        params = {"ids": ",".join(batch)}

        resp = requests.get(url, headers=headers, params=params)

        # Gestion des rate limits
        while resp.status_code == 429:
            wait_time = int(resp.headers.get("Retry-After", "1"))
            logger.warning(f"Rate limited, waiting {wait_time}s")
            time.sleep(wait_time)
            resp = requests.get(url, headers=headers, params=params)

        resp.raise_for_status()
        return resp.json()["artists"]

    @task
    def fetch_spotify_data(spotify_ids: list, max_workers=5, delay_between_batches=0.05):
        """
        Récupère les informations des artistes Spotify en batchs.

        Args:
            spotify_ids (list): Liste d'IDs Spotify.
            max_workers (int): Nombre de threads pour paralléliser les appels.
            delay_between_batches (float): Délai en secondes entre chaque batch.
        """
        if not spotify_ids:
            logger.warning("Aucun ID Spotify fourni")
            return []

        # Filtrer uniquement les IDs valides (22 caractères)
        valid_ids = [i for i in spotify_ids if isinstance(i, str) and len(i) == 22]
        if not valid_ids:
            logger.warning("Aucun ID Spotify valide à traiter")
            return []

        token = get_spotify_token()  # ta fonction pour récupérer le token
        results = []

        # Découper en batchs de 50
        batches = [valid_ids[i:i+50] for i in range(0, len(valid_ids), 50)]

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(fetch_batch, batch, token) for batch in batches]
            for f in as_completed(futures):
                results.extend(f.result())
                time.sleep(delay_between_batches)

        logger.info(f"Spotify: récupéré {len(results)} artistes")
        return results

    # ----------------
    # Étape 4 : Sauvegarde Spotify -> BQ
    # ----------------
    @task
    def upload_spotify_to_bq(artists_data: list):
        if not artists_data:
            logger.warning("Pas de données Spotify à charger")
            return

        rows = []
        for artist in artists_data:
            if not artist:
                continue  # ignorer les None
            rows.append({
                "spotify_id": artist.get("id"),
                "name": artist.get("name"),
                "followers": artist.get("followers", {}).get("total") if artist.get("followers") else None,
                "popularity": artist.get("popularity"),
                "genres": ",".join(artist.get("genres", [])) if artist.get("genres") else None,
            })

        df = pd.DataFrame(rows)
        logger.info(f"Dataframe Spotify: {df.shape}")

        client = bigquery.Client(project=PROJECT_ID)
        table_ref = client.dataset(DATASET_ID).table(TABLE_ID_SPOTIFY)

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )

        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        logger.info(f"Loaded {len(df)} rows into {PROJECT_ID}.{DATASET_ID}.{TABLE_ID_SPOTIFY}")

    # ----------------
    # Orchestration
    # ----------------
    ids = upload_wikidata_to_bq()
    artists = fetch_spotify_data(ids)
    upload_spotify_to_bq(artists)
