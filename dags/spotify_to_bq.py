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

PROJECT_ID = "music-data-eng"
DATASET_ID = "music_dataset"
TABLE_ID_WIKIDATA = "wikidata_artists"
TABLE_ID_SPOTIFY = "spotify_artists"

# Les credentials Spotify doivent être dans tes Variables/Connexions Airflow
SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
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
    # Étape 1 : Upload Wikidata -> BQ
    # ----------------
    
    @task
    def upload_wikidata_to_bq():
        url = "https://query.wikidata.org/sparql"
        query = """
        SELECT ?item ?itemLabel ?prop ?propLabel ?value WHERE {
            ?item wdt:P2722 ?deezer.              # seulement les entités avec Deezer ID
            ?item ?prop ?value.                   # toutes les propriétés
            SERVICE wikibase:label { 
                bd:serviceParam wikibase:language "en". 
            }
        }
        """
        headers = {"Accept": "application/json"}
        response = requests.get(url, params={"query": query}, headers=headers)
        response.raise_for_status()
        data = response.json()

        rows = []
        for entry in data["results"]["bindings"]:
            rows.append({
                "item": entry.get("item", {}).get("value"),
                "itemLabel": entry.get("itemLabel", {}).get("value"),
                "prop": entry.get("prop", {}).get("value"),
                "propLabel": entry.get("propLabel", {}).get("value"),
                "value": entry.get("value", {}).get("value"),
            })

        df_long = pd.DataFrame(rows)
        logger.info(f"Dataframe long format: {df_long.shape}")

        # ---- Transformation en wide format ----
        df_wide = (
            df_long
            .pivot_table(
                index=["item", "itemLabel"],
                columns="propLabel",
                values="value",
                aggfunc=lambda x: ";".join(set(x))  # si plusieurs valeurs, concaténer
            )
            .reset_index()
        )

        logger.info(f"Dataframe wide format: {df_wide.shape}")
        logger.info(f"Colonnes disponibles: {df_wide.columns.tolist()[:50]} ...")

        # ---- Upload vers BigQuery ----
        client = bigquery.Client(project=PROJECT_ID)
        table_ref = client.dataset(DATASET_ID).table(TABLE_ID_WIKIDATA)

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )

        job = client.load_table_from_dataframe(df_wide, table_ref, job_config=job_config)
        job.result()

        logger.info(f"Loaded {len(df_wide)} rows into {PROJECT_ID}.{DATASET_ID}.{TABLE_ID_WIKIDATA}")

        # ⚠️ renvoyer uniquement les IDs Spotify pour la suite
        return df_wide["Spotify artist ID"].dropna().unique().tolist() \
            if "Spotify artist ID" in df_wide.columns else []

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
        if resp.status_code == 429:
            wait_time = int(resp.headers.get("Retry-After", "5"))
            logger.warning(f"Rate limited, waiting {wait_time}s")
            import time; time.sleep(wait_time)
            resp = requests.get(url, headers=headers, params=params)

        resp.raise_for_status()
        return resp.json()["artists"]

    @task
    def fetch_spotify_data(spotify_ids: list):
        if not spotify_ids:
            logger.warning("Aucun ID Spotify trouvé")
            return []

        token = get_spotify_token()
        results = []

        batches = [spotify_ids[i:i+50] for i in range(0, len(spotify_ids), 50)]
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(fetch_batch, batch, token) for batch in batches]
            for f in as_completed(futures):
                results.extend(f.result())

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
            rows.append({
                "spotify_id": artist.get("id"),
                "name": artist.get("name"),
                "followers": artist.get("followers", {}).get("total"),
                "popularity": artist.get("popularity"),
                "genres": ",".join(artist.get("genres", [])),
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
