from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
from google.cloud import bigquery
import pandas as pd
import requests
import base64
from loguru import logger
import os
import time
import io
import threading

PROJECT_ID = "music-data-eng"
DATASET_ID = "music_dataset"
TABLE_ID_WIKIDATA = "wikidata_artists"
TABLE_ID_SPOTIFY_DIM = "spotify_artists_dim"  # Dimension table
TABLE_ID_SPOTIFY_FACT = "spotify_artists_fact"  # Fact table (partitioned)
TABLE_ID_SPOTIFY_TOP_TRACKS = "spotify_top_tracks"

SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")

# ---- Rate limiters for Spotify endpoints ----
class RateLimiter:
    def __init__(self, requests_per_second):
        self.min_interval = 1.0 / requests_per_second
        self._last_call = 0.0
        self._lock = threading.Lock()
    
    def wait_if_needed(self):
        with self._lock:
            now = time.time()
            elapsed = now - self._last_call
            if elapsed < self.min_interval:
                time.sleep(self.min_interval - elapsed)
            self._last_call = time.time()

# Separate rate limiters for different endpoints
artists_limiter = RateLimiter(15)  # 15 requests/second for artists
tracks_limiter = RateLimiter(5)    # 5 requests/second for top tracks

def rate_limited_request(method, url, headers=None, params=None, limiter=None):
    if limiter:
        limiter.wait_if_needed()
    return requests.request(method, url, headers=headers, params=params)

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
    schedule="0 2 * * *",   # daily at 2 AM
    catchup=False,
    tags=["wikidata", "spotify"],
    default_args=default_args,
) as dag:

    # ----------------
    # Step 1: Load Wikidata artists into BigQuery
    # ----------------
    @task
    def upload_wikidata_to_bq():
        """
        Query Wikidata for artists and load results into BigQuery.
        Returns a list of Spotify IDs for downstream tasks.
        """
        query = """
        SELECT 
            ?item 
            ?itemLabel 
            ?deezer_artist_id 
            ?spotify_artist_id 
            ?youtube_artist_id 
            ?artist_official_website_url 
        WHERE {
            ?item wdt:P2722 ?deezer_artist_id.
            OPTIONAL { ?item wdt:P1902 ?spotify_artist_id. }
            OPTIONAL { ?item wdt:P2397 ?youtube_artist_id. }
            OPTIONAL { ?item wdt:P856 ?artist_official_website_url. }
            SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
        }
        """
        url = "https://query.wikidata.org/sparql"
        headers = {"Accept": "text/csv"}
        response = requests.get(url, params={"query": query}, headers=headers)
        response.raise_for_status()

        df = pd.read_csv(io.StringIO(response.text))
        logger.info(f"Wikidata query returned {df.shape[0]} rows with {df.shape[1]} columns")

        client = bigquery.Client(project=PROJECT_ID)
        table_ref = client.dataset(DATASET_ID).table(TABLE_ID_WIKIDATA)
        job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        logger.info(f"Loaded {len(df)} Wikidata artists into {PROJECT_ID}.{DATASET_ID}.{TABLE_ID_WIKIDATA}")

        return df["spotify_artist_id"].dropna().unique().tolist() if "spotify_artist_id" in df.columns else []

    # ----------------
    # Step 2: Get Spotify token
    # ----------------
    def get_spotify_token():
        """
        Retrieve a Spotify API access token using client credentials.
        """
        url = "https://accounts.spotify.com/api/token"
        auth_str = f"{SPOTIFY_CLIENT_ID}:{SPOTIFY_CLIENT_SECRET}"
        b64_auth = base64.b64encode(auth_str.encode()).decode()
        headers = {"Authorization": f"Basic {b64_auth}"}
        data = {"grant_type": "client_credentials"}

        resp = requests.post(url, headers=headers, data=data)
        resp.raise_for_status()
        return resp.json()["access_token"]

    # ----------------
    # Step 3: Fetch Spotify artist data
    # ----------------
    @task
    def fetch_spotify_artists_data(spotify_ids: list):
        """
        Fetch Spotify artist metadata in sequential batches.
        Rate limit: 15 requests/second.
        """
        if not spotify_ids:
            logger.warning("No Spotify IDs found to process")
            return []

        valid_ids = [i for i in spotify_ids if isinstance(i, str) and len(i) == 22]
        if not valid_ids:
            logger.warning("No valid Spotify IDs available")
            return []

        token = get_spotify_token()
        results = []

        batches = [valid_ids[i:i+50] for i in range(0, len(valid_ids), 50)]
        
        for i, batch in enumerate(batches):
            url = "https://api.spotify.com/v1/artists"
            headers = {"Authorization": f"Bearer {token}"}
            params = {"ids": ",".join(batch)}

            resp = rate_limited_request("GET", url, headers=headers, params=params, limiter=artists_limiter)

            while resp.status_code == 429:
                wait_time = int(resp.headers.get("Retry-After", "1"))
                logger.warning(f"Spotify API rate limit reached, waiting {wait_time}s")
                time.sleep(wait_time)
                resp = rate_limited_request("GET", url, headers=headers, params=params, limiter=artists_limiter)

            resp.raise_for_status()
            batch_results = resp.json()["artists"]
            results.extend(batch_results)
            
            if (i + 1) % 10 == 0:
                logger.info(f"Processed {i + 1}/{len(batches)} Spotify artist batches")

        logger.info(f"Fetched {len(results)} Spotify artists")
        return results

    # ----------------
    # Step 4: Build dimension and fact tables
    # ----------------
    @task
    def create_dimension_and_fact_tables(artists_data: list):
        """
        Split Spotify data into dimension (static) and fact (dynamic) tables.
        """
        if not artists_data:
            logger.warning("No Spotify artist data to transform")
            return {"dimension": [], "fact": []}

        current_date = datetime.now().date()
        dimension_rows, fact_rows = [], []
        
        for artist in artists_data:
            if not artist or not artist.get("id"):
                continue
                
            dimension_rows.append({
                "spotify_artist_id": artist["id"],
                "spotify_artist_name": artist.get("name"),
                "spotify_artist_genres": ",".join(artist.get("genres", [])) if artist.get("genres") else None,
                "spotify_artist_url": artist.get("external_urls", {}).get("spotify"),
            })
            
            fact_rows.append({
                "spotify_artist_id": artist["id"],
                "spotify_artist_popularity": artist.get("popularity"),
                "spotify_artist_total_followers": artist.get("followers", {}).get("total") if artist.get("followers") else None,
                "date": current_date,
            })

        logger.info(f"Prepared {len(dimension_rows)} dimension rows and {len(fact_rows)} fact rows")
        return {"dimension": dimension_rows, "fact": fact_rows}

    @task
    def upload_dimension_table(dimension_data: list):
        """
        Load dimension table into BigQuery with WRITE_TRUNCATE.
        """
        if not dimension_data:
            logger.warning("No dimension data to load into BigQuery")
            return []

        df = pd.DataFrame(dimension_data)
        logger.info(f"Uploading {df.shape[0]} rows to dimension table")

        client = bigquery.Client(project=PROJECT_ID)
        table_ref = client.dataset(DATASET_ID).table(TABLE_ID_SPOTIFY_DIM)
        job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)

        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        logger.info(f"Loaded {len(df)} rows into {PROJECT_ID}.{DATASET_ID}.{TABLE_ID_SPOTIFY_DIM}")

        return df["spotify_artist_id"].dropna().unique().tolist()

    @task
    def upload_fact_table(fact_data: list):
        """
        Load fact table into an ingestion-time partitioned BigQuery table.
        """
        if not fact_data:
            logger.warning("No fact data to load into BigQuery")
            return

        df = pd.DataFrame(fact_data)
        logger.info(f"Uploading {df.shape[0]} rows to fact table")

        client = bigquery.Client(project=PROJECT_ID)
        table_ref = client.dataset(DATASET_ID).table(TABLE_ID_SPOTIFY_FACT)
        
        try:
            client.get_table(table_ref)
        except Exception:
            schema = [
                bigquery.SchemaField("spotify_artist_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("spotify_artist_popularity", "INTEGER"),
                bigquery.SchemaField("spotify_artist_total_followers", "INTEGER"),
                bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
            ]
            table = bigquery.Table(table_ref, schema=schema)
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=None,
                expiration_ms=7 * 24 * 60 * 60 * 1000,
            )
            client.create_table(table)
            logger.info(f"Created new partitioned fact table {PROJECT_ID}.{DATASET_ID}.{TABLE_ID_SPOTIFY_FACT}")

        job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        logger.info(f"Loaded {len(df)} rows into {PROJECT_ID}.{DATASET_ID}.{TABLE_ID_SPOTIFY_FACT}")

    # ----------------
    # Step 5: Fetch Spotify top tracks
    # ----------------
    @task
    def fetch_spotify_top_tracks(artist_ids: list):
        """
        Fetch top tracks for each Spotify artist sequentially.
        Rate limit: 5 requests/second.
        """
        if not artist_ids:
            logger.warning("No Spotify IDs provided for top tracks")
            return []

        token = get_spotify_token()
        all_tracks = []
        total_artists = len(artist_ids)
        start_time = time.time()

        for i, artist_id in enumerate(artist_ids):
            url = f"https://api.spotify.com/v1/artists/{artist_id}/top-tracks"
            headers = {"Authorization": f"Bearer {token}"}
            params = {"market": "US"}

            resp = rate_limited_request("GET", url, headers=headers, params=params, limiter=tracks_limiter)

            while resp.status_code == 429:
                wait_time = int(resp.headers.get("Retry-After", "1"))
                logger.warning(f"Spotify API rate limit (top tracks), waiting {wait_time}s")
                time.sleep(wait_time)
                resp = rate_limited_request("GET", url, headers=headers, params=params, limiter=tracks_limiter)

            if resp.status_code == 404:
                continue
                
            resp.raise_for_status()
            tracks = resp.json().get("tracks", [])

            for t in tracks:
                all_tracks.append({
                    "spotify_track_artists": ",".join([a["id"] for a in t.get("artists", [])]) if t.get("artists") else None,
                    "spotify_track_artist_names": ",".join([a["name"] for a in t.get("artists", [])]) if t.get("artists") else None,
                    "spotify_track_id": t.get("id"),
                    "spotify_track_name": t.get("name"),
                    "spotify_track_popularity": t.get("popularity"),
                    "spotify_track_url": t.get("external_urls", {}).get("spotify"),
                    "spotify_track_markets": ",".join(t.get("available_markets", [])),   
                    "spotify_track_isrc": t.get("external_ids", {}).get("isrc"),                   
                    "spotify_track_ean": t.get("external_ids", {}).get("ean"),                     
                    "spotify_track_upc": t.get("external_ids", {}).get("upc"),
                })

            if (i + 1) % 100 == 0 or i + 1 == total_artists:
                elapsed = time.time() - start_time
                avg_time = elapsed / (i + 1)
                eta = avg_time * (total_artists - (i + 1))
                logger.info(f"Processed {i + 1}/{total_artists} artists for top tracks. ETA {eta:.1f}s")

        logger.info(f"Fetched {len(all_tracks)} top tracks")
        return all_tracks

    @task
    def upload_top_tracks_to_bq(tracks: list):
        """
        Load top tracks into BigQuery with WRITE_TRUNCATE.
        """
        if not tracks:
            logger.warning("No top tracks to load into BigQuery")
            return

        df = pd.DataFrame(tracks)
        logger.info(f"Uploading {df.shape[0]} rows to top tracks table")

        client = bigquery.Client(project=PROJECT_ID)
        table_ref = client.dataset(DATASET_ID).table(TABLE_ID_SPOTIFY_TOP_TRACKS)
        job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)

        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        logger.info(f"Loaded {len(df)} rows into {PROJECT_ID}.{DATASET_ID}.{TABLE_ID_SPOTIFY_TOP_TRACKS}")

    @task
    def merge_fact_to_dimension():
        """
        Merge the latest fact data into the dimension table.
        """
        client = bigquery.Client(project=PROJECT_ID)
        query = f"""
        MERGE `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID_SPOTIFY_DIM}` AS dim
        USING (
            SELECT spotify_artist_id, spotify_artist_popularity, spotify_artist_total_followers
            FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID_SPOTIFY_FACT}`
            WHERE date = CURRENT_DATE()
        ) AS fact
        ON dim.spotify_artist_id = fact.spotify_artist_id
        WHEN MATCHED THEN
            UPDATE SET 
                dim.spotify_artist_popularity = fact.spotify_artist_popularity,
                dim.spotify_artist_total_followers = fact.spotify_artist_total_followers
        """
        query_job = client.query(query)
        query_job.result()
        logger.info("Merged today's fact data into dimension table")

    # ----------------
    # Orchestration
    # ----------------
    ids = upload_wikidata_to_bq()
    artists = fetch_spotify_artists_data(ids)
    tables_data = create_dimension_and_fact_tables(artists)
    
    artist_ids = upload_dimension_table(tables_data["dimension"])
    upload_fact = upload_fact_table(tables_data["fact"])
    
    tracks = fetch_spotify_top_tracks(artist_ids)
    upload_tracks = upload_top_tracks_to_bq(tracks)
    
    merge_task = merge_fact_to_dimension()
    
    [upload_fact, upload_tracks] >> merge_task
