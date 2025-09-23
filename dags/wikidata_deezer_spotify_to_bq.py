from airflow import DAG
from airflow.decorators import task
from datetime import datetime
from google.cloud import bigquery
import pandas as pd
from datetime import timedelta
import requests
from loguru import logger

PROJECT_ID = "music-data-eng"
DATASET_ID = "music_dataset"
TABLE_ID = "deezer_artists"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="upload_wikidata_with_deezer_to_bq_taskflow",
    start_date=datetime(2025, 1, 1),  # date de départ
    schedule="*/2 * * * *",                # exécution quotidienne
    catchup=False,                     # ne pas rattraper le passé
    tags=["demo"],
) as dag:

    @task
    def upload_wikidata_to_bq():
        
        url = "https://query.wikidata.org/sparql"
        query = """
        SELECT ?item ?itemLabel ?deezer ?spotify ?youtube WHERE {
        ?item wdt:P2722 ?deezer.
        OPTIONAL { ?item wdt:P1902 ?spotify. }
        OPTIONAL { ?item wdt:P2397 ?youtube. }
        SERVICE wikibase:label { bd:serviceParam wikibase:language "fr,en". }
        }
        """

        headers = {"Accept": "application/json"}
        response = requests.get(url, params={"query": query}, headers=headers)
        data = response.json()

        rows = []
        for entry in data["results"]["bindings"]:
            rows.append({
                "label": entry.get("itemLabel", {}).get("value"),
                "wikidata": entry["item"]["value"],
                "deezer": entry.get("deezer", {}).get("value"),
                "spotify": entry.get("spotify", {}).get("value"),
                "youtube": entry.get("youtube", {}).get("value"),
            })

        df = pd.DataFrame(rows)
        
      
        # logger.info(f"Dataframe columns: {df.columns}")
        # # Créer le client BigQuery
        # client = bigquery.Client(project=PROJECT_ID)
        
        # # Référence de la table
        # table_ref = client.dataset(DATASET_ID).table(TABLE_ID)
        
        # # Configurer l'import
        # job_config = bigquery.LoadJobConfig(
        #     write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        # )
        
        # # Charger le DataFrame dans BigQuery
        # job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        # job.result()  # Attendre que le job se termine
        # logger.info(f"Loaded {len(df)} rows into {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}")
        # print(f"{len(df)} lignes chargées dans {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}")
        ids= list(df['deezer'])
        chunk_size = 1000
        return [ids[i:i + chunk_size] for i in range(0, len(ids), chunk_size)]
    
    @task
    def expand_chunk(chunk):
        return chunk

    @task
    def load_deezer_data(id):
        url = f"https://api.deezer.com/artist/{id}"
        response = requests.get(url)
        if response.status_code != 200:
            logger.error(f"HTTP {response.status_code} pour {url}")
            return None

        try:
             data = response.json()
        except ValueError:
            logger.error(f"Réponse non JSON pour {url}: {response.text[:200]}")
            return None

        if "error" in data:
            logger.warning(f"Erreur Deezer pour {id}: {data['error']}")
            return None

        data = response.json()
        row=[]
        row.append(  {
            "id": id,
            "name": data.get("name"),
            "link": data.get("link"),
            "nb_fan": data.get("nb_fan"),
            "nb_album": data.get("nb_album"),
        })
        
        df = pd.DataFrame(row)
        
      
        logger.info(f"Dataframe columns: {df.columns}")
        # Créer le client BigQuery
        client = bigquery.Client(project=PROJECT_ID)
        
        # Référence de la table
        table_ref = client.dataset(DATASET_ID).table(TABLE_ID)
        
        # Configurer l'import
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        
        # Charger le DataFrame dans BigQuery
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()  # Attendre que le job se termine
        logger.info(f"Loaded {len(df)} rows into {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}")
        print(f"{len(df)} lignes chargées dans {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}")
       

    def pipeline():
        chunks=upload_wikidata_to_bq()
        ids=expand_chunk.expand(chunk=chunks)
        load_deezer_data(id=ids)
    

    
    pipeline()