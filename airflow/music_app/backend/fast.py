from fastapi import FastAPI
from google.cloud import bigquery
from openai import OpenAI
import os
import json
import re
from pydantic import BaseModel

music_data_api = FastAPI()

PROJECT_ID = "music-data-eng"
DATASET_ID = "music_dataset"

OPEN_API_KEY = os.getenv("OPENAI_API_KEY")

# ----- Clients -----
bq_client = bigquery.Client(project=PROJECT_ID)
openai_client = OpenAI()

# ----- Schéma de requête -----
class QueryRequest(BaseModel):
    question: str

# ----- Helpers SQL -----
def clean_llm_sql(sql: str) -> str:
    """
    Nettoie le SQL produit par le LLM :
    - supprime le markdown inutile
    - remplace les références non qualifiées à artists_union
    """
    sql = sql.replace("```sql", "").replace("```", "").strip()

    # Remplacer toute référence non qualifiée à artists_union
    sql = re.sub(
        r"\bFROM\s+artists_union\b",
        "FROM `music-data-eng.music_dataset.artists_union`",
        sql,
        flags=re.IGNORECASE,
    )

    # Idem pour les JOIN éventuels
    sql = re.sub(
        r"\bJOIN\s+artists_union\b",
        "JOIN `music-data-eng.music_dataset.artists_union`",
        sql,
        flags=re.IGNORECASE,
    )
    
    return sql


def make_genre_case_insensitive(sql_query: str) -> str:
    """
    Corrige les erreurs fréquentes liées à spotify_artist_genres :
    1. Si le LLM écrit LOWER(spotify_artist_genres), on remplace
       par UNNEST + LOWER(genre).
    2. Si le LLM génère une condition du type:
         'Electro' IN UNNEST(spotify_artist_genres)
       on la transforme en EXISTS insensible à la casse.
    """

    # --- Cas 1 : LOWER appliqué directement à un ARRAY ---
    if re.search(r"LOWER\s*\(\s*spotify_artist_genres\s*\)", sql_query, re.IGNORECASE):
        # Ajouter UNNEST si absent
        if "UNNEST(spotify_artist_genres)" not in sql_query.upper():
            sql_query = re.sub(
                r"(FROM\s+[^\s]+)",
                r"\1, UNNEST(spotify_artist_genres) AS genre",
                sql_query,
                flags=re.IGNORECASE,
            )
        # Remplacer LOWER(spotify_artist_genres) par LOWER(genre)
        sql_query = re.sub(
            r"LOWER\s*\(\s*spotify_artist_genres\s*\)",
            "LOWER(genre)",
            sql_query,
            flags=re.IGNORECASE,
        )

    # --- Cas 2 : '<genre>' IN UNNEST(spotify_artist_genres) ---
    pattern = r"'([^']+)'\s+IN\s+UNNEST\((\w+)\)"
    def repl(match):
        genre = match.group(1)
        column = match.group(2)
        return (
            f"EXISTS ("
            f"SELECT 1 FROM UNNEST({column}) AS g "
            f"WHERE LOWER(g) = LOWER('{genre}')"
            f")"
        )
    sql_query = re.sub(pattern, repl, sql_query, flags=re.IGNORECASE)

    return sql_query


# ----- Endpoints -----
@music_data_api.get("/")
def read_root():
    return {"message": "Bienvenue sur notre API music data hub"}

@music_data_api.post("/ask")
def ask_bigquery(req: QueryRequest):
    """
    L'utilisateur pose une question → 
    le LLM génère une requête SQL → 
    on nettoie et corrige le SQL → 
    on exécute dans BigQuery → 
    on retourne le résultat.
    """

    # Étape 1 : construire le prompt pour le LLM
    prompt = f"""
    You are a data assistant. The dataset is `{PROJECT_ID}.{DATASET_ID}`.
    Table:
    - `{PROJECT_ID}.{DATASET_ID}.artists_union` (
        spotify_artist_id STRING,
        spotify_artist_name STRING,
        spotify_artist_genres ARRAY<STRING>,
        spotify_artist_popularity INT64,
        spotify_artist_total_followers INT64,
        deezer_link STRING,
        deezer_name STRING,
        deezer_nb_fan INT64,
        ingestion_date TIMESTAMP
    )

    Important rules:
    - Always reference the table with full path: `{PROJECT_ID}.{DATASET_ID}.artists_union`
    - Never use just "artists_union".
    - `spotify_artist_genres` is an ARRAY<STRING>, use UNNEST() directly, do NOT use SPLIT().
    - Genre filters must be case-insensitive with LOWER().
    - Artists in teh response should be unique.
    - Return only valid BigQuery SQL, no markdown, no explanation.    
    Question: {req.question}
    """

    llm_resp = openai_client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}]
    )

    # Étape 2 : SQL brut du LLM
    raw_sql = llm_resp.choices[0].message.content.strip()

    # Étape 3 : nettoyage et correction
    cleaned_sql = clean_llm_sql(raw_sql)
    fixed_sql = make_genre_case_insensitive(cleaned_sql)

    # 🔎 Logging (console)
    print("=== SQL BRUT (LLM) ===")
    print(raw_sql)
    print("=== SQL NETTOYÉ ===")
    print(cleaned_sql)
    print("=== SQL FINAL CORRIGÉ ===")
    print(fixed_sql)

    # Étape 4 : exécution dans BigQuery
    try:
        df = bq_client.query(fixed_sql).to_dataframe()
        # Conversion en JSON compatible
        result_preview = json.loads(df.head(10).to_json(orient="records"))
    except Exception as e:
        return {"error": str(e), "sql": fixed_sql}

    return {
        "question": req.question,
        "sql_raw": raw_sql,
        "sql_cleaned": cleaned_sql,
        "sql_final": fixed_sql,
        "results": result_preview
    }