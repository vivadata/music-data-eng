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

# ----- Request schema -----
class QueryRequest(BaseModel):
    question: str

# ----- SQL cleaning helpers -----
def clean_llm_sql(sql: str) -> str:
    """Strip markdown, whitespace and fix unqualified table names."""
    sql = sql.replace("```sql", "").replace("```", "").strip()

    # Remplacer toute référence non qualifiée à artists_union
    # Exemple: FROM artists_union → FROM `music-data-eng.music_dataset.artists_union`
    sql = re.sub(
        r"\bFROM\s+artists_union\b",
        "FROM `music-data-eng.music_dataset.artists_union`",
        sql,
        flags=re.IGNORECASE,
    )

    # Pareil pour les JOIN éventuels
    sql = re.sub(
        r"\bJOIN\s+artists_union\b",
        "JOIN `music-data-eng.music_dataset.artists_union`",
        sql,
        flags=re.IGNORECASE,
    )
    
    return sql

def make_genre_case_insensitive(sql: str) -> str:
    """
    Converts any pattern like:
      'Electro' IN UNNEST(spotify_artist_genres)
    to a case-insensitive EXISTS check that works in BigQuery arrays.
    """
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
    
    return re.sub(pattern, repl, sql, flags=re.IGNORECASE)

# ----- Endpoints -----
@music_data_api.get("/")
def read_root():
    return {"message": "Welcome to our music data api"}

@music_data_api.post("/ask")
def ask_bigquery(req: QueryRequest):
    """Ask a question -> LLM builds SQL -> Run SQL -> Return result"""

    # Step 1: Ask LLM to propose a SQL query
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
    - Return only valid BigQuery SQL, no markdown, no explanation.    
    Question: {req.question}
    """

    llm_resp = openai_client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}]
    )

    # Step 2: Extract SQL and clean it
    sql_query = llm_resp.choices[0].message.content.strip()
    sql_query = clean_llm_sql(sql_query)
    sql_query = make_genre_case_insensitive(sql_query)

    # Step 2: Run query in BigQuery
    try:
        df = bq_client.query(sql_query).to_dataframe()
        # Convert to JSON-safe Python types
        result_preview = json.loads(df.head(10).to_json(orient="records"))
    except Exception as e:
        return {"error": str(e), "sql": sql_query}

    return {
        "question": req.question,
        "sql": sql_query,
        "results": result_preview
    }