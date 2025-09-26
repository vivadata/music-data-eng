from fastapi import FastAPI
from pydantic import BaseModel
from google.cloud import bigquery
from openai import OpenAI
import os
import loguru
import json
import re

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
    """Strip markdown and whitespace"""
    sql = sql.replace("```sql", "").replace("```", "").strip()
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

# Example data model
class Artist(BaseModel):
    id: str
    name: str
    genres: list[str]
    popularity: int

# In-memory mock data (replace later with BigQuery / DB calls)
artists_db = [
    {"id": "1", "name": "Artist A", "genres": ["pop"], "popularity": 80},
    {"id": "2", "name": "Artist B", "genres": ["rock"], "popularity": 70},
    {"id": "3", "name": "Artist C", "genres": ["jazz"], "popularity": 65},
]

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
    - artists_union(
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
    - `spotify_artist_genres` is an ARRAY<STRING>, not a string. Use UNNEST() directly, do NOT use SPLIT().
    - Always make genre filters case-insensitive using:
        EXISTS (SELECT 1 FROM UNNEST(spotify_artist_genres) g WHERE LOWER(g) = LOWER('electro'))
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

@music_data_api.get("/artists")
def get_artists():
    return {"artists": artists_db}

@music_data_api.get("/artists/{artist_id}")
def get_artist(artist_id: str):
    artist = next((a for a in artists_db if a["id"] == artist_id), None)
    if not artist:
        return {"error": "Artist not found"}
    return artist

@music_data_api.get("/stats/top-genres")
def top_genres():
    from collections import Counter
    all_genres = [g for a in artists_db for g in a["genres"]]
    return {"genres": Counter(all_genres)}

@music_data_api.get("/predict")
def predict():
    pass

@music_data_api.get("/predict")
def predict_post():
    pass