import requests
import pandas as pd

url = "https://query.wikidata.org/sparql"
query = """
SELECT ?item ?itemLabel ?deezer ?spotify ?youtube WHERE {
  ?item wdt:P2722 ?deezer.
  OPTIONAL { ?item wdt:P1902 ?spotify. }
  OPTIONAL { ?item wdt:P2397 ?youtube. }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "fr,en". }
}
"""

headers = {"Accept": "application/sparql-results+json"}
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
df.to_csv("data/wikidata_deezer_spotify_youtube.csv", index=False, encoding="utf-8")
print(f"{len(df)} résultats exportés dans wikidata_deezer_spotify_youtube.csv")
