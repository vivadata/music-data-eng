import requests

url = "https://query.wikidata.org/sparql"
query = """
SELECT ?item ?itemLabel ?deezer ?spotify ?youtube WHERE {
  ?item wdt:P2722 ?deezer.
  OPTIONAL { ?item wdt:P1902 ?spotify. }
  OPTIONAL { ?item wdt:P2397 ?youtube. }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "fr,en". }
}
"""

headers = {
    "Accept": "text/csv",
    "User-Agent": "music-data-exporter/0.1 (https://github.com/yourusername)"
}

response = requests.get(url, params={"query": query}, headers=headers)

if response.status_code == 200:
    with open("data/wikidata_deezer_spotify_youtube.csv", "wb") as f:
        f.write(response.content)
    print("✅ Export terminé : wikidata_deezer_spotify_youtube.csv")
else:
    print(f"❌ Erreur {response.status_code}")
    print(response.text[:500])
