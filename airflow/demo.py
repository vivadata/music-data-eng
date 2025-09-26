import requests
import pandas as pd
from datetime import date

def fetch_top_items(item_type="tracks"):
    """
    Récupère les top 50 tracks ou artists depuis Deezer API.
    """
    url = f"https://api.deezer.com/chart/0/{item_type}"
    resp = requests.get(url)
    if resp.status_code != 200:
        print(f"Erreur API: {resp.status_code}")
        return []
    return resp.json()['data']

def prepare_rows(tracks, artists):
    """
    Prépare les données tracks et artists pour un DataFrame.
    """
    today = date.today().isoformat()
    rows = []

    # Tracks
    for t in tracks:
        rows.append({
            "date": today,
            "type": "track",
            "id": t['id'],
            "name": t['title'],
            "artist": t['artist']['name'],
            "rank": t['rank'],
        })

    # Artists
    for a in artists:
        rows.append({
            "date": today,
            "type": "artist",
            "id": a['id'],
            "name": a['name'],
            "rank": a.get('position', None),  # utiliser la position de l'API
        })

    return rows

def main():
    tracks = fetch_top_items("tracks")
    artists = fetch_top_items("artists")
    rows = prepare_rows(tracks, artists)
    
    # Créer un DataFrame
    df = pd.DataFrame(rows)
    print(df)
    return df

if __name__ == "__main__":
    df = main()