# 🎵 Music Data Engineering Project

## 1. What – Sujet du projet
Ce projet consiste à **collecter, transformer et analyser des données musicales**, puis à permettre **des requêtes interactives (PKI)** via **Hugging Face** sur des datasets stockés dans **BigQuery**.  
Une **API REST avec FastAPI** expose les données et les résultats des requêtes, packagée avec **Docker** et déployable sur **Kubernetes**.  

### Cas d’usage possibles
- Obtenir les artistes les plus populaires par année ou par genre  
- Identifier les tendances de consommation musicale par pays  
- Générer automatiquement des résumés ou insights à partir des datasets  
- Interroger les données avec un langage naturel (NLQ → SQL via Hugging Face)  
- Consommer les résultats via une API REST (FastAPI)  
- Déployer le système sur le cloud (Kubernetes + GCP BigQuery)  

---

## 2. Why – Pertinence du projet
- Les plateformes musicales génèrent des volumes massifs de données complexes à explorer.  
- **BigQuery** offre un stockage scalable et performant.  
- **FastAPI** fournit une API REST moderne, rapide et auto-documentée.  
- **Docker** garantit la portabilité et l’isolation des environnements.  
- **Kubernetes** permet un déploiement scalable et résilient en production.  
- **Hugging Face** permet de transformer des requêtes en langage naturel en **requêtes SQL intelligentes**.  

---

## 3. How – Stack et Architecture

### Stack technique
- **Langages** : Python, SQL  
- **Ingestion & ETL** : Apache Airflow, Apache Spark  
- **Stockage** : Google **BigQuery**  
- **Analyse & Visualisation** : Pandas, Matplotlib, Seaborn, Streamlit  
- **API** : **FastAPI**  
- **Conteneurisation** : **Docker**  
- **Orchestration** : **Kubernetes** (GKE / EKS / AKS)  
- **PKI / Requêtes intelligentes** : Hugging Face (Transformers & API)  

### Architecture proposée
```text
[Sources de données] 
        ↓
   [Ingestion]  -> API (Spotify, Last.fm, Deezer) / CSV (Kaggle)
        ↓
   [Stockage]   -> BigQuery
        ↓
[Transformation] -> ETL/ELT avec Python & Spark
        ↓
[API Layer] -> FastAPI (Dockerized)
        ↓
[Orchestration] -> Kubernetes (scaling, monitoring)
        ↓
[Analyse & PKI] -> Hugging Face + Dashboards
```

---

## 4. Sources de données
- **Spotify API** : métadonnées sur chansons, artistes, albums, playlists  
  👉 [Documentation](https://developer.spotify.com/documentation/web-api/)  
- **Last.fm API** : tendances musicales et écoutes utilisateurs  
  👉 [Documentation](https://www.last.fm/api)  
- **Deezer API** : informations sur artistes, albums, genres et tendances d’écoutes  
  👉 [Documentation](https://developers.deezer.com/api)  
- **Hugging Face** : modèles pour interrogations intelligentes (NLQ → SQL, Q&A sur données)  
  👉 [Models](https://huggingface.co/models)  
- **Datasets publics Kaggle** :  
  - [Spotify Dataset 1921-2020](https://www.kaggle.com/datasets/zaheenhamidani/ultimate-spotify-tracks-db)  
  - [Billboard Hot 100 Songs](https://www.kaggle.com/datasets/rakannimer/billboard-2000-2019)  

---


