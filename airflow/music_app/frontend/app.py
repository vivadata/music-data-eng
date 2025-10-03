import streamlit as st
import requests
import pandas as pd
import os

BACKEND_URL = os.getenv("BACKEND_URL")

st.set_page_config(page_title="Music Data Hub API", layout="centered")
st.title(":rocket: Démo Streamlit avec API")

# Zone de texte multiligne
user_input = st.text_area("Entrez votre question :", height=150)

# Bouton d'envoi
if st.button("Envoyer à l'API"):
    if user_input.strip():
        try:
            # Requête
            url = f"{BACKEND_URL}/ask"
            payload = {"question": user_input}
            response = requests.post(url, json=payload)
            if response.status_code == 200:
                data = response.json()

                # Afficher la question
                st.subheader("🙋 Question")
                st.write(data.get("question", ""))

                # Afficher les SQL (dans des expanders pour éviter d’encombrer)
                with st.expander("📝 SQL brut (LLM)"):
                    st.code(data.get("sql_raw", ""), language="sql")

                with st.expander("🧹 SQL nettoyé"):
                    st.code(data.get("sql_cleaned", ""), language="sql")

                with st.expander("✅ SQL final exécuté"):
                    st.code(data.get("sql_final", ""), language="sql")

                # Afficher les résultats
                if "results" in data:
                    df = pd.DataFrame(data["results"])
                    st.subheader("📊 Résultats")
                    st.dataframe(df)

                    # --- Détection automatique pour graphique ---
                    date_cols = [col for col in df.columns if "date" in col or "day" in col]
                    numeric_cols = df.select_dtypes(include="number").columns.tolist()

                    if date_cols and numeric_cols:
                        st.subheader("📈 Visualisation automatique")

                        date_col = date_cols[0]

                        # Conversion timestamp → datetime
                        if pd.api.types.is_numeric_dtype(df[date_col]):
                            df[date_col] = pd.to_datetime(df[date_col], unit="ms", errors="coerce")
                        else:
                            df[date_col] = pd.to_datetime(df[date_col], errors="coerce")

                        df = df.sort_values(by=date_col)

                        # Affiche toutes les colonnes numériques en fonction du temps
                        st.line_chart(df.set_index(date_col)[numeric_cols])

                else:
                    st.warning("Aucun résultat retourné par l'API.")

            else:
                st.error(f"Erreur API : {response.status_code}\n{response.text}")

        except Exception as e:
            st.error(f"Erreur lors de l'appel API : {e}")
    else:
        st.warning("Veuillez entrer une question avant d'envoyer.")