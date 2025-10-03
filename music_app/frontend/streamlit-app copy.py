import streamlit as st
import pandas as pd
import kagglehub
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
import plotly.graph_objects as go
import nbformat
import plotly.express as px
import faiss
import requests
import os

@st.cache_data
def load_data():
    # path = kagglehub.dataset_download("maharshipandya/-spotify-tracks-dataset")
    data = pd.read_csv("/home/bousquetcedric34/code/music-data-eng/music_app/frontend/dataset.csv")
    # data = data.drop_duplicates(subset=['track_name'], keep='first')
    data = data.dropna()
    data = data.drop_duplicates(subset=['track_name'], keep='first')
    st.write("longueur du dataset : ", len(data))
    data = data.reset_index(drop=True)
    return data

# @st.cache_data
# def load_subset():
#    subset = data.iloc[:, :1000]
#    subset.drop(["Unnamed: 0", "track_id"], axis=1, inplace=True)
#    return subset

def show_genres_3d(df):
  # On prend les 8 audio features comme coordonnées de l'espace 3D et le genre musical comme positions particulières à afficher
  pca_X = df[["danceability", "energy", "loudness", "speechiness", "acousticness", "liveness", "valence",  "tempo"]]
  pca_y = df["track_genre"].astype("string")
  pca_Xy = pca_X.copy()
  pca_Xy["track_genre"] = pca_y

  # On calcule la position des genres musicaux dans l'espace 3d
  genre_features = pca_Xy.groupby('track_genre').mean()
  genre_features = genre_features[~genre_features.index.isin(["sleep", "comedy"])]

  # On fait une standardisation parce que les audio features n'ont pas toutes la même échelle
  pca_scaler = StandardScaler()
  std_pca_X = pca_scaler.fit_transform(genre_features)

  # On fait une analyse en composantes principales en 3D
  pca_3d = PCA(n_components=3)
  pca_genres_3d = pca_3d.fit_transform(std_pca_X)

  # On construit un dataframe pour affichage des données dans la PCA
  pca_df_3d = pd.DataFrame(pca_genres_3d, columns=['PCA1', 'PCA2', 'PCA3'])
  pca_df_3d['genre'] = genre_features.index.tolist()

  # On affiche la PCA avec plotly express
  fig = go.Figure()
  fig = px.scatter_3d(pca_df_3d, x='PCA1', y='PCA2', z='PCA3', color="genre")

  fig.update_layout(
    autosize=False,
    width=900,
    height=900,
    )

  # fig.show()
  st.plotly_chart(fig)

def select_song(df):

  default_value = ["All"]
  
  with st.form("my_form"):
    # 1. Sélection de l'artiste
    artist = st.selectbox("Choisis un artiste", sorted(df["artists"].unique()))

    # 2. Sélection de la chanson (parmi celles de l'artiste)
    songs = df.loc[df["artists"] == artist, "track_name"].unique()
    song_name = st.selectbox("Choisis une chanson", songs)

    # 3. Récupération du track_id associé
    if song_name:
        track_row = df.loc[(df["artists"] == artist) & (df["track_name"] == song_name)]
        if not track_row.empty:
            track_id = track_row.iloc[0]["track_id"]
        else:
            track_id = None
    else:
        track_id = None

    # 4. Appel de la fonction
    if track_id is None:
        st.warning("⚠️ Impossible de trouver le track_id pour cette chanson.")
    else:
        try:
            song = find_nearest_neigboors(df, track_id)
            st.success(f"Track trouvée : {song}")
        except ValueError as e:
            st.error(str(e))
    # options_artists = tuple(sorted(set(df['artists'].astype("string"))))
    # options_artists = list(options_artists)
    # options_artists = default_value + options_artists
    # artist = st.selectbox(
    #   "Select an artist in the following list",
    #   options_artists,
    #   index=0,
    #   placeholder="Select an artist ..."
    #                       )
    # st.write("You selected:", artist)
    
    # df = df[df["artists"] == artist]

    # options_songs = tuple(sorted(set(df['track_name'].astype("string"))))
    # song = st.selectbox(
    #   "Select a song in the following list",
    #   options_songs,
    #   index=0,
    #   placeholder="Select a song ..."
    #                       )
    st.write("You selected:", song, "by: ", artist)
    st.write("Here are the similar songs in the Kaggle dataset:")

    submitted = st.form_submit_button("Submit")
    if submitted:
      track_id = df[df["track_name"] == song]["track_id"].item()
      st.write("track Id: ", track_id)
      return track_id
    
def create_faiss_index(song_features):
  
  # dimension de l'Index
  d = song_features.shape[1]
  # st.write("dimension :", d)

  # Création de l'Index
  index = faiss.IndexFlatL2(d)
  # st.write("is_trained: ", index.is_trained)

  # Transformer les audio features en float32
  song_features_32 = song_features.astype('float32')
  # st.write("song_features_32 shape:", song_features_32.shape)

  # Ajouter les vecteurs à l'index
  index.add(song_features_32)

  return index, song_features_32

def find_nearest_neigboors(df, track_id):

  if track_id is None:
        raise ValueError("track_id est None — aucune recherche possible.")
  
  matches = df.loc[df["track_id"] == track_id, "track_name"]

  if len(matches) == 0:
    raise ValueError(f"Aucune track trouvée pour track_id={track_id}")
  elif len(matches) > 1:
    raise ValueError(f"Plusieurs tracks trouvées pour track_id={track_id}")
  else:
    song = matches.item()

  artist = df[df["track_id"] == track_id]["artists"].item()
  # st.write("You are looking at NN of song: ", song, "by: ", artist, "track_id: ", track_id)

   # audio features pour caractériser les chansons
  song_X = df[["danceability", "energy", "loudness", "speechiness", "acousticness", "liveness", "valence",  "tempo"]]
  
  # Application d'une standardisation parce que toutes les features n'ont pas la même échelle
  scaler = StandardScaler()
  song_features = scaler.fit_transform(song_X)

  index, song_features_32 = create_faiss_index(song_features)

  # Un data set qui contient les informations sur les chansons en plus des audio features
  detailed_song_X = pd.DataFrame(song_features, columns=["danceability", "energy", "loudness", "speechiness", "acousticness", "liveness", "valence",  "tempo"])

  detailed_song_X.loc[:, "track_id"] = df.loc[:, "track_id"]
  detailed_song_X.loc[:, "artists"] = df.loc[:, "artists"]
  detailed_song_X.loc[:, "album_name"] = df.loc[:, "album_name"]
  detailed_song_X.loc[:, "track_name"] = df.loc[:, "track_name"]
  detailed_song_X.loc[:, "popularity"] = df.loc[:, "popularity"]
  detailed_song_X.loc[:, "duration_ms"] = df.loc[:, "duration_ms"]
  detailed_song_X.loc[:, "explicit"] = df.loc[:, "explicit"]

  # st.write("detailed_song_X shape:", detailed_song_X.shape)

  # Calculer l'index du dataset pour le track_id
  Id = detailed_song_X[detailed_song_X["track_id"] == track_id].index
  # st.write("The selected song's Id: ", Id)

  # Requête sur l'index Id 
  xq = song_features_32[Id].reshape(1, -1)

  # Nombre de vecteurs à extraire
  k = 10

  # Requête avec FAISS
  D, I = index.search(xq, k)

  similar_ids = I.flatten()
  # st.write("similar Ids:", similar_ids)
  similar_songs = df.iloc[similar_ids].copy()
  song_list = similar_songs[["artists", "album_name", "track_name", "popularity"]]
  # song_list.groupby(["track_name"]).first()

  st.dataframe(song_list)

def parameters(df, genre="acoustic"):
    
    options = tuple(sorted(set(df['track_genre'].astype("string"))))
    genre = st.selectbox(
    "To show more songs, select a genre in the following list",
    options,
    index=0,
    placeholder="Select a musical genre ..."
                        )
    st.write("You selected:", genre)
    
    with st.sidebar:
        st.write("Choose the audio features")
        slider_danceability = st.slider("danceability",
                                        df["danceability"][df.track_genre == genre].min(),
                                        df["danceability"][df.track_genre == genre].max(),
                                        (df["danceability"][df.track_genre == genre].min(),
                                         df["danceability"][df.track_genre == genre].max()))
        slider_energy = st.slider("energy",
                                  df["energy"][df.track_genre == genre].min(),
                                  df["energy"][df.track_genre == genre].max(),
                                  (df["energy"].min(),
                                  df["energy"].max()))
        slider_loudness = st.slider("loudness", df["loudness"][df.track_genre == genre].min(),
                                    df["loudness"][df.track_genre == genre].max(),
                                    (df["loudness"][df.track_genre == genre].min(),
                                    df["loudness"][df.track_genre == genre].max()))
        slider_speechiness = st.slider("speechiness",
                                    df["speechiness"][df.track_genre == genre].min(),
                                    df["speechiness"][df.track_genre == genre].max(),
                                    (df["speechiness"][df.track_genre == genre].min(),
                                    df["speechiness"][df.track_genre == genre].max()))
        slider_acousticness = st.slider("acousticness",
                                    df["acousticness"][df.track_genre == genre].min(),
                                    df["acousticness"][df.track_genre == genre].max(),
                                    (df["acousticness"][df.track_genre == genre].min(),
                                    df["acousticness"][df.track_genre == genre].max()))
        slider_liveness = st.slider("liveness",
                                    df["liveness"][df.track_genre == genre].min(),
                                    df["liveness"][df.track_genre == genre].max(),
                                    (df["liveness"][df.track_genre == genre].min(),
                                     df["liveness"][df.track_genre == genre].max()))
        slider_valence = st.slider("valence",
                                   df["valence"][df.track_genre == genre].min(),
                                   df["valence"][df.track_genre == genre].max(),
                                  (df["valence"][df.track_genre == genre].min(),
                                   df["valence"][df.track_genre == genre].max()))
        slider_tempo = st.slider("tempo",
                                 df["tempo"][df.track_genre == genre].min(),
                                 df["tempo"][df.track_genre == genre].max(),
                                (df["tempo"][df.track_genre == genre].min(),
                                 df["tempo"][df.track_genre == genre].max()))
    
    

    return slider_danceability, slider_energy, slider_loudness,\
           slider_speechiness, slider_acousticness, slider_liveness,\
           slider_valence, slider_tempo, genre

def filter(df, slider_danceability, slider_energy, slider_loudness,
           slider_speechiness, slider_acousticness, slider_liveness,
           slider_valence, slider_tempo, genre):
    
    # st.write("Début de filter :", df.iloc[1,:])
    
    df = df[
                (df["danceability"] >= slider_danceability[0])
                &
                (df["danceability"] <= slider_danceability[1])
                  ]
    # st.write("Après danceability :", df.iloc[1,:])

    df = df[
                (df["energy"] >= slider_energy[0])
                &
                (df["energy"] <= slider_energy[1])
                  ]
    

    df = df[
                (df["loudness"] >= slider_loudness[0])
                &
                (df["loudness"] <= slider_loudness[1])
                  ]
    # st.write("slider_loudness", slider_loudness)
    # st.write("avant genre :", df.iloc[1,:])

    df = df[
                (df["speechiness"] >= slider_speechiness[0])
                &
                (df["speechiness"] <= slider_speechiness[1])
                  ]

        

    df = df[
                (df["acousticness"] >= slider_acousticness[0])
                &
                (df["acousticness"] <= slider_acousticness[1])
                  ]

    df = df[
                (df["liveness"] >= slider_liveness[0])
                &
                (df["liveness"] <= slider_liveness[1])
                  ]

    df = df[
                (df["valence"] >= slider_valence[0])
                &
                (df["valence"] <= slider_valence[1])
                  ]
    # st.write("Fin de filter :", df.iloc[1,:])
    df = df[
                (df["tempo"] >= slider_tempo[0])
                &
                (df["tempo"] <= slider_tempo[1])
                  ]

    

    df = df[df["track_genre"] == genre]

    # st.write("Fin de filter :", df.iloc[1,:])

    return df

if __name__=="__main__":
    # genre =
    st.set_page_config(layout="wide")
    st.title("Music Hub Streamlit application")
    st.header("An application to find similar songs based on your favorite song")
    st.write("""The database is based on the [Spotify Kaggle dataset](https://www.kaggle.com/datasets/maharshipandya/-spotify-tracks-dataset).
                The number of songs is 114,000, and it takes too much time to display the whole database.
                The application is currently showing a random sample of one thousand songs in the database").
                """)
    
    df = load_data()
    
    tab1, tab2, tab3 = st.tabs(
       ["Browse the songs", "Find similar songs", "Show similar musical genres"], default="Browse the songs")
    
    with tab1:
      
      slider_danceability, slider_energy, slider_loudness,\
      slider_speechiness, slider_acousticness, slider_liveness,\
      slider_valence, slider_tempo, genre = parameters(df)

      df = filter(df, slider_danceability, slider_energy, slider_loudness,
                      slider_speechiness, slider_acousticness, slider_liveness,
                      slider_valence, slider_tempo, genre)
      
      if df.empty:
        st.write("Aucune chanson avec votre sélection. Modifiez vos critères...")
      else:
        st.write("Nombre de chansons sélectionnées : ", len(df))
        st.dataframe(df.iloc[:, 2:])

    with tab2:
      # df1 = load_data()
      # track_id = select_song(df1)
      # find_nearest_neigboors(df1, track_id)
    
      df1 = load_data()

      st.subheader("Find similar songs")

      # with st.form("select_song_form"):
      # 1. Sélection de l'artiste
      artist = st.selectbox("Choisis un artiste", sorted(df1["artists"].unique()))

      # 2. Sélection de la chanson de cet artiste
      songs = df1.loc[df1["artists"] == artist, "track_name"].unique()
      song_name = st.selectbox("Choisis une chanson", sorted(songs))

      # 3. Bouton submit
      submitted = st.button("Chercher les chansons similaires")

      if submitted:
      # Récupération sécurisée du track_id
        track_row = df1.loc[(df1["artists"] == artist) & (df1["track_name"] == song_name)]
        if not track_row.empty:
            track_id = track_row.iloc[0]["track_id"]
            st.write(f"Vous avez choisi : **{song_name}** par **{artist}**")
            st.write("track Id :", track_id)

            # Appel de la fonction de recherche
            try:
                find_nearest_neigboors(df1, track_id)
            except ValueError as e:
                st.error(str(e))
        else:
            st.warning("⚠️ Impossible de trouver le track_id pour cette chanson.")


    with tab3:
      df2 = load_data()
      show_genres_3d(df2)
    
    # st.write("chargement de df :", df.iloc[1,:])
    # show_genres_3d(df)
    # find_nearest_neigboors()
    

    # st.write(slider_danceability)
    # st.write(genre)

    
    
    #st.write("Etat de df :", df.iloc[1,:])
    
    #submitted = st.form_submit_button("Submit")
    #    if submitted:
    #        st.write("Find the similar songs !")

    
    
    
    
    