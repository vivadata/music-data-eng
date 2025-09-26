import streamlit as st
import pandas as pd
import kagglehub
from st_aggrid import AgGrid

@st.cache_data
def load_data():
    path = kagglehub.dataset_download("maharshipandya/-spotify-tracks-dataset")
    data = pd.read_csv("/home/bousquetcedric34/.cache/kagglehub/datasets/maharshipandya/-spotify-tracks-dataset/versions/1/dataset.csv")
    data = data.drop_duplicates(subset=['track_name'], keep='first')
    return data

@st.cache_data
def load_subset():
    subset = data.iloc[:, :1000]
    subset.drop(["Unnamed: 0", "track_id"], axis=1, inplace=True)
    return subset

def sidebar():
    with st.sidebar.form("my_form"):
        st.write("Choose the audio features")
        slider_val = st.slider("danceability")
        slider_val = st.slider("energy")
        slider_val = st.slider("loudness")
        slider_val = st.slider("speechiness")
        slider_val = st.slider("acousticness")
        slider_val = st.slider("liveness")
        slider_val = st.slider("valence")
        slider_val = st.slider("tempo")

        submitted = st.form_submit_button("Submit")
        if submitted:
            st.write("Find the similar songs !")

if __name__=="__main__":
    st.set_page_config(layout="wide")
    st.title("Music Hub Streamlit application")
    st.header("An application to find similar songs based on your favorite song")
    st.write("""The database is based on the [Spotify Kaggle dataset](https://www.kaggle.com/datasets/maharshipandya/-spotify-tracks-dataset).
                The number of songs is 114,000, and it takes too much time to display the whole database.
                The application is currently showing a random sample of one thousand songs in the database").
                """)
    
    
    data = load_data()
    

    subset = load_subset()
    grid_return = AgGrid(subset)
    # st.dataframe(subset)

    sidebar()

    options = tuple(sorted(set(data['track_genre'].astype("string"))))
    option = st.selectbox(
    "To show more songs, select a genre in the following list",
    options,
    index=None,
    placeholder="Select a musical genre..."
                        )
    st.write("You selected:", option)
    subset = data[data["track_genre"] == option]
    subset.drop(["Unnamed: 0", "track_id"], axis=1, inplace=True)
    st.dataframe(subset)

    
    
    
    
    