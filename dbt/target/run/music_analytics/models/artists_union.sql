

  create or replace view `music-data-eng`.`music_dataset`.`artists_union`
  OPTIONS()
  as with deezer as (
    select *
    from `music-data-eng`.`music_dataset`.`deezer_results`
),
spotify as (
    select *
    from `music-data-eng`.`music_dataset`.`spotify_artists`
)

select
    s.spotify_artist_id,
    s.spotify_artist_name,
    s.spotify_artist_genres,
    s.spotify_artist_url,
    s.spotify_artist_total_followers,
    s.spotify_artist_popularity,
    d.link as deezer_link,
    d.name as deezer_name,
    d.nb_fan as deezer_nb_fan,
    current_date() as ingestion_date
from spotify s
left join deezer d
on s.spotify_artist_name = d.name;

