with deezer as (
    select *
    from {{ source('music_dataset', 'deezer_artists') }}
),
spotify as (
    select *
    from {{ source('music_dataset', 'spotify_artists') }}
)

select
    s.spotify_artist_id,
    s.spotify_artist_name,
    s.spotify_artist_genres,
    s.spotify_artist_url,
    s.spotify_artist_total_followers,
    s.spotify_artist_popularity,
    d.deezer_artist_url,
    d.deezer_artist_name,
    d.deezer_artist_total_followers,
    current_date() as ingestion_date
from spotify s
left join deezer d
on s.spotify_artist_name = d.deezer_artist_name
