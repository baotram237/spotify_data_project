WITH album_raw_data AS (
    SELECT 
        album_id, 
        name
    FROM {{ source("spotify", "bronze_spotify_new_albums") }} 
),

track_raw_data AS (
    SELECT
        album_id, 
        track_id,
        duration_ms
    FROM {{ source("spotify", "bronze_spotify_tracks") }} 
),

enrich_album_info AS (
    SELECT 
        ab.album_id,
        ab.name AS album_name,
        SUM(duration_ms) AS album_duration_ms
    FROM album_raw_data ab
        JOIN track_raw_data tr 
        ON ab.album_id = tr.album_id
    GROUP BY ab.album_id, ab.name
)

SELECT
    *
FROM enrich_album_info

