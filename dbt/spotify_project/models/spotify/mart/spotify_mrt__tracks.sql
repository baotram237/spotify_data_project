
WITH source_data AS (
    SELECT 
        *
    FROM {{ source("spotify", "bronze_spotify_tracks") }} 
)
SELECT * FROM source_data