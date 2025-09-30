
WITH source_data AS (
    SELECT 
        *
    FROM {{ source("spotify", "bronze_spotify_new_albums") }} 
)
SELECT * FROM source_data