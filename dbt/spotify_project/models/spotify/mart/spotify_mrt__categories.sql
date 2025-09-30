
WITH source_data AS (
    SELECT 
        *
    FROM {{ source("spotify", "bronze_spotify_categories") }} 
)
SELECT * FROM source_data