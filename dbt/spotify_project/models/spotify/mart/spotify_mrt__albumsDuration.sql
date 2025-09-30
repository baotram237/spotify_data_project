
WITH source_data AS (
    SELECT 
        *
    FROM {{ ref("spotify_int__albumDurationDetails") }}
)
SELECT * FROM source_data