WITH source AS (
    SELECT * FROM {{ ref('raw_meteo') }}
)
SELECT
    CAST(id_meteo AS INT) AS id_meteo,
    CAST(pluviometrie_mm AS FLOAT) AS pluviometrie_mm,
    CAST(temperature_moyenne_celsius AS FLOAT) AS temperature_moyenne_celsius
FROM source
WHERE id_meteo IS NOT NULL -- Suppression des lignes vides