WITH source AS (
    SELECT * FROM {{ ref('raw_parcelles') }}
)
SELECT
    CAST(id_parcelle AS INT) AS id_parcelle,
    CAST(surface_hectares AS FLOAT) AS surface_hectares,
    localisation,
    type_sol
FROM source
WHERE id_parcelle IS NOT NULL -- Suppression des lignes vides