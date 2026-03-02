SELECT 
    date_id,
    -- Transformation des formats variés en format Date standard (PostgreSQL)
    TO_DATE(date_complete, 'YYYY-MM-DD') AS date_standard
FROM {{ ref('raw_calendrier') }}
WHERE date_id IS NOT NULL -- Suppression des lignes vides