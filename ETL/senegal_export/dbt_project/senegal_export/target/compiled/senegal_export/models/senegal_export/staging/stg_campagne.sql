-- models/staging/stg_campagne.sql
WITH source AS (
    SELECT * FROM "data_warehouse"."public_raw"."raw_campagne"
)
SELECT
    id_campagne, -- Vérifie bien l'orthographe ici (minuscule/majuscule)
    id_parcelle,
    id_produit,
    id_meteo,
    date_id,
    CAST(NULLIF(tonnage_recolte, 'NA') AS FLOAT) AS tonnage_recolte,
    CAST(NULLIF(tonnage_exporte, 'NA') AS FLOAT) AS tonnage_exporte,
    -- Conversion EUR/USD -> FCFA
    CASE 
        WHEN devise_origine = 'EUR' THEN CAST(NULLIF(cout_intrants_devise, 'NA') AS FLOAT) * 655.95
        WHEN devise_origine = 'USD' THEN CAST(NULLIF(cout_intrants_devise, 'NA') AS FLOAT) * 600
        ELSE CAST(NULLIF(cout_intrants_devise, 'NA') AS FLOAT)
    END AS cout_intrants_fcfa
FROM source