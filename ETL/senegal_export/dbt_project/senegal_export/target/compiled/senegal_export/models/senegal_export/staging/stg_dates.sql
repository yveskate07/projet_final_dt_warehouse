WITH source AS (
    SELECT * FROM "data_warehouse"."public_raw"."raw_calendrier"
    WHERE date_id IS NOT NULL
),

base_dates AS (
    -- On crée la vraie date une seule fois pour s'en servir ensuite
    SELECT 
        date_id,
        TO_DATE(CAST(date_id AS TEXT), 'YYYYMMDD') AS date_standard_date
    FROM source
)

SELECT 
    date_id,
    
    -- La date au format texte 'DD-MM-AAAA' comme tu l'as demandé
    TO_CHAR(date_standard_date, 'DD-MM-YYYY') AS date_standard_str,
    
    -- La date au format natif (utile pour les jointures)
    date_standard_date,
    
    -- ===== MOIS =====
    EXTRACT(MONTH FROM date_standard_date)::INTEGER AS mois_id,
    TO_CHAR(date_standard_date, 'MM') AS mois_format_str,       -- ex: '07', '08'
    TO_CHAR(date_standard_date, 'Month') AS mois_nom,           -- ex: 'July     ', 'August   '
    
    -- ===== TRIMESTRE =====
    EXTRACT(QUARTER FROM date_standard_date)::INTEGER AS trimestre_id,
    'T' || EXTRACT(QUARTER FROM date_standard_date) AS trimestre_nom, -- ex: 'T3', 'T4'
    
    -- ===== SEMESTRE =====
    CASE 
        WHEN EXTRACT(MONTH FROM date_standard_date) <= 6 THEN 1 
        ELSE 2 
    END AS semestre_id,
    CASE 
        WHEN EXTRACT(MONTH FROM date_standard_date) <= 6 THEN 'S1' 
        ELSE 'S2' 
    END AS semestre_nom

FROM base_dates