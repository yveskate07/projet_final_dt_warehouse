-- WITH stg_campagne AS (
--     SELECT * FROM {{ ref('stg_campagne') }}
-- ),
-- parcelles AS (
--     SELECT * FROM {{ ref('raw_parcelles') }}
-- ),
-- produits AS (
--     SELECT * FROM {{ ref('raw_produits') }}
-- ),
-- meteo AS (
--     SELECT * FROM {{ ref('raw_meteo') }}
-- )

-- SELECT
--     c.id_campagne,
--     p.nom_produit,
--     p.variete,
--     pa.localisation,
--     -- Q1: Rendement (Tonnage / Surface)
--     c.tonnage_recolte / NULLIF(pa.surface_hectares, 0) AS rendement_par_hectare,
--     -- Q2: Taux de perte
--     ((c.tonnage_recolte - c.tonnage_exporte) / NULLIF(c.tonnage_recolte, 0)) * 100 AS taux_perte_pourcentage,
--     -- Q3: Influence Pluviométrie
--     m.pluviometrie_mm,
--     -- Q4: Coût en FCFA
--     c.cout_intrants_fcfa
-- FROM stg_campagne c
-- LEFT JOIN parcelles pa ON c.id_parcelle = pa.id_parcelle
-- LEFT JOIN produits p ON c.id_produit = p.id_produit
-- LEFT JOIN meteo m ON c.id_meteo = m.id_meteo

WITH stg_campagne AS (
    SELECT * FROM {{ ref('stg_campagne') }}
),
parcelles AS (
    SELECT * FROM {{ ref('raw_parcelles') }}
),
produits AS (
    SELECT * FROM {{ ref('raw_produits') }}
),
meteo AS (
    SELECT * FROM {{ ref('raw_meteo') }}
)

SELECT
    c.id_campagne,
    c.id_produit,    -- AJOUTÉ : Pour lier stg_produits
    c.id_parcelle,   -- AJOUTÉ : Pour lier stg_parcelles (ta carte !)
    c.id_meteo,      -- AJOUTÉ : Pour lier la météo
    p.nom_produit,
    p.variete,
    pa.localisation,
    -- Q1: Rendement (Tonnage / Surface)
    c.tonnage_recolte / NULLIF(pa.surface_hectares, 0) AS rendement_par_hectare,
    -- Q2: Taux de perte
    ((c.tonnage_recolte - c.tonnage_exporte) / NULLIF(c.tonnage_recolte, 0)) * 100 AS taux_perte_pourcentage,
    -- Q3: Influence Pluviométrie
    m.pluviometrie_mm,
    -- Q4: Coût en FCFA
    c.cout_intrants_fcfa
FROM stg_campagne c
LEFT JOIN parcelles pa ON c.id_parcelle = pa.id_parcelle
LEFT JOIN produits p ON c.id_produit = p.id_produit
LEFT JOIN meteo m ON c.id_meteo = m.id_meteo