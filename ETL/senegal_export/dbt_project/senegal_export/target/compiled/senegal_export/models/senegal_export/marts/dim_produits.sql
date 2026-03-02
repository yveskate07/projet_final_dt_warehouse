-- models/marts/dim_produits.sql
-- Dimension Produits avec prix en FCFA.

WITH stg AS (
    SELECT * FROM "data_warehouse"."public"."stg_produits"
)

SELECT
    MD5(id_produit)          AS id_produit_sk,
    id_produit,
    nom_produit,
    variete,
    categorie,
    prix_export_fcfa_tonne
FROM stg