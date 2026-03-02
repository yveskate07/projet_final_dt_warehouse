-- models/marts/dim_fournisseurs.sql

WITH stg AS (
    SELECT * FROM "data_warehouse"."public"."stg_fournisseurs"
)

SELECT
    MD5(id_fournisseur)      AS id_fournisseur_sk,
    id_fournisseur,
    nom_fournisseur,
    type_engrais,
    region_fournisseur,
    note_qualite
FROM stg