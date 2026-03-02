-- models/marts/dim_parcelles.sql
-- Dimension Parcelles avec clé surrogate.

WITH stg AS (
    SELECT * FROM "data_warehouse"."public"."stg_parcelles"
)

SELECT
    -- Clé surrogate (hash MD5 de la clé naturelle)
    MD5(id_parcelle)         AS id_parcelle_sk,
    id_parcelle,
    surface_hectares,
    zone,
    ville,
    agence,
    type_sol,
    localisation_gps
FROM stg