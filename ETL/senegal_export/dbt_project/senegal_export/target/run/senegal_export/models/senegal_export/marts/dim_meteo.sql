
  create view "data_warehouse"."public"."dim_meteo__dbt_tmp"
    
    
  as (
    -- models/marts/dim_meteo.sql

WITH stg AS (
    SELECT * FROM "data_warehouse"."public"."stg_meteo"
)

SELECT
    MD5(id_meteo)            AS id_meteo_sk,
    id_meteo,
    zone,
    annee,
    mois,
    pluviometrie_mm,
    temperature_moy_c,
    indice_secheresse
FROM stg
  );