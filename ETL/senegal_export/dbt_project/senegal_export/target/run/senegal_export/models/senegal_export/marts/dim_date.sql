
  create view "data_warehouse"."public"."dim_date__dbt_tmp"
    
    
  as (
    -- models/marts/dim_date.sql

WITH stg AS (
    SELECT * FROM "data_warehouse"."public"."stg_dates"
)

SELECT
    MD5(id_date)             AS id_date_sk,
    id_date,
    date_complete,
    jour,
    mois,
    nom_mois,
    trimestre,
    annee,
    saison,
    est_weekend
FROM stg
  );