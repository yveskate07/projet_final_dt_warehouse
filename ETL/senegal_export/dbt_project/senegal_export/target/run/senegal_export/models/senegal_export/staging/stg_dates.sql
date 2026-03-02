
  create view "data_warehouse"."public"."stg_dates__dbt_tmp"
    
    
  as (
    SELECT 
    date_id,
    -- Transformation des formats variés en format Date standard (PostgreSQL)
    TO_DATE(date_complete, 'YYYY-MM-DD') AS date_standard
FROM "data_warehouse"."public_raw"."raw_calendrier"
WHERE date_id IS NOT NULL -- Suppression des lignes vides
  );