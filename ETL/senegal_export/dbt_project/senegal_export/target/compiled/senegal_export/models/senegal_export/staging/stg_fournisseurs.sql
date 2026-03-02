with source as (

    select
        id_fournisseur::int     as supplier_id,
        nom_fournisseur         as supplier_name,
        type_engrais_fourni     as fertilizer_type
    from "data_warehouse"."public_raw"."raw_fournisseurs"

)

select * from source