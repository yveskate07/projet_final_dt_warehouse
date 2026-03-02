with source as (

    select
        id_fournisseur::int     as supplier_id,
        nom_fournisseur         as supplier_name,
        type_engrais_fourni     as fertilizer_type
    from {{ ref('raw_fournisseurs') }}

)

select * from source