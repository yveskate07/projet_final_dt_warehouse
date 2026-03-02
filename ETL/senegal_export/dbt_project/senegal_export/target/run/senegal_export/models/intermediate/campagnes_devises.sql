
  create view "data_warehouse"."public"."campagnes_devises__dbt_tmp"
    
    
  as (
    with campaigns as (

    select *
    from "data_warehouse"."public"."stg_campagne"

),

converted as (

    select
        *,
        case
            when currency = 'EUR' then input_cost * 1.10
            when currency = 'USD' then input_cost
            else null
        end as input_cost_usd
    from campaigns
)

select * from converted
  );