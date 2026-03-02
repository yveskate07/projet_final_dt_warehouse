-- macros/convert_to_fcfa.sql
-- Convertit un montant vers FCFA selon la devise source.
-- Utilise les variables dbt_project.yml : usd_to_fcfa, eur_to_fcfa

{% macro convert_to_fcfa(montant_col, devise_col) %}
    CASE
        WHEN UPPER({{ devise_col }}) = 'USD'  THEN {{ montant_col }} * {{ var('usd_to_fcfa') }}
        WHEN UPPER({{ devise_col }}) = 'EUR'  THEN {{ montant_col }} * {{ var('eur_to_fcfa') }}
        WHEN UPPER({{ devise_col }}) = 'FCFA' THEN {{ montant_col }}
        ELSE {{ montant_col }}
    END
{% endmacro %}
