WITH source AS (
    SELECT * FROM {{ ref('raw_produits') }}
)
SELECT
    CAST(id_produit AS INT) AS id_produit,
    nom_produit,
    variete
FROM source
WHERE id_produit IS NOT NULL -- Suppression des lignes vides