-- models/staging/stg_products.sql
WITH source AS (
    SELECT * FROM {{ source('ecommerce_raw', 'products') }}
),

cleaned AS (
    SELECT
        product_id,
        INITCAP(product_name)                       AS product_name,
        INITCAP(category)                           AS category,
        ROUND(price, 2)                             AS price,
        CASE
            WHEN price < 20   THEN 'Budget'
            WHEN price < 100  THEN 'Mid-Range'
            WHEN price >= 100 THEN 'Premium'
            ELSE 'Unknown'
        END                                         AS price_tier,
        CONCAT(
            SUBSTRING(UPPER(category), 1, 3),
            '-',
            LPAD(product_id::VARCHAR, 4, '0')
        )                                           AS product_code
    FROM source
    WHERE product_id IS NOT NULL
)

SELECT * FROM cleaned