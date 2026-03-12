-- models/staging/stg_orders.sql
WITH source AS (
    SELECT * FROM {{ source('ecommerce_raw', 'orders') }}
),

cleaned AS (
    SELECT
        order_id,
        customer_id,
        product_id,
        LOWER(status)                               AS status,
        quantity,
        ROUND(amount, 2)                            AS amount,
        order_date::DATE                            AS order_date,
        UPPER(region)                               AS region,
        ROUND(amount * quantity, 2)                 AS line_total,
        YEAR(order_date::DATE)                      AS order_year,
        MONTH(order_date::DATE)                     AS order_month,
        CASE
            WHEN amount * quantity >= 1000 THEN 'Premium'
            WHEN amount * quantity >= 500  THEN 'Standard'
            ELSE 'Basic'
        END                                         AS order_tier,
        CASE WHEN status = 'completed' THEN 1 ELSE 0 END AS is_completed,
        CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END AS is_cancelled
    FROM source
    WHERE order_id IS NOT NULL
        AND amount > 0
        AND status IN ('completed','pending','cancelled','returned')
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY order_id ORDER BY amount DESC
    ) = 1  -- removes duplicates
)

SELECT * FROM cleaned