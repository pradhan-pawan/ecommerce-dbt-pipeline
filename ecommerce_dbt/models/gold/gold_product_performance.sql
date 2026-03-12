-- models/gold/gold_product_performance.sql
WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
    WHERE status = 'completed'
),

products AS (
    SELECT * FROM {{ ref('stg_products') }}
),

product_stats AS (
    SELECT
        product_id,
        COUNT(order_id)                             AS total_orders,
        SUM(quantity)                               AS total_units_sold,
        ROUND(SUM(line_total), 2)                   AS total_revenue,
        ROUND(AVG(line_total), 2)                   AS avg_order_value,
        COUNT(DISTINCT customer_id)                 AS unique_customers
    FROM orders
    GROUP BY product_id
)

SELECT
    p.product_id,
    p.product_name,
    p.category,
    p.price,
    p.price_tier,
    p.product_code,
    COALESCE(s.total_orders, 0)                     AS total_orders,
    COALESCE(s.total_units_sold, 0)                 AS total_units_sold,
    COALESCE(s.total_revenue, 0)                    AS total_revenue,
    COALESCE(s.avg_order_value, 0)                  AS avg_order_value,
    COALESCE(s.unique_customers, 0)                 AS unique_customers,
    ROW_NUMBER() OVER (
        PARTITION BY p.category ORDER BY s.total_revenue DESC
    )                                               AS rank_in_category
FROM products p
LEFT JOIN product_stats s ON p.product_id = s.product_id