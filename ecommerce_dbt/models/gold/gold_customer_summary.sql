-- models/gold/gold_customer_summary.sql
WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

customer_orders AS (
    SELECT
        customer_id,
        COUNT(order_id)                             AS total_orders,
        ROUND(SUM(line_total), 2)                   AS total_spend,
        ROUND(AVG(line_total), 2)                   AS avg_order_value,
        MAX(order_date)                             AS last_order_date,
        MIN(order_date)                             AS first_order_date,
        SUM(is_completed)                           AS completed_orders,
        SUM(is_cancelled)                           AS cancelled_orders,
        COUNT(DISTINCT product_id)                  AS unique_products_bought,
        ROUND(SUM(is_cancelled) / COUNT(order_id) * 100, 1) AS cancel_rate
    FROM orders
    GROUP BY customer_id
)

SELECT
    c.customer_id,
    c.full_name,
    c.country,
    c.age_group,
    c.is_active,
    c.join_date,
    c.days_as_customer,
    o.total_orders,
    o.total_spend,
    o.avg_order_value,
    o.last_order_date,
    o.first_order_date,
    o.completed_orders,
    o.cancelled_orders,
    o.unique_products_bought,
    o.cancel_rate,
    CASE
        WHEN o.total_spend >= 2000 THEN 'VIP'
        WHEN o.total_spend >= 1000 THEN 'Gold'
        WHEN o.total_spend >= 500  THEN 'Silver'
        ELSE 'Bronze'
    END                                             AS loyalty_tier,
    {{ safe_divide('o.completed_orders', 'o.total_orders') }} AS completion_rate,
    {{ audit_timestamp() }}
FROM customers c
LEFT JOIN customer_orders o ON c.customer_id = o.customer_id