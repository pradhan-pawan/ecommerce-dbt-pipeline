{{ config(
    materialized='incremental',
    unique_key=['product_id', 'order_date'],
    schema='GOLD'
) }}

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
    WHERE status = 'completed'
    {% if is_incremental() %}
        AND order_date > (SELECT MAX(order_date) FROM {{ this }})
    {% endif %}
),

products AS (
    SELECT * FROM {{ ref('stg_products') }}
),

daily AS (
    SELECT
        o.product_id,
        p.product_name,
        p.category,
        p.price_tier,
        DATE_TRUNC('day', o.order_date)     AS order_date,
        COUNT(o.order_id)                   AS total_orders,
        SUM(o.quantity)                     AS total_units_sold,
        ROUND(SUM(o.line_total), 2)         AS total_revenue,
        ROUND(AVG(o.line_total), 2)         AS avg_order_value,
        COUNT(DISTINCT o.customer_id)       AS unique_customers
    FROM orders o
    JOIN products p ON o.product_id = p.product_id
    GROUP BY 1, 2, 3, 4, 5
)

SELECT
    *,
    -- 7-day rolling revenue
    ROUND(AVG(total_revenue) OVER (
        PARTITION BY product_id
        ORDER BY order_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 2)                                   AS revenue_7day_rolling_avg,

    -- Day over day change
    ROUND(total_revenue - LAG(total_revenue) OVER (
        PARTITION BY product_id ORDER BY order_date
    ), 2)                                   AS revenue_dod_change,

    -- Cumulative revenue
    ROUND(SUM(total_revenue) OVER (
        PARTITION BY product_id
        ORDER BY order_date
    ), 2)                                   AS cumulative_revenue,

    -- Daily rank across all products
    RANK() OVER (
        PARTITION BY order_date
        ORDER BY total_revenue DESC
    )                                       AS daily_revenue_rank

FROM daily
ORDER BY product_id, order_date