-- models/gold/gold_sales_trends.sql
WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
    WHERE status = 'completed'
),

monthly_sales AS (
    SELECT
        region,
        order_year,
        order_month,
        ROUND(SUM(line_total), 2)                   AS monthly_revenue,
        COUNT(order_id)                             AS monthly_orders,
        COUNT(DISTINCT customer_id)                 AS unique_customers
    FROM orders
    GROUP BY region, order_year, order_month
)

SELECT
    region,
    order_year,
    order_month,
    monthly_revenue,
    monthly_orders,
    unique_customers,
    RANK() OVER (
        PARTITION BY order_year, order_month
        ORDER BY monthly_revenue DESC
    )                                               AS rank_in_month,
    ROUND(SUM(monthly_revenue) OVER (
        PARTITION BY region
        ORDER BY order_year, order_month
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ), 2)                                           AS running_revenue,
    LAG(monthly_revenue) OVER (
        PARTITION BY region
        ORDER BY order_year, order_month
    )                                               AS prev_month_revenue,
    ROUND((monthly_revenue - LAG(monthly_revenue) OVER (
        PARTITION BY region ORDER BY order_year, order_month)
    ) / NULLIF(LAG(monthly_revenue) OVER (
        PARTITION BY region ORDER BY order_year, order_month), 0) * 100, 1)
                                                    AS mom_change_pct
FROM monthly_sales