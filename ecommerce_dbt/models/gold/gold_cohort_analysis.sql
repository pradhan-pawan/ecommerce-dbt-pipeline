-- models/gold/gold_cohort_analysis.sql
-- Cohort Analysis: Track customer retention by join month
WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
    WHERE status = 'completed'
),

-- Get each customer's cohort (first purchase month)
customer_cohorts AS (
    SELECT
        customer_id,
        DATE_TRUNC('month', MIN(order_date))            AS cohort_month
    FROM orders
    GROUP BY customer_id
),

-- Get all orders with cohort info
orders_with_cohort AS (
    SELECT
        o.customer_id,
        o.order_date,
        o.line_total,
        c.cohort_month,
        DATEDIFF(
            'month',
            c.cohort_month,
            DATE_TRUNC('month', o.order_date)
        )                                               AS month_number
    FROM orders o
    LEFT JOIN customer_cohorts c ON o.customer_id = c.customer_id
),

-- Cohort size (how many customers in each cohort)
cohort_sizes AS (
    SELECT
        cohort_month,
        COUNT(DISTINCT customer_id)                     AS cohort_size
    FROM customer_cohorts
    GROUP BY cohort_month
),

-- Retention per cohort per month
cohort_retention AS (
    SELECT
        cohort_month,
        month_number,
        COUNT(DISTINCT customer_id)                     AS active_customers,
        ROUND(SUM(line_total), 2)                       AS cohort_revenue
    FROM orders_with_cohort
    GROUP BY cohort_month, month_number
)

SELECT
    cr.cohort_month,
    cr.month_number,
    cs.cohort_size,
    cr.active_customers,
    cr.cohort_revenue,
    ROUND(cr.active_customers / cs.cohort_size * 100, 1) AS retention_rate_pct,
    {{ audit_timestamp() }}
FROM cohort_retention cr
LEFT JOIN cohort_sizes cs ON cr.cohort_month = cs.cohort_month
ORDER BY cr.cohort_month, cr.month_number