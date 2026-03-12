-- models/gold/gold_rfm_scores.sql
-- RFM Analysis: Recency, Frequency, Monetary scoring
WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
    WHERE status = 'completed'
),

customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

country_regions AS (
    SELECT * FROM {{ ref('country_regions') }}
),

rfm_base AS (
    SELECT
        customer_id,
        DATEDIFF('day', MAX(order_date), CURRENT_DATE)  AS recency_days,
        COUNT(order_id)                                  AS frequency,
        ROUND(SUM(line_total), 2)                        AS monetary
    FROM orders
    GROUP BY customer_id
),

rfm_scored AS (
    SELECT
        customer_id,
        recency_days,
        frequency,
        monetary,
        -- Recency score: lower days = higher score
        NTILE(5) OVER (ORDER BY recency_days DESC)       AS r_score,
        -- Frequency score: higher frequency = higher score
        NTILE(5) OVER (ORDER BY frequency ASC)           AS f_score,
        -- Monetary score: higher spend = higher score
        NTILE(5) OVER (ORDER BY monetary ASC)            AS m_score
    FROM rfm_base
),

rfm_segments AS (
    SELECT
        customer_id,
        recency_days,
        frequency,
        monetary,
        r_score,
        f_score,
        m_score,
        ROUND((r_score + f_score + m_score) / 3.0, 2)   AS rfm_avg,
        CONCAT(r_score, f_score, m_score)                AS rfm_cell,
        CASE
            WHEN r_score >= 4 AND f_score >= 4 THEN 'Champions'
            WHEN r_score >= 3 AND f_score >= 3 THEN 'Loyal Customers'
            WHEN r_score >= 4 AND f_score <= 2 THEN 'Recent Customers'
            WHEN r_score <= 2 AND f_score >= 3 THEN 'At Risk'
            WHEN r_score <= 2 AND f_score <= 2 THEN 'Lost Customers'
            ELSE 'Potential Loyalists'
        END                                              AS rfm_segment
    FROM rfm_scored
)

SELECT
    r.customer_id,
    c.full_name,
    c.country,
    cr.region,
    cr.currency,
    cr.is_tier1,
    c.age_group,
    r.recency_days,
    r.frequency,
    r.monetary,
    r.r_score,
    r.f_score,
    r.m_score,
    r.rfm_avg,
    r.rfm_cell,
    r.rfm_segment,
    {{ audit_timestamp() }}
FROM rfm_segments r
LEFT JOIN customers c ON r.customer_id = c.customer_id
LEFT JOIN country_regions cr ON c.country = cr.country