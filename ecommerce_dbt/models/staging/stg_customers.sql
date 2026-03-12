-- models/staging/stg_customers.sql
WITH source AS (
    SELECT * FROM {{ source('ecommerce_raw', 'customers') }}
),

cleaned AS (
    SELECT
        customer_id,
        INITCAP(first_name)                         AS first_name,
        INITCAP(last_name)                          AS last_name,
        LOWER(email)                                AS email,
        UPPER(country)                              AS country,
        age,
        join_date::DATE                             AS join_date,
        CASE WHEN is_active = 'Y' THEN TRUE 
             ELSE FALSE END                         AS is_active,
        CONCAT(first_name, ' ', last_name)          AS full_name,
        DATEDIFF('day', join_date, CURRENT_DATE)    AS days_as_customer,
        CASE 
            WHEN age < 25 THEN 'Youth'
            WHEN age BETWEEN 25 AND 44 THEN 'Adult'
            WHEN age >= 45 THEN 'Senior'
            ELSE 'Unknown'
        END                                         AS age_group
    FROM source
    WHERE customer_id IS NOT NULL
        AND first_name IS NOT NULL
)

SELECT * FROM cleaned