{% snapshot customers_snapshot %}

{{
    config(
        target_schema='STAGING',
        unique_key='customer_id',
        strategy='check',
        check_cols=['country', 'age', 'is_active'],
        invalidate_hard_deletes=True
    )
}}

SELECT
    customer_id,
    full_name,
    email,
    country,
    age,
    age_group,
    is_active,
    join_date
FROM {{ ref('stg_customers') }}

{% endsnapshot %}