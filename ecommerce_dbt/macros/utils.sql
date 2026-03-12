-- macros/utils.sql

-- Macro 1: Convert cents to dollars
{% macro cents_to_dollars(column_name) %}
    ROUND({{ column_name }} / 100.0, 2)
{% endmacro %}

-- Macro 2: Classify spend into tiers
{% macro loyalty_tier(spend_column) %}
    CASE
        WHEN {{ spend_column }} >= 2000 THEN 'VIP'
        WHEN {{ spend_column }} >= 1000 THEN 'Gold'
        WHEN {{ spend_column }} >= 500  THEN 'Silver'
        ELSE 'Bronze'
    END
{% endmacro %}

-- Macro 3: Safe divide (avoid division by zero)
{% macro safe_divide(numerator, denominator) %}
    CASE
        WHEN {{ denominator }} = 0 THEN 0
        ELSE ROUND({{ numerator }} / {{ denominator }}, 2)
    END
{% endmacro %}

-- Macro 4: Current timestamp label
{% macro audit_timestamp() %}
    CURRENT_TIMESTAMP() AS created_at
{% endmacro %}