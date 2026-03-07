{% macro cents_to_dollars(column_name, scale=2) %}
    ROUND({{ column_name }} / 100.0, {{ scale }})
{% endmacro %}

{% macro safe_divide(numerator, denominator, default=0) %}
    CASE
        WHEN {{ denominator }} = 0 OR {{ denominator }} IS NULL THEN {{ default }}
        ELSE {{ numerator }} / {{ denominator }}
    END
{% endmacro %}

{% macro timestamp_to_date(timestamp_column) %}
    DATE({{ timestamp_column }})
{% endmacro %}
