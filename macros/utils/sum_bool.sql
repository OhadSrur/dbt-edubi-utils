{% macro sum_bool(col_name) %}
        COALESCE(sum(CASE WHEN {{col_name}} THEN 1 ELSE 0 END),0)
{% endmacro %}