{% macro case_when_is_true_sum_interval(measure,predicate, when_is_false=FALSE) %}
        CASE WHEN {%- if not (when_is_false) %} {{ predicate }}{% else %} not({{ predicate }}){% endif %}
        THEN sum({{measure}}::interval)
        ELSE '0'::interval
        END
{%- endmacro %}