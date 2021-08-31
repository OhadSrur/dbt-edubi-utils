{% macro case_when_is_true_sum(measure,predicate, when_is_false=FALSE) %}
        CASE WHEN {%- if not (when_is_false) %} {{ predicate }}{% else %} not ({{ predicate }}){% endif %}
        THEN sum({{measure}})
        ELSE 0
        END
{%- endmacro %}