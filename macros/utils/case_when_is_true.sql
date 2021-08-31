{% macro case_when_is_true(predicate, when_is_false=FALSE) %}
        CASE WHEN {%- if not (when_is_false) %} {{ predicate }} {% else %} not ({{ predicate }}) {% endif %}
        THEN TRUE
        ELSE FALSE
        END
{%- endmacro %}