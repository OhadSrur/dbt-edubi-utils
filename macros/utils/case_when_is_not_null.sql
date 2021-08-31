{% macro case_when_is_not_null(column_name, when_is_null=FALSE) %}
        CASE WHEN {%- if not (when_is_null) %} {{ column_name }} IS NOT NULL {% else %} {{ column_name }} IS NULL {% endif %}
        THEN TRUE
        ELSE FALSE
        END
{%- endmacro %}