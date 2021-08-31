{% macro case_when_is_positive(column_name, when_is_negative=FALSE) %}
        CASE WHEN {{ column_name }} 
        {%- if not (when_is_negative) %} >0 {% else %} <0 {% endif %}
        THEN TRUE
        ELSE FALSE
        END
{%- endmacro %}