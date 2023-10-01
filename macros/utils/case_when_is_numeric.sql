{# Checks if the value is a number and then returns true #}
{% macro case_when_is_numeric(string_value, when_is_string=FALSE) %}
        case when {{string_value}} is null then null
        when {%- if not (when_is_string) %} {{string_value}} ~ '^[0-9\.]+$' {% else %} not ({{string_value}} ~ '^[0-9\.]+$' ){% endif %}
        then TRUE
        else FALSE
        end 
{%- endmacro %}