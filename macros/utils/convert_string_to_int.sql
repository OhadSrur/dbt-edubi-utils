{% macro convert_string_to_int(string_value) %}
        case when {{string_value}} ~ '^[0-9\.]+$' 
        then {{string_value}}::INT
        else null
        end 
{%- endmacro %}