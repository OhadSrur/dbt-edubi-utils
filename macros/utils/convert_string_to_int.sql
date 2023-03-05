{% macro convert_string_to_int(string_value,default_none_int_value) %}
        case when {{string_value}} ~ '^[0-9\.]+$' 
        then {{string_value}}::INT
        else {{ default_none_int_value | default('null') }}
        end 
{%- endmacro %}