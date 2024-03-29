{% macro convert_string_to_int(string_value,default_none_int_value,cast_type='int') %}
        case when {{string_value}} ~ '^[0-9\.]+$' and {{string_value}} !~ '\.\.'
        then {{string_value}}::{{cast_type}}
        else {{ default_none_int_value | default('null') }}
        end 
{%- endmacro %}