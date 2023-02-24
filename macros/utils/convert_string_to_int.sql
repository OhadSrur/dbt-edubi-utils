{% macro convert_string_to_int(string_value,default_none_int_value) %}
        case when {{string_value}} ~ '^[0-9\.]+$' and {{string_value}} like '__.%'
        then {{string_value}}::INT
        when {{string_value}} ~ '^[0-9\.]+$' and {{string_value}} like '_.%'
        then left({{string_value}},1)::INT
        else {{ default_none_int_value | default('null') }}
        end 
{%- endmacro %}