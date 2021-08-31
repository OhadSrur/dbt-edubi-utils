{% macro convert_key_zero_to_null(key_zero) %}
        case {{key_zero}} when 0 then null else {{key_zero}} end
{%- endmacro %}