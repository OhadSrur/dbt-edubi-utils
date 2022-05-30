{% macro convert_key_empty_to_null(key_zero) %}
        case {{key_zero}} when '' then null else {{key_zero}} end
{%- endmacro %}