{# Replace a value within a column with NULL#}
{% macro convert_key_value_to_null(column,key_value) %}
        case {{column}} when {{key_value}} then null else {{column}} end
{%- endmacro %}