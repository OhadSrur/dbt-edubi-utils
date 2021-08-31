{% macro convert_date_to_int(key_date) %}
        to_char({{ key_date }}::DATE, 'YYYYMMDD')::integer
{%- endmacro %}