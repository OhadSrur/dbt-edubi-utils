{# Names with trailing spaces, are removed #}
{# For example: " ABC  " will be "ABC" #}
{% macro clean_trailing_spaces(string_value) %}
        trim({{string_value}})
{%- endmacro %}