{# Names with duplicate spaces, will be cleaned to one space #}
{# For example: "ABC   DE" will be "ABC DE" #}
{% macro clean_duplicate_spaces(string_value) %}
        REGEXP_REPLACE({{string_value}}, ' {2,}', ' ')
{%- endmacro %}