{# Extract subject_code by removing the year level from the assessment code #}
{% macro extract_subject_code(string_col) %}
        SUBSTRING({{string_col}} FROM '\d*(\D.*)')
{%- endmacro %}