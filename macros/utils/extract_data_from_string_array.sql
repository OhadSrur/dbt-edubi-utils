{# Use this macro to strip array characters (such as brackets) from a VARCHAR column #}
{% macro extract_data_from_string_array(column_name) %}
    regexp_replace({{ column_name }}, '["\[\]]', '', 'g')
{% endmacro %}