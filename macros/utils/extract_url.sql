{% macro extract_url(column_name) %}
    substring({{ column_name }} from
        E'(https?://[^\\s\\]]+)')
{% endmacro %}