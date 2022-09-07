{# This macro generate hash value for the record #}
{% macro record_hash(table_name) %}
        md5({{ table_name }}::text)
{% endmacro %}