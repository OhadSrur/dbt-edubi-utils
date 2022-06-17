{% macro remove_network_and_keep_username(column_name) %}
        CASE WHEN POSITION('\' in {{ column_name }}) > 0
        THEN split_part({{ column_name }}, '\', 2)
        ELSE {{ column_name }}
        END
{%- endmacro %}