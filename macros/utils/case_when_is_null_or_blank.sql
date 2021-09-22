{%- macro case_when_is_null_or_blank(column_name,new_value) %}
        CASE WHEN {{ column_name }} IS NULL OR {{ column_name }}::varchar=''
        THEN '{{ new_value }}'::VARCHAR
        ELSE {{ column_name }}::VARCHAR
        END
{%- endmacro %}