{%- macro case_when_is_null_or_blank(column_name,new_value,datatype='varchar') %}
        CASE WHEN {{ column_name }} IS NULL OR {{ column_name }}::varchar=''
        THEN '{{ new_value }}'::{{ datatype }}
        ELSE {{ column_name }}::{{ datatype }}
        END
{%- endmacro %}