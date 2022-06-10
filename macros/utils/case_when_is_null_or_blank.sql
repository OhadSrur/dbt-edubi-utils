{%- macro case_when_is_null_or_blank(column_name,new_value,datatype='varchar',sql_expression=FALSE) %}
        CASE WHEN {{ column_name }} IS NULL OR {{ column_name }}::varchar=''
        THEN {% if not (sql_expression) %}'{% endif %}{{ new_value }}{%- if not (sql_expression) %}'{% endif %}::{{ datatype }}
        ELSE {{ column_name }}::{{ datatype }}
        END
{%- endmacro %}