{% macro case_when_is_value_then_expression(column_name,predicate,new_expression,else_expression) %}
        CASE {{ column_name }} WHEN {{ predicate }}  
        THEN {{ new_expression }} 
        ELSE {{ else_expression | default(column_name) }} 
        END
{%- endmacro %}