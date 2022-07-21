{% macro rank(partition_by,order_by,function_name='row_number') %}
        {{function_name}}() 
        over(
            partition by {{partition_by}} 
            order by {{order_by}})    
{% endmacro %}