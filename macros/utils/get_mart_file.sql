   
{% macro get_mart_file(model_part,mart_name,var_name) %}

    {% set mart_file = model_part ~ '_' ~ var(var_name) ~ '__' ~ mart_name %}

    {{ return(mart_file) }}

{% endmacro %}