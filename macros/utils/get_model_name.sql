   
{% macro get_model_name(model_part,model_name,var_name) %}

    {% set model_filename = model_part ~ '_' ~ var(var_name) ~ '__' ~ model_name %}

    {{ return(model_filename) }}

{% endmacro %}