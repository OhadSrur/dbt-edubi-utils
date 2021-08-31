{% macro simple_cte(tuple_list) %}

WITH{% for cte_ref in tuple_list %} {{ cte_ref[0] }} AS (

    SELECT * 
    FROM {{ ref(cte_ref[1]) }}
{%- if cte_ref[2] is string %}
    WHERE {{ cte_ref[2] }}
{%- endif %}
)
    {%- if not loop.last -%}
    ,
    {%- endif -%}
    
    {%- endfor -%}

{%- endmacro %}