{% macro cte(tuple_list, cte_ref_type='ref', source_name='') %}
  {% if (tuple_list is not string and tuple_list is not iterable) or tuple_list is mapping or tuple_list|length <= 0 %}
    {% do exceptions.raise_compiler_error('"tuple_list" must be a string or a list') %}
  {% endif %}
  {% if cte_ref_type not in ['ref','var','source'] %}
    {% do exceptions.raise_compiler_error('"cte_ref_type" must be a [ref, var, source]') %}
  {% endif %}
WITH{% for cte_ref in tuple_list %} {{ cte_ref[0] }} AS (

    SELECT * 
{%- if cte_ref_type == 'ref' %}
    FROM {{ ref(cte_ref[1]) }}
{%- elif cte_ref_type == 'var' %}
    FROM {{ var(cte_ref[1]) }}
{%- elif cte_ref_type == 'source' %}
    FROM {{ source(source_name,cte_ref[1]) }}
{%- endif %}
{%- if cte_ref[2] is string %}
    WHERE {{ cte_ref[2] }}
{%- endif %}
)
    {%- if not loop.last -%}
    ,
    {%- endif -%}
    
    {%- endfor -%}

{%- endmacro %}