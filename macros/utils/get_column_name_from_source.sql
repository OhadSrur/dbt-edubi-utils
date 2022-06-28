{% macro get_column_name_from_source(column_source,column_code,column_code_value,column_name,prefix_column='') %}
  {% set query_text %}
    select {{ column_name }}
    from {{ ref(column_source) }}
    where   {{ column_code }} = '{{ column_code_value }}'
  {% endset %}
  
  {% set col_name_results = run_query(query_text) %}
    {% if execute %}
        {% set col_name = col_name_results.columns[0].values() %}
    {% else %}
        {% set col_name = [] %}
    {% endif %}

{{ log("Return value from abstract: " ~ prefix_column ~ col_name[0]) }}
{{ return(prefix_column ~ col_name[0]) }}

{% endmacro %}