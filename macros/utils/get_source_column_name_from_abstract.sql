{% macro get_source_column_name_from_abstract(source_system_code,abstracted_column_name,prefix_column='') %}
  {% set query_text %}
    select original_system_column_name
    from {{ ref('map_key_columns_to_abstract') }}
    where   source_system_code = '{{var(source_system_code)}}'
    and     abstracted_column_name = '{{abstracted_column_name}}'
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