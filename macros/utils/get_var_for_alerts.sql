{# This macro is used for getting the variable values for the alerts #}
{% macro get_var_for_alerts(model_source,alert_name,var_name) %}
  {% set query_text %}
    select alert_value
    from {{ ref(model_source) }}
    where  alert_model_name='{{ alert_name }}' and var_name = '{{ var_name }}'
  {% endset %}
  
  {% set query_results = run_query(query_text) %}
      {% if execute %}
        {% set query_result = query_results.columns[0].values() %}
    {% else %}
        {% set query_result = [] %}
    {% endif %}

{{ log("Return value: " ~ query_result[0]) }}
{{ return(query_result[0]) }}

{% endmacro %}