{% macro limit_data_in_dev(column_name, dev_days_of_data=3, where_clause='where') %}
  {# where_clause can be 'where' or 'and' #}

  {% if target.name == 'dev' %}
    {{ where_clause }} {{ column_name }} >= dateadd(
        'day',
        -{{ dev_days_of_data }},
        {{ dbt_utils.current_timestamp() }}
    )
  {% endif %}
{% endmacro %}