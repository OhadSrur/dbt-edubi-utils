{% macro create_primary_key(model_name, column_name) %}
  ALTER TABLE {{model_name}} ADD PRIMARY KEY ({{column_name}});
  {% if execute %}
    {{ log('PK created for: ' ~model_name ~ ', column: ' ~ column_name) }}
  {% endif %}
{% endmacro %}