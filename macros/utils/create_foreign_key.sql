{% macro create_foreign_key(model_name, model_column_name, ref_table, ref_column_name) %}
  ALTER TABLE {{ model_name }} ADD FOREIGN KEY ({{model_column_name}}) REFERENCES {{ ref_table }}({{ref_column_name}})
  {% if execute %}
    {{ log('FK created for: ' ~model_name ~ ', column: ' ~ model_column_name ~ ', ref Table: ' ~ ref_table ~ ', ref column: '~ ref_column_name) }}
  {% endif %}
{% endmacro %}