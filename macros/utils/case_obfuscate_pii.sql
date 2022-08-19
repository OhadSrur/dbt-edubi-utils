{# This macro will apply obfuscation logic only when it is not running on prod #}
{% macro case_obfuscate_pii(col_name,col_function) %}
  {% if target.name == 'prod' %}
        {{col_name}}
  {%- else -%}
        {{col_function}}
  {%- endif -%}
{% endmacro %}