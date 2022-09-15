{# This macro will apply obfuscation logic only when it is not running on prod #}
{# The default is to obfuscate data in dev. Add the following to remove obfuscation. --vars 'obfuscate_pii: false' #}
{% macro case_obfuscate_pii(col_name,col_function) %}
  {% if target.name == 'prod' or not(var('obfuscate_pii',true)) %}
        {{col_name}}
  {%- else -%}
        {{col_function}}
  {%- endif -%}
{% endmacro %}