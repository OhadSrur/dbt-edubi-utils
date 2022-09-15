{# This macro will apply obfuscation logic only when it is not running on prod. And will assign null if the original value is null/empty #}
{# The default is to obfuscate data in dev. Add the following to remove obfuscation. --vars 'obfuscate_pii: false' #}
{% macro case_obfuscate_pii_keep_empty(col_name,col_function) %}
  {% if target.name == 'prod' or not(var('obfuscate_pii',true)) %}
        {{col_name}}
  {%- else -%}
        CASE WHEN {{ col_name }} IS NULL OR {{ col_name }}::varchar=''
        THEN null
        ELSE {{ col_function }}
        END
  {%- endif -%}
{% endmacro %}