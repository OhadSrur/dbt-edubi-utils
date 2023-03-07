{# This macro will swap the lookup values with the key lookup value provided. Default is to obfuscate value in dev #}
{# Add the following to remove obfuscation description. --vars 'obfuscate_description: false' #}
{% macro obfuscate_lookups(col_id,col_name,table_name,obfuscate_table='lookups',obfuscate_schema_name='fake') %}
  {% if target.name == 'prod' or var('obfuscate_description',false) %}
        {{col_name}}
  {%- else -%}     
        coalesce((select lookup_new_value
        FROM {{obfuscate_schema_name}}.{{obfuscate_table}} as uf
        WHERE table_name='{{ table_name }}' and {{col_id}}::varchar=uf.lookup_code
        Limit 1),{{col_name}})
  {%- endif -%}
{% endmacro %}