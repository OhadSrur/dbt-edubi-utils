{# This macro will swap the keyword values with the key lookup value provided. Default is to obfuscate description #}
{# Add the following to remove obfuscation description. --vars 'obfuscate_description: false' #}
{% macro obfuscate_descriptions(col_name,client_code,obfuscate_table='descriptive_codes',obfuscate_schema_name='fake') %}
  {% if target.name == 'prod' or var('obfuscate_description',false) %}
        {{col_name}}
  {%- else -%}     
        coalesce((select replace({{col_name}},uf.keyword,uf.keyword_replacement)
        FROM {{obfuscate_schema_name}}.{{obfuscate_table}} as uf
        WHERE client_code=lower('{{ client_code }}') and {{col_name}} ilike '%' || uf.keyword || '%'
        Limit 1),{{col_name}})
  {%- endif -%}
{% endmacro %}