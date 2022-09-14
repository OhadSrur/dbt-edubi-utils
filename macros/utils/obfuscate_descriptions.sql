{# This macro will swap the value with the data requested from fake #}
{% macro obfuscate_descriptions(col_name,client_code,obfuscate_table='descriptive_codes',obfuscate_schema_name='fake') %}
  {% if target.name == 'prod' %}
            {{col_name}}
  {%- else -%}     
            coalesce((select replace({{col_name}},uf.keyword,uf.keyword_replacement)
            FROM {{obfuscate_schema_name}}.{{obfuscate_table}} as uf
            WHERE client_code=lower('{{ client_code }}') and {{col_name}} ilike '%' || uf.keyword || '%'
            Limit 1),{{col_name}})
  {%- endif -%}
{% endmacro %}