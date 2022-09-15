{# This macro will swap the PII data with the data requested from fake #}
{# The default is to obfuscate data in dev. Add the following to remove obfuscation. --vars 'obfuscate_pii: false' #}
{% macro obfuscate_pii(col_name,obfuscate_table,obfuscate_col_name,col_id,obfuscate_seed,obfuscate_schema_name='fake') %}
  {% if target.name == 'prod' or not(var('obfuscate_pii',true)) %}
        {{col_name}}
  {%- else -%}
        (select uf.{{obfuscate_col_name}}
        FROM {{obfuscate_schema_name}}.{{obfuscate_table}} as uf
        WHERE row_id = fake.h_int(concat({{col_id}},'{{obfuscate_seed}}')))
  {%- endif -%}
{% endmacro %}