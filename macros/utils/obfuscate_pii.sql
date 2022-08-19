{# This macro will swap the PII data with the data requested from fake #}
{% macro obfuscate_pii(col_name,obfuscate_table,obfuscate_col_name,col_id,obfuscate_seed,obfuscate_schema_name='fake') %}
  {% if target.name == 'prod' %}
    {{col_name}}
  {%- else -%}
    (select uf.{{obfuscate_col_name}}
    FROM {{obfuscate_schema_name}}.{{obfuscate_table}} as uf
	  WHERE row_id = fake.h_int(concat({{col_id}},'{{obfuscate_seed}}')))
  {%- endif -%}
{% endmacro %}