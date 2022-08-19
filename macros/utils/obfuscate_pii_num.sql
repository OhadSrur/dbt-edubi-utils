{# This macro will swap the PII numeric with a fake number up to max change of +-change_rate #}
{% macro obfuscate_pii_num(col_name,col_id,obfuscate_seed,change_rate='50.00',obfuscate_function='rand_perc_change',obfuscate_schema_name='fake') %}
  {% if target.name == 'prod' %}
    {{col_name}}
  {%- else -%}
    round({{col_name}} * (1 - {{obfuscate_schema_name}}.{{obfuscate_function}}(concat({{col_id}},'{{obfuscate_seed}}'),{{change_rate}})),2)
  {%- endif -%}
{% endmacro %}