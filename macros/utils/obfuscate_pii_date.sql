{# This macro will swap the PII date with a fake date between the interval set multiplying by a random seed #}
{# example: select (now() + rand_perc_change(concat(1944,'jpc'),100.00) * interval '3 days')::date #}
{% macro obfuscate_pii_date(col_name,col_id,obfuscate_seed,max_interval='365',date_interval='days',obfuscate_schema_name='fake',obfuscate_function='rand_perc_change',date_type='date') %}
  {% if target.name == 'prod' %}
        {{col_name}}
  {%- else -%}
        ({{col_name}} + {{obfuscate_schema_name}}.{{obfuscate_function}}(concat({{col_id}},'{{obfuscate_seed}}'),100.00) * interval '{{max_interval}} {{date_interval}}')::{{date_type}}
  {%- endif -%}
{% endmacro %}