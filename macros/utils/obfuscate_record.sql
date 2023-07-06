{# This macro converts the ID+Seed to hash INT value, and modulo it with the sample size of the fake dataset #}
{# Add the following to remove obfuscation. --vars 'obfuscate_record: false' #}
{%- macro obfuscate_record(col_name,client_code,sample_size=40200) -%}
    {{ return(adapter.dispatch('obfuscate_record', 'edubi_utils')(col_name,client_code,sample_size=40200)) }}
{% endmacro %}

{%- macro duckdb__obfuscate_record(col_name,client_code,sample_size=40200) -%}
  {% if target.name == 'prod' or var('obfuscate_record',false) %}
        {{col_name}}
  {%- else -%}
        hash(concat({{col_name}},{{client_code}})) % {{sample_size}}
  {%- endif -%}
{% endmacro %}

{%- macro postgres__obfuscate_record(col_name,client_code,sample_size=40200) -%}
  {% if target.name == 'prod' or var('obfuscate_record',false) %}
        {{col_name}}
  {%- else -%}
        fake.h_int(concat({{col_name}},{{client_code}}))
  {%- endif -%}
{% endmacro %}