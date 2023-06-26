{# Column values from linked tables in Airtable are wrapped with '[]', for ex: '[80]' #}
{# This macro will extract the numerical value and cast it#}
{% macro airtable_extract_int(column_name,cast_to='int') -%}
        substring({{column_name}} from '\d+')::{{cast_to}}
{%- endmacro -%}