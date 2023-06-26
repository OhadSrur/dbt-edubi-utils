{# Column values from linked tables in Airtable are wrapped with '[]', for ex: "['A']" #}
{# This macro will extract the varchar value from '[]'#}
{% macro airtable_extract_value(column_name) %}
        substring({{column_name}} from '[A-Za-z0-9]+')
{%- endmacro %}