{% macro convert_date_to_age(key_date,date_part='') %}
        {%- if date_part != '' %}
        date_part('{{date_part}}',age({{key_date}}::date))
        {%- else -%}
        age({{key_date}}::date)
        {%- endif %}
{%- endmacro %}