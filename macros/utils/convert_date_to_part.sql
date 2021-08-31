{%- macro convert_date_to_part(datepart, date) -%}
        EXTRACT({{datepart}} from {{date}}::timestamp)
{%- endmacro %}