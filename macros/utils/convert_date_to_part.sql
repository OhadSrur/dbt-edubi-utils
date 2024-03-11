{%- macro convert_date_to_part(datepart, date,casting = 'timestamp') -%}
        EXTRACT({{datepart}} from {{date}}::{{casting}})::int
{%- endmacro %}