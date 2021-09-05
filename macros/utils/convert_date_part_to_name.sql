{%- macro convert_date_part_to_name(datepart, date) -%}
        TO_CHAR({{date}}, '{{datepart}}')
{%- endmacro %}