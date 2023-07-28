{# This macro converts timestamp from varchar type to data/time format #}
{%- macro convert_timestamp(date,date_format,output_format) -%}
        to_timestamp({{date}},{{date_format}})::{{output_format}}
{%- endmacro %}