{% macro convert_interval_to_numeric(time_interval,division) %}
        round( (extract(epoch from {{ time_interval }})/{{ division }})::numeric,2)
{%- endmacro %}