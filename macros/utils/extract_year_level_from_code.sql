{# Use this macro to extract number from class codes #}
{% macro extract_year_level_from_code(string_col,position='left',default_none_int_value='null') %}
        case when {{ position }}({{string_col}},1) = 'K' then 0
        when {{ position }}({{string_col}},2) ~ '^[0-9]+$'  and {{ position }}({{string_col}},2)::int between 0 and 12 then {{ position }}({{string_col}},2)::int
        when {{ position }}({{string_col}},2) ~ '^[0-9]+$'  and {{ position }}({{string_col}},2)::int > 12 then null
        when {{ position }}({{string_col}},1) ~ '^[0-9]+$' then {{ position }}({{string_col}},1)::int
        when {{ position }}({{string_col}},2) ~ '^Y[0-9\.]+$' then right(left({{string_col}},2),1)::int
        else {{ default_none_int_value | default('null') }}
        end
{%- endmacro %}