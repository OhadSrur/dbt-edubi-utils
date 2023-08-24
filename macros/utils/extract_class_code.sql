{# Use this macro to extract the class code from SEQTA class_unit_code #}
{% macro extract_class_code(string_col,default_none_int_value='null') %}
        case 
        when {{string_col}} ~ '^[^.]+\.' THEN SUBSTRING({{string_col}} FROM POSITION('.' IN {{string_col}}) + 1)
        else {{ default_none_int_value | default('null') }}
        end
{%- endmacro %}