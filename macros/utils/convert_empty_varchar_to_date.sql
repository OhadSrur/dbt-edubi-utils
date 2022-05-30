{% macro convert_empty_varchar_to_date(key_date,date_value='1900-01-01',cast_to_null=FALSE) %}
        case {{key_date}}::VARCHAR when '' 
        then {%- if not (cast_to_null) %}'{{ date_value }}'{% else %} null{% endif %}::DATE 
        else {{key_date}}::DATE end
{%- endmacro %}