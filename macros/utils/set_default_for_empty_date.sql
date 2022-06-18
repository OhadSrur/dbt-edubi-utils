{% macro set_default_for_empty_date(key_date,date_value='1900-01-01',cast_option='DATE',where='1=1') %}
        case when {{key_date}}::VARCHAR = '' or {{key_date}} is null and {{where}}
        then '{{ date_value }}'::{{cast_option}}
        else {{key_date}}
        end
{%- endmacro %}